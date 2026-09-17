/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"aurora.local/agent/protocol"
	bolt "go.etcd.io/bbolt"
)

func ackAllHistory(t *testing.T, s *Store) {
	t.Helper()
	st, err := s.InspectHot()
	if err != nil {
		t.Fatal(err)
	}
	ack := map[string]any{"version": "native-v1alpha1", "kind": "ObservationAck", "cluster": s.c.Cluster, "incarnation": s.c.Incarnation, "node": s.c.Node, "journal": s.c.Journal, "committedCursor": fmt.Sprint(st.Cursor)}
	if err := s.Ack(protocol.Canonical(ack), caller(s.c), 1024); err != nil {
		t.Fatal(err)
	}
}

func TestJournalHotHistoryBoundAndColdReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	s := open(t, path, config())
	seedHistory(t, s, "stop", 1000)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, err := read(b)
		if err != nil {
			return err
		}
		for key, a := range st.Attempts {
			a.Execution = &Execution{Phase: "terminal", Outcome: "stopped", Cleanup: "complete"}
			st.Attempts[key] = a
		}
		return save(b, st)
	}); err != nil {
		t.Fatal(err)
	}
	ackAllHistory(t, s)
	hot, err := s.InspectHot()
	if err != nil {
		t.Fatal(err)
	}
	if len(hot.Attempts) != 0 || len(hot.Commands) != 0 || len(hot.Sequences) != 0 || len(hot.Observations) != 0 {
		t.Fatal("retired history retained hot", hot)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket([]byte("state")).Get([]byte("snapshot"))
		if len(data) > 2048 {
			t.Fatalf("hot snapshot grows with lifetime history: %d", len(data))
		}
		// Old readers must reject the added storage layout field.
		d := json.NewDecoder(bytes.NewReader(data))
		d.DisallowUnknownFields()
		var legacy State
		if d.Decode(&legacy) == nil {
			t.Fatal("old binary would silently lose replay history")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path, config())
	defer s.Close()
	stop := historyBody(t, "stop", 0)
	prior, err := s.Admit(delivery(config(), stop), caller(config()))
	if err != nil || prior.Cursor != "1" {
		t.Fatal(prior, err)
	}
	stop["graceMillis"] = uint64(1)
	if _, err := s.Admit(delivery(config(), stop), caller(config())); err == nil {
		t.Fatal("conflicting cold command reused")
	}
	run := historyBody(t, "run", 0)
	result, err := s.Admit(delivery(config(), run), caller(config()))
	if err != nil || result.Outcome != "rejected-stopped" {
		t.Fatal(result, err)
	}
	attempt, ok, err := s.InspectAttempt(attemptKey(run["identity"].(map[string]any)))
	if err != nil || !ok || !attempt.Stopped || attempt.Sequence != 2 {
		t.Fatal(attempt, ok, err)
	}
	st, err := s.Inspect()
	if err != nil || len(st.Commands) != 1001 || len(st.Attempts) != 1000 {
		t.Fatal("lost cold history", err)
	}
}

func TestJournalLegacyMigrationAndRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	s := open(t, path, config())
	seedHistory(t, s, "stop", 3)
	// Reconstruct the exact old monolithic storage, without keyed buckets.
	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, err := read(b)
		if err != nil {
			return err
		}
		for _, name := range historyBuckets {
			if err := b.DeleteBucket([]byte(name)); err != nil {
				return err
			}
		}
		data := protocol.Canonical(st)
		if err := b.Put([]byte("snapshot"), data); err != nil {
			return err
		}
		return b.Put([]byte("sha256"), []byte(fmt.Sprintf("%x", sha256.Sum256(data))))
	}); err != nil {
		t.Fatal(err)
	}
	// Migration must roll back its buckets and snapshot as one transaction.
	sentinel := errors.New("migration interrupted")
	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, err := read(b)
		if err != nil {
			return err
		}
		if err := save(b, st); err != nil {
			return err
		}
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("state")).Bucket([]byte("commands")) != nil {
			t.Fatal("partial migration committed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = open(t, path, config())
	defer s.Close()
	ackAllHistory(t, s)
	s.beforeCommit = func() error { return sentinel }
	stop := historyBody(t, "stop", 0)
	stop["command"] = "late-stop"
	if _, err := s.Admit(delivery(config(), stop), caller(config())); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	s.beforeCommit = nil
	a, _, err := s.InspectAttempt(attemptKey(stop["identity"].(map[string]any)))
	if err != nil || a.Sequence != 1 {
		t.Fatal("cold mutation survived rollback", a, err)
	}
	r, err := s.Admit(delivery(config(), historyBody(t, "stop", 0)), caller(config()))
	if err != nil || r.Cursor != "1" {
		t.Fatal(r, err)
	}
}

func TestJournalColdCorruptionFailsClosed(t *testing.T) {
	for _, mode := range []string{"value", "entry", "bucket"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state")
			s := open(t, path, config())
			seedHistory(t, s, "stop", 1)
			ackAllHistory(t, s)
			if err := s.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("state"))
				c := b.Bucket([]byte("commands"))
				switch mode {
				case "value":
					return c.Put([]byte("stop-history-0"), []byte("bad"))
				case "entry":
					return c.Delete([]byte("stop-history-0"))
				default:
					return b.DeleteBucket([]byte("commands"))
				}
			}); err != nil {
				t.Fatal(err)
			}
			s.Close()
			if restored, err := Open(path, config()); err == nil {
				restored.Close()
				t.Fatal("lost replay evidence accepted")
			}
		})
	}
}

func TestJournalKeepsUnacknowledgedSupervisorHot(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "state"), config())
	defer s.Close()
	body := fixture(t, "run")
	key := attemptKey(body["identity"].(map[string]any))
	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, err := read(b)
		if err != nil {
			return err
		}
		st.FormatVersion = 3
		st.RuntimeScope = &RuntimeScope{}
		st.Attempts[key] = Attempt{Body: body, Execution: &Execution{Phase: "terminal", Outcome: "stopped", Cleanup: "complete"}, Supervisor: &SupervisorRef{Version: supervisorVersion, Token: fmt.Sprintf("%064d", 0)}}
		return save(b, st)
	}); err != nil {
		t.Fatal(err)
	}
	hot, err := s.InspectHot()
	if err != nil || len(hot.Attempts) != 1 {
		t.Fatal("pending terminal supervisor lost", err)
	}
	r := &Runtime{store: s}
	if err := r.update(key, func(a *Attempt) error { a.Supervisor.Acknowledged = true; return nil }, false); err != nil {
		t.Fatal(err)
	}
	hot, err = s.InspectHot()
	if err != nil || len(hot.Attempts) != 0 {
		t.Fatal("acknowledged supervisor remains hot", err)
	}
}
