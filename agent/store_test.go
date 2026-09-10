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
	"aurora.local/agent/protocol"
	"errors"
	bolt "go.etcd.io/bbolt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func config() Config {
	return Config{Cluster: "lab", Incarnation: "recovery-a", Node: "agent-a", Journal: "journal-a", Boot: "boot-a", Runtime: "runtime-a", Session: "session-a", Epoch: "9007199254740993", Peer: "scheduler", CPU: 100, Memory: 16777216}
}
func caller(c Config) Caller { return Caller{Peer: c.Peer, Session: c.Session, Epoch: c.Epoch} }
func fixture(t *testing.T, name string) map[string]any {
	t.Helper()
	b, e := os.ReadFile("../protocol/native-v1alpha1/fixtures/valid/" + name + ".json")
	if e != nil {
		t.Fatal(e)
	}
	v, e := protocol.Validate(b)
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func delivery(c Config, b map[string]any) []byte {
	return protocol.Canonical(map[string]any{"version": "native-v1alpha1", "kind": "Delivery", "authority": map[string]any{"cluster": c.Cluster, "incarnation": c.Incarnation, "session": c.Session, "schedulerEpoch": c.Epoch}, "body": b, "bodySha256": protocol.Digest(b)})
}
func open(t *testing.T, path string, c Config) *Store {
	t.Helper()
	s, e := Open(path, c)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func TestAdmissionReopenReplayAuthority(t *testing.T) {
	c := config()
	path := filepath.Join(t.TempDir(), "state")
	s := open(t, path, c)
	b := fixture(t, "run")
	d := delivery(c, b)
	r, e := s.Admit(d, caller(c))
	if e != nil || r.Outcome != "accepted" {
		t.Fatal(r, e)
	}
	if _, e = Open(path, c); e == nil {
		t.Fatal("second owner")
	}
	s.Close()
	s = open(t, path, c)
	r2, e := s.Admit(d, caller(c))
	if e != nil || r2 != r {
		t.Fatal(r2, e)
	}
	b["desiredRevision"] = "18446744073709551615"
	if _, e = s.Admit(delivery(c, b), caller(c)); e == nil {
		t.Fatal("conflict")
	}
	s.Close()
	c.Epoch = "18446744073709551615"
	c.Session = "session-new"
	s = open(t, path, c)
	defer s.Close()
	if _, e = s.Admit(d, caller(config())); e == nil {
		t.Fatal("old session")
	}
	r2, e = s.Admit(delivery(c, fixture(t, "run")), caller(c))
	if e != nil || r2 != r {
		t.Fatal("refresh replay", r2, e)
	}
}
func TestStopTombstoneAndReservation(t *testing.T) {
	c := config()
	path := filepath.Join(t.TempDir(), "state")
	s := open(t, path, c)
	stop := fixture(t, "stop")
	r, e := s.Admit(delivery(c, stop), caller(c))
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	s = open(t, path, c)
	defer s.Close()
	time.Sleep(time.Millisecond)
	r2, e := s.Admit(delivery(c, stop), caller(c))
	if e != nil || r2 != r {
		t.Fatal("deadline extended")
	}
	r, e = s.Admit(delivery(c, fixture(t, "run")), caller(c))
	if e != nil || r.Outcome != "rejected-stopped" {
		t.Fatal(r, e)
	}
	st, _ := s.Inspect()
	if len(st.Attempts) != 1 || st.Cursor != 2 || len(st.Observations) != 2 {
		t.Fatal(st)
	}
	// An accepted run stays reserved even after Stop, since no cleanup was proven.
	s2 := open(t, filepath.Join(t.TempDir(), "state"), c)
	defer s2.Close()
	s2.Admit(delivery(c, fixture(t, "run")), caller(c))
	s2.Admit(delivery(c, stop), caller(c))
	b := fixture(t, "run")
	b["command"] = "other"
	b["identity"].(map[string]any)["attempt"] = "other"
	r, e = s2.Admit(delivery(c, b), caller(c))
	if e != nil || r.Outcome != "rejected-capacity" {
		t.Fatal(r, e)
	}
}
func TestConcurrentCapacity(t *testing.T) {
	c := config()
	s := open(t, filepath.Join(t.TempDir(), "state"), c)
	defer s.Close()
	var wg sync.WaitGroup
	results := make(chan Result, 12)
	for i := 0; i < 12; i++ {
		b := fixture(t, "run")
		b["command"] = "command-" + strconv.Itoa(i)
		b["identity"].(map[string]any)["attempt"] = "attempt-" + strconv.Itoa(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := s.Admit(delivery(c, b), caller(c))
			if e != nil {
				t.Error(e)
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)
	accepted := 0
	for r := range results {
		if r.Outcome == "accepted" {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatal(accepted)
	}
	st, e := s.Inspect()
	if e != nil || st.Cursor != 12 || len(st.Attempts) != 1 {
		t.Fatal(st, e)
	}
}
func TestRollbackCorruptionAndACK(t *testing.T) {
	c := config()
	path := filepath.Join(t.TempDir(), "state")
	s := open(t, path, c)
	d := delivery(c, fixture(t, "run"))
	s.beforeCommit = func() error { return errors.New("injected precommit failure") }
	if _, e := s.Admit(d, caller(c)); e == nil {
		t.Fatal("missing failure")
	}
	s.Close()
	s = open(t, path, c)
	st, _ := s.Inspect()
	if st.Cursor != 0 || len(st.Attempts) != 0 {
		t.Fatal("partial commit")
	}
	s.Admit(d, caller(c))
	ack := map[string]any{"version": "native-v1alpha1", "kind": "ObservationAck", "cluster": c.Cluster, "incarnation": c.Incarnation, "node": c.Node, "journal": c.Journal, "committedCursor": "2"}
	if e := s.Ack(protocol.Canonical(ack), caller(c), 1); e == nil {
		t.Fatal("future ACK")
	}
	ack["committedCursor"] = "1"
	ack["journal"] = "wrong"
	if e := s.Ack(protocol.Canonical(ack), caller(c), 1); e == nil {
		t.Fatal("wrong ACK scope")
	}
	ack["journal"] = c.Journal
	if e := s.Ack(protocol.Canonical(ack), caller(c), 1); e != nil {
		t.Fatal(e)
	}
	st, _ = s.Inspect()
	if len(st.Observations) != 0 || len(st.Commands) != 1 || len(st.Attempts) != 1 {
		t.Fatal(st)
	}
	s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("state")).Put([]byte("snapshot"), []byte("{}")) })
	s.Close()
	if x, e := Open(path, c); e == nil {
		x.Close()
		t.Fatal("corruption accepted")
	}
}
func TestCrashHelper(t *testing.T) {
	mode := os.Getenv("AURORA_TEST_CRASH")
	if mode == "" {
		return
	}
	c := config()
	s := open(t, os.Getenv("AURORA_TEST_STATE"), c)
	if mode == "before" {
		s.beforeCommit = func() error { os.Exit(71); return nil }
	}
	s.Admit(delivery(c, fixture(t, "run")), caller(c))
	os.Exit(72)
}
func TestCrashCommitBoundaries(t *testing.T) {
	for _, mode := range []string{"before", "after"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state")
			s := open(t, path, config())
			s.Close()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
			cmd.Env = append(os.Environ(), "AURORA_TEST_CRASH="+mode, "AURORA_TEST_STATE="+path)
			e := cmd.Run()
			if e == nil {
				t.Fatal("expected abrupt exit")
			}
			s = open(t, path, config())
			defer s.Close()
			st, e := s.Inspect()
			want := uint64(0)
			if mode == "after" {
				want = 1
			}
			if e != nil || st.Cursor != want || uint64(len(st.Commands)) != want || uint64(len(st.Attempts)) != want || uint64(len(st.Observations)) != want {
				t.Fatal(st, e)
			}
			r, e := s.Admit(delivery(config(), fixture(t, "run")), caller(config()))
			if e != nil || r.Cursor != "1" {
				t.Fatal(r, e)
			}
		})
	}
}
func TestSocketsAndAttemptIdentity(t *testing.T) {
	c := config()
	c.CPU = 10000
	c.Memory = 1073741824
	s := open(t, filepath.Join(t.TempDir(), "state"), c)
	defer s.Close()
	b := fixture(t, "service-run-a")
	r, e := s.Admit(delivery(c, b), caller(c))
	if e != nil || r.Outcome != "accepted" {
		t.Fatal(r, e)
	}
	b["command"] = "another"
	b["identity"].(map[string]any)["run"] = "another"
	if _, e = s.Admit(delivery(c, b), caller(c)); e == nil {
		t.Fatal("attempt identity mutation")
	}
	b["identity"].(map[string]any)["attempt"] = "another"
	r, e = s.Admit(delivery(c, b), caller(c))
	if e != nil || r.Outcome != "rejected-socket" {
		t.Fatal(r, e)
	}
	b["command"] = "network-other"
	b["assignment"].(map[string]any)["ports"].([]any)[0].(map[string]any)["network"] = "separate"
	r, e = s.Admit(delivery(c, b), caller(c))
	if e != nil || r.Outcome != "accepted" {
		t.Fatal(r, e)
	}
}
func TestRejectBadEnrollmentAndConfig(t *testing.T) {
	c := config()
	s := open(t, filepath.Join(t.TempDir(), "state"), c)
	defer s.Close()
	b := fixture(t, "run")
	b["target"].(map[string]any)["runtime"] = "stale"
	if _, e := s.Admit(delivery(c, b), caller(c)); e == nil {
		t.Fatal("wrong enrollment")
	}
	for _, raw := range []string{`{}`, `{"cluster":"lab","cluster":"lab"}`} {
		if _, e := ReadConfig([]byte(raw)); e == nil {
			t.Fatal("bad config")
		}
	}
}
func TestSessionRefreshAndCorruptFiles(t *testing.T) {
	c := config()
	path := filepath.Join(t.TempDir(), "state")
	s := open(t, path, c)
	s.Close()
	old := caller(c)
	c.Session = "reconnected"
	s = open(t, path, c)
	if _, e := s.Admit(delivery(c, fixture(t, "run")), old); e == nil {
		t.Fatal("old same-epoch session")
	}
	if _, e := s.Admit(delivery(c, fixture(t, "run")), caller(c)); e != nil {
		t.Fatal(e)
	}
	s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		st.FormatVersion = 99
		return save(b, st)
	})
	s.Close()
	if x, e := Open(path, c); e == nil {
		x.Close()
		t.Fatal("future format")
	}
	for _, data := range [][]byte{nil, []byte("not-a-bbolt-database")} {
		path := filepath.Join(t.TempDir(), "state")
		os.WriteFile(path, data, 0600)
		if x, e := Open(path, c); e == nil {
			x.Close()
			t.Fatal("corrupt file")
		}
	}
}
func TestCounterExhaustionAndBoundedPruning(t *testing.T) {
	c := config()
	s := open(t, filepath.Join(t.TempDir(), "state"), c)
	defer s.Close()
	for i := 0; i < 3; i++ {
		b := fixture(t, "stop")
		b["command"] = "stop-" + strconv.Itoa(i)
		if _, e := s.Admit(delivery(c, b), caller(c)); e != nil {
			t.Fatal(e)
		}
	}
	ack := map[string]any{"version": "native-v1alpha1", "kind": "ObservationAck", "cluster": c.Cluster, "incarnation": c.Incarnation, "node": c.Node, "journal": c.Journal, "committedCursor": "3"}
	for remaining := 2; remaining >= 0; remaining-- {
		if e := s.Ack(protocol.Canonical(ack), caller(c), 1); e != nil {
			t.Fatal(e)
		}
		st, _ := s.Inspect()
		if len(st.Observations) != remaining || len(st.Commands) != 3 || st.Ack != 3 || st.Cursor != 3 {
			t.Fatal(st)
		}
	}
	s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		st.Cursor = ^uint64(0)
		return save(b, st)
	})
	b := fixture(t, "stop")
	b["command"] = "overflow"
	if _, e := s.Admit(delivery(c, b), caller(c)); e == nil {
		t.Fatal("cursor wrapped")
	}
	st, e := s.Inspect()
	if e != nil || st.Cursor != ^uint64(0) || len(st.Commands) != 3 {
		t.Fatal(st, e)
	}
}

func TestCorruptAttemptKindAndSymlink(t *testing.T) {
	c := config()
	path := filepath.Join(t.TempDir(), "state")
	s := open(t, path, c)
	link := filepath.Join(t.TempDir(), "link")
	if e := os.Symlink(path, link); e != nil {
		t.Fatal(e)
	}
	if x, e := Open(link, c); e == nil {
		x.Close()
		t.Fatal("symlink accepted")
	}
	if e := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		st.Attempts["invalid"] = Attempt{Body: fixture(t, "batch")}
		return save(b, st)
	}); e != nil {
		t.Fatal(e)
	}
	s.Close()
	if x, e := Open(path, c); e == nil {
		x.Close()
		t.Fatal("wrong-kind attempt accepted")
	}
}
func TestConflictingStopIdentityHasNoEffects(t *testing.T) {
	c := config()
	s := open(t, filepath.Join(t.TempDir(), "state"), c)
	defer s.Close()
	s.Admit(delivery(c, fixture(t, "run")), caller(c))
	stop := fixture(t, "stop")
	stop["identity"].(map[string]any)["run"] = "conflicting"
	if _, e := s.Admit(delivery(c, stop), caller(c)); e == nil {
		t.Fatal("conflicting stop accepted")
	}
	st, e := s.Inspect()
	if e != nil || st.Cursor != 1 || len(st.Commands) != 1 {
		t.Fatal(st, e)
	}
	for _, a := range st.Attempts {
		if a.Stopped {
			t.Fatal("rejected Stop changed state")
		}
	}
	if _, e := s.Admit(delivery(c, fixture(t, "stop")), caller(c)); e != nil {
		t.Fatal(e)
	}
}

func TestEnrollmentMarkerDetectsJournalLoss(t *testing.T) {
	c := config()
	path := filepath.Join(t.TempDir(), "state")
	s := open(t, path, c)
	s.Admit(delivery(c, fixture(t, "run")), caller(c))
	s.Close()
	if e := os.Remove(path); e != nil {
		t.Fatal(e)
	}
	if x, e := Open(path, c); e == nil {
		x.Close()
		t.Fatal("missing database silently reseeded")
	}
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		t.Fatal("missing database recreated", e)
	}
}
func TestEnrollmentMarkerCorruptionAndLoss(t *testing.T) {
	for _, mode := range []string{"missing", "corrupt", "mismatch", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			c := config()
			path := filepath.Join(t.TempDir(), "state")
			s := open(t, path, c)
			s.Close()
			owner := path + ".owner"
			switch mode {
			case "missing":
				os.Remove(owner)
			case "corrupt":
				os.WriteFile(owner, []byte("{"), 0600)
			case "mismatch":
				other := c
				other.Journal = "another"
				os.WriteFile(owner, protocol.Canonical(marker(other)), 0600)
			case "symlink":
				data, _ := os.ReadFile(owner)
				os.Remove(owner)
				target := filepath.Join(t.TempDir(), "marker")
				os.WriteFile(target, data, 0600)
				os.Symlink(target, owner)
			}
			if x, e := Open(path, c); e == nil {
				x.Close()
				t.Fatal("invalid marker accepted")
			}
		})
	}
}
