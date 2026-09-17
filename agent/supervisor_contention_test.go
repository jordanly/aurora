//go:build linux

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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestDeadSupervisorJournalContentionRetriesWithoutLosingReservation(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			root := t.TempDir()
			cfg := config()
			s := open(t, filepath.Join(root, "state"), cfg)
			defer s.Close()
			body := fixture(t, "run")
			if _, err := s.Admit(delivery(cfg, body), caller(cfg)); err != nil {
				t.Fatal(err)
			}
			key := attemptKey(body["identity"].(map[string]any))
			scope, err := observedScope()
			if err != nil {
				t.Fatal(err)
			}
			// The current PID with a deliberately wrong start identity deterministically
			// represents a dead supervisor, without scheduling a real process teardown.
			if err := s.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("state"))
				st, err := read(b)
				if err != nil {
					return err
				}
				st.FormatVersion = 3
				st.RuntimeScope = &scope
				a := st.Attempts[key]
				a.Execution = &Execution{Phase: "released", Outcome: "running", Cleanup: "pending"}
				a.Supervisor = &SupervisorRef{Version: supervisorVersion, Token: fmt.Sprintf("%064d", 0), Process: ProcessIdentity{PID: os.Getpid(), Start: "not-this-process"}}
				st.Attempts[key] = a
				return save(b, st)
			}); err != nil {
				t.Fatal(err)
			}
			r := &Runtime{store: s, opts: RuntimeOptions{Root: filepath.Join(root, "work")}, scope: scope}
			childPath := filepath.Join(filepath.Dir(supervisorSocket(r.opts.Root, key)), "journal.db")
			if err := os.MkdirAll(filepath.Dir(childPath), 0700); err != nil {
				t.Fatal(err)
			}
			child := open(t, childPath, cfg)
			defer child.Close()
			code := 7
			terminal := Execution{Phase: "terminal", Outcome: "failed", Cleanup: "complete", ExitCode: &code}
			if err := child.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("state"))
				st, err := read(b)
				if err != nil {
					return err
				}
				st.FormatVersion = 3
				st.RuntimeScope = &scope
				st.Cursor = 1
				st.Attempts[key] = Attempt{Body: body, Execution: &terminal, Sequence: 1}
				st.Sequences[key] = 1
				st.ExecutionEvents = []ExecutionEvent{{Sequence: 1, Execution: terminal}}
				return save(b, st)
			}); err != nil {
				t.Fatal(err)
			}
			before, err := s.Inspect()
			if err != nil {
				t.Fatal(err)
			}
			// Holding child open owns the exclusive bbolt lock, so a readonly recovery
			// open must time out even though the synthetic supervisor identity is dead.
			if err := r.Tick(context.Background()); err != nil {
				t.Fatal("transient child lock escaped Tick", err)
			}
			if !r.supervisorRetry[key].After(time.Now()) {
				t.Fatal("journal contention did not schedule retry")
			}
			unchanged, err := s.Inspect()
			if err != nil || !reflect.DeepEqual(before, unchanged) {
				t.Fatal("contention changed reservation or observations", err)
			}
			if !onlyAttempt(unchanged).Reserved() {
				t.Fatal("contention released reservation")
			}
			if corrupt {
				if err := child.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("state")).Put([]byte("snapshot"), []byte("{}")) }); err != nil {
					t.Fatal(err)
				}
			}
			if err := child.Close(); err != nil {
				t.Fatal(err)
			}
			if corrupt {
				// Advance the existing retry deadline without a timing-dependent sleep.
				r.supervisorRetry[key] = time.Time{}
				err := r.Tick(context.Background())
				var unavailable *supervisorUnavailable
				if err == nil || errors.As(err, &unavailable) {
					t.Fatal("corrupt child journal treated as transient", err)
				}
				st, err := s.Inspect()
				if err != nil {
					t.Fatal(err)
				}
				a := onlyAttempt(st)
				if !a.Reserved() || a.Supervisor.Imported != 0 || a.Execution.Outcome != "lost" || a.Execution.Cleanup != "unknown" {
					t.Fatal("corruption fabricated completion", a)
				}
				return
			}
			runUntil(t, r, func(st State) bool { return onlyAttempt(st).Execution.Cleanup == "complete" })
			st, err := s.Inspect()
			if err != nil {
				t.Fatal(err)
			}
			a := onlyAttempt(st)
			if a.Reserved() || a.Supervisor.Imported != 1 || a.Execution.ExitCode == nil || *a.Execution.ExitCode != code || st.Cursor != before.Cursor+1 {
				t.Fatal("released lock did not recover exact terminal event once", a)
			}
		})
	}
}
