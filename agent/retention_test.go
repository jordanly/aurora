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
	"testing"
	"time"

	"aurora.local/agent/protocol"
	bolt "go.etcd.io/bbolt"
)

func retainRequest(s *Store, activate bool, tickets ...uint64) []byte {
	values := []any{}
	for _, ticket := range tickets {
		values = append(values, fmt.Sprint(ticket))
	}
	return protocol.Canonical(map[string]any{"journal": s.c.Journal, "activate": activate, "tickets": values})
}
func ticketBody(t *testing.T, kind string, ticket uint64) map[string]any {
	body := historyBody(t, kind, int(ticket))
	body["identity"].(map[string]any)["ticket"] = fmt.Sprint(ticket)
	return body
}
func activateRetention(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.Retain(retainRequest(s, true), caller(s.c)); err != nil {
		t.Fatal(err)
	}
}
func TestRetentionLongLivedHoleAndReplayRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	s := open(t, path, config())
	activateRetention(t, s)
	s.runtimeRoot = t.TempDir()
	liveKey := attemptKey(ticketBody(t, "run", 1)["identity"].(map[string]any))
	if err := os.MkdirAll(filepath.Join(s.runtimeRoot, liveKey), 0700); err != nil {
		t.Fatal(err)
	}
	run := ticketBody(t, "run", 1)
	if _, err := s.Admit(delivery(s.c, run), caller(s.c)); err != nil {
		t.Fatal(err)
	}
	for ticket := uint64(2); ticket < MaxRetainedTickets+20; ticket++ {
		stop := ticketBody(t, "stop", ticket)
		key := attemptKey(stop["identity"].(map[string]any))
		for _, dir := range []string{filepath.Join(s.runtimeRoot, key), filepath.Join(s.runtimeRoot, ".supervisors", key)} {
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "payload"), make([]byte, 4096), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Admit(delivery(s.c, stop), caller(s.c)); err != nil {
			t.Fatal(err)
		}
		ackAllHistory(t, s)
		if _, err := s.Retain(retainRequest(s, false, ticket), caller(s.c)); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(s.runtimeRoot)
	if err != nil || len(entries) != 2 {
		t.Fatal("artifact history grew", entries, err)
	}
	supervisors, err := os.ReadDir(filepath.Join(s.runtimeRoot, ".supervisors"))
	if err != nil || len(supervisors) != 0 {
		t.Fatal("supervisor event journals grew", supervisors, err)
	}
	st, err := s.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Retention.Retired) != 1 || st.Retention.Retired[0].First != 2 || len(st.Attempts) != 1 || len(st.Commands) != 1 {
		t.Fatal("unbounded history or live ticket lost", st.Retention)
	}
	if state, e := s.Retain(retainRequest(s, false, 1), caller(s.c)); e != nil || retired(state, 1) {
		t.Fatal("retired live reservation", e)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path, config())
	defer s.Close()
	if _, err = s.Admit(delivery(s.c, ticketBody(t, "stop", 2)), caller(s.c)); err == nil {
		t.Fatal("retired replay accepted")
	}
	if _, err = s.Admit(delivery(s.c, historyBody(t, "run", 2000)), caller(s.c)); err == nil {
		t.Fatal("legacy delivery after activation")
	}
	if _, err = s.Admit(delivery(s.c, run), caller(s.c)); err != nil {
		t.Fatal("live replay lost", err)
	}
}
func TestRetentionCrashRollbackAndUnackedFence(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "journal"), config())
	defer s.Close()
	activateRetention(t, s)
	stop := ticketBody(t, "stop", 1)
	if _, err := s.Admit(delivery(s.c, stop), caller(s.c)); err != nil {
		t.Fatal(err)
	}
	if state, err := s.Retain(retainRequest(s, false, 1), caller(s.c)); err != nil || retired(state, 1) {
		t.Fatal("unacknowledged retired", err)
	}
	ackAllHistory(t, s)
	s.beforeCommit = func() error { return errors.New("crash before commit") }
	if _, err := s.Retain(retainRequest(s, false, 1), caller(s.c)); err == nil {
		t.Fatal("commit hook ignored")
	}
	s.beforeCommit = nil
	st, err := s.Inspect()
	if err != nil || len(st.Commands) != 1 || len(st.Retention.Retired) != 0 {
		t.Fatal("retirement not atomic", err)
	}
	if _, err = s.Admit(delivery(s.c, stop), caller(s.c)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Retain(retainRequest(s, false, 1), caller(s.c)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Retain(retainRequest(s, false, 1), caller(s.c)); err != nil {
		t.Fatal("retry retirement", err)
	}
}
func TestRetentionStopBeforeRunAndCommandBound(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "journal"), config())
	defer s.Close()
	activateRetention(t, s)
	stop := ticketBody(t, "stop", 1)
	if _, err := s.Admit(delivery(s.c, stop), caller(s.c)); err != nil {
		t.Fatal(err)
	}
	result, err := s.Admit(delivery(s.c, ticketBody(t, "run", 1)), caller(s.c))
	if err != nil || result.Outcome != "rejected-stopped" {
		t.Fatal(result, err)
	}
	stop["command"] = "different-stop-command"
	if _, err = s.Admit(delivery(s.c, stop), caller(s.c)); err == nil {
		t.Fatal("unbounded Stop identities")
	}
	ackAllHistory(t, s)
	if _, err = s.Retain(retainRequest(s, false, 1), caller(s.c)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Admit(delivery(s.c, ticketBody(t, "run", 1)), caller(s.c)); err == nil {
		t.Fatal("late Run resurrected")
	}
}
func TestRetentionActivationRequiresQuiescence(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "journal"), config())
	defer s.Close()
	if _, err := s.Admit(delivery(s.c, fixture(t, "run")), caller(s.c)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Retain(retainRequest(s, true), caller(s.c)); err == nil {
		t.Fatal("unacked legacy activation")
	}
	ackAllHistory(t, s)
	if _, err := s.Retain(retainRequest(s, true), caller(s.c)); err == nil {
		t.Fatal("live legacy activation")
	}
}
func TestRetentionGarbageAdmissionFence(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "journal"), config())
	defer s.Close()
	activateRetention(t, s)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		st.Retention.Garbage = []RetentionGarbage{{Key: fmt.Sprintf("%064d", 0), Attempt: Attempt{Body: ticketBody(t, "stop", 1)}}}
		return save(b, st)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Admit(delivery(s.c, ticketBody(t, "run", 2)), caller(s.c)); !errors.Is(err, ErrInventoryCapacity) {
		t.Fatal("garbage did not backpressure admission", err)
	}
}

func TestRetentionCrashAfterFenceBeforeArtifactDeletion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	root := t.TempDir()
	s := open(t, path, config())
	activateRetention(t, s)
	s.runtimeRoot = root
	stop := ticketBody(t, "stop", 1)
	key := attemptKey(stop["identity"].(map[string]any))
	artifact := filepath.Join(root, key)
	if err := os.Mkdir(artifact, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(artifact, "stdout.log"), []byte("retained log"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Admit(delivery(s.c, stop), caller(s.c)); err != nil {
		t.Fatal(err)
	}
	ackAllHistory(t, s)
	s.beforeGarbage = func() error { return errors.New("crash after committed replay fence") }
	if _, err := s.Retain(retainRequest(s, false, 1), caller(s.c)); err == nil {
		t.Fatal("missing crash")
	}
	st, err := s.Inspect()
	if err != nil || !retired(st.Retention, 1) || len(st.Retention.Garbage) != 1 || len(st.Commands) != 0 {
		t.Fatal("non-atomic garbage intent", err)
	}
	if _, err = os.Stat(artifact); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path, config())
	defer s.Close()
	s.runtimeRoot = root
	if _, err = s.Admit(delivery(s.c, stop), caller(s.c)); err == nil {
		t.Fatal("replay after committed fence")
	}
	if _, err = s.Retain(retainRequest(s, false), caller(s.c)); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(artifact); !os.IsNotExist(err) {
		t.Fatal("artifact survived recovery", err)
	}
	st, err = s.Inspect()
	if err != nil || len(st.Retention.Garbage) != 0 {
		t.Fatal("garbage intent not completed", err)
	}
}

func TestReadinessBacklogCoalescesAndPreservesTerminal(t *testing.T) {
	body := ticketBody(t, "run", 1)
	key := attemptKey(body["identity"].(map[string]any))
	st := State{Sequences: map[string]uint64{}, Observations: make([]map[string]any, MaxObservationBacklog)}
	a := Attempt{Body: body, Execution: &Execution{Phase: "released", Outcome: "running", Cleanup: "pending"}}
	for i := 0; i < 20000; i++ {
		before := *a.Execution
		a.Execution.Ready = !a.Execution.Ready
		if err := recordExecutionTransition(&st, key, &a, &before, true, false); err != nil {
			t.Fatal(err)
		}
	}
	if !a.PendingReadiness || st.Cursor != 0 || len(st.Observations) != MaxObservationBacklog {
		t.Fatal("readiness backlog grew")
	}
	before := *a.Execution
	a.Execution = &Execution{Phase: "terminal", Outcome: "stopped", Cleanup: "complete"}
	if err := recordExecutionTransition(&st, key, &a, &before, true, false); err != nil {
		t.Fatal(err)
	}
	if a.PendingReadiness || st.Cursor != 1 || len(st.Observations) != MaxObservationBacklog+1 {
		t.Fatal("terminal outcome was coalesced")
	}
}

func TestSupervisorEventsPruneAndLatestReadinessFlushes(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "journal"), config())
	defer s.Close()
	body := fixture(t, "run")
	key := attemptKey(body["identity"].(map[string]any))
	if err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		st.FormatVersion = 3
		st.RuntimeScope = &RuntimeScope{}
		st.Attempts[key] = Attempt{Body: body, Execution: &Execution{Phase: "released", Outcome: "running", Cleanup: "pending"}}
		return save(b, st)
	}); err != nil {
		t.Fatal(err)
	}
	r := &Runtime{store: s, opts: RuntimeOptions{captureEvents: true}}
	for i := 0; i < MaxSupervisorEvents+100; i++ {
		if err := r.update(key, func(a *Attempt) error { a.Execution.Ready = !a.Execution.Ready; return nil }, true); err != nil {
			t.Fatal(err)
		}
	}
	st, err := s.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.ExecutionEvents) != MaxSupervisorEvents || len(st.Observations) != 0 || !st.Attempts[key].PendingReadiness {
		t.Fatal("unbounded helper history")
	}
	if err = s.ackSupervisorEvents(MaxSupervisorEvents); err != nil {
		t.Fatal(err)
	}
	if err = r.flushReadiness(); err != nil {
		t.Fatal(err)
	}
	st, err = s.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	if st.EventBase != MaxSupervisorEvents || len(st.ExecutionEvents) != 1 || st.ExecutionEvents[0].Sequence != MaxSupervisorEvents+1 || st.Attempts[key].PendingReadiness {
		t.Fatal("readiness flush sequence lost")
	}
	if err = s.ackSupervisorEvents(MaxSupervisorEvents - 1); err == nil {
		t.Fatal("supervisor ACK rollback accepted")
	}
	path := s.db.Path()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path, config())
	defer s.Close()
	if st, err = s.Inspect(); err != nil || st.EventBase != MaxSupervisorEvents || len(st.ExecutionEvents) != 1 {
		t.Fatal("event fence not durable", err)
	}
}

func TestRetentionSerializesCachedRuntimeSnapshot(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "journal"), config())
	defer s.Close()
	activateRetention(t, s)
	if _, err := s.Admit(delivery(s.c, ticketBody(t, "stop", 1)), caller(s.c)); err != nil {
		t.Fatal(err)
	}
	ackAllHistory(t, s)
	r, err := NewRuntime(s, RuntimeOptions{Root: filepath.Join(t.TempDir(), "work"), Network: "agent-container"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	snapshotted := make(chan struct{})
	release := make(chan struct{})
	r.hooks.afterSnapshot = func() { close(snapshotted); <-release }
	ticked := make(chan error, 1)
	go func() { ticked <- r.Tick(context.Background()) }()
	<-snapshotted
	retained := make(chan error, 1)
	go func() { _, e := s.Retain(retainRequest(s, false, 1), caller(s.c)); retained <- e }()
	select {
	case e := <-retained:
		close(release)
		t.Fatal("retention raced cached Tick", e)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err = <-ticked; err != nil {
		t.Fatal(err)
	}
	if err = <-retained; err != nil {
		t.Fatal(err)
	}
	st, err := s.Inspect()
	if err != nil || retired(st.Retention, 1) {
		t.Fatal("new terminal observation discarded", err)
	}
	ackAllHistory(t, s)
	if _, err = s.Retain(retainRequest(s, false, 1), caller(s.c)); err != nil {
		t.Fatal(err)
	}
}

func TestDisconnectedReadinessPersistsAndFlushesAfterAck(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	s := open(t, path, config())
	body := fixture(t, "run")
	key := attemptKey(body["identity"].(map[string]any))
	if err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		st.FormatVersion = 2
		st.RuntimeScope = &RuntimeScope{}
		a := Attempt{Body: body, Execution: &Execution{Phase: "released", Outcome: "running", Cleanup: "pending"}}
		for i := 0; i < MaxObservationBacklog; i++ {
			if e = observeAttempt(&st, key, &a); e != nil {
				return e
			}
		}
		st.Attempts[key] = a
		return save(b, st)
	}); err != nil {
		t.Fatal(err)
	}
	r := &Runtime{store: s}
	if err := r.update(key, func(a *Attempt) error { a.Execution.Ready = true; return nil }, true); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path, config())
	defer s.Close()
	r = &Runtime{store: s}
	st, err := s.Inspect()
	if err != nil || !st.Attempts[key].PendingReadiness || st.Cursor != MaxObservationBacklog {
		t.Fatal("pending readiness not durable", err)
	}
	ackAllHistory(t, s) // One bounded ACK prune frees 1024 slots.
	if err = r.flushReadiness(); err != nil {
		t.Fatal(err)
	}
	st, err = s.Inspect()
	if err != nil || st.Attempts[key].PendingReadiness || st.Cursor != MaxObservationBacklog+1 || st.Observations[len(st.Observations)-1]["ready"] != true {
		t.Fatal("latest readiness not flushed", err)
	}
	if err = r.update(key, func(a *Attempt) error {
		a.Execution.Phase = "terminal"
		a.Execution.Outcome = "failed"
		a.Execution.HealthFailure = "health-check-failed"
		a.Execution.Ready = false
		a.Execution.Cleanup = "complete"
		return nil
	}, true); err != nil {
		t.Fatal(err)
	}
	st, err = s.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	last := st.Observations[len(st.Observations)-1]
	if last["state"] != "failed" || last["cleanup"] != "complete" || last["reason"] != "health-check-failed" {
		t.Fatal("terminal health failure lost", last)
	}
}

func TestRuntimeRootRestartFencePreservesGarbage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	root := filepath.Join(t.TempDir(), "work")
	s := open(t, path, config())
	activateRetention(t, s)
	r, err := NewRuntime(s, RuntimeOptions{Root: root, Network: "agent-container"})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	stop := ticketBody(t, "stop", 1)
	key := attemptKey(stop["identity"].(map[string]any))
	if _, err = s.Admit(delivery(s.c, stop), caller(s.c)); err != nil {
		t.Fatal(err)
	}
	ackAllHistory(t, s)
	if err = os.Mkdir(filepath.Join(root, key), 0700); err != nil {
		t.Fatal(err)
	}
	s.beforeGarbage = func() error { return errors.New("interrupted artifact GC") }
	if _, err = s.Retain(retainRequest(s, false, 1), caller(s.c)); err == nil {
		t.Fatal("expected interrupted GC")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path, config())
	defer s.Close()
	changed := filepath.Join(t.TempDir(), "new-root")
	if _, err = NewRuntime(s, RuntimeOptions{Root: changed, Network: "agent-container"}); err == nil {
		t.Fatal("changed runtime root accepted")
	}
	if _, err = os.Stat(changed); !os.IsNotExist(err) {
		t.Fatal("changed root mutated before rejection", err)
	}
	st, err := s.Inspect()
	if err != nil || st.RuntimeRoot != root || len(st.Retention.Garbage) != 1 {
		t.Fatal("root rejection changed durable GC", err)
	}
	if _, err = os.Stat(filepath.Join(root, key)); err != nil {
		t.Fatal("owned artifact removed by wrong root", err)
	}
	r, err = NewRuntime(s, RuntimeOptions{Root: root, Network: "agent-container"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = s.Retain(retainRequest(s, false), caller(s.c)); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(root, key)); !os.IsNotExist(err) {
		t.Fatal("correct-root GC did not resume", err)
	}
}
