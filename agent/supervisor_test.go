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
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func supervisorOpts(t *testing.T, root string) RuntimeOptions {
	o := testRuntimeOpts(t, root)
	o.Supervise = true
	o.SupervisorArgs = []string{"-test.run=^TestSupervisorChild$", "--", "supervise"}
	return o
}
func TestSupervisorChild(t *testing.T) {
	for _, arg := range os.Args {
		if arg == "supervise" {
			if e := SuperviseHelper(); e != nil {
				fmt.Fprintln(os.Stderr, e)
				os.Exit(91)
			}
			os.Exit(0)
		}
	}
}
func TestSupervisorDaemon(t *testing.T) {
	for i, arg := range os.Args {
		if arg != "supervisor-daemon" {
			continue
		}
		root := os.Args[i+1]
		c := config()
		s := open(t, filepath.Join(root, "state"), c)
		b := fixture(t, "run")
		p := b["assignment"].(map[string]any)
		p["ports"] = []any{}
		p["readiness"] = map[string]any{"kind": "none"}
		p["argv"] = []any{"/bin/sh", "-c", "echo launched >> '" + root + "/launches'; while [ ! -f '" + root + "/release' ]; do sleep 0.02; done; i=0; while [ $i -lt 2048 ]; do printf x; i=$((i+1)); done; echo exact-stderr >&2; exit 7"}
		if _, e := s.Admit(delivery(c, b), caller(c)); e != nil {
			t.Fatal(e)
		}
		r, e := NewRuntime(s, supervisorOpts(t, filepath.Join(root, "work")))
		if e != nil {
			t.Fatal(e)
		}
		if len(os.Args) > i+2 && os.Args[i+2] == "crash-before-ack" {
			r.hooks.beforeSupervisorAck = func() { os.Exit(0) }
		}
		for {
			if e = r.Tick(context.Background()); e != nil {
				t.Fatal(e)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
func waitFile(t *testing.T, path string) {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		if b, e := os.ReadFile(path); e == nil && len(b) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("file did not appear", path)
}
func TestSupervisorDaemonCrashExactExitAndReplay(t *testing.T) {
	root := t.TempDir()
	exe, _ := os.Executable()
	cmd := exec.Command(exe, "-test.run=^TestSupervisorDaemon$", "--", "supervisor-daemon", root)
	var output strings.Builder
	cmd.Stdout = &output
	cmd.Stderr = &output
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	waitFile(t, filepath.Join(root, "launches"))
	if e := cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	cmd.Wait()
	if e := os.WriteFile(filepath.Join(root, "release"), []byte("go"), 0600); e != nil {
		t.Fatal(e)
	}
	time.Sleep(150 * time.Millisecond)
	c := config()
	s := open(t, filepath.Join(root, "state"), c)
	defer s.Close()
	r, e := NewRuntime(s, supervisorOpts(t, filepath.Join(root, "work")))
	if e != nil {
		t.Fatalf("recover %v; daemon %s", e, output.String())
	}
	runUntil(t, r, func(st State) bool {
		a := onlyAttempt(st)
		return a.Execution != nil && a.Execution.Cleanup == "complete"
	})
	st, _ := s.Inspect()
	a := onlyAttempt(st)
	if a.Execution.Outcome != "failed" || a.Execution.ExitCode == nil || *a.Execution.ExitCode != 7 || a.Execution.Signal != 0 || a.Execution.StdoutBytes != 1024 || a.Execution.StdoutDropped != 1024 {
		t.Fatalf("exact durable exit/log counters: %+v", a.Execution)
	}
	cursor := st.Cursor
	if e = r.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	st, _ = s.Inspect()
	if st.Cursor != cursor {
		t.Fatal("duplicate import")
	}
	b, _ := os.ReadFile(filepath.Join(root, "launches"))
	if string(b) != "launched\n" {
		t.Fatal("replayed launch", string(b))
	}
	r.Close()
	if _, e = NewRuntime(s, testRuntimeOpts(t, filepath.Join(root, "work"))); e == nil {
		t.Fatal("downgrade accepted")
	}
}
func TestSupervisorImportFailureAndLoss(t *testing.T) {
	for _, loss := range []bool{false, true} {
		t.Run(fmt.Sprint(loss), func(t *testing.T) {
			root := t.TempDir()
			c := config()
			s := open(t, filepath.Join(root, "state"), c)
			defer s.Close()
			b := runtimeBody(t, "sleep", filepath.Join(root, "marker"), 0)
			p := b["assignment"].(map[string]any)
			p["ports"] = []any{}
			p["readiness"] = map[string]any{"kind": "none"}
			if _, e := s.Admit(delivery(c, b), caller(c)); e != nil {
				t.Fatal(e)
			}
			r, e := NewRuntime(s, supervisorOpts(t, filepath.Join(root, "work")))
			if e != nil {
				t.Fatal(e)
			}
			runUntil(t, r, func(st State) bool { return onlyAttempt(st).Execution.Outcome == "running" })
			st, _ := s.Inspect()
			a := onlyAttempt(st)
			defer unix.Kill(a.Supervisor.Process.PID, unix.SIGKILL)
			if loss {
				unix.Kill(a.Supervisor.Process.PID, unix.SIGKILL)
				time.Sleep(50 * time.Millisecond)
				runUntil(t, r, func(st State) bool { return onlyAttempt(st).Execution.Cleanup == "complete" })
				st, _ = s.Inspect()
				a = onlyAttempt(st)
				if a.Execution.Outcome != "lost" || a.Execution.ExitCode != nil {
					t.Fatalf("fabricated outcome %+v", a.Execution)
				}
				return
			}
			stop := fixture(t, "stop")
			if _, e := s.Admit(delivery(c, stop), caller(c)); e != nil {
				t.Fatal(e)
			}
			r.hooks.beforePersist = func(Attempt) error { return errors.New("injected node journal failure") }
			_ = r.Tick(context.Background())
			time.Sleep(100 * time.Millisecond)
			if e = r.Tick(context.Background()); e == nil {
				t.Fatal("failed import accepted")
			}
			r.hooks.beforePersist = nil
			runUntil(t, r, func(st State) bool { return onlyAttempt(st).Execution.Cleanup == "complete" })
			st, _ = s.Inspect()
			a = onlyAttempt(st)
			if a.Execution.Outcome != "stopped" || a.Execution.Signal != int(unix.SIGTERM) {
				t.Fatalf("lost exact signal after failed import %+v", a.Execution)
			}
		})
	}
}

func TestSupervisorProtocolAndMonotonicStop(t *testing.T) {
	root := t.TempDir()
	c := config()
	s := open(t, filepath.Join(root, "state"), c)
	defer s.Close()
	b := fixture(t, "run")
	p := b["assignment"].(map[string]any)
	p["ports"] = []any{}
	p["readiness"] = map[string]any{"kind": "none"}
	marker := filepath.Join(root, "ready")
	p["argv"] = []any{"/bin/sh", "-c", "trap '' TERM; echo ready > '" + marker + "'; while :; do sleep 0.02; done"}
	if _, e := s.Admit(delivery(c, b), caller(c)); e != nil {
		t.Fatal(e)
	}
	r, e := NewRuntime(s, supervisorOpts(t, filepath.Join(root, "work")))
	if e != nil {
		t.Fatal(e)
	}
	runUntil(t, r, func(st State) bool { return onlyAttempt(st).Execution.Outcome == "running" })
	waitFile(t, marker)
	st, _ := s.Inspect()
	a := onlyAttempt(st)
	defer unix.Kill(a.Supervisor.Process.PID, unix.SIGKILL)
	key := attemptKey(b["identity"].(map[string]any))
	for _, bad := range []supervisorRequest{{Version: 0, Token: a.Supervisor.Token, Scope: r.scope}, {Version: 2, Token: a.Supervisor.Token, Scope: r.scope}, {Version: 1, Token: "wrong", Scope: r.scope}} {
		f, address, e := socketAddress(r.opts.Root, key)
		if e != nil {
			t.Fatal(e)
		}
		conn, e := net.DialUnix("unix", nil, &net.UnixAddr{Name: address, Net: "unix"})
		f.Close()
		if e != nil {
			t.Fatal(e)
		}
		json.NewEncoder(conn).Encode(bad)
		var reply supervisorReply
		e = json.NewDecoder(conn).Decode(&reply)
		conn.Close()
		if e != nil || reply.Error == "" {
			t.Fatal("invalid local control accepted", bad, e)
		}
	}
	before := time.Now()
	if e = r.update(key, func(a *Attempt) error {
		a.Stopped = true
		a.Deadline = time.Now().Add(time.Hour).UnixMilli()
		a.DeadlineMono = monoMillis() + 50
		return nil
	}, false); e != nil {
		t.Fatal(e)
	}
	runUntil(t, r, func(st State) bool { return onlyAttempt(st).Execution.Phase == "terminal" })
	st, _ = s.Inspect()
	a = onlyAttempt(st)
	if a.Execution.Signal != int(unix.SIGKILL) || time.Since(before) > time.Second {
		t.Fatalf("wall clock extended monotonic stop: %+v", a.Execution)
	}
}

func TestSupervisorMissingJournalRetainsReservation(t *testing.T) {
	root := t.TempDir()
	c := config()
	s := open(t, filepath.Join(root, "state"), c)
	defer s.Close()
	b := runtimeBody(t, "sleep", filepath.Join(root, "marker"), 0)
	p := b["assignment"].(map[string]any)
	p["ports"] = []any{}
	p["readiness"] = map[string]any{"kind": "none"}
	if _, e := s.Admit(delivery(c, b), caller(c)); e != nil {
		t.Fatal(e)
	}
	r, e := NewRuntime(s, supervisorOpts(t, filepath.Join(root, "work")))
	if e != nil {
		t.Fatal(e)
	}
	runUntil(t, r, func(st State) bool { return onlyAttempt(st).Execution.Outcome == "running" })
	st, _ := s.Inspect()
	a := onlyAttempt(st)
	if e = unix.Kill(a.Supervisor.Process.PID, unix.SIGKILL); e != nil {
		t.Fatal(e)
	}
	// SIGKILL delivery is asynchronous. Exercise the dead-supervisor recovery
	// branch only after its identity is gone or a zombie; a still-live supervisor
	// correctly blocks admission without claiming that its journal is lost.
	deadline := time.Now().Add(5 * time.Second)
	for {
		process, err := processInfo(a.Supervisor.Process.PID)
		if os.IsNotExist(err) || (err == nil && (process.Start != a.Supervisor.Process.Start || process.State == "Z" || process.State == "X")) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("supervisor did not stop after SIGKILL: %+v", process)
		}
		time.Sleep(10 * time.Millisecond)
	}
	key := attemptKey(b["identity"].(map[string]any))
	path := filepath.Join(filepath.Dir(supervisorSocket(r.opts.Root, key)), "journal.db")
	if e = os.Rename(path, path+".quarantined"); e != nil {
		t.Fatal(e)
	}
	if e = r.Tick(context.Background()); e == nil {
		t.Fatal("missing supervisor journal accepted")
	}
	st, _ = s.Inspect()
	a = onlyAttempt(st)
	if !a.Reserved() || a.Execution.Outcome != "lost" || a.Execution.Cleanup != "unknown" || a.Execution.ExitCode != nil {
		t.Fatalf("missing journal released or fabricated outcome: attempt=%+v execution=%+v", a, a.Execution)
	}
}

func TestSupervisorTerminalAckCrashBoundary(t *testing.T) {
	root := t.TempDir()
	if e := os.WriteFile(filepath.Join(root, "release"), []byte("go"), 0600); e != nil {
		t.Fatal(e)
	}
	exe, _ := os.Executable()
	cmd := exec.Command(exe, "-test.run=^TestSupervisorDaemon$", "--", "supervisor-daemon", root, "crash-before-ack")
	var output strings.Builder
	cmd.Stdout = &output
	cmd.Stderr = &output
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e, output.String())
		}
	case <-time.After(5 * time.Second):
		cmd.Process.Kill()
		<-done
		t.Fatal("daemon failed to hit ACK crash boundary", output.String())
	}
	s := open(t, filepath.Join(root, "state"), config())
	defer s.Close()
	st, _ := s.Inspect()
	a := onlyAttempt(st)
	if a.Execution.Cleanup != "complete" || a.Supervisor.Acknowledged {
		t.Fatalf("wrong crash boundary %+v", a)
	}
	pid := a.Supervisor.Process.PID
	defer unix.Kill(pid, unix.SIGKILL)
	if p, e := processInfo(pid); e != nil || p.State == "Z" {
		t.Fatal("supervisor did not survive pending ACK")
	}
	cursor := st.Cursor
	r, e := NewRuntime(s, supervisorOpts(t, filepath.Join(root, "work")))
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	st, _ = s.Inspect()
	a = onlyAttempt(st)
	if !a.Supervisor.Acknowledged || a.Execution.ExitCode == nil || *a.Execution.ExitCode != 7 || st.Cursor != cursor {
		t.Fatalf("ACK replay changed terminal outcome/cursor %+v", a)
	}
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		p, e := processInfo(pid)
		if os.IsNotExist(e) || (e == nil && p.State == "Z") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("terminal supervisor leaked after ACK replay")
}

func TestSupervisorWaitDelayRetainsKnownExit(t *testing.T) {
	root := t.TempDir()
	s := open(t, filepath.Join(root, "state"), config())
	defer s.Close()
	b := fixture(t, "run")
	p := b["assignment"].(map[string]any)
	p["ports"] = []any{}
	p["readiness"] = map[string]any{"kind": "none"}
	p["argv"] = []any{"/bin/sh", "-c", "sleep 0.6 & exit 0"}
	if _, e := s.Admit(delivery(config(), b), caller(config())); e != nil {
		t.Fatal(e)
	}
	r, e := NewRuntime(s, supervisorOpts(t, filepath.Join(root, "work")))
	if e != nil {
		t.Fatal(e)
	}
	runUntil(t, r, func(st State) bool { return onlyAttempt(st).Execution.Phase == "terminal" })
	st, _ := s.Inspect()
	a := onlyAttempt(st)
	if a.Execution.ExitCode == nil || *a.Execution.ExitCode != 0 {
		t.Fatalf("WaitDelay discarded known root exit: %+v", a.Execution)
	}
	unix.Kill(a.Supervisor.Process.PID, unix.SIGKILL)
	time.Sleep(700 * time.Millisecond)
	runUntil(t, r, func(st State) bool { return onlyAttempt(st).Execution.Cleanup == "complete" })
	st, _ = s.Inspect()
	a = onlyAttempt(st)
	if a.Execution.ExitCode == nil || *a.Execution.ExitCode != 0 {
		t.Fatalf("supervisor loss discarded durable root exit: %+v", a.Execution)
	}
}

func TestSupervisorTimeoutPreservesWorkAndReconcilesOtherAttempts(t *testing.T) {
	root := t.TempDir()
	c := config()
	c.CPU *= 3
	c.Memory *= 3
	s := open(t, filepath.Join(root, "state"), c)
	defer s.Close()
	opts := supervisorOpts(t, filepath.Join(root, "work"))
	r, err := NewRuntime(s, opts)
	if err != nil {
		t.Fatal(err)
	}
	var paused int
	defer func() {
		if paused != 0 {
			unix.Kill(paused, unix.SIGCONT)
		}
		if r.closed {
			reopened, reopenErr := NewRuntime(s, opts)
			if reopenErr != nil {
				t.Error(reopenErr)
				return
			}
			r = reopened
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}()
	add := func(name, mode string) string {
		b := runtimeBody(t, mode, filepath.Join(root, name), 0)
		b["command"] = "command-" + name
		id := b["identity"].(map[string]any)
		id["attempt"] = "attempt-" + name
		id["run"] = "run-" + name
		id["instance"] = "instance-" + name
		p := b["assignment"].(map[string]any)
		p["ports"] = []any{}
		p["readiness"] = map[string]any{"kind": "none"}
		result, e := s.Admit(delivery(c, b), caller(c))
		if e != nil || result.Outcome != "accepted" {
			t.Fatalf("admit: %+v %v", result, e)
		}
		return attemptKey(id)
	}
	first := add("first", "sleep")
	second := add("second", "sleep")
	runUntil(t, r, func(st State) bool {
		return st.Attempts[first].Execution != nil && st.Attempts[second].Execution != nil && st.Attempts[first].Execution.Outcome == "running" && st.Attempts[second].Execution.Outcome == "running"
	})
	before, err := s.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	paused = before.Attempts[first].Supervisor.Process.PID
	if err = unix.Kill(paused, unix.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	// Pause an actual authenticated supervisor; its socket stays connected but
	// cannot reply. This reaches the same one-second read timeout as the soak.
	start := time.Now()
	if err = r.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 900*time.Millisecond || len(r.supervisorRetry) != 1 {
		t.Fatal("did not exercise actual supervisor timeout")
	}
	batch := add("batch", "batch")
	runUntil(t, r, func(st State) bool {
		return st.Attempts[batch].Execution != nil && st.Attempts[batch].Execution.Cleanup == "complete"
	})
	assertPreserved := func() {
		t.Helper()
		st, e := s.Inspect()
		if e != nil {
			t.Fatal(e)
		}
		if st.Config != before.Config {
			t.Fatal("authority changed")
		}
		for _, key := range []string{first, second} {
			a := st.Attempts[key]
			original := before.Attempts[key]
			if a.Stopped || !a.Reserved() || a.Execution.ExitCode != nil || a.Execution.PID != original.Execution.PID || a.Execution.Start != original.Execution.Start {
				t.Fatalf("unrelated stop or identity change: %+v", a.Execution)
			}
			p, e := processInfo(a.Execution.PID)
			if e != nil || p.Start != a.Execution.Start || p.State == "Z" || p.State == "X" {
				t.Fatalf("workload not alive: %+v %v", p, e)
			}
		}
	}
	assertPreserved()
	// An internal fatal server/runtime exit must detach without a bulk Stop.
	if err = finishHTTPSRuntime(r, false); err != nil {
		t.Fatal(err)
	}
	assertPreserved()
	// Restart while the supervisor remains unavailable must retain the same
	// durable reservation and retry later, not fail into a keeper restart loop.
	reopened, err := NewRuntime(s, opts)
	if err != nil {
		t.Fatal(err)
	}
	r = reopened
	if len(r.supervisorRetry) != 1 {
		t.Fatal("restart did not retain unavailable supervisor")
	}
	assertPreserved()
	if err = unix.Kill(paused, unix.SIGCONT); err != nil {
		t.Fatal(err)
	}
	paused = 0
	runUntil(t, r, func(st State) bool { return len(r.supervisorRetry) == 0 })
	assertPreserved()
	// A reachable supervisor rejecting our token is not a transport retry.
	state, err := s.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	token := state.Attempts[first].Supervisor.Token
	defer func() {
		if restoreErr := r.update(first, func(a *Attempt) error { a.Supervisor.Token = token; return nil }, false); restoreErr != nil {
			t.Error(restoreErr)
		}
	}()
	wrongToken := "0" + token[1:]
	if token[0] == '0' {
		wrongToken = "1" + token[1:]
	}
	if err = r.update(first, func(a *Attempt) error { a.Supervisor.Token = wrongToken; return nil }, false); err != nil {
		t.Fatal(err)
	}
	if err = r.Tick(context.Background()); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("authentication failure was not fatal: %v", err)
	}
	if err = r.update(first, func(a *Attempt) error { a.Supervisor.Token = token; return nil }, false); err != nil {
		t.Fatal(err)
	}
	assertPreserved()
	// Explicit operator shutdown retains its existing drain behavior.
	if err = finishHTTPSRuntime(r, true); err != nil {
		t.Fatal(err)
	}
	st, err := s.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{first, second} {
		a := st.Attempts[key]
		if !a.Stopped || a.Reserved() || a.Execution.Outcome != "stopped" {
			t.Fatalf("explicit shutdown failed: %+v", a)
		}
	}
}

func TestSupervisorRetryClassificationRejectsIntegrityFailures(t *testing.T) {
	for _, err := range []error{io.ErrUnexpectedEOF, unix.EACCES, errors.New("supervisor peer mismatch"), errors.New("supervisor event gap"), &json.SyntaxError{Offset: 1}} {
		var retry *supervisorUnavailable
		if errors.As(supervisorIOError(err), &retry) {
			t.Fatalf("integrity failure treated as retryable: %v", err)
		}
	}
	for _, err := range []error{io.EOF, unix.EPIPE, unix.ECONNRESET, os.ErrDeadlineExceeded} {
		var retry *supervisorUnavailable
		if !errors.As(supervisorIOError(err), &retry) {
			t.Fatalf("availability failure not retryable: %v", err)
		}
	}
}
