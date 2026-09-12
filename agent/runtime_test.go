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
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func testRuntimeOpts(t *testing.T, root string) RuntimeOptions {
	t.Helper()
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	return RuntimeOptions{Root: root, Network: "test-network", HelperPath: exe, HelperArgs: []string{"-test.run=^TestRuntimeHelperProcess$", "--", "helper"}, LogBytes: 1024}
}
func TestRuntimeHelperProcess(t *testing.T) {
	for _, arg := range os.Args {
		if arg == "helper" {
			if e := LaunchHelper(); e != nil {
				fmt.Fprintln(os.Stderr, e)
				os.Exit(81)
			}
			os.Exit(0)
		}
	}
}
func TestRuntimeWorkload(t *testing.T) {
	index := -1
	for i, arg := range os.Args {
		if arg == "workload" {
			index = i
			break
		}
	}
	if index < 0 {
		return
	}
	mode := os.Args[index+1]
	marker := os.Args[index+2]
	f, e := os.OpenFile(marker, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		os.Exit(82)
	}
	f.Write([]byte("launch\n"))
	f.Close()
	switch mode {
	case "batch":
		fmt.Print(strings.Repeat("o", 8192))
		fmt.Fprint(os.Stderr, strings.Repeat("e", 4096))
		os.Exit(0)
	case "env":
		b, _ := json.Marshal(os.Environ())
		os.WriteFile(marker+".env", b, 0600)
		os.Exit(0)
	case "service", "ignore":
		if mode == "ignore" {
			signal.Ignore(syscall.SIGTERM)
		}
		ln, e := net.Listen("tcp4", "127.0.0.1:"+os.Args[index+3])
		if e != nil {
			os.Exit(83)
		}
		defer ln.Close()
		if mode == "service" {
			signals := make(chan os.Signal, 1)
			signal.Notify(signals, syscall.SIGTERM)
			go func() { <-signals; os.Exit(0) }()
		}
		for {
			conn, e := ln.Accept()
			if e != nil {
				os.Exit(84)
			}
			conn.Close()
		}
	case "descendant":
		child := exec.Command("/bin/sleep", "60")
		child.Env = []string{}
		child.Stdout = os.Stdout
		child.Stderr = os.Stderr
		if e := child.Start(); e != nil {
			os.Exit(86)
		}
		p, e := processInfo(child.Process.Pid)
		if e != nil {
			os.Exit(87)
		}
		raw, _ := json.Marshal(p.ProcessIdentity)
		if e := os.WriteFile(marker+".child", raw, 0600); e != nil {
			os.Exit(88)
		}
		os.Exit(0)
	case "sleep":
		time.Sleep(time.Minute)
		os.Exit(0)
	}
	os.Exit(85)
}
func runtimeBody(t *testing.T, mode, marker string, port int) map[string]any {
	b := fixture(t, "run")
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	p := b["assignment"].(map[string]any)
	p["argv"] = []any{exe, "-test.run=^TestRuntimeWorkload$", "--", "workload", mode, marker}
	p["env"] = map[string]any{"ONLY": "explicit"}
	p["stop"].(map[string]any)["graceMillis"] = uint64(50)
	if port > 0 {
		p["argv"] = append(p["argv"].([]any), strconv.Itoa(port))
		p["ports"] = []any{map[string]any{"name": "http", "number": uint64(port), "protocol": "tcp", "family": "ipv4", "network": "test-network"}}
		p["readiness"] = map[string]any{"kind": "tcp", "port": "http", "intervalMillis": uint64(10), "timeoutMillis": uint64(50)}
	}
	return b
}
func freePort(t *testing.T) int {
	ln, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}
func runUntil(t *testing.T, r *Runtime, predicate func(State) bool) {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		if e := r.Tick(context.Background()); e != nil {
			t.Fatal(e)
		}
		st, e := r.store.Inspect()
		if e != nil {
			t.Fatal(e)
		}
		if predicate(st) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	st, _ := r.store.Inspect()
	t.Fatalf("runtime condition timed out: %+v", st)
}
func onlyAttempt(st State) Attempt {
	for _, a := range st.Attempts {
		return a
	}
	return Attempt{}
}
func TestRuntimeBatchReplayEnvironmentAndLogs(t *testing.T) {
	for _, mode := range []string{"batch", "env"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "state")
			root := filepath.Join(dir, "work")
			marker := filepath.Join(dir, "effects")
			c := config()
			s := open(t, path, c)
			r, e := NewRuntime(s, testRuntimeOpts(t, root))
			if e != nil {
				t.Fatal(e)
			}
			b := runtimeBody(t, mode, marker, 0)
			d := delivery(c, b)
			if _, e = s.Admit(d, caller(c)); e != nil {
				t.Fatal(e)
			}
			runUntil(t, r, func(st State) bool {
				a := onlyAttempt(st)
				return a.Execution != nil && a.Execution.Cleanup == "complete"
			})
			st, _ := s.Inspect()
			a := onlyAttempt(st)
			if a.Execution.Outcome != "succeeded" || a.Reserved() || a.Execution.ExitCode == nil || *a.Execution.ExitCode != 0 {
				t.Fatal(a)
			}
			if mode == "batch" {
				if a.Execution.StdoutBytes != 1024 || a.Execution.StdoutDropped != 7168 || a.Execution.StderrBytes != 1024 || a.Execution.StderrDropped != 3072 {
					t.Fatal(a.Execution)
				}
				for _, name := range []string{"stdout.log", "stderr.log"} {
					fi, e := os.Stat(filepath.Join(root, attemptKey(b["identity"].(map[string]any)), name))
					if e != nil || fi.Size() != 1024 {
						t.Fatal(fi, e)
					}
				}
			} else {
				raw, _ := os.ReadFile(marker + ".env")
				if string(raw) != `["ONLY=explicit"]` {
					t.Fatal(string(raw))
				}
			}
			r.Close()
			s.Close()
			s = open(t, path, c)
			r, e = NewRuntime(s, testRuntimeOpts(t, root))
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			defer r.Close()
			if _, e = s.Admit(d, caller(c)); e != nil {
				t.Fatal(e)
			}
			if e = r.Tick(context.Background()); e != nil {
				t.Fatal(e)
			}
			effects, _ := os.ReadFile(marker)
			if string(effects) != "launch\n" {
				t.Fatal("replayed effect", string(effects))
			}
		})
	}
}
func TestRuntimeServiceReadinessStopAndEscalation(t *testing.T) {
	for _, mode := range []string{"service", "ignore"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			c := config()
			s := open(t, filepath.Join(dir, "state"), c)
			r, e := NewRuntime(s, testRuntimeOpts(t, filepath.Join(dir, "work")))
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			defer r.Close()
			b := runtimeBody(t, mode, filepath.Join(dir, "effects"), freePort(t))
			if _, e = s.Admit(delivery(c, b), caller(c)); e != nil {
				t.Fatal(e)
			}
			runUntil(t, r, func(st State) bool { a := onlyAttempt(st); return a.Execution != nil && a.Execution.Ready })
			stop := fixture(t, "stop")
			stop["graceMillis"] = uint64(50)
			if _, e = s.Admit(delivery(c, stop), caller(c)); e != nil {
				t.Fatal(e)
			}
			st, _ := s.Inspect()
			if onlyAttempt(st).Execution.Ready {
				t.Fatal("Stop retained ready")
			}
			runUntil(t, r, func(st State) bool { return onlyAttempt(st).Execution.Cleanup == "complete" })
			st, _ = s.Inspect()
			a := onlyAttempt(st)
			if a.Execution.Outcome != "stopped" || a.Reserved() {
				t.Fatal(a)
			}
			if mode == "ignore" && a.Execution.Signal != int(syscall.SIGKILL) {
				t.Fatal("no escalation", a.Execution)
			}
			ln, e := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", b["assignment"].(map[string]any)["ports"].([]any)[0].(map[string]any)["number"].(uint64)))
			if e != nil {
				t.Fatal("port not released", e)
			}
			ln.Close()
		})
	}
}
func TestRuntimeStopBeforeRun(t *testing.T) {
	dir := t.TempDir()
	c := config()
	s := open(t, filepath.Join(dir, "state"), c)
	defer s.Close()
	r, e := NewRuntime(s, testRuntimeOpts(t, filepath.Join(dir, "work")))
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	marker := filepath.Join(dir, "effects")
	s.Admit(delivery(c, fixture(t, "stop")), caller(c))
	s.Admit(delivery(c, runtimeBody(t, "batch", marker, 0)), caller(c))
	r.Tick(context.Background())
	if _, e = os.Stat(marker); !os.IsNotExist(e) {
		t.Fatal("stopped attempt executed")
	}
	st, e := s.Inspect()
	if e != nil {
		t.Fatal(e)
	}
	a := onlyAttempt(st)
	if a.Body["kind"] != "Stop" || a.Execution == nil || a.Execution.Phase != "terminal" || a.Execution.Outcome != "stopped" || a.Execution.Cleanup != "complete" || a.Execution.PID != 0 || a.Reserved() {
		t.Fatal("pure Stop tombstone not completed", a)
	}
	last := st.Observations[len(st.Observations)-1]
	if last["state"] != "stopped" || last["cleanup"] != "complete" || last["ready"] != false {
		t.Fatal("Stop observation incomplete", last)
	}
	cursor := st.Cursor
	for i := 0; i < 3; i++ {
		if e = r.Tick(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	st, e = s.Inspect()
	if e != nil || st.Cursor != cursor {
		t.Fatal("Stop completion emitted repeatedly", st.Cursor, e)
	}
}

func TestRuntimeCrashDriver(t *testing.T) {
	idx := -1
	for i, arg := range os.Args {
		if arg == "runtime-crash" {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	dir, stage := os.Args[idx+1], os.Args[idx+2]
	c := config()
	s := open(t, filepath.Join(dir, "state"), c)
	r, e := NewRuntime(s, testRuntimeOpts(t, filepath.Join(dir, "work")))
	if e != nil {
		t.Fatal(e)
	}
	marker := filepath.Join(dir, "effects")
	b := runtimeBody(t, "sleep", marker, 0)
	if _, e = s.Admit(delivery(c, b), caller(c)); e != nil {
		t.Fatal(e)
	}
	crash := func() { os.Exit(91) }
	switch stage {
	case "intent":
		r.hooks.afterIntent = crash
	case "spawn":
		r.hooks.afterSpawn = crash
	case "identity":
		r.hooks.afterIdentity = crash
	case "gate-intent":
		r.hooks.beforeGate = crash
	case "release":
		r.hooks.afterRelease = func() {
			until := time.Now().Add(time.Second)
			for time.Now().Before(until) {
				if _, e := os.Stat(marker); e == nil {
					break
				}
				time.Sleep(time.Millisecond)
			}
			crash()
		}
	}
	r.Tick(context.Background())
	os.Exit(92)
}
func TestRuntimeCrashRecoveryNeverRelaunches(t *testing.T) {
	for _, stage := range []string{"intent", "spawn", "identity", "gate-intent", "release"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestRuntimeCrashDriver$", "--", "runtime-crash", dir, stage)
			output, e := cmd.CombinedOutput()
			if e == nil {
				t.Fatal("expected crash")
			}
			if ee, ok := e.(*exec.ExitError); !ok || ee.ExitCode() != 91 {
				t.Fatalf("wrong crash: %v %s", e, output)
			}
			time.Sleep(30 * time.Millisecond)
			c := config()
			s := open(t, filepath.Join(dir, "state"), c)
			defer s.Close()
			r, e := NewRuntime(s, testRuntimeOpts(t, filepath.Join(dir, "work")))
			if e != nil {
				t.Fatal(e)
			}
			defer r.Close()
			st, e := s.Inspect()
			if e != nil {
				t.Fatal(e)
			}
			a := onlyAttempt(st)
			if a.Execution.Outcome != "lost" || a.Execution.Cleanup != "complete" {
				t.Fatal(a.Execution)
			}
			if e = r.Tick(context.Background()); e != nil {
				t.Fatal(e)
			}
			raw, _ := os.ReadFile(filepath.Join(dir, "effects"))
			if stage != "release" && len(raw) != 0 {
				t.Fatal("gate escaped", string(raw))
			}
			if strings.Count(string(raw), "launch") > 1 {
				t.Fatal("duplicate execution")
			}
			if _, e = s.Admit(delivery(c, runtimeBody(t, "sleep", filepath.Join(dir, "effects"), 0)), caller(c)); e != nil {
				t.Fatal(e)
			}
			r.Tick(context.Background())
			again, _ := os.ReadFile(filepath.Join(dir, "effects"))
			if string(again) != string(raw) {
				t.Fatal("replay relaunched")
			}
		})
	}
}

func TestRuntimeRejectsUnrelatedPIDAndChangedScope(t *testing.T) {
	dir := t.TempDir()
	c := config()
	s := open(t, filepath.Join(dir, "state"), c)
	defer s.Close()
	opts := testRuntimeOpts(t, filepath.Join(dir, "work"))
	r, e := NewRuntime(s, opts)
	if e != nil {
		t.Fatal(e)
	}
	b := runtimeBody(t, "sleep", filepath.Join(dir, "effects"), 0)
	s.Admit(delivery(c, b), caller(c))
	foreign := exec.Command("/bin/sleep", "60")
	if e = foreign.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { foreign.Process.Kill(); foreign.Wait() }()
	p, e := processInfo(foreign.Process.Pid)
	if e != nil {
		t.Fatal(e)
	}
	key := attemptKey(b["identity"].(map[string]any))
	if e = r.update(key, func(a *Attempt) error {
		a.Execution = &Execution{Phase: "released", Outcome: "unknown", Cleanup: "pending", PID: p.PID, Start: "1"}
		return nil
	}, false); e != nil {
		t.Fatal(e)
	}
	r.Close()
	r, e = NewRuntime(s, opts)
	if e != nil {
		t.Fatal(e)
	}
	st, _ := s.Inspect()
	a := onlyAttempt(st)
	if a.Execution.Outcome != "lost" || a.Execution.Cleanup != "unknown" || !a.Reserved() {
		t.Fatal(a)
	}
	if e = foreign.Process.Signal(syscall.Signal(0)); e != nil {
		t.Fatal("unrelated process signaled", e)
	}
	r.Close()
	if e = s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("state"))
		st, e := read(bucket)
		if e != nil {
			return e
		}
		st.RuntimeScope.Boot = "different-real-boot"
		return save(bucket, st)
	}); e != nil {
		t.Fatal(e)
	}
	if _, e = NewRuntime(s, opts); e == nil {
		t.Fatal("actual boot change accepted")
	}
	if e = foreign.Process.Signal(syscall.Signal(0)); e != nil {
		t.Fatal(e)
	}
}
func TestRuntimeIntentCommitFailureDoesNotSpawn(t *testing.T) {
	dir := t.TempDir()
	c := config()
	s := open(t, filepath.Join(dir, "state"), c)
	defer s.Close()
	r, e := NewRuntime(s, testRuntimeOpts(t, filepath.Join(dir, "work")))
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	marker := filepath.Join(dir, "effects")
	s.Admit(delivery(c, runtimeBody(t, "batch", marker, 0)), caller(c))
	r.hooks.beforePersist = func(a Attempt) error {
		if a.Execution != nil && a.Execution.Phase == "intent" {
			return errors.New("injected commit failure")
		}
		return nil
	}
	if e = r.Tick(context.Background()); e == nil {
		t.Fatal("missing commit failure")
	}
	if _, e = os.Stat(marker); !os.IsNotExist(e) {
		t.Fatal("executed without intent commit")
	}
	st, _ := s.Inspect()
	if onlyAttempt(st).Execution != nil {
		t.Fatal("partial intent")
	}
}
func TestRuntimeUnrelatedReadinessAndShutdown(t *testing.T) {
	dir := t.TempDir()
	c := config()
	s := open(t, filepath.Join(dir, "state"), c)
	defer s.Close()
	r, e := NewRuntime(s, testRuntimeOpts(t, filepath.Join(dir, "work")))
	if e != nil {
		t.Fatal(e)
	}
	port := freePort(t)
	b := runtimeBody(t, "sleep", filepath.Join(dir, "effects"), port)
	s.Admit(delivery(c, b), caller(c))
	if e = r.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	ln, e := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if e != nil {
		t.Fatal(e)
	}
	defer ln.Close()
	for i := 0; i < 3; i++ {
		time.Sleep(10 * time.Millisecond)
		if e = r.Tick(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	st, _ := s.Inspect()
	if onlyAttempt(st).Execution.Ready {
		t.Fatal("unrelated listener accepted as ready")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if e = r.Shutdown(ctx); e != nil {
		t.Fatal(e)
	}
	st, _ = s.Inspect()
	if onlyAttempt(st).Reserved() {
		t.Fatal("shutdown did not confirm cleanup")
	}
}
func TestRuntimeStopGateSerialization(t *testing.T) {
	dir := t.TempDir()
	c := config()
	s := open(t, filepath.Join(dir, "state"), c)
	defer s.Close()
	r, e := NewRuntime(s, testRuntimeOpts(t, filepath.Join(dir, "work")))
	if e != nil {
		t.Fatal(e)
	}
	b := runtimeBody(t, "sleep", filepath.Join(dir, "effects"), 0)
	s.Admit(delivery(c, b), caller(c))
	atGate := make(chan struct{})
	release := make(chan struct{})
	r.hooks.afterIdentity = func() { close(atGate); <-release }
	ticked := make(chan error, 1)
	go func() { ticked <- r.Tick(context.Background()) }()
	<-atGate
	stopped := make(chan error, 1)
	go func() {
		stop := fixture(t, "stop")
		stop["graceMillis"] = uint64(0)
		_, e := s.Admit(delivery(c, stop), caller(c))
		stopped <- e
	}()
	select {
	case e := <-stopped:
		t.Fatal("Stop committed through launch gate lock", e)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if e = <-ticked; e != nil {
		t.Fatal(e)
	}
	if e = <-stopped; e != nil {
		t.Fatal(e)
	}
	runUntil(t, r, func(st State) bool { return onlyAttempt(st).Execution.Cleanup == "complete" })
	r.Close()
}

func TestRuntimeReleasesCapacityOnlyAfterCleanup(t *testing.T) {
	dir := t.TempDir()
	c := config()
	s := open(t, filepath.Join(dir, "state"), c)
	defer s.Close()
	r, e := NewRuntime(s, testRuntimeOpts(t, filepath.Join(dir, "work")))
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	first := runtimeBody(t, "batch", filepath.Join(dir, "first"), 0)
	s.Admit(delivery(c, first), caller(c))
	other := runtimeBody(t, "batch", filepath.Join(dir, "second"), 0)
	other["command"] = "second-command"
	other["identity"].(map[string]any)["attempt"] = "second-attempt"
	result, e := s.Admit(delivery(c, other), caller(c))
	if e != nil || result.Outcome != "rejected-capacity" {
		t.Fatal(result, e)
	}
	runUntil(t, r, func(st State) bool {
		return onlyAttempt(st).Execution != nil && onlyAttempt(st).Execution.Cleanup == "complete"
	})
	other["command"] = "third-command"
	result, e = s.Admit(delivery(c, other), caller(c))
	if e != nil || result.Outcome != "accepted" {
		t.Fatal(result, e)
	}
}
func TestRuntimeRootSafety(t *testing.T) {
	dir := t.TempDir()
	c := config()
	s := open(t, filepath.Join(dir, "state"), c)
	defer s.Close()
	target := filepath.Join(dir, "target")
	os.Mkdir(target, 0700)
	link := filepath.Join(dir, "link")
	os.Symlink(target, link)
	if _, e := NewRuntime(s, testRuntimeOpts(t, filepath.Join(link, "work"))); e == nil {
		t.Fatal("symlink ancestor accepted")
	}
	if _, e := os.Stat(filepath.Join(target, "work")); !os.IsNotExist(e) {
		t.Fatal("root preparation escaped before rejection")
	}
	public := filepath.Join(dir, "public")
	if err := os.Mkdir(public, 0700); err != nil {
		t.Fatal(err)
	}
	// The verified build launcher uses umask 077. Set the intentionally unsafe
	// permission explicitly so this test exercises the same case under any umask.
	if err := os.Chmod(public, 0755); err != nil {
		t.Fatal(err)
	}
	if _, e := NewRuntime(s, testRuntimeOpts(t, public)); e == nil {
		t.Fatal("public runtime root accepted")
	}
}

func TestRuntimeLeaderlessDescendantRetainsReservation(t *testing.T) {
	if e := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); e != nil {
		t.Fatal(e)
	}
	defer unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 0, 0, 0, 0)
	dir := t.TempDir()
	c := config()
	s := open(t, filepath.Join(dir, "state"), c)
	defer s.Close()
	r, e := NewRuntime(s, testRuntimeOpts(t, filepath.Join(dir, "work")))
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	marker := filepath.Join(dir, "effects")
	b := runtimeBody(t, "descendant", marker, 0)
	s.Admit(delivery(c, b), caller(c))
	started := time.Now()
	runUntil(t, r, func(st State) bool {
		a := onlyAttempt(st)
		return a.Execution != nil && a.Execution.Phase == "terminal"
	})
	if time.Since(started) > 2*time.Second {
		t.Fatal("inherited log pipe blocked termination")
	}
	raw, e := os.ReadFile(marker + ".child")
	if e != nil {
		t.Fatal(e)
	}
	var child ProcessIdentity
	if e = json.Unmarshal(raw, &child); e != nil {
		t.Fatal(e)
	}
	fd, e := unix.PidfdOpen(child.PID, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer unix.Close(fd)
	p, e := processInfo(child.PID)
	if e != nil || p.Start != child.Start {
		t.Fatal("test descendant identity", e)
	}
	defer func() {
		unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
		var status unix.WaitStatus
		if _, e := unix.Wait4(child.PID, &status, 0, nil); e != nil {
			t.Error("test-owned descendant reap", e)
		}
	}()
	st, e := s.Inspect()
	if e != nil {
		t.Fatal(e)
	}
	a := onlyAttempt(st)
	if a.Execution.Cleanup != "unknown" || !a.Reserved() {
		t.Fatal("leaderless descendant released", a)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e = r.Shutdown(ctx); e == nil {
		t.Fatal("uncertain cleanup reported success")
	}
}
func TestRuntimePIDNamespaceChangeRefusesRecovery(t *testing.T) {
	dir := t.TempDir()
	c := config()
	s := open(t, filepath.Join(dir, "state"), c)
	defer s.Close()
	opts := testRuntimeOpts(t, filepath.Join(dir, "work"))
	r, e := NewRuntime(s, opts)
	if e != nil {
		t.Fatal(e)
	}
	r.Close()
	if e = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		st.RuntimeScope.PIDNamespace = "pid:[different-container]"
		return save(b, st)
	}); e != nil {
		t.Fatal(e)
	}
	if _, e = NewRuntime(s, opts); e == nil {
		t.Fatal("container PID namespace change accepted")
	}
}

func TestRuntimeAdmissionPreservesTerminalObservations(t *testing.T) {
	for _, outcome := range []string{"succeeded", "stopped"} {
		t.Run(outcome, func(t *testing.T) {
			dir := t.TempDir()
			c := config()
			s := open(t, filepath.Join(dir, "state"), c)
			defer s.Close()
			r, e := NewRuntime(s, testRuntimeOpts(t, filepath.Join(dir, "work")))
			if e != nil {
				t.Fatal(e)
			}
			defer r.Close()
			mode := "batch"
			if outcome == "stopped" {
				mode = "sleep"
			}
			body := runtimeBody(t, mode, filepath.Join(dir, "effects"), 0)
			if _, e = s.Admit(delivery(c, body), caller(c)); e != nil {
				t.Fatal(e)
			}
			if outcome == "stopped" {
				if e = r.Tick(context.Background()); e != nil {
					t.Fatal(e)
				}
				stop := fixture(t, "stop")
				stop["graceMillis"] = uint64(0)
				if _, e = s.Admit(delivery(c, stop), caller(c)); e != nil {
					t.Fatal(e)
				}
			}
			runUntil(t, r, func(st State) bool {
				a := onlyAttempt(st)
				return a.Execution != nil && a.Execution.Cleanup == "complete"
			})
			st, e := s.Inspect()
			if e != nil {
				t.Fatal(e)
			}
			cursor, seq := st.Cursor, onlyAttempt(st).Sequence
			rejected := runtimeBody(t, mode, filepath.Join(dir, "effects"), 0)
			rejected["command"] = "distinct-rejected-run"
			result, e := s.Admit(delivery(c, rejected), caller(c))
			if e != nil || result.Outcome == "accepted" {
				t.Fatal(result, e)
			}
			check := func() {
				t.Helper()
				st, e := s.Inspect()
				if e != nil {
					t.Fatal(e)
				}
				last := st.Observations[len(st.Observations)-1]
				if last["state"] != outcome || last["cleanup"] != "complete" || last["ready"] != false || onlyAttempt(st).Reserved() {
					t.Fatal("terminal observation regressed", last)
				}
			}
			check()
			late := fixture(t, "stop")
			late["command"] = "distinct-late-stop"
			if _, e = s.Admit(delivery(c, late), caller(c)); e != nil {
				t.Fatal(e)
			}
			check()
			st, e = s.Inspect()
			if e != nil || st.Cursor != cursor+2 || onlyAttempt(st).Sequence != seq+2 {
				t.Fatal("admission counters", st.Cursor, e)
			}
			if e = r.Tick(context.Background()); e != nil {
				t.Fatal(e)
			}
			check()
		})
	}
}
