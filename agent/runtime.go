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
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"aurora.local/agent/protocol"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

// RuntimeScope records observed kernel identity, separately from enrollment labels.
type RuntimeScope struct {
	Boot         string `json:"boot"`
	PIDNamespace string `json:"pidNamespace"`
	NetNamespace string `json:"netNamespace"`
	InitStart    string `json:"initStart"`
}
type ProcessIdentity struct {
	PID   int    `json:"pid"`
	Start string `json:"start"`
}
type Execution struct {
	Phase         string `json:"phase"`
	PID           int    `json:"pid"`
	Start         string `json:"start"`
	Outcome       string `json:"outcome"`
	Cleanup       string `json:"cleanup"`
	Ready         bool   `json:"ready"`
	ExitCode      *int   `json:"exitCode,omitempty"`
	Signal        int    `json:"signal,omitempty"`
	StdoutBytes   int64  `json:"stdoutBytes"`
	StderrBytes   int64  `json:"stderrBytes"`
	StdoutDropped int64  `json:"stdoutDropped"`
	StderrDropped int64  `json:"stderrDropped"`
}

func validateExecution(e *Execution) error {
	switch e.Phase {
	case "intent", "spawned", "released", "terminal":
	default:
		return errors.New("invalid persisted execution phase")
	}
	switch e.Outcome {
	case "unknown", "running", "succeeded", "failed", "stopped", "lost":
	default:
		return errors.New("invalid persisted outcome")
	}
	switch e.Cleanup {
	case "unknown", "pending", "complete":
	default:
		return errors.New("invalid persisted cleanup")
	}
	if e.PID < 0 || (e.PID > 0 && e.Start == "") || e.StdoutBytes < 0 || e.StderrBytes < 0 || e.StdoutDropped < 0 || e.StderrDropped < 0 {
		return errors.New("invalid persisted execution identity/log counters")
	}
	if e.Cleanup == "complete" && e.Phase != "terminal" {
		return errors.New("nonterminal cleanup claim")
	}
	return nil
}

type RuntimeOptions struct {
	Root, Network, HelperPath string
	HelperArgs                []string
	LogBytes                  int64
	Supervise                 bool
	SupervisorArgs            []string
	captureEvents             bool
}
type Runtime struct {
	shutdownMu sync.Mutex
	store      *Store
	opts       RuntimeOptions
	scope      RuntimeScope
	mu         sync.Mutex
	live       map[string]*liveProcess
	closed     bool
	hooks      runtimeHooks
}
type runtimeHooks struct {
	beforeGate          func()
	beforePersist       func(Attempt) error
	afterIntent         func()
	afterSpawn          func()
	afterIdentity       func()
	afterRelease        func()
	beforeSupervisorAck func()
}
type liveProcess struct {
	pidfd          int
	cmd            *exec.Cmd
	done           chan error
	stdout, stderr *boundedLog
	identity       ProcessIdentity
	termSent       bool
	finished       bool
	waitErr        error
	lastProbe      time.Time
}

func observedScope() (RuntimeScope, error) {
	var s RuntimeScope
	b, e := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if e != nil {
		return s, e
	}
	s.Boot = strings.TrimSpace(string(b))
	s.PIDNamespace, e = os.Readlink("/proc/self/ns/pid")
	if e != nil {
		return s, e
	}
	s.NetNamespace, e = os.Readlink("/proc/self/ns/net")
	if e != nil {
		return s, e
	}
	p, e := processInfo(1)
	if e != nil {
		return s, e
	}
	s.InitStart = p.Start
	return s, nil
}

// NewRuntime recovers every previously consumed launch intent before new work.
// It requires the same observed kernel scope and an exclusively owned Store.
func NewRuntime(s *Store, opts RuntimeOptions) (*Runtime, error) {
	if !filepath.IsAbs(opts.Root) || opts.Network == "" {
		return nil, errors.New("absolute runtime root and network domain required")
	}
	if opts.LogBytes == 0 {
		opts.LogBytes = 1 << 20
	}
	if opts.LogBytes < 1024 || opts.LogBytes > 16<<20 {
		return nil, errors.New("log limit must be 1KiB..16MiB")
	}
	if opts.HelperPath == "" {
		var e error
		opts.HelperPath, e = os.Executable()
		if e != nil {
			return nil, e
		}
	}
	if len(opts.HelperArgs) == 0 {
		opts.HelperArgs = []string{"__launch-helper"}
	}
	if filepath.Clean(opts.Root) != opts.Root {
		return nil, errors.New("runtime root traversal/noncanonical path")
	}
	for current := opts.Root; current != "/"; current = filepath.Dir(current) {
		if fi, e := os.Lstat(current); e == nil {
			if fi.Mode()&os.ModeSymlink != 0 {
				return nil, errors.New("runtime root symlink ancestor")
			}
		} else if !os.IsNotExist(e) {
			return nil, e
		}
	}
	if e := os.MkdirAll(opts.Root, 0700); e != nil {
		return nil, e
	}
	if fi, e := os.Lstat(opts.Root); e != nil || !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("runtime root must be a real directory")
	}
	resolved, e := filepath.EvalSymlinks(opts.Root)
	if e != nil || resolved != filepath.Clean(opts.Root) {
		return nil, errors.New("runtime root symlink ancestor")
	}
	fi, e := os.Stat(opts.Root)
	if e != nil {
		return nil, e
	}
	if fi.Mode().Perm()&0077 != 0 {
		return nil, errors.New("runtime root must be private")
	}
	if stat, ok := fi.Sys().(*syscall.Stat_t); !ok || stat.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("runtime root owner mismatch")
	}
	scope, e := observedScope()
	if e != nil {
		return nil, e
	}
	r := &Runtime{store: s, opts: opts, scope: scope, live: map[string]*liveProcess{}}
	s.effectMu.Lock()
	defer s.effectMu.Unlock()
	if s.runtimeActive {
		return nil, errors.New("runtime already owns store")
	}
	e = s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		if st.RuntimeScope != nil && *st.RuntimeScope != scope {
			return errors.New("actual boot/runtime scope changed; external fencing/re-enrollment required")
		}
		if st.FormatVersion == 3 && !opts.Supervise && !opts.captureEvents {
			return errors.New("supervisor journal requires --supervise; downgrade refused")
		}
		st.FormatVersion = 2
		if opts.Supervise || opts.captureEvents {
			st.FormatVersion = 3
		}
		st.RuntimeScope = &scope
		return save(b, st)
	})
	if e != nil {
		return nil, e
	}
	if e = r.recover(); e != nil {
		return nil, e
	}
	s.runtimeActive = true
	s.runtimeDraining = false
	return r, nil
}
func (r *Runtime) recover() error {
	st, e := r.store.Inspect()
	if e != nil {
		return e
	}
	for key, a := range st.Attempts {
		if a.Execution == nil || (a.Execution.Cleanup == "complete" && (a.Supervisor == nil || a.Supervisor.Acknowledged)) {
			continue
		}
		if a.Supervisor != nil {
			if e := r.pollSupervisor(key, a); e != nil {
				return e
			}
			continue
		}
		x := a.Execution
		complete := false
		if x.PID == 0 {
			complete = x.Phase == "intent"
		} else {
			complete, e = cleanupOwned(ProcessIdentity{x.PID, x.Start}, unix.SIGKILL)
			if e != nil {
				complete = false
			}
			if !complete && e == nil {
				until := time.Now().Add(500 * time.Millisecond)
				for time.Now().Before(until) {
					time.Sleep(10 * time.Millisecond)
					complete, e = groupEmpty(ProcessIdentity{x.PID, x.Start})
					if e != nil || complete {
						break
					}
				}
			}
		}
		e = r.update(key, func(a *Attempt) error {
			a.Execution.Phase = "terminal"
			a.Execution.Outcome = "lost"
			a.Execution.Ready = false
			a.Execution.Cleanup = "unknown"
			if complete {
				a.Execution.Cleanup = "complete"
			}
			return nil
		}, true)
		if e != nil {
			return e
		}
	}
	return nil
}

// Tick starts newly admitted Runs once, applies committed Stop tombstones, and
// journals real outcomes/readiness. Call regularly while services are active.
func (r *Runtime) Tick(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("runtime closed")
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	st, e := r.store.Inspect()
	if e != nil {
		return e
	}
	keys := make([]string, 0, len(st.Attempts))
	for k := range st.Attempts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		a := st.Attempts[key]
		if a.Execution == nil {
			if a.Stopped || a.Body["kind"] == "Stop" {
				if e = r.update(key, func(a *Attempt) error {
					a.Execution = &Execution{Phase: "terminal", Outcome: "stopped", Cleanup: "complete"}
					return nil
				}, true); e != nil {
					return e
				}
			} else {
				if r.opts.Supervise {
					e = r.startSupervisor(key)
				} else {
					e = r.start(key)
				}
				if e != nil {
					return e
				}
			}
			continue
		}
		if a.Supervisor != nil && (!a.Supervisor.Acknowledged || a.Execution.Cleanup != "complete") {
			if e = r.pollSupervisor(key, a); e != nil {
				return e
			}
			continue
		}
		if live := r.live[key]; live != nil {
			if e = r.poll(ctx, key, a, live); e != nil {
				return e
			}
		}
	}
	return nil
}

func (r *Runtime) update(key string, change func(*Attempt) error, observe bool) error {
	return r.store.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		a, ok := st.Attempts[key]
		if !ok {
			return errors.New("missing attempt")
		}
		if e = change(&a); e != nil {
			return e
		}
		if observe {
			if e = observeAttempt(&st, key, &a); e != nil {
				return e
			}
		}
		st.Attempts[key] = a
		if r.opts.captureEvents && a.Execution != nil {
			st.ExecutionEvents = append(st.ExecutionEvents, ExecutionEvent{Sequence: uint64(len(st.ExecutionEvents) + 1), Execution: *a.Execution})
		}
		if r.hooks.beforePersist != nil {
			if e = r.hooks.beforePersist(a); e != nil {
				return e
			}
		}
		return save(b, st)
	})
}
func observeAttempt(st *State, key string, a *Attempt) error {
	if st.Cursor == math.MaxUint64 || st.Sequences[key] == math.MaxUint64 {
		return errors.New("observation counter exhausted")
	}
	st.Cursor++
	st.Sequences[key]++
	a.Sequence = st.Sequences[key]
	x := a.Execution
	if x == nil {
		return errors.New("missing execution observation")
	}
	st.Observations = append(st.Observations, map[string]any{"version": "native-v1alpha1", "kind": "Observation", "identity": a.Body["identity"], "source": a.Body["target"], "sequence": strconv.FormatUint(a.Sequence, 10), "cursor": strconv.FormatUint(st.Cursor, 10), "state": x.Outcome, "ready": x.Ready, "cleanup": x.Cleanup})
	return nil
}

func (r *Runtime) start(key string) error {
	s := r.store
	s.effectMu.Lock()
	defer s.effectMu.Unlock()
	var body map[string]any
	skip := false
	if e := r.update(key, func(a *Attempt) error {
		if a.Stopped || a.Execution != nil {
			skip = true
			return nil
		}
		a.Execution = &Execution{Phase: "intent", Outcome: "unknown", Cleanup: "pending"}
		body = a.Body
		return nil
	}, false); e != nil {
		return e
	}
	if skip {
		return nil
	}
	if r.hooks.afterIntent != nil {
		r.hooks.afterIntent()
	}
	failed := func(cause error) error {
		return r.update(key, func(a *Attempt) error {
			a.Execution.Phase = "terminal"
			a.Execution.Outcome = "failed"
			a.Execution.Cleanup = "complete"
			return nil
		}, true)
	}
	p := body["assignment"].(map[string]any)
	for _, raw := range p["ports"].([]any) {
		port := raw.(map[string]any)
		if port["network"] != r.opts.Network {
			return failed(errors.New("wrong runtime network"))
		}
		ln, e := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", port["number"].(uint64)))
		if e != nil {
			return failed(e)
		}
		ln.Close()
	}
	dir := filepath.Join(r.opts.Root, key)
	if e := os.Mkdir(dir, 0700); e != nil {
		return failed(e)
	}
	spec, e := os.OpenFile(filepath.Join(dir, "run.json"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if e != nil {
		return failed(e)
	}
	defer spec.Close()
	if _, e = spec.Write(protocol.Canonical(body)); e != nil {
		return failed(e)
	}
	if _, e = spec.Seek(0, 0); e != nil {
		return failed(e)
	}
	stdout, e := newBoundedLog(filepath.Join(dir, "stdout.log"), r.opts.LogBytes)
	if e != nil {
		return failed(e)
	}
	stderr, e := newBoundedLog(filepath.Join(dir, "stderr.log"), r.opts.LogBytes)
	if e != nil {
		stdout.Close()
		return failed(e)
	}
	gateRead, gateWrite, e := os.Pipe()
	if e != nil {
		stdout.Close()
		stderr.Close()
		return failed(e)
	}
	defer gateRead.Close()
	defer gateWrite.Close()
	readyRead, readyWrite, e := os.Pipe()
	if e != nil {
		stdout.Close()
		stderr.Close()
		return failed(e)
	}
	defer readyRead.Close()
	defer readyWrite.Close()
	cmd := exec.Command(r.opts.HelperPath, r.opts.HelperArgs...)
	cmd.Dir = dir
	cmd.Env = []string{}
	cmd.ExtraFiles = []*os.File{gateRead, spec, readyWrite}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	pidfd := -1
	cmd.WaitDelay = 250 * time.Millisecond
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Pdeathsig: unix.SIGKILL, PidFD: &pidfd}
	live := &liveProcess{cmd: cmd, done: make(chan error, 1), stdout: stdout, stderr: stderr}
	started := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		e := cmd.Start()
		started <- e
		if e == nil {
			live.done <- cmd.Wait()
		}
	}()
	if e = <-started; e != nil {
		stdout.Close()
		stderr.Close()
		return failed(e)
	}
	live.pidfd = pidfd
	if pidfd < 0 {
		gateWrite.Close()
		select {
		case <-live.done:
		case <-time.After(time.Second):
			return errors.New("pidfd unsupported; helper exit unconfirmed")
		}
		stdout.Close()
		stderr.Close()
		return failed(errors.New("kernel pidfd required"))
	}
	if r.hooks.afterSpawn != nil {
		r.hooks.afterSpawn()
	}
	gateRead.Close()
	readyWrite.Close()
	gateAttempted := false
	abort := func(cause error) error {
		gateWrite.Close()
		signalErr := unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0)
		finished := false
		select {
		case <-live.done:
			finished = true
		case <-time.After(time.Second):
		}
		if !finished {
			r.live[key] = live
			return r.update(key, func(a *Attempt) error {
				a.Stopped = true
				a.Deadline = time.Now().UnixMilli()
				a.Execution.Phase = "terminal"
				a.Execution.Outcome = "lost"
				a.Execution.Cleanup = "unknown"
				if live.identity.PID > 0 {
					a.Execution.PID = live.identity.PID
					a.Execution.Start = live.identity.Start
				}
				return nil
			}, true)
		}
		stdout.Close()
		stderr.Close()
		unix.Close(pidfd)
		if gateAttempted || signalErr != nil && signalErr != unix.ESRCH {
			complete, err := groupEmpty(live.identity)
			if err != nil {
				complete = false
			}
			return r.update(key, func(a *Attempt) error {
				a.Execution.Phase = "terminal"
				a.Execution.Outcome = "failed"
				a.Execution.Cleanup = "unknown"
				if complete {
					a.Execution.Cleanup = "complete"
				}
				return nil
			}, true)
		}
		return failed(cause)
	}
	readyRead.SetReadDeadline(time.Now().Add(5 * time.Second))
	var ready [1]byte
	if _, e = io.ReadFull(readyRead, ready[:]); e != nil || ready[0] != 'R' {
		return abort(errors.New("helper readiness failed"))
	}
	info, e := processInfo(cmd.Process.Pid)
	if e != nil {
		return abort(e)
	}
	if info.Group != info.PID || info.Session != info.PID {
		return abort(errors.New("helper containment identity"))
	}
	live.identity = ProcessIdentity{info.PID, info.Start}
	if e = r.update(key, func(a *Attempt) error {
		a.Execution.Phase = "spawned"
		a.Execution.PID = info.PID
		a.Execution.Start = info.Start
		return nil
	}, false); e != nil {
		original := e
		_ = abort(e)
		return original
	}
	if r.hooks.afterIdentity != nil {
		r.hooks.afterIdentity()
	}
	if e = r.update(key, func(a *Attempt) error {
		if a.Stopped {
			return errors.New("Stop before launch gate")
		}
		a.Execution.Phase = "released"
		return nil
	}, false); e != nil {
		return abort(e)
	}
	if r.hooks.beforeGate != nil {
		r.hooks.beforeGate()
	}
	gateAttempted = true
	if _, e = gateWrite.Write([]byte{'G'}); e != nil {
		return abort(e)
	}
	gateWrite.Close()
	r.live[key] = live
	if r.hooks.afterRelease != nil {
		r.hooks.afterRelease()
	}
	return r.update(key, func(a *Attempt) error {
		a.Execution.Outcome = "running"
		a.Execution.Ready = p["readiness"].(map[string]any)["kind"] == "none"
		return nil
	}, true)
}

func (r *Runtime) poll(ctx context.Context, key string, a Attempt, p *liveProcess) error {
	if !p.finished {
		select {
		case p.waitErr = <-p.done:
			p.finished = true
		default:
		}
	}
	if a.Stopped && !p.finished {
		signal := unix.SIGTERM
		if time.Now().UnixMilli() >= a.Deadline {
			signal = unix.SIGKILL
		}
		if !p.termSent || signal == unix.SIGKILL {
			_, e := cleanupOwned(p.identity, signal)
			if e != nil {
				return e
			}
			p.termSent = true
		}
	}
	if p.finished {
		complete, e := groupEmpty(p.identity)
		if e != nil {
			complete = false
		}
		outcome := "succeeded"
		code := 0
		signal := 0
		if p.waitErr != nil {
			outcome = "failed"
			if ee, ok := p.waitErr.(*exec.ExitError); ok {
				code = ee.ExitCode()
				if status, ok := ee.Sys().(syscall.WaitStatus); ok && status.Signaled() {
					signal = int(status.Signal())
				}
			} else {
				code = -1
			}
		}
		if r.opts.captureEvents && p.cmd.ProcessState != nil {
			code = p.cmd.ProcessState.ExitCode()
			if status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				signal = int(status.Signal())
			}
		}
		if a.Stopped {
			outcome = "stopped"
		}
		if e := errors.Join(p.stdout.Close(), p.stderr.Close()); e != nil {
			return e
		}
		outBytes, outDropped := p.stdout.Counts()
		errBytes, errDropped := p.stderr.Counts()
		e = r.update(key, func(a *Attempt) error {
			x := a.Execution
			x.Phase = "terminal"
			x.Outcome = outcome
			x.Ready = false
			x.Cleanup = "unknown"
			if complete {
				x.Cleanup = "complete"
			}
			x.ExitCode = &code
			x.Signal = signal
			x.StdoutBytes = outBytes
			x.StderrBytes = errBytes
			x.StdoutDropped = outDropped
			x.StderrDropped = errDropped
			return nil
		}, true)
		if e != nil {
			return e
		}
		unix.Close(p.pidfd)
		delete(r.live, key)
		return nil
	}
	readiness := a.Body["assignment"].(map[string]any)["readiness"].(map[string]any)
	if readiness["kind"] == "tcp" {
		interval := time.Duration(readiness["intervalMillis"].(uint64)) * time.Millisecond
		if time.Since(p.lastProbe) >= interval {
			p.lastProbe = time.Now()
			port := uint64(0)
			for _, raw := range a.Body["assignment"].(map[string]any)["ports"].([]any) {
				v := raw.(map[string]any)
				if v["name"] == readiness["port"] {
					port = v["number"].(uint64)
				}
			}
			ready := ownsListener(p.identity, int(port))
			if ready {
				timeout := time.Duration(readiness["timeoutMillis"].(uint64)) * time.Millisecond
				if timeout > 250*time.Millisecond {
					timeout = 250 * time.Millisecond
				}
				probeCtx, cancel := context.WithTimeout(ctx, timeout)
				conn, e := (&net.Dialer{}).DialContext(probeCtx, "tcp4", fmt.Sprintf("127.0.0.1:%d", port))
				cancel()
				ready = e == nil
				if conn != nil {
					conn.Close()
				}
			}
			if ready != a.Execution.Ready {
				return r.update(key, func(a *Attempt) error {
					if !a.Stopped && a.Execution.Phase != "terminal" {
						a.Execution.Ready = ready
					}
					return nil
				}, true)
			}
		}
	}
	return nil
}

// Shutdown persists local stop intent without inventing scheduler command IDs.
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.shutdownMu.Lock()
	defer r.shutdownMu.Unlock()
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return nil
	}
	r.store.effectMu.Lock()
	r.store.runtimeDraining = true
	e := r.store.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		for key, a := range st.Attempts {
			if a.Body["kind"] != "Run" || (a.Execution != nil && a.Execution.Cleanup == "complete") {
				continue
			}
			deadline := time.Now().UnixMilli() + int64(a.Body["assignment"].(map[string]any)["stop"].(map[string]any)["graceMillis"].(uint64))
			if !a.Stopped || a.Deadline > deadline {
				a.Deadline = deadline
			}
			if r.opts.Supervise {
				mono := monoMillis() + int64(a.Body["assignment"].(map[string]any)["stop"].(map[string]any)["graceMillis"].(uint64))
				if a.DeadlineMono == 0 || mono < a.DeadlineMono {
					a.DeadlineMono = mono
				}
			}
			a.Stopped = true
			if a.Execution != nil {
				a.Execution.Ready = false
			}
			st.Attempts[key] = a
		}
		return save(b, st)
	})
	r.store.effectMu.Unlock()
	if e != nil {
		return e
	}
	for {
		if e = r.Tick(ctx); e != nil {
			return e
		}
		r.mu.Lock()
		active := len(r.live)
		r.mu.Unlock()
		if active == 0 {
			st, err := r.store.Inspect()
			if err != nil {
				return err
			}
			for _, a := range st.Attempts {
				if a.Reserved() {
					if r.opts.Supervise && a.Supervisor != nil && a.Execution.Phase != "terminal" {
						active++
						continue
					}
					return errors.New("cleanup remains unconfirmed")
				}
			}
			if active == 0 {
				return r.Close()
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func (r *Runtime) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	if len(r.live) != 0 {
		return errors.New("active processes require Shutdown")
	}
	r.closed = true
	r.store.effectMu.Lock()
	r.store.runtimeActive = false
	r.store.effectMu.Unlock()
	return nil
}

type boundedLog struct {
	mu                      sync.Mutex
	file                    *os.File
	limit, written, dropped int64
	closed                  bool
	closeErr                error
}

func newBoundedLog(path string, limit int64) (*boundedLog, error) {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW, 0600)
	if e != nil {
		return nil, e
	}
	return &boundedLog{file: f, limit: limit}, nil
}
func (l *boundedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := len(p)
	keep := int64(n)
	if keep > l.limit-l.written {
		keep = l.limit - l.written
	}
	if keep > 0 {
		w, e := l.file.Write(p[:keep])
		l.written += int64(w)
		if e != nil {
			return w, e
		}
	}
	l.dropped += int64(n) - keep
	return n, nil
}
func (l *boundedLog) Counts() (int64, int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.written, l.dropped
}
func (l *boundedLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return l.closeErr
	}
	l.closed = true
	if e := l.file.Sync(); e != nil {
		l.closeErr = errors.Join(e, l.file.Close())
		return l.closeErr
	}
	l.closeErr = l.file.Close()
	return l.closeErr
}
