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

package task

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

var executing atomic.Bool

type Options struct {
	StateDir, HelperPath string
	HelperArgs           []string
}
type Result struct {
	PrimaryResult      string                   `json:"primaryResult"`
	FinalizationResult string                   `json:"finalizationResult"`
	Cleanup            string                   `json:"cleanup"`
	States             map[string]*ProcessState `json:"processes"`
}
type Event struct {
	WaitError      string  `json:"waitError,omitempty"`
	LogError       string  `json:"logError,omitempty"`
	Kind           string  `json:"kind"`
	Name           string  `json:"name,omitempty"`
	Run            int     `json:"run,omitempty"`
	PID            int     `json:"pid,omitempty"`
	Start          string  `json:"start,omitempty"`
	Outcome        string  `json:"outcome,omitempty"`
	ExitCode       *int    `json:"exitCode,omitempty"`
	Signal         int     `json:"signal,omitempty"`
	StdoutDropped  int64   `json:"stdoutDropped,omitempty"`
	StderrDropped  int64   `json:"stderrDropped,omitempty"`
	ManifestDigest string  `json:"manifestDigest,omitempty"`
	Result         *Result `json:"result,omitempty"`
}
type uncertainLaunch struct{ error }

type journal struct{ f *os.File }

func (j *journal) append(v Event) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if _, e = j.f.Write(append(b, '\n')); e != nil {
		return e
	}
	return j.f.Sync()
}

type logSink struct {
	f                       *os.File
	limit, written, dropped int64
}

func (l *logSink) Write(p []byte) (int, error) {
	n := len(p)
	keep := int64(n)
	if keep > l.limit-l.written {
		keep = l.limit - l.written
	}
	if keep > 0 {
		w, e := l.f.Write(p[:keep])
		l.written += int64(w)
		if e != nil {
			return w, e
		}
	}
	l.dropped += int64(n) - keep
	return n, nil
}

type child struct {
	cmd      *exec.Cmd
	fd       int
	start    string
	done     chan error
	out, err *logSink
	name     string
	run      int
}

// Execute consumes a private state directory exactly once. Any existing journal,
// including a torn initial record, prevents replay and relaunch. The surviving
// outer supervisor owns recovery; a dead task runner cannot adopt unknown children.
func Execute(ctx context.Context, m Manifest, o Options) (res Result, err error) {
	if !executing.CompareAndSwap(false, true) {
		return res, errors.New("Execute requires a dedicated process")
	}
	defer executing.Store(false)
	if err = m.Normalize(); err != nil {
		return
	}
	if !filepath.IsAbs(o.StateDir) || filepath.Clean(o.StateDir) != o.StateDir {
		return res, errors.New("absolute canonical task state dir required")
	}
	if o.HelperPath == "" {
		o.HelperPath, err = os.Executable()
		if err != nil {
			return
		}
	}
	if len(o.HelperArgs) == 0 {
		o.HelperArgs = []string{"task-child"}
	}
	for current := o.StateDir; current != "/"; current = filepath.Dir(current) {
		if fi, e := os.Lstat(current); e == nil {
			if fi.Mode()&os.ModeSymlink != 0 {
				return res, errors.New("task state directory symlink ancestor")
			}
		} else if !os.IsNotExist(e) {
			return res, e
		}
	}
	if err = os.Mkdir(o.StateDir, 0700); err != nil && !os.IsExist(err) {
		return
	}
	err = nil
	parent, e := os.Open(filepath.Dir(o.StateDir))
	if e != nil {
		return res, e
	}
	e = parent.Sync()
	parent.Close()
	if e != nil {
		return res, e
	}
	real, e := filepath.EvalSymlinks(o.StateDir)
	if e != nil || real != o.StateDir {
		return res, errors.New("task state directory symlink")
	}
	fi, e := os.Stat(o.StateDir)
	if e != nil {
		return res, e
	}
	if fi.Mode().Perm()&0077 != 0 || fi.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return res, errors.New("task state directory must be private and owned")
	}
	f, e := os.OpenFile(filepath.Join(o.StateDir, "task.journal"), os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW, 0600)
	if e != nil {
		return res, fmt.Errorf("task journal already exists or unavailable; never relaunch: %w", e)
	}
	defer f.Close()
	j := &journal{f}
	dir, e := os.Open(o.StateDir)
	if e != nil {
		return res, e
	}
	e = dir.Sync()
	dir.Close()
	if e != nil {
		return res, e
	}
	b, _ := json.Marshal(m)
	digest := sha256.Sum256(b)
	if e = j.append(Event{Kind: "manifest", ManifestDigest: hex.EncodeToString(digest[:])}); e != nil {
		return res, e
	}
	// Execute is a dedicated process entry point. Subreaping makes descendants
	// left by an exited root observable before any retry can be admitted.
	if e = unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); e != nil {
		return res, e
	}
	p := NewPlanner(m)
	res.States = p.States
	res.Cleanup = "unknown"
	live := map[string]*child{}
	defer func() {
		until := time.Now().Add(time.Second)
		for _, c := range live {
			_ = unix.PidfdSendSignal(c.fd, unix.SIGKILL, nil, 0)
		}
		for _, c := range live {
			select {
			case <-c.done:
			case <-time.After(max(time.Duration(0), time.Until(until))):
			}
			unix.Close(c.fd)
			c.out.f.Close()
			c.err.f.Close()
		}
	}()
	final := false
	hasFinalizers, ranFinalizers := false, false
	for _, q := range m.Processes {
		if q.Finalizer {
			hasFinalizers = true
		}
	}
	var deadline time.Time
	timer := time.NewTicker(10 * time.Millisecond)
	defer timer.Stop()
	for {
		now := time.Now()
		if !final {
			r := p.Result(false)
			if ctx.Err() != nil {
				r = "stopped"
			}
			if r != "" {
				res.PrimaryResult = r
				final = true
				deadline = now.Add(time.Duration(m.FinalizationWaitMillis) * time.Millisecond)
				if stop, ok := ctx.Deadline(); ok && stop.Before(deadline) {
					deadline = stop
				}
				if e = j.append(Event{Kind: "cleaning", Result: &res}); e != nil {
					return res, e
				}
				for _, c := range live {
					if e = unix.PidfdSendSignal(c.fd, unix.SIGKILL, nil, 0); e != nil && e != unix.ESRCH {
						return res, e
					}
				}
			}
		}
		for n, c := range live {
			select {
			case waitErr := <-c.done:
				code, signal, outcome := exactOutcome(c.cmd.ProcessState)
				waitDetail := ""
				if waitErr != nil {
					waitDetail = waitErr.Error()
				}

				closeErr := errors.Join(c.out.f.Sync(), c.err.f.Sync(), c.out.f.Close(), c.err.f.Close())
				unix.Close(c.fd)
				delete(live, n)
				logDetail := ""
				if closeErr != nil {
					logDetail = closeErr.Error()
				}
				p.Exit(n, outcome, now)
				if e = j.append(Event{Kind: "exit", WaitError: waitDetail, LogError: logDetail, Name: n, Run: c.run, PID: c.cmd.Process.Pid, Start: c.start, Outcome: outcome, ExitCode: &code, Signal: signal, StdoutDropped: c.out.dropped, StderrDropped: c.err.dropped}); e != nil {
					return res, e
				}
				if closeErr != nil {
					return res, closeErr
				}
				var exitErr *exec.ExitError
				if outcome == "lost" || waitErr != nil && !errors.As(waitErr, &exitErr) && !errors.Is(waitErr, exec.ErrWaitDelay) {
					return res, errors.New("child wait or log copy uncertain; exact available exit journaled")
				}

			default:
			}
		}
		orphans, orphanErr := cleanupOrphans(live)
		if orphanErr != nil {
			return res, orphanErr
		}
		if orphans && !final {
			res.PrimaryResult = "failed"
			final = true
			deadline = now.Add(time.Duration(m.FinalizationWaitMillis) * time.Millisecond)
			if stop, ok := ctx.Deadline(); ok && stop.Before(deadline) {
				deadline = stop
			}
			if e = j.append(Event{Kind: "orphan-cleaning", Result: &res}); e != nil {
				return res, e
			}
			for _, c := range live {
				_ = unix.PidfdSendSignal(c.fd, unix.SIGKILL, nil, 0)
			}
		}
		if orphans {
			if final && now.After(deadline.Add(time.Second)) {
				return res, errors.New("orphan cleanup unconfirmed")
			}
			<-timer.C
			continue
		}
		if final && len(live) == 0 {
			if !hasFinalizers {
				res.FinalizationResult = "skipped"
				break
			}
			if !now.Before(deadline) {
				res.FinalizationResult = "timeout"
				if !ranFinalizers {
					res.FinalizationResult = "skipped"
				}
				break
			}
			if r := p.Result(true); r != "" {
				res.FinalizationResult = r
				break
			}
		}
		if final && !now.Before(deadline) {
			res.FinalizationResult = "timeout"
			for _, c := range live {
				_ = unix.PidfdSendSignal(c.fd, unix.SIGKILL, nil, 0)
			}
			if len(live) == 0 {
				break
			}
			if now.After(deadline.Add(time.Second)) {
				return res, errors.New("cleanup wait unconfirmed")
			}
			<-timer.C
			continue
		}
		// Cleaning must finish before any finalizer is admitted.
		cleaning := false
		if final {
			for n := range live {
				for _, q := range m.Processes {
					if q.Name == n && !q.Finalizer {
						cleaning = true
					}
				}
			}
		}
		if !cleaning {
			for _, n := range p.Runnable(now, final) {
				if final && !time.Now().Before(deadline) || !final && ctx.Err() != nil {
					break
				}
				var q Process
				for _, x := range m.Processes {
					if x.Name == n {
						q = x
						break
					}
				}
				if final {
					ranFinalizers = true
				}
				p.Start(n)
				if e = j.append(Event{Kind: "intent", Name: n, Run: p.States[n].Runs}); e != nil {
					return res, e
				}
				cutoff := deadline
				if !final {
					cutoff, _ = ctx.Deadline()
				}
				c, e := launch(q, p.States[n].Runs, m, o, j, cutoff)
				if e != nil {
					var uncertain uncertainLaunch
					if errors.As(e, &uncertain) {
						return res, e
					}
					p.Exit(n, "failed", time.Now())
					if e = j.append(Event{Kind: "start-failed", Name: n, Run: p.States[n].Runs, Outcome: "failed"}); e != nil {
						return res, e
					}
					continue
				}
				live[n] = c
			}
		}
		<-timer.C
	}
	res.Cleanup = "complete"
	if e = j.append(Event{Kind: "terminal", Result: &res}); e != nil {
		return res, e
	}
	return res, nil
}

func launch(p Process, run int, m Manifest, o Options, j *journal, cutoff time.Time) (*child, error) {
	c := &child{name: p.Name, run: run, fd: -1, done: make(chan error, 1)}
	makeLog := func(stream string) (*logSink, error) {
		f, e := os.OpenFile(filepath.Join(o.StateDir, fmt.Sprintf("%s.%d.%s", p.Name, run, stream)), os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW, 0600)
		return &logSink{f: f, limit: m.LogBytes}, e
	}
	var e error
	c.out, e = makeLog("stdout")
	if e != nil {
		return nil, e
	}
	c.err, e = makeLog("stderr")
	if e != nil {
		c.out.f.Close()
		return nil, e
	}
	success := false
	defer func() {
		if !success {
			c.out.f.Close()
			c.err.f.Close()
		}
	}()
	gr, gw, e := os.Pipe()
	if e != nil {
		return nil, e
	}
	defer gr.Close()
	defer gw.Close()
	sr, sw, e := os.Pipe()
	if e != nil {
		return nil, e
	}
	defer sr.Close()
	defer sw.Close()
	rr, rw, e := os.Pipe()
	if e != nil {
		return nil, e
	}
	defer rr.Close()
	defer rw.Close()
	cmd := exec.Command(o.HelperPath, o.HelperArgs...)
	c.cmd = cmd
	cmd.Env = []string{}
	cmd.Dir = o.StateDir
	cmd.Stdout = c.out
	cmd.Stderr = c.err
	cmd.ExtraFiles = []*os.File{gr, sr, rw}
	cmd.WaitDelay = 100 * time.Millisecond
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL, PidFD: &c.fd}
	started := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		e := cmd.Start()
		started <- e
		if e == nil {
			c.done <- cmd.Wait()
		}
	}()
	if e = <-started; e != nil {
		return nil, e
	}
	abort := func(e error) (*child, error) {
		gw.Close()
		if c.fd >= 0 {
			_ = unix.PidfdSendSignal(c.fd, unix.SIGKILL, nil, 0)
		}
		select {
		case <-c.done:
		case <-time.After(time.Second):
			e = uncertainLaunch{errors.Join(e, errors.New("helper wait unconfirmed"))}
		}
		if c.fd >= 0 {
			unix.Close(c.fd)
		}
		return nil, e
	}
	if c.fd < 0 {
		return abort(errors.New("pidfd unavailable"))
	}
	gr.Close()
	sr.Close()
	rw.Close()
	data, _ := json.Marshal(p)
	handshakeDeadline := time.Now().Add(time.Second)
	if !cutoff.IsZero() && cutoff.Before(handshakeDeadline) {
		handshakeDeadline = cutoff
	}
	sw.SetWriteDeadline(handshakeDeadline)
	if _, e = sw.Write(data); e != nil {
		return abort(e)
	}
	sw.Close()
	rr.SetReadDeadline(handshakeDeadline)
	var token [1]byte
	if _, e = io.ReadFull(rr, token[:]); e != nil || token[0] != 'R' {
		return abort(errors.New("task helper readiness failed"))
	}
	c.start, e = processStart(cmd.Process.Pid)
	if e != nil {
		return abort(e)
	}
	if e = j.append(Event{Kind: "released", Name: p.Name, Run: run, PID: cmd.Process.Pid, Start: c.start}); e != nil {
		return abort(uncertainLaunch{e})
	}
	if !cutoff.IsZero() && !time.Now().Before(cutoff) {
		return abort(errors.New("launch deadline exhausted"))
	}
	if _, e = gw.Write([]byte{'G'}); e != nil {
		return abort(uncertainLaunch{e})
	}
	success = true
	return c, nil
}
func processStart(pid int) (string, error) {
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if e != nil {
		return "", e
	}
	s := string(b)
	i := strings.LastIndex(s, ") ")
	if i < 0 {
		return "", errors.New("proc stat malformed")
	}
	f := strings.Fields(s[i+2:])
	if len(f) < 20 {
		return "", errors.New("proc stat short")
	}
	if _, e = strconv.ParseUint(f[19], 10, 64); e != nil {
		return "", e
	}
	return f[19], nil
}

// ChildHelper is an internal same-binary boundary authorized by inherited fds:
// 3 launch gate, 4 process JSON, 5 readiness. It never sources a shell/profile.
func ChildHelper() error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	parent := os.Getppid()
	if parent <= 1 {
		return errors.New("orphan helper")
	}
	if e := unix.Prctl(unix.PR_SET_PDEATHSIG, uintptr(unix.SIGKILL), 0, 0, 0); e != nil {
		return e
	}
	if os.Getppid() != parent {
		return errors.New("parent changed")
	}
	if e := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); e != nil {
		return e
	}
	gate, spec, ready := os.NewFile(3, "gate"), os.NewFile(4, "process"), os.NewFile(5, "ready")
	defer gate.Close()
	defer spec.Close()
	defer ready.Close()
	data, e := io.ReadAll(io.LimitReader(spec, 1048577))
	if e != nil || len(data) > 1048576 {
		return errors.New("invalid helper spec size")
	}
	var p Process
	d := json.NewDecoder(strings.NewReader(string(data)))
	d.DisallowUnknownFields()
	if e = d.Decode(&p); e != nil {
		return e
	}
	if len(p.Argv) == 0 || !filepath.IsAbs(p.Argv[0]) {
		return errors.New("invalid argv")
	}
	fi, e := os.Stat(p.Argv[0])
	if e != nil {
		return e
	}
	if !fi.Mode().IsRegular() || fi.Mode()&(os.ModeSetuid|os.ModeSetgid) != 0 {
		return errors.New("executable must be regular non-setid")
	}
	if _, e = unix.Getxattr(p.Argv[0], "security.capability", nil); e == nil {
		return errors.New("file capabilities unsupported")
	} else if e != unix.ENODATA && e != unix.ENOTSUP {
		return e
	}
	keys := make([]string, 0, len(p.Env))
	for k := range p.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	env := []string{}
	for _, k := range keys {
		env = append(env, k+"="+p.Env[k])
	}
	if _, e = ready.Write([]byte{'R'}); e != nil {
		return e
	}
	ready.Close()
	var token [1]byte
	if _, e = io.ReadFull(gate, token[:]); e != nil {
		return e
	}
	if token[0] != 'G' || os.Getppid() != parent {
		return errors.New("invalid launch authority")
	}
	gate.Close()
	spec.Close()
	return syscall.Exec(p.Argv[0], p.Argv, env)
}

// cleanupOrphans pins adopted descendants before signaling and reaps only those
// exact PIDs. Original children are exclusively reaped by their exec.Cmd.Wait.
func cleanupOrphans(live map[string]*child) (bool, error) {
	roots := map[int]bool{}
	for _, c := range live {
		roots[c.cmd.Process.Pid] = true
	}
	entries, e := os.ReadDir("/proc")
	if e != nil {
		return false, e
	}
	found := false
	for _, entry := range entries {
		pid, e := strconv.Atoi(entry.Name())
		if e != nil || roots[pid] {
			continue
		}
		b, e := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return false, e
		}
		s := string(b)
		i := strings.LastIndex(s, ") ")
		if i < 0 {
			continue
		}
		f := strings.Fields(s[i+2:])
		if len(f) < 20 {
			continue
		}
		ppid, e := strconv.Atoi(f[1])
		if e != nil || ppid != os.Getpid() {
			continue
		}
		found = true
		fd, e := unix.PidfdOpen(pid, 0)
		if e == unix.ESRCH {
			continue
		}
		if e != nil {
			return true, e
		}
		start, e := processStart(pid)
		if e != nil {
			unix.Close(fd)
			if os.IsNotExist(e) {
				continue
			}
			return true, e
		}
		if start != f[19] {
			unix.Close(fd)
			return true, errors.New("orphan identity changed")
		}
		e = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
		unix.Close(fd)
		if e != nil && e != unix.ESRCH {
			return true, e
		}
		var status unix.WaitStatus
		_, e = unix.Wait4(pid, &status, unix.WNOHANG, nil)
		if e != nil && e != unix.ECHILD {
			return true, e
		}
	}
	return found, nil
}

func exactOutcome(state *os.ProcessState) (code, signal int, outcome string) {
	code, signal, outcome = -1, 0, "lost"
	if state == nil {
		return
	}
	code = state.ExitCode()
	outcome = "failed"
	if state.Success() {
		outcome = "succeeded"
	}
	if ws, ok := state.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		signal = int(ws.Signal())
	}
	return
}
