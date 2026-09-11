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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"aurora.local/agent/protocol"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

const supervisorVersion = 1

// SupervisorRef is private, durable attachment identity. Imported advances in
// the same node transaction as the corresponding observation.
type SupervisorRef struct {
	Acknowledged bool
	Version      int
	Token        string
	Process      ProcessIdentity
	Imported     uint64
}
type ExecutionEvent struct {
	Sequence  uint64
	Execution Execution
}
type supervisorSpec struct {
	Version                               int
	Token, Key, Root, Network, HelperPath string
	HelperArgs                            []string
	LogBytes                              int64
	Config                                Config
	Body                                  map[string]any
	Scope                                 RuntimeScope
}
type supervisorRequest struct {
	Version  int
	Token    string
	Scope    RuntimeScope
	Deadline int64
	Mono     int64
	Stop     bool
	Imported uint64
}
type supervisorReply struct {
	Version int
	Token   string
	Events  []ExecutionEvent
	Attempt Attempt
	Error   string
}

func monoMillis() int64 {
	var t unix.Timespec
	if unix.ClockGettime(unix.CLOCK_BOOTTIME, &t) != nil {
		return 0
	}
	return t.Nano() / 1e6
}
func syncDir(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func socketAddress(root, key string) (*os.File, string, error) {
	f, e := os.Open(filepath.Dir(supervisorSocket(root, key)))
	if e != nil {
		return nil, "", e
	}
	return f, fmt.Sprintf("/proc/self/fd/%d/control.sock", f.Fd()), nil
}
func supervisorSocket(root, key string) string {
	return filepath.Join(root, ".supervisors", key, "control.sock")
}
func peer(conn *net.UnixConn, pid int) error {
	raw, e := conn.SyscallConn()
	if e != nil {
		return e
	}
	var inner error
	e = raw.Control(func(fd uintptr) {
		c, err := unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if err != nil {
			inner = err
			return
		}
		if c.Uid != uint32(os.Geteuid()) || (pid > 0 && int(c.Pid) != pid) {
			inner = errors.New("supervisor peer mismatch")
		}
	})
	if e != nil {
		return e
	}
	return inner
}
func (r *Runtime) startSupervisor(key string) error {
	r.store.effectMu.Lock()
	defer r.store.effectMu.Unlock()
	var a Attempt
	token := make([]byte, 32)
	if _, e := rand.Read(token); e != nil {
		return e
	}
	ref := &SupervisorRef{Version: supervisorVersion, Token: hex.EncodeToString(token)}
	if e := r.update(key, func(v *Attempt) error {
		a = *v
		if v.Stopped || v.Execution != nil {
			return errors.New("attempt no longer launchable")
		}
		v.Execution = &Execution{Phase: "intent", Outcome: "unknown", Cleanup: "pending"}
		v.Supervisor = ref
		return nil
	}, false); e != nil {
		return e
	}
	if r.hooks.afterIntent != nil {
		r.hooks.afterIntent()
	}
	dir := filepath.Dir(supervisorSocket(r.opts.Root, key))
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	for _, path := range []string{filepath.Dir(dir), dir} {
		fi, e := os.Lstat(path)
		if e != nil {
			return e
		}
		stat, ok := fi.Sys().(*syscall.Stat_t)
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 || fi.Mode().Perm()&0077 != 0 || !ok || stat.Uid != uint32(os.Geteuid()) {
			return errors.New("supervisor directory must be private, owned and non-symlink")
		}
	}
	if e := syncDir(filepath.Dir(dir)); e != nil {
		return e
	}
	if e := syncDir(r.opts.Root); e != nil {
		return e
	}
	spec := supervisorSpec{supervisorVersion, ref.Token, key, r.opts.Root, r.opts.Network, r.opts.HelperPath, r.opts.HelperArgs, r.opts.LogBytes, r.store.c, a.Body, r.scope}
	f, e := os.OpenFile(filepath.Join(dir, "spec.json"), os.O_CREATE|os.O_EXCL|os.O_RDWR|unix.O_NOFOLLOW, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	if e = json.NewEncoder(f).Encode(spec); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = syncDir(dir); e != nil {
		return e
	}
	if _, e = f.Seek(0, 0); e != nil {
		return e
	}
	gr, gw, e := os.Pipe()
	if e != nil {
		return e
	}
	defer gr.Close()
	defer gw.Close()
	rr, rw, e := os.Pipe()
	if e != nil {
		return e
	}
	defer rr.Close()
	defer rw.Close()
	args := r.opts.SupervisorArgs
	if len(args) == 0 {
		args = []string{"__supervise"}
	}
	cmd := exec.Command(r.opts.HelperPath, args...)
	cmd.Env = []string{}
	cmd.ExtraFiles = []*os.File{gr, f, rw}
	cmd.SysProcAttr = &unix.SysProcAttr{Setsid: true}
	if e = cmd.Start(); e != nil {
		return e
	}
	go cmd.Wait() // daemon reaps only the supervisor; never its workload.
	gr.Close()
	rw.Close()
	rr.SetReadDeadline(time.Now().Add(5 * time.Second))
	var ready [1]byte
	if _, e = io.ReadFull(rr, ready[:]); e != nil || ready[0] != 'R' {
		return errors.New("supervisor readiness failed")
	}
	info, e := processInfo(cmd.Process.Pid)
	if e != nil {
		return e
	}
	ref.Process = info.ProcessIdentity
	if e = r.update(key, func(v *Attempt) error { v.Supervisor = ref; return nil }, false); e != nil {
		return e
	}
	if r.hooks.afterIdentity != nil {
		r.hooks.afterIdentity()
	}
	if _, e = gw.Write([]byte{'G'}); e != nil {
		return e
	}
	gw.Close()
	if r.hooks.afterRelease != nil {
		r.hooks.afterRelease()
	}
	return nil
}

// SuperviseHelper is an internal inherited-descriptor-only entry point. The
// isolated Runtime is the execution-engine boundary for a future process planner.
func SuperviseHelper() error {
	gate := os.NewFile(3, "supervisor-gate")
	specFile := os.NewFile(4, "supervisor-spec")
	ready := os.NewFile(5, "supervisor-ready")
	defer gate.Close()
	defer specFile.Close()
	defer ready.Close()
	var spec supervisorSpec
	d := json.NewDecoder(io.LimitReader(specFile, 2<<20))
	d.DisallowUnknownFields()
	if e := d.Decode(&spec); e != nil {
		return e
	}
	if spec.Version != supervisorVersion || len(spec.Token) != 64 {
		return errors.New("unsupported supervisor spec")
	}
	body, e := protocol.Validate(protocol.Canonical(spec.Body))
	if e != nil {
		return e
	}
	spec.Body = body
	if body["kind"] != "Run" || attemptKey(body["identity"].(map[string]any)) != spec.Key {
		return errors.New("immutable supervisor identity mismatch")
	}
	scope, e := observedScope()
	if e != nil {
		return e
	}
	if scope != spec.Scope {
		return errors.New("supervisor scope mismatch")
	}
	dir := filepath.Dir(supervisorSocket(spec.Root, spec.Key))
	s, e := Open(filepath.Join(dir, "journal.db"), spec.Config)
	if e != nil {
		return e
	}
	defer s.Close()
	// A supervisor is never replay-started. A pre-existing journal is evidence of
	// consumed ownership and requires daemon recovery, not another execution.
	st, e := s.Inspect()
	if e != nil {
		return e
	}
	if len(st.Attempts) != 0 {
		return errors.New("supervisor replay refused")
	}
	e = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		st.Attempts[spec.Key] = Attempt{Body: body}
		return save(b, st)
	})
	if e != nil {
		return e
	}
	rt, e := NewRuntime(s, RuntimeOptions{Root: spec.Root, Network: spec.Network, HelperPath: spec.HelperPath, HelperArgs: spec.HelperArgs, LogBytes: spec.LogBytes, captureEvents: true})
	if e != nil {
		return e
	}
	socketDir, address, e := socketAddress(spec.Root, spec.Key)
	if e != nil {
		return e
	}
	ln, e := net.ListenUnix("unix", &net.UnixAddr{Name: address, Net: "unix"})
	socketDir.Close()
	if e != nil {
		return e
	}
	ln.SetUnlinkOnClose(false)
	defer os.Remove(supervisorSocket(spec.Root, spec.Key))
	defer ln.Close()
	if _, e = ready.Write([]byte{'R'}); e != nil {
		return e
	}
	ready.Close()
	var g [1]byte
	if _, e = io.ReadFull(gate, g[:]); e != nil || g[0] != 'G' {
		return errors.New("supervisor gate not released")
	}
	gate.Close()
	requests := make(chan *net.UnixConn)
	go func() {
		for {
			c, e := ln.AcceptUnix()
			if e != nil {
				return
			}
			requests <- c
		}
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case c := <-requests:
			c.SetDeadline(time.Now().Add(time.Second))
			if peer(c, 0) != nil {
				c.Close()
				continue
			}
			var q supervisorRequest
			decoder := json.NewDecoder(io.LimitReader(c, 4096))
			decoder.DisallowUnknownFields()
			err := decoder.Decode(&q)
			reply := supervisorReply{Version: supervisorVersion, Token: spec.Token}
			if err == nil && (q.Version != supervisorVersion || q.Token != spec.Token || q.Scope != scope) {
				err = errors.New("supervisor protocol/identity mismatch")
			}
			if err == nil && q.Stop {
				err = s.db.Update(func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte("state"))
					v, e := read(b)
					if e != nil {
						return e
					}
					a := v.Attempts[spec.Key]
					deadline := q.Deadline
					remaining := deadline - time.Now().UnixMilli()
					if remaining < 0 {
						remaining = 0
					}
					mono := monoMillis() + remaining
					if q.Mono > 0 {
						mono = q.Mono
					}
					if !a.Stopped || deadline < a.Deadline {
						a.Deadline = deadline
					}
					if v.StopMono == 0 || mono < v.StopMono {
						v.StopMono = mono
					}
					a.Stopped = true
					v.Attempts[spec.Key] = a
					return save(b, v)
				})
			}
			if err == nil {
				var state State
				state, err = s.Inspect()
				if err == nil {
					reply.Attempt = state.Attempts[spec.Key]
					for _, event := range state.ExecutionEvents {
						if event.Sequence > q.Imported {
							reply.Events = append(reply.Events, event)
							if len(reply.Events) >= 256 {
								break
							}
						}
					}
				}
			}
			if err != nil {
				reply.Error = err.Error()
			}
			json.NewEncoder(c).Encode(reply)
			c.Close()
			if err == nil && reply.Attempt.Execution != nil && reply.Attempt.Execution.Cleanup == "complete" && len(reply.Events) == 0 {
				return nil
			}
		case <-ticker.C:
			state, err := s.Inspect()
			if err != nil {
				return err
			}
			if state.StopMono > 0 && monoMillis() >= state.StopMono {
				err = s.db.Update(func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte("state"))
					v, e := read(b)
					if e != nil {
						return e
					}
					a := v.Attempts[spec.Key]
					if a.Deadline > time.Now().UnixMilli() {
						a.Deadline = time.Now().UnixMilli()
						v.Attempts[spec.Key] = a
					}
					return save(b, v)
				})
				if err != nil {
					return err
				}
			}
			if err = rt.Tick(context.Background()); err != nil {
				return err
			}
		}
	}
}
func (r *Runtime) pollSupervisor(key string, a Attempt) error {
	ref := a.Supervisor
	if ref.Version != supervisorVersion {
		return errors.New("unsupported supervisor version")
	}
	socketDir, address, e := socketAddress(r.opts.Root, key)
	if e != nil {
		return r.supervisorLost(key, a)
	}
	c, e := net.DialUnix("unix", nil, &net.UnixAddr{Name: address, Net: "unix"})
	socketDir.Close()
	if e != nil {
		return r.supervisorLost(key, a)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	if e = peer(c, ref.Process.PID); e != nil {
		return e
	}
	p, e := processInfo(ref.Process.PID)
	if e != nil || p.Start != ref.Process.Start {
		return errors.New("supervisor process identity mismatch")
	}
	q := supervisorRequest{supervisorVersion, ref.Token, r.scope, a.Deadline, a.DeadlineMono, a.Stopped, ref.Imported}
	if e = json.NewEncoder(c).Encode(q); e != nil {
		return e
	}
	var reply supervisorReply
	d := json.NewDecoder(io.LimitReader(c, 16<<20))
	d.DisallowUnknownFields()
	if e = d.Decode(&reply); e != nil {
		return e
	}
	if reply.Error != "" {
		return errors.New(reply.Error)
	}
	if reply.Version != supervisorVersion || reply.Token != ref.Token {
		return errors.New("supervisor reply identity mismatch")
	}
	if protocol.Digest(reply.Attempt.Body) != protocol.Digest(a.Body) {
		return errors.New("supervisor immutable spec mismatch")
	}
	for _, event := range reply.Events {
		if e = validateExecution(&event.Execution); e != nil {
			return e
		}
		e = r.update(key, func(v *Attempt) error {
			if event.Sequence != v.Supervisor.Imported+1 {
				return errors.New("supervisor event gap")
			}
			v.Supervisor.Imported = event.Sequence
			x := event.Execution
			v.Execution = &x
			return nil
		}, true)
		if e != nil {
			return e
		}
	}
	if len(reply.Events) == 0 && a.Execution.Cleanup == "complete" {
		return r.update(key, func(v *Attempt) error { v.Supervisor.Acknowledged = true; return nil }, false)
	}
	if len(reply.Events) > 0 && reply.Events[len(reply.Events)-1].Execution.Cleanup == "complete" {
		if r.hooks.beforeSupervisorAck != nil {
			r.hooks.beforeSupervisorAck()
		}
		st, e := r.store.Inspect()
		if e != nil {
			return e
		}
		return r.pollSupervisor(key, st.Attempts[key])
	}
	return nil
}
func (r *Runtime) supervisorLost(key string, a Attempt) error {
	quarantine := func(cause error) error {
		e := r.update(key, func(v *Attempt) error {
			v.Execution.Phase = "terminal"
			v.Execution.Outcome = "lost"
			v.Execution.Cleanup = "unknown"
			v.Execution.Ready = false
			v.Execution.ExitCode = nil
			v.Execution.Signal = 0
			return nil
		}, true)
		return errors.Join(cause, e)
	}
	if a.Supervisor.Process.PID > 0 {
		p, e := processInfo(a.Supervisor.Process.PID)
		if e == nil && p.Start == a.Supervisor.Process.Start && p.State != "Z" && p.State != "X" {
			return errors.New("live supervisor unavailable; admission blocked")
		}
		if e != nil && !os.IsNotExist(e) {
			return e
		}
	}
	if a.Execution.Cleanup == "complete" {
		return r.update(key, func(v *Attempt) error { v.Supervisor.Acknowledged = true; return nil }, false)
	}
	// A dead supervisor's journal supplies the last durable child identity; it
	// cannot supply an exit that was never durably captured.
	path := filepath.Join(filepath.Dir(supervisorSocket(r.opts.Root, key)), "journal.db")
	if _, err := os.Stat(path); err != nil && a.Supervisor.Process.PID > 0 {
		return quarantine(fmt.Errorf("supervisor journal unavailable; cleanup unconfirmed: %w", err))
	}
	if _, e := os.Stat(path); e == nil {
		db, e := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true, Timeout: 100 * time.Millisecond})
		if e != nil {
			return e
		}
		var st State
		e = db.View(func(tx *bolt.Tx) error { var err error; st, err = read(tx.Bucket([]byte("state"))); return err })
		db.Close()
		if e != nil {
			return quarantine(e)
		}
		if st.RuntimeScope == nil || *st.RuntimeScope != r.scope {
			return quarantine(errors.New("supervisor journal scope mismatch"))
		}
		local, ok := st.Attempts[key]
		if !ok || protocol.Digest(local.Body) != protocol.Digest(a.Body) {
			return quarantine(errors.New("supervisor journal immutable spec mismatch"))
		}
		if local.Execution != nil {
			a.Execution = local.Execution
		}
		for _, event := range st.ExecutionEvents {
			if event.Sequence > a.Supervisor.Imported {
				e = r.update(key, func(v *Attempt) error {
					if event.Sequence != v.Supervisor.Imported+1 {
						return errors.New("supervisor event gap")
					}
					v.Supervisor.Imported = event.Sequence
					x := event.Execution
					v.Execution = &x
					return nil
				}, true)
				if e != nil {
					return e
				}
				a.Supervisor.Imported = event.Sequence
			}
		}
		if a.Execution.Cleanup == "complete" {
			return nil
		}
	}
	complete := a.Execution.PID == 0 && a.Supervisor.Process.PID == 0
	if a.Execution.PID > 0 {
		var e error
		complete, e = cleanupOwned(ProcessIdentity{a.Execution.PID, a.Execution.Start}, unix.SIGKILL)
		if e != nil {
			complete = false
		}
	}
	if a.Execution.Phase == "terminal" && a.Execution.Outcome == "lost" && ((complete && a.Execution.Cleanup == "complete") || (!complete && a.Execution.Cleanup == "unknown")) {
		return nil
	}
	return r.update(key, func(v *Attempt) error {
		x := *a.Execution
		knownExit := x.Phase == "terminal" && x.ExitCode != nil
		x.Phase = "terminal"
		if !knownExit {
			x.Outcome = "lost"
		}
		x.Ready = false
		x.Cleanup = "unknown"
		if !knownExit {
			x.ExitCode = nil
			x.Signal = 0
		}
		if complete {
			x.Cleanup = "complete"
		}
		v.Execution = &x
		return nil
	}, true)
}
