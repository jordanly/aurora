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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"aurora.local/agent/protocol"
	"golang.org/x/sys/unix"
)

// LaunchHelper is an internal same-binary entry point. Only inherited private
// descriptors authorize execution: fd3 gate, fd4 immutable Run, fd5 readiness.
// The helper never reads ambient configuration or inherits the agent environment.
func LaunchHelper() error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	parent := os.Getppid()
	if parent <= 1 {
		return errors.New("orphan launch helper")
	}
	if e := unix.Prctl(unix.PR_SET_PDEATHSIG, uintptr(unix.SIGKILL), 0, 0, 0); e != nil {
		return e
	}
	if os.Getppid() != parent {
		return errors.New("launch parent changed")
	}
	if e := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); e != nil {
		return e
	}
	gate := os.NewFile(3, "launch-gate")
	spec := os.NewFile(4, "immutable-run")
	ready := os.NewFile(5, "helper-ready")
	defer gate.Close()
	defer spec.Close()
	defer ready.Close()
	data, e := io.ReadAll(io.LimitReader(spec, 1048577))
	if e != nil {
		return e
	}
	if len(data) > 1048576 {
		return errors.New("launch spec limit")
	}
	body, e := protocol.Validate(data)
	if e != nil {
		return e
	}
	if body["kind"] != "Run" {
		return errors.New("helper requires Run")
	}
	p := body["assignment"].(map[string]any)
	argv := make([]string, len(p["argv"].([]any)))
	for i, a := range p["argv"].([]any) {
		argv[i] = a.(string)
	}
	fi, e := os.Stat(argv[0])
	if e != nil {
		return e
	}
	if !fi.Mode().IsRegular() || fi.Mode()&(os.ModeSetuid|os.ModeSetgid) != 0 {
		return errors.New("trusted executable must be regular and non-setid")
	}
	if _, e := unix.Getxattr(argv[0], "security.capability", nil); e == nil {
		return errors.New("file-capability executable unsupported")
	} else if e != unix.ENODATA && e != unix.ENOTSUP {
		return e
	}
	envMap := p["env"].(map[string]any)
	keys := make([]string, 0, len(envMap))
	for k := range envMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	env := make([]string, 0, len(keys))
	for _, k := range keys {
		env = append(env, k+"="+envMap[k].(string))
	}
	if _, e = ready.Write([]byte{'R'}); e != nil {
		return e
	}
	ready.Close()
	var token [1]byte
	if _, e = io.ReadFull(gate, token[:]); e != nil {
		return e
	}
	if token[0] != 'G' {
		return errors.New("invalid launch gate")
	}
	gate.Close()
	spec.Close()
	if os.Getppid() != parent {
		return errors.New("launch parent lost")
	}
	return syscall.Exec(argv[0], argv, env)
}

type procInfo struct {
	ProcessIdentity
	Group, Session int
	State          string
}

func processInfo(pid int) (procInfo, error) {
	var p procInfo
	p.PID = pid
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if e != nil {
		// procfs may report ESRCH, not ENOENT, when the task exits between
		// opening stat and reading it. Both mean this process is gone.
		if errors.Is(e, unix.ESRCH) {
			return p, os.ErrNotExist
		}
		return p, e
	}
	s := string(b)
	end := strings.LastIndex(s, ") ")
	if end < 0 {
		return p, errors.New("malformed proc stat")
	}
	fields := strings.Fields(s[end+2:])
	if len(fields) < 20 {
		return p, errors.New("short proc stat")
	}
	p.State = fields[0]
	p.Group, e = strconv.Atoi(fields[2])
	if e != nil {
		return p, e
	}
	p.Session, e = strconv.Atoi(fields[3])
	if e != nil {
		return p, e
	}
	p.Start = fields[19]
	if _, e = strconv.ParseUint(p.Start, 10, 64); e != nil {
		return p, e
	}
	return p, nil
}
func groupMembers(id ProcessIdentity) ([]procInfo, error) {
	entries, e := os.ReadDir("/proc")
	if e != nil {
		return nil, e
	}
	var found []procInfo
	for _, entry := range entries {
		pid, e := strconv.Atoi(entry.Name())
		if e != nil {
			continue
		}
		p, e := processInfo(pid)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return nil, e
		}
		if p.Session == id.PID || p.Group == id.PID {
			if p.Session != id.PID || p.Group != id.PID {
				return nil, errors.New("owned process changed group/session")
			}
			if p.State != "Z" && p.State != "X" {
				found = append(found, p)
			}
		}
	}
	return found, nil
}
func groupEmpty(id ProcessIdentity) (bool, error) {
	p, e := processInfo(id.PID)
	if e == nil && p.Start != id.Start {
		return false, errors.New("process identity reused")
	}
	if e != nil && !os.IsNotExist(e) {
		return false, e
	}
	members, e := groupMembers(id)
	return len(members) == 0, e
}

// cleanupOwned pins the root before checking its start identity, then pins each
// member before signaling. No PID or process-group signal fallback is permitted.
func cleanupOwned(id ProcessIdentity, signal unix.Signal) (bool, error) {
	fd, e := unix.PidfdOpen(id.PID, 0)
	if e == unix.ESRCH {
		return groupEmpty(id)
	}
	if e != nil {
		return false, e
	}
	defer unix.Close(fd)
	root, e := processInfo(id.PID)
	if os.IsNotExist(e) {
		return groupEmpty(id)
	}
	if e != nil {
		return false, e
	}
	if root.Start != id.Start || root.Group != id.PID || root.Session != id.PID {
		return false, errors.New("unverified process ownership")
	}
	members, e := groupMembers(id)
	if e != nil {
		return false, e
	}
	for _, member := range members {
		childfd, e := unix.PidfdOpen(member.PID, 0)
		if e == unix.ESRCH {
			continue
		}
		if e != nil {
			return false, e
		}
		current, e := processInfo(member.PID)
		if os.IsNotExist(e) {
			unix.Close(childfd)
			continue
		}
		if e != nil {
			unix.Close(childfd)
			return false, e
		}
		if current.Start != member.Start || current.Group != id.PID || current.Session != id.PID {
			unix.Close(childfd)
			return false, errors.New("process changed during cleanup")
		}
		e = unix.PidfdSendSignal(childfd, signal, nil, 0)
		unix.Close(childfd)
		if e != nil && e != unix.ESRCH {
			return false, e
		}
	}
	return len(members) == 0, nil
}

// ownsListener rejects unrelated readiness listeners by matching socket inode
// descriptors owned by the immutable root process, then checking LISTEN tcp4.
func ownsListener(id ProcessIdentity, port int) bool {
	p, e := processInfo(id.PID)
	if e != nil || p.Start != id.Start || p.State == "Z" {
		return false
	}
	fds, e := os.ReadDir(fmt.Sprintf("/proc/%d/fd", id.PID))
	if e != nil {
		return false
	}
	inodes := map[string]bool{}
	for _, fd := range fds {
		target, e := os.Readlink(filepath.Join(fmt.Sprintf("/proc/%d/fd", id.PID), fd.Name()))
		if e == nil && strings.HasPrefix(target, "socket:[") && strings.HasSuffix(target, "]") {
			inodes[strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")] = true
		}
	}
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/net/tcp", id.PID))
	if e != nil {
		return false
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 10 || f[3] != "0A" || !inodes[f[9]] {
			continue
		}
		address := strings.Split(f[1], ":")
		if len(address) != 2 || (address[0] != "00000000" && address[0] != "0100007F") {
			continue
		}
		n, e := strconv.ParseUint(address[1], 16, 16)
		if e == nil && int(n) == port {
			return true
		}
	}
	return false
}
