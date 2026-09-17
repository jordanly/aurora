//go:build linux

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

func TestIsolationProfileCannotDowngrade(t *testing.T) {
	o := &IsolationOptions{CgroupRoot: "/sys/fs/cgroup/aurora", RootFS: "/root/rootfs", UIDBase: 200000, UIDCount: 2, PidsMax: 128, WorkBytes: 1 << 20}
	ref := &IsolationRef{UID: 200000, CgroupRoot: o.CgroupRoot, Fingerprint: o.fingerprint()}
	if e := (&Runtime{}).checkIsolation(ref); e == nil {
		t.Fatal("isolation silently downgraded")
	}
	r := &Runtime{opts: RuntimeOptions{Isolation: o}}
	if e := r.checkIsolation(ref); e != nil {
		t.Fatal(e)
	}
	for _, modify := range []func(*IsolationOptions){func(v *IsolationOptions) { v.RootFS += "-other" }, func(v *IsolationOptions) { v.CgroupRoot += "-other" }, func(v *IsolationOptions) { v.WorkBytes++ }, func(v *IsolationOptions) { v.UIDCount++ }} {
		copy := *o
		modify(&copy)
		r.opts.Isolation = &copy
		if e := r.checkIsolation(ref); e == nil {
			t.Fatal("changed profile accepted")
		}
	}
	r.opts.Isolation = o
	if e := r.checkIsolation(nil); e == nil {
		t.Fatal("trusted attempt adopted")
	}
}
func TestIsolationInvalidLimitsFailClosed(t *testing.T) {
	for _, o := range []IsolationOptions{{}, {UIDBase: 200000, UIDCount: MaxRetainedTickets - 1, PidsMax: 128, WorkBytes: 1 << 20}, {UIDBase: 1, UIDCount: ^uint32(0), PidsMax: 128, WorkBytes: 1 << 20}, {UIDBase: 200000, UIDCount: MaxRetainedTickets, PidsMax: 1, WorkBytes: 1 << 20}, {UIDBase: 200000, UIDCount: MaxRetainedTickets, PidsMax: 128, WorkBytes: 1}} {
		if e := o.validate(); e == nil || e.Error() != "invalid isolation UID range, pid limit or work size" {
			t.Fatal("invalid profile did not fail its limit preflight", e)
		}
	}
}
func TestIsolationRequestedProfileNeverFallsBack(t *testing.T) {
	dir := t.TempDir()
	c := config()
	s := open(t, filepath.Join(dir, "state"), c)
	opts := testRuntimeOpts(t, filepath.Join(dir, "work"))
	opts.Isolation = &IsolationOptions{CgroupRoot: filepath.Join(dir, "not-cgroup"), RootFS: filepath.Join(dir, "rootfs"), UIDBase: 200000, UIDCount: MaxRetainedTickets, PidsMax: 128, WorkBytes: 1 << 20}
	if r, e := NewRuntime(s, opts); e == nil || r != nil {
		t.Fatal("unavailable isolation fell back to a running trusted runtime")
	}
	if s.runtimeActive || s.runtimeIsolation {
		t.Fatal("failed isolation startup published capability")
	}
	body := fixture(t, "run")
	p := body["assignment"].(map[string]any)
	p["resources"].(map[string]any)["memoryEnforcement"] = "hard"
	p["requiredCapabilities"] = []any{"hard-memory"}
	result, e := s.Admit(delivery(c, body), caller(c))
	if e != nil {
		t.Fatal(e)
	}
	if result.Outcome != "rejected-capability" {
		t.Fatal("unavailable isolation admitted hard memory", result)
	}
}

func TestIsolationSupervisorCannotChangeIdentity(t *testing.T) {
	o := &IsolationOptions{CgroupRoot: "/sys/fs/cgroup/aurora", RootFS: "/root/rootfs", UIDBase: 200000, UIDCount: 2, PidsMax: 128, WorkBytes: 1 << 20}
	r := &Runtime{opts: RuntimeOptions{Isolation: o}}
	ref := &IsolationRef{UID: 200000, CgroupRoot: o.CgroupRoot, Fingerprint: o.fingerprint()}
	previous := &Execution{Isolation: ref}
	changed := *ref
	changed.UID++
	for _, next := range []*Execution{{}, {Isolation: &changed}, nil} {
		if e := r.verifyIsolationObservation(previous, next); e == nil {
			t.Fatal("supervisor changed task isolation identity")
		}
	}
	if e := r.verifyIsolationObservation(previous, &Execution{Isolation: ref}); e != nil {
		t.Fatal(e)
	}
}

func TestIsolationUIDReservedThroughGarbage(t *testing.T) {
	c := config()
	s := open(t, filepath.Join(t.TempDir(), "state"), c)
	o := &IsolationOptions{CgroupRoot: "/sys/fs/cgroup/aurora", RootFS: "/root/rootfs", UIDBase: 200000, UIDCount: 1, PidsMax: 128, WorkBytes: 1 << 20}
	r := &Runtime{store: s, opts: RuntimeOptions{Isolation: o}}
	key := strings.Repeat("a", 64)
	ref, e := r.isolationIdentity(key)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		st.Retention = &RetentionState{Version: 1, Retired: []TicketRange{}, Garbage: []RetentionGarbage{{Key: strings.Repeat("b", 64), Attempt: Attempt{Execution: &Execution{Phase: "terminal", Outcome: "succeeded", Cleanup: "complete", Isolation: ref}}}}}
		return save(b, st)
	}); e != nil {
		t.Fatal(e)
	}
	if _, e = r.isolationIdentity(key); e == nil {
		t.Fatal("UID reused before durable artifact GC")
	}
}

func TestIsolationUIDSkipsColdTerminalOwner(t *testing.T) {
	c := config()
	dir := t.TempDir()
	s := open(t, filepath.Join(dir, "state"), c)
	r, e := NewRuntime(s, testRuntimeOpts(t, filepath.Join(dir, "work")))
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	o := &IsolationOptions{CgroupRoot: "/sys/fs/cgroup/aurora", RootFS: "/root/rootfs", UIDBase: 200000, UIDCount: 2, PidsMax: 128, WorkBytes: 1 << 20}
	r.opts.Isolation = o
	target := strings.Repeat("a", 64)
	chosen, e := r.isolationIdentity(target)
	if e != nil {
		t.Fatal(e)
	}
	body := fixture(t, "run")
	if _, e = s.Admit(delivery(c, body), caller(c)); e != nil {
		t.Fatal(e)
	}
	key := attemptKey(body["identity"].(map[string]any))
	if e = r.update(key, func(a *Attempt) error {
		a.Execution = &Execution{Phase: "terminal", Outcome: "succeeded", Cleanup: "complete", Isolation: chosen}
		return nil
	}, false); e != nil {
		t.Fatal(e)
	}
	hot, e := s.InspectHot()
	if e != nil {
		t.Fatal(e)
	}
	if _, ok := hot.Attempts[key]; ok {
		t.Fatal("fixture did not reach cold history")
	}
	next, e := r.isolationIdentity(target)
	if e != nil {
		t.Fatal(e)
	}
	if next.UID == chosen.UID {
		t.Fatal("cold terminal UID reused before retirement")
	}
}

func waitIsolationKernel(t *testing.T, r *Runtime, predicate func(State) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		if e := r.Tick(ctx); e != nil {
			t.Fatal(e)
		}
		st, e := r.store.Inspect()
		if e != nil {
			t.Fatal(e)
		}
		if predicate(st) {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("isolated kernel condition timed out: %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func kernelCounters(t *testing.T, path string) map[string]uint64 {
	t.Helper()
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	values := map[string]uint64{}
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		n, e := strconv.ParseUint(fields[1], 10, 64)
		if e != nil {
			t.Fatal(e)
		}
		values[fields[0]] = n
	}
	return values
}

// Run only in a dedicated root-owned Linux lab with cpu+memory+pids delegation.
// The opt-in variables are not a fallback: a requested test fails every missing
// prerequisite and leaves no successful enforcement claim for an unavailable host.
func TestIsolationKernelEnforcement(t *testing.T) {
	base := os.Getenv("AURORA_ISOLATION_TEST_ROOT")
	if base == "" {
		t.Skip("requires dedicated privileged cgroup-v2 lab")
	}
	cgroup := os.Getenv("AURORA_ISOLATION_TEST_CGROUP")
	rootfs := filepath.Join(base, "rootfs")
	if e := os.MkdirAll(rootfs, 0755); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(rootfs, "readonly-probe"), []byte("immutable"), 0666); e != nil {
		t.Fatal(e)
	}
	if e := os.Chmod(filepath.Join(rootfs, "readonly-probe"), 0666); e != nil {
		t.Fatal(e)
	}
	for _, d := range []string{"work", "proc", "dev"} {
		if e := os.MkdirAll(filepath.Join(rootfs, d), 0755); e != nil {
			t.Fatal(e)
		}
	}
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	binary, e := os.ReadFile(exe)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(rootfs, "aurora-test"), binary, 0755); e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"boundary", "memory", "cpu", "health", "escape"} {
		t.Run(mode, func(t *testing.T) {
			dir, e := os.MkdirTemp(base, "attempt-")
			if e != nil {
				t.Fatal(e)
			}
			c := config()
			c.CPU = 1000
			c.Memory = 256 << 20
			s := open(t, filepath.Join(dir, "state"), c)
			t.Cleanup(func() {
				if e := s.Close(); e != nil {
					t.Errorf("isolated test store close: %v", e)
				}
			})
			o := supervisorOpts(t, filepath.Join(dir, "work"))
			o.LogBytes = 16 << 10
			o.Isolation = &IsolationOptions{CgroupRoot: cgroup, RootFS: rootfs, UIDBase: 200000, UIDCount: 65536, PidsMax: 64, WorkBytes: 1 << 20}
			r, e := NewRuntime(s, o)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() {
				if r != nil {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					if e := r.Shutdown(ctx); e != nil {
						t.Errorf("isolated test shutdown: %v", e)
					}
				}
			})
			var oomBefore uint64
			if mode == "memory" {
				oomBefore = kernelCounters(t, filepath.Join(cgroup, "memory.events"))["oom_kill"]
			}

			b := fixture(t, "run")
			p := b["assignment"].(map[string]any)
			p["argv"] = []any{"/aurora-test", "-test.run=^TestIsolationWorkload$", "--", "isolation-workload", mode, strconv.Itoa(os.Getpid())}
			p["env"] = map[string]any{}
			p["ports"] = []any{}
			p["readiness"] = map[string]any{"kind": "none"}
			p["resources"] = map[string]any{"cpuMillis": uint64(100), "memoryBytes": uint64(64 << 20), "memoryEnforcement": "hard"}
			p["requiredCapabilities"] = []any{"hard-memory"}
			p["stop"].(map[string]any)["graceMillis"] = uint64(100)
			if mode == "memory" {
				p["resources"].(map[string]any)["cpuMillis"] = uint64(1000)
			}
			if mode == "health" {
				port := freePort(t)
				p["argv"] = append(p["argv"].([]any), strconv.Itoa(port))
				p["ports"] = []any{map[string]any{"name": "http", "number": uint64(port), "protocol": "tcp", "family": "ipv4", "network": "test-network"}}
				p["readiness"] = map[string]any{"kind": "tcp", "port": "http", "intervalMillis": uint64(20), "timeoutMillis": uint64(100), "startupTimeoutMillis": uint64(3000), "failureThreshold": uint64(2)}
				p["resources"].(map[string]any)["cpuMillis"] = uint64(1000)
			}

			result, e := s.Admit(delivery(c, b), caller(c))
			if e != nil || result.Outcome != "accepted" {
				t.Fatalf("admit: %+v %v", result, e)
			}
			key := attemptKey(b["identity"].(map[string]any))
			if mode == "health" {
				waitIsolationKernel(t, r, func(st State) bool { a := onlyAttempt(st); return a.Execution != nil && a.Execution.Ready })
				if e = os.WriteFile(filepath.Join(o.Root, key, "work", "parent-checked"), nil, 0600); e != nil {
					t.Fatal(e)
				}
			}
			if mode == "cpu" {
				waitIsolationKernel(t, r, func(st State) bool {
					a := onlyAttempt(st)
					return a.Execution != nil && a.Execution.Outcome == "running"
				})
				quota, e := os.ReadFile(filepath.Join(cgroup, key, "cpu.max"))
				if e != nil {
					t.Fatal(e)
				}
				if strings.TrimSpace(string(quota)) != "100000 1000000" {
					t.Fatalf("unexpected cpu.max: %s", quota)
				}
				waitIsolationKernel(t, r, func(st State) bool {
					return kernelCounters(t, filepath.Join(cgroup, key, "cpu.stat"))["nr_throttled"] > 0
				})
				t.Logf("enforced cpu.max=%s cpu.stat=%v", strings.TrimSpace(string(quota)), kernelCounters(t, filepath.Join(cgroup, key, "cpu.stat")))
				if e = r.Close(); e != nil {
					t.Fatal(e)
				}
				if e = s.Close(); e != nil {
					t.Fatal(e)
				}
				s = open(t, filepath.Join(dir, "state"), c)
				r, e = NewRuntime(s, o)
				if e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(filepath.Join(o.Root, key, "work", "parent-checked"), nil, 0600); e != nil {
					t.Fatal(e)
				}
			}
			waitIsolationKernel(t, r, func(st State) bool {
				a := onlyAttempt(st)
				return a.Execution != nil && a.Execution.Cleanup == "complete" && a.Supervisor != nil && a.Supervisor.Acknowledged
			})
			st, e := s.Inspect()
			if e != nil {
				t.Fatal(e)
			}
			a := onlyAttempt(st)
			stdout, _ := os.ReadFile(filepath.Join(o.Root, key, "stdout.log"))
			stderr, _ := os.ReadFile(filepath.Join(o.Root, key, "stderr.log"))
			t.Logf("%s execution=%+v stdout=%s stderr=%s", mode, a.Execution, stdout, stderr)
			if mode == "memory" {
				oomAfter := kernelCounters(t, filepath.Join(cgroup, "memory.events"))["oom_kill"]
				t.Logf("delegated memory.events oom_kill %d -> %d", oomBefore, oomAfter)
				if oomAfter <= oomBefore {
					t.Fatal("memory OOM counter did not increase")
				}
				if a.Execution.Signal != int(unix.SIGKILL) || a.Execution.Outcome != "failed" {
					t.Fatal("memory was not enforced", a.Execution)
				}
			} else if mode == "health" {
				if a.Execution.HealthFailure != "health-check-failed" || a.Execution.Outcome != "failed" {
					t.Fatal("isolated listener health ownership failed", a.Execution)
				}
			} else if a.Execution.Outcome != "succeeded" {
				t.Fatal("isolated workload failed", a.Execution)
			}
			if mode == "escape" {
				var child ProcessIdentity
				if e := json.Unmarshal(stdout, &child); e != nil {
					t.Fatal(e)
				}
				if info, e := processInfo(child.PID); e == nil && info.Start == child.Start && info.State != "Z" && info.State != "X" {
					t.Fatal("escaped child survived task cgroup cleanup", info)
				}
			}
			if _, e = os.Stat(filepath.Join(cgroup, key)); !os.IsNotExist(e) {
				t.Fatal("task cgroup retained", e)
			}
			deadline := time.Now().Add(30 * time.Second)
			for {
				info, e := processInfo(a.Supervisor.Process.PID)
				if os.IsNotExist(e) || e == nil && (info.State == "Z" || info.Start != a.Supervisor.Process.Start) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("supervisor did not exit")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if e = cleanupIsolationArtifacts(o.Root, key, a); e != nil {
				t.Fatal(e)
			}
			if e = r.Close(); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestIsolationWorkload(t *testing.T) {
	index := -1
	for i, a := range os.Args {
		if a == "isolation-workload" {
			index = i
			break
		}
	}
	if index < 0 {
		return
	}
	mode := os.Args[index+1]
	fail := func(e error) {
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(72)
		}
	}
	waitParent := func() {
		deadline := time.Now().Add(30 * time.Second)
		for {
			if _, e := os.Stat("/work/parent-checked"); e == nil {
				return
			}
			if time.Now().After(deadline) {
				fail(errors.New("parent did not acknowledge isolated probe"))
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	switch mode {
	case "boundary":
		if os.Getuid() < 200000 {
			fail(errors.New("task UID not dropped"))
		}
		if e := os.WriteFile("/readonly-probe", []byte("x"), 0600); !errors.Is(e, unix.EROFS) {
			fail(fmt.Errorf("root filesystem was not mounted read-only: %v", e))
		}
		parent, _ := strconv.Atoi(os.Args[index+2])
		if e := syscall.Kill(parent, 0); e != unix.EPERM {
			fail(fmt.Errorf("agent task-user signal boundary: %v", e))
		}
		if _, e := os.ReadFile(fmt.Sprintf("/proc/%d/root/etc/passwd", parent)); e == nil {
			fail(errors.New("agent filesystem exposed through proc"))
		}
		b, e := os.ReadFile("/proc/self/status")
		fail(e)
		status := string(b)
		if !strings.Contains(status, "CapEff:\t0000000000000000") || !strings.Contains(status, "NoNewPrivs:\t1") {
			fail(errors.New("capability/no-new-privilege boundary missing"))
		}
		f, e := os.Create("/work/fill")
		fail(e)
		_, e = f.Write(make([]byte, 2<<20))
		f.Close()
		if !errors.Is(e, unix.ENOSPC) {
			fail(fmt.Errorf("work size not enforced: %v", e))
		}
		json.NewEncoder(os.Stdout).Encode(map[string]any{"uid": os.Getuid(), "rootReadOnly": true, "agentSignalDenied": true, "agentProcDenied": true, "workENOSPC": true, "noCapabilities": true})
	case "health":
		port, _ := strconv.Atoi(os.Args[index+3])
		ln, e := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
		fail(e)
		waitParent()
		ln.Close()
		time.Sleep(30 * time.Second)
		os.Exit(74)
	case "escape":
		cmd := exec.Command("/aurora-test", "-test.run=^TestIsolationWorkload$", "--", "isolation-workload", "escaped-child", "0")
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		fail(cmd.Start())
		info, e := processInfo(cmd.Process.Pid)
		fail(e)
		json.NewEncoder(os.Stdout).Encode(info.ProcessIdentity)
	case "escaped-child":
		time.Sleep(30 * time.Second)
	case "memory":
		all := [][]byte{}
		for i := 0; i < 128; i++ {
			b := make([]byte, 1<<20)
			for j := 0; j < len(b); j += 4096 {
				b[j] = 1
			}
			all = append(all, b)
		}
		fmt.Fprintln(os.Stderr, len(all))
		os.Exit(73)
	case "cpu":
		var before, after syscall.Rusage
		syscall.Getrusage(syscall.RUSAGE_SELF, &before)
		start := time.Now()
		n := 0
		for time.Since(start) < 2*time.Second {
			n++
		}
		syscall.Getrusage(syscall.RUSAGE_SELF, &after)
		used := (after.Utime.Sec-before.Utime.Sec)*1000000 + after.Utime.Usec - before.Utime.Usec + (after.Stime.Sec-before.Stime.Sec)*1000000 + after.Stime.Usec - before.Stime.Usec
		if used > 600000 {
			fail(fmt.Errorf("CPU limit ineffective: %d usec", used))
		}
		json.NewEncoder(os.Stdout).Encode(map[string]any{"cpuUsec": used, "wallMillis": time.Since(start).Milliseconds(), "iterations": n})
		waitParent()
	default:
		fail(errors.New("unknown isolation test workload"))
	}
	os.Exit(0)
}
