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
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 2 && os.Args[1] == "task-runner-test" {
		_, e := Execute(context.Background(), manifest(proc("a", "exec /bin/sleep 10")), Options{StateDir: os.Args[2], HelperPath: os.Args[0]})
		if e != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	if len(os.Args) > 1 && os.Args[1] == "task-child" {
		if e := ChildHelper(); e != nil {
			os.Exit(125)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
func manifest(ps ...Process) Manifest {
	return Manifest{Version: "task-v1alpha1", Semantics: "native-v1", MaxRuns: 20, FinalizationWaitMillis: 1000, LogBytes: 1024, Processes: ps}
}
func proc(n, script string) Process {
	return Process{Name: n, Argv: []string{"/bin/sh", "-c", script}, MaxFailedRuns: 1}
}
func TestStrictManifest(t *testing.T) {
	good := manifest(proc("a", "exit 0"))
	good.Normalize()
	b, _ := json.Marshal(good)
	_, canon, e := Decode(b)
	if e != nil {
		t.Fatal(e)
	}
	_, again, e := Decode(canon)
	if e != nil || string(again) != string(canon) {
		t.Fatalf("canonical mismatch %v", e)
	}
	for _, s := range []string{strings.Replace(string(b), `"version":`, `"version":"task-v1alpha1","version":`, 1), strings.Replace(string(b), `"version":`, `"unknown":0,"version":`, 1), strings.Replace(string(b), `"task-v1alpha1"`, `"future"`, 1), string(b) + " {}"} {
		if _, _, e := Decode([]byte(s)); e == nil {
			t.Fatal("accepted", s)
		}
	}
	for _, mut := range []func(*Manifest){func(m *Manifest) { m.Processes[0].AfterSuccess = []string{"a"} }, func(m *Manifest) { m.MaxRuns = 0 }, func(m *Manifest) {
		m.Processes = append(m.Processes, proc("b", ""))
		m.Processes[0].Daemon = true
		m.Processes[1].AfterSuccess = []string{"a"}
	}, func(m *Manifest) {
		m.Processes = append(m.Processes, proc("b", ""))
		m.Processes[0].Ephemeral = true
		m.Processes[1].AfterSuccess = []string{"a"}
	}} {
		v := manifest(proc("a", ""))
		mut(&v)
		if e := v.Normalize(); e == nil {
			t.Fatal("invalid manifest accepted")
		}
	}
}

// Source-derived traces from legacy test_task_planner.py, test_failure_limit.py.
func TestLegacyTraces(t *testing.T) {
	now := time.Unix(100, 0)
	a, b, c := proc("a", ""), proc("b", ""), proc("c", "")
	b.AfterSuccess = []string{"a"}
	c.AfterSuccess = []string{"b"}
	m := manifest(c, b, a)
	m.Normalize()
	p := NewPlanner(m)
	if got := p.Runnable(now, false); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatal(got)
	}
	p.Start("a")
	p.Exit("a", "succeeded", now)
	if got := p.Runnable(now, false); !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatal(got)
	}
	p.Start("b")
	p.Exit("b", "failed", now)
	if len(p.Runnable(now, false)) != 0 || p.Result(false) != "failed" {
		t.Fatal(p.States)
	}
	d := proc("daemon", "")
	d.Daemon = true
	d.MaxFailedRuns = 2
	d.RestartDelayMillis = 10000
	m = manifest(d)
	m.Normalize()
	p = NewPlanner(m)
	p.Start("daemon")
	p.Exit("daemon", "failed", now)
	if len(p.Runnable(now.Add(9*time.Second), false)) != 0 || len(p.Runnable(now.Add(10*time.Second), false)) != 1 {
		t.Fatal("post-exit delay")
	}
	p.Start("daemon")
	p.Exit("daemon", "succeeded", now)
	if p.States["daemon"].Status != "pending" {
		t.Fatal("daemon did not restart")
	}
	p.Start("daemon")
	p.Exit("daemon", "failed", now)
	if p.States["daemon"].FailedRuns != 2 || p.Result(false) != "failed" {
		t.Fatal("failed-run budget")
	}
	for _, sem := range []string{"native-v1", "thermos-v1"} {
		m = manifest(proc("a", ""))
		m.Semantics = sem
		m.Normalize()
		p = NewPlanner(m)
		p.Start("a")
		p.Exit("a", "failed", now)
		want := "failed"
		if sem == "thermos-v1" {
			want = "succeeded"
		}
		if p.Result(false) != want {
			t.Fatal(sem, p.Result(false))
		}
	}
	e := proc("e", "")
	e.Ephemeral = true
	m = manifest(e)
	m.Normalize()
	p = NewPlanner(m)
	if p.Result(false) != "succeeded" {
		t.Fatal("ephemeral holds completion")
	}
	p.Start("e")
	p.Exit("e", "failed", now)
	if p.States["e"].Status != "finished" {
		t.Fatal("ephemeral failure not finished")
	}
	d.MaxFailedRuns = 0
	d.UnlimitedFailures = true
	m = manifest(d)
	m.MaxRuns = 2
	m.Normalize()
	p = NewPlanner(m)
	for i := 0; i < 2; i++ {
		p.Start("daemon")
		p.Exit("daemon", "lost", now)
	}
	if p.States["daemon"].FailedRuns != 0 || p.Result(false) != "failed" {
		t.Fatal("lost guard")
	}
}
func TestConcurrencyNameOrder(t *testing.T) {
	m := manifest(proc("c", ""), proc("b", ""), proc("a", ""))
	m.MaxConcurrency = 2
	m.Normalize()
	p := NewPlanner(m)
	now := time.Now()
	if got := p.Runnable(now, false); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatal(got)
	}
	p.Start("a")
	p.Start("b")
	p.Exit("a", "succeeded", now)
	if got := p.Runnable(now, false); !reflect.DeepEqual(got, []string{"c"}) {
		t.Fatal(got)
	}
}
func execute(t *testing.T, m Manifest) (Result, string, error) {
	t.Helper()
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, e := Execute(ctx, m, Options{StateDir: dir, HelperPath: os.Args[0]})
	return r, dir, e
}
func TestActualDAGRetryAndFinalizers(t *testing.T) {
	a := proc("a", "test -f retry && exit 0; touch retry; exit 7")
	a.MaxFailedRuns = 2
	b := proc("b", "test -f retry && printf '%s' \"$ONLY\"; test -z \"$HOME\"")
	b.Env = map[string]string{"ONLY": "explicit"}
	b.AfterSuccess = []string{"a"}
	f := proc("final", "exit 9")
	f.Finalizer = true
	m := manifest(b, a, f)
	m.MaxConcurrency = 1
	r, dir, e := execute(t, m)
	if e != nil {
		t.Fatal(e)
	}
	if r.PrimaryResult != "succeeded" || r.FinalizationResult != "failed" || r.States["a"].Runs != 2 {
		t.Fatalf("%+v", r)
	}
	out, e := os.ReadFile(filepath.Join(dir, "b.1.stdout"))
	if e != nil || string(out) != "explicit" {
		t.Fatal(string(out), e)
	}
	if _, e = Execute(context.Background(), m, Options{StateDir: dir}); e == nil {
		t.Fatal("journal replay launched")
	}
}
func TestFinalizerAfterPrimaryFailure(t *testing.T) {
	a := proc("a", "exit 2")
	f := proc("final", "printf cleanup")
	f.Finalizer = true
	r, dir, e := execute(t, manifest(a, f))
	if e != nil || r.PrimaryResult != "failed" || r.FinalizationResult != "succeeded" {
		t.Fatal(r, e)
	}
	if _, e = os.Stat(filepath.Join(dir, "final.1.stdout")); e != nil {
		t.Fatal(e)
	}
}
func TestFinalizerSharedDeadline(t *testing.T) {
	f := proc("final", "exec /bin/sleep 5")
	f.Finalizer = true
	m := manifest(proc("a", "exit 0"), f)
	m.FinalizationWaitMillis = 60
	start := time.Now()
	r, _, e := execute(t, m)
	if e != nil || r.PrimaryResult != "succeeded" || r.FinalizationResult != "timeout" || time.Since(start) > time.Second {
		t.Fatal(r, e, time.Since(start))
	}
}
func TestBoundedLogs(t *testing.T) {
	r, dir, e := execute(t, manifest(proc("a", "i=0; while [ $i -lt 2000 ]; do printf x; printf y >&2; i=$((i+1)); done")))
	if e != nil || r.PrimaryResult != "succeeded" {
		t.Fatal(r, e)
	}
	for _, stream := range []string{"stdout", "stderr"} {
		fi, e := os.Stat(filepath.Join(dir, "a.1."+stream))
		if e != nil || fi.Size() != 1024 {
			t.Fatal(fi, e)
		}
	}
}
func TestOrphanStopsRetry(t *testing.T) {
	a := proc("a", "/bin/sleep 5 & exit 1")
	a.MaxFailedRuns = 3
	r, _, e := execute(t, manifest(a))
	if e != nil || r.PrimaryResult != "failed" || r.States["a"].Runs != 1 {
		t.Fatal(r, e)
	}
}

func TestConverter(t *testing.T) {
	x := resolvedExport{Version: "thermos-resolved-v1", TrustedOfflineExport: true, DefaultsApplied: true, SourceDigests: []string{strings.Repeat("a", 64)}, Bindings: map[string]string{}, Task: resolvedTask{FinalizationWait: 1, Processes: []resolvedProcess{{Name: "a", Cmdline: "printf '%s' '{{literal}}'", Env: map[string]string{}, AfterSuccess: []string{}, MinDuration: 0.25}}}}
	b, _ := json.Marshal(x)
	m, f, e := ConvertResolvedJSON(b, 10)
	if e != nil || len(f) == 0 || m.Processes[0].Argv[2] != x.Task.Processes[0].Cmdline || m.Processes[0].RestartDelayMillis != 250 || m.Processes[0].MaxFailedRuns != 0 {
		t.Fatal(m, f, e)
	}
	bad := strings.Replace(string(b), `"task":`, `"container":{},"task":`, 1)
	if _, f, e = ConvertResolvedJSON([]byte(bad), 10); e == nil || f[len(f)-1].Disposition != "rejected" {
		t.Fatal(f, e)
	}
	var raw map[string]any
	if e = json.Unmarshal(b, &raw); e != nil {
		t.Fatal(e)
	}
	process := raw["task"].(map[string]any)["processes"].([]any)[0].(map[string]any)
	delete(process, "max_failures")
	missing, _ := json.Marshal(raw)
	if _, _, e = ConvertResolvedJSON(missing, 10); e == nil {
		t.Fatal("omitted failed-run default became unlimited")
	}
	x.TrustedOfflineExport = false
	b, _ = json.Marshal(x)
	if _, _, e = ConvertResolvedJSON(b, 10); e == nil {
		t.Fatal("untrusted export accepted")
	}
}
func TestStartFailureRunsFinalizer(t *testing.T) {
	a := proc("a", "")
	a.Argv = []string{"/no/such/program"}
	f := proc("final", "exit 0")
	f.Finalizer = true
	r, _, e := execute(t, manifest(a, f))
	if e != nil || r.PrimaryResult != "failed" || r.FinalizationResult != "succeeded" {
		t.Fatal(r, e)
	}
}

func TestJournalFailureBeforeGate(t *testing.T) {
	dir := t.TempDir()
	file, e := os.Create(filepath.Join(dir, "closed"))
	if e != nil {
		t.Fatal(e)
	}
	file.Close()
	p := proc("a", "touch forbidden")
	m := manifest(p)
	m.Normalize()
	_, e = launch(p, 1, m, Options{StateDir: dir, HelperPath: os.Args[0], HelperArgs: []string{"task-child"}}, &journal{file}, time.Time{})
	if e == nil {
		t.Fatal("closed journal accepted")
	}
	if _, e = os.Stat(filepath.Join(dir, "forbidden")); !os.IsNotExist(e) {
		t.Fatal("workload released before journal", e)
	}
}
func TestRunnerCrashNeverResumes(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	cmd := exec.Command(os.Args[0], "task-runner-test", dir)
	cmd.Env = []string{}
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	until := time.Now().Add(3 * time.Second)
	released := false
	for time.Now().Before(until) {
		b, _ := os.ReadFile(filepath.Join(dir, "task.journal"))
		if strings.Contains(string(b), `"kind":"released"`) {
			released = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !released {
		t.Fatal("runner did not release")
	}
	cmd.Process.Kill()
	cmd.Wait()
	m := manifest(proc("a", "exec /bin/sleep 10"))
	if _, e := Execute(context.Background(), m, Options{StateDir: dir}); e == nil {
		t.Fatal("crashed journal resumed")
	}
	for i := 0; i < 20; i++ {
		found, e := cleanupOrphans(map[string]*child{})
		if e != nil {
			t.Fatal(e)
		}
		if !found {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestIndependentWorkSurvivesBlockedBranch(t *testing.T) {
	a, b, c := proc("a", ""), proc("b", ""), proc("c", "")
	b.AfterSuccess = []string{"a"}
	m := manifest(a, b, c)
	m.Semantics = "thermos-v1"
	m.Normalize()
	p := NewPlanner(m)
	now := time.Now()
	p.Start("a")
	p.Exit("a", "failed", now)
	if p.Result(false) != "" {
		t.Fatal("independent c prematurely stopped")
	}
	p.Start("c")
	p.Exit("c", "succeeded", now)
	if p.Result(false) != "failed" {
		t.Fatal("blocked DAG succeeded")
	}
}
func TestExactSuccessDespiteInheritedLogPipe(t *testing.T) {
	r, dir, e := execute(t, manifest(proc("a", "/bin/sleep 5 & exit 0")))
	if e != nil || r.PrimaryResult != "failed" {
		t.Fatal(r, e)
	}
	b, e := os.ReadFile(filepath.Join(dir, "task.journal"))
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var ev Event
		if e = json.Unmarshal([]byte(line), &ev); e != nil {
			t.Fatal(e)
		}
		if ev.Kind == "exit" && ev.Name == "a" {
			found = true
			if ev.Outcome != "succeeded" || ev.ExitCode == nil || *ev.ExitCode != 0 {
				t.Fatalf("exact exit lost: %+v", ev)
			}
		}
	}
	if !found {
		t.Fatal("missing exit")
	}
}

func TestWaitDelayPreservesExactOutcome(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "/bin/sleep 0.2 & exit 0")
	cmd.Env = []string{}
	cmd.Stdout = io.Discard
	cmd.WaitDelay = time.Millisecond
	e := cmd.Run()
	if !errors.Is(e, exec.ErrWaitDelay) {
		t.Fatal(e)
	}
	code, signal, outcome := exactOutcome(cmd.ProcessState)
	if code != 0 || signal != 0 || outcome != "succeeded" {
		t.Fatal(code, signal, outcome)
	}
	for i := 0; i < 20; i++ {
		found, e := cleanupOrphans(map[string]*child{})
		if e != nil {
			t.Fatal(e)
		}
		if !found {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
}
