/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func helper(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	t.Cleanup(cancel)
	encoded, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(), "GO_NATIVE_WORKLOAD_HELPER=1", "GO_NATIVE_WORKLOAD_ARGS="+string(encoded))
	return cmd
}

func startService(t *testing.T, extra ...string) (*exec.Cmd, string, func() error) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	ln.Close()
	evidence := filepath.Join(t.TempDir(), "events with spaces.jsonl")
	args := []string{"--mode", "service", "--identity", "service-a", "--evidence", evidence, "--port", port}
	cmd := helper(t, append(args, extra...)...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	var waitErr error
	wait := func() error { once.Do(func() { waitErr = cmd.Wait() }); return waitErr }
	t.Cleanup(func() { cmd.Process.Kill(); wait() })
	return cmd, "http://127.0.0.1:" + port, wait
}

var client = &http.Client{Timeout: 300 * time.Millisecond}

func response(t *testing.T, url string, want int) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r, err := client.Get(url)
		if err == nil {
			b, readErr := io.ReadAll(io.LimitReader(r.Body, 1024))
			r.Body.Close()
			if readErr == nil && r.StatusCode == want {
				return string(b)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never returned %d", url, want)
	return ""
}

func TestServiceReadinessProbeAndTermination(t *testing.T) {
	cmd, base, wait := startService(t, "--ready-delay-ms", "700")
	response(t, base+"/ready", http.StatusServiceUnavailable)
	response(t, base+"/ready", http.StatusOK)
	if got := strings.TrimSpace(response(t, base+"/identity", http.StatusOK)); got != "service-a" {
		t.Fatal(got)
	}
	_, port, err := net.SplitHostPort(strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	output, err := helper(t, "--mode", "probe", "--port", port).Output()
	if err != nil {
		t.Fatal(err)
	}
	var probe map[string]any
	if err := json.Unmarshal(output, &probe); err != nil || probe["identity"] != "service-a" || probe["ready"] != true {
		t.Fatalf("probe %s %v", output, err)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := wait(); err != nil {
		t.Fatal(err)
	}
}

func TestRefusedTermStillHonorsInterrupt(t *testing.T) {
	cmd, base, wait := startService(t, "--refuse-sigterm")
	response(t, base+"/ready", http.StatusOK)
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	response(t, base+"/identity", http.StatusOK)
	if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	if err := wait(); err != nil {
		t.Fatal(err)
	}
}

func TestExactBindConflict(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	evidence := filepath.Join(t.TempDir(), "launch.jsonl")
	cmd := helper(t, "--mode", "service", "--identity", "conflict", "--evidence", evidence,
		"--port", strconv.Itoa(ln.Addr().(*net.TCPAddr).Port))
	err = cmd.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 98 {
		t.Fatalf("expected bind exit98, got %v", err)
	}
	data, err := os.ReadFile(evidence)
	if err != nil {
		t.Fatal(err)
	}
	var event map[string]any
	if json.Unmarshal(data, &event) != nil || event["identity"] != "conflict" || event["event"] != "launch" {
		t.Fatalf("bad evidence %s", data)
	}
}

func TestEvidenceRefusesSymlinksNonregularAndUnsafePaths(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	directoryLink := filepath.Join(root, "directory-link")
	if err := os.Symlink(root, directoryLink); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(root, "fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{link, filepath.Join(directoryLink, "new"), fifo, root + "/../escape", root + "/bad\npath"} {
		if err := appendEvidence(path, map[string]any{"test": true}); err == nil {
			t.Fatalf("accepted %q", path)
		}
	}
	if data, _ := os.ReadFile(target); string(data) != "preserve" {
		t.Fatalf("target changed: %s", data)
	}
}

func TestBatchExitAndPrivateAppend(t *testing.T) {
	evidence := filepath.Join(t.TempDir(), "launch.jsonl")
	cmd := helper(t, "--mode", "batch", "--identity", "batch-a", "--evidence", evidence, "--exit-code", "7")
	err := cmd.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 7 {
		t.Fatal(err)
	}
	info, err := os.Stat(evidence)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, err)
	}
	data, err := os.ReadFile(evidence)
	var event map[string]any
	if err != nil || json.Unmarshal(data, &event) != nil || event["identity"] != "batch-a" {
		t.Fatalf("evidence %s %v", data, err)
	}
}

func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_NATIVE_WORKLOAD_HELPER") != "1" {
		return
	}
	var args []string
	if err := json.Unmarshal([]byte(os.Getenv("GO_NATIVE_WORKLOAD_ARGS")), &args); err != nil {
		os.Exit(64)
	}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	os.Args = append([]string{os.Args[0]}, args...)
	main()
	os.Exit(0)
}

func TestPortCheckProvesExactBindAvailability(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	err = helper(t, "--mode", "port-check", "--port", port).Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 98 {
		t.Fatalf("busy port accepted: %v", err)
	}
	ln.Close()
	data, err := helper(t, "--mode", "port-check", "--port", port).Output()
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if json.Unmarshal(data, &value) != nil || value["available"] != true {
		t.Fatalf("port proof %s", data)
	}
}
