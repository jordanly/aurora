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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type taskCLIResult struct {
	PrimaryResult string `json:"primaryResult"`
	Cleanup       string `json:"cleanup"`
}

// Build the actual command binary so Execute's package-wide dedicated-process
// guard and its same-binary task-child helper are both exercised.
func buildTaskCLI(t *testing.T) string {
	t.Helper()
	moduleRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("locating agent module: %v", err)
	}
	if _, err := os.Stat(filepath.Join(moduleRoot, "go.mod")); err != nil {
		t.Fatalf("locating agent module: %v", err)
	}
	goTool := filepath.Join(runtime.GOROOT(), "bin", "go")
	out := filepath.Join(t.TempDir(), "aurora-agent")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, goTool, "build", "-o", out, "./cmd/aurora-agent")
	cmd.Dir = moduleRoot
	cmd.Env = os.Environ()
	if b, err := cmd.CombinedOutput(); err != nil {
		if ctx.Err() != nil {
			t.Fatalf("build aurora-agent timed out: %v", ctx.Err())
		}
		t.Fatalf("build aurora-agent: %v\n%s", err, b)
	}
	return out
}

func runTaskCLI(t *testing.T, binary string, args ...string) (stdout, stderr []byte, err error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	var out, eout bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &eout
	err = cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("aurora-agent timed out: %v", ctx.Err())
	}
	return out.Bytes(), eout.Bytes(), err
}

func canonicalTaskManifest(marker string) string {
	m := map[string]any{
		"version": "task-v1alpha1", "semantics": "native-v1", "maxConcurrency": 1,
		"maxRuns": 1, "taskMaxFailures": 0, "finalizationWaitMillis": 1000, "logBytes": 65536,
		"processes": []any{map[string]any{
			"name": "a", "argv": []string{"/bin/sh", "-c", "printf x >> \"$1\"", "task", marker},
			"env": map[string]string{}, "afterSuccess": []string{}, "maxFailedRuns": 1,
		}},
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func TestExecuteTaskCLIIsSingleUseAndReturnsResult(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("task execution requires Linux pidfds")
	}
	binary := buildTaskCLI(t)
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	marker := filepath.Join(dir, "ran")
	manifest := canonicalTaskManifest(marker)

	out, errOut, err := runTaskCLI(t, binary, "execute-task", "--manifest-json", manifest, "--state-dir", state)
	if err != nil {
		t.Fatalf("execute-task failed: %v\nstderr: %s\nstdout: %s", err, errOut, out)
	}
	var result taskCLIResult
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("invalid execute result %q: %v", out, err)
	}
	if result.PrimaryResult != "succeeded" || result.Cleanup != "complete" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if got, err := os.ReadFile(marker); err != nil || string(got) != "x" {
		t.Fatalf("first execution marker = %q, err=%v", got, err)
	}

	if _, errOut, err = runTaskCLI(t, binary, "execute-task", "--manifest-json", manifest, "--state-dir", state); err == nil {
		t.Fatal("replayed task unexpectedly succeeded")
	} else if !strings.Contains(string(errOut), "journal already exists") {
		t.Fatalf("replay did not report journal refusal: %s", errOut)
	}
	if got, err := os.ReadFile(marker); err != nil || string(got) != "x" {
		t.Fatalf("replay relaunched process: %q, err=%v", got, err)
	}

	var finalizerManifest map[string]any
	if err := json.Unmarshal([]byte(manifest), &finalizerManifest); err != nil {
		t.Fatal(err)
	}
	finalizerManifest["processes"] = append(finalizerManifest["processes"].([]any), map[string]any{
		"name": "cleanup", "argv": []string{"/bin/sh", "-c", "exit 7"},
		"env": map[string]string{}, "afterSuccess": []string{}, "finalizer": true, "maxFailedRuns": 1,
	})
	finalizerBytes, _ := json.Marshal(finalizerManifest)
	out, errOut, err = runTaskCLI(t, binary, "execute-task", "--manifest-json", string(finalizerBytes), "--state-dir", filepath.Join(dir, "finalizer-state"))
	if err != nil {
		t.Fatalf("primary success with failed finalizer returned error: %v\nstderr: %s", err, errOut)
	}
	var finalizerResult struct {
		PrimaryResult      string `json:"primaryResult"`
		FinalizationResult string `json:"finalizationResult"`
		Cleanup            string `json:"cleanup"`
	}
	if err := json.Unmarshal(out, &finalizerResult); err != nil {
		t.Fatal(err)
	}
	if finalizerResult.PrimaryResult != "succeeded" || finalizerResult.FinalizationResult != "failed" || finalizerResult.Cleanup != "complete" {
		t.Fatalf("independent primary/finalizer outcomes lost: %+v", finalizerResult)
	}
}

func TestTaskCLIRejectsInvalidInputsWithoutCreatingState(t *testing.T) {
	binary := buildTaskCLI(t)
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"unknown flag", []string{"execute-task", "--bogus", "--manifest-json", "{}"}},
		{"malformed manifest", []string{"execute-task", "--manifest-json", "{}"}},
		{"positional argument", []string{"execute-task", "--manifest-json", "{}", "extra"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := filepath.Join(t.TempDir(), "must-not-exist")
			args := append([]string{}, tc.args...)
			args = append(args, "--state-dir", state)
			_, _, err := runTaskCLI(t, binary, args...)
			if err == nil {
				t.Fatal("invalid invocation unexpectedly succeeded")
			}
			if _, statErr := os.Stat(state); !os.IsNotExist(statErr) {
				t.Fatalf("invalid invocation created state: %v", statErr)
			}
		})
	}
}

func TestConvertTaskCLIRequiresFiniteRunsAndDoesNotEvaluateShell(t *testing.T) {
	binary := buildTaskCLI(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "must-not-run")
	export := map[string]any{
		"version": "thermos-resolved-v1", "trustedOfflineExport": true, "defaultsApplied": true,
		"sourceDigests": []string{"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		"bindings":      map[string]string{}, "task": map[string]any{
			"max_concurrency": 1, "max_failures": 0, "finalization_wait": 0,
			"processes": []any{map[string]any{"name": "a", "cmdline": "touch '" + strings.ReplaceAll(marker, "'", "'\\\"'\\\"'") + "'",
				"env": map[string]string{}, "after_success": []string{}, "daemon": false, "ephemeral": false,
				"finalizer": false, "max_failures": 1, "min_duration": 0}},
		},
	}
	b, _ := json.Marshal(export)
	document := filepath.Join(dir, "resolved.json")
	if err := os.WriteFile(document, b, 0600); err != nil {
		t.Fatal(err)
	}
	out, _, err := runTaskCLI(t, binary, "convert-task", "--document", document, "--max-runs", "3")
	if err != nil {
		t.Fatalf("convert-task failed: %v", err)
	}
	var converted struct {
		Manifest struct {
			MaxRuns   int `json:"maxRuns"`
			Processes []struct {
				Argv []string `json:"argv"`
			} `json:"processes"`
		} `json:"manifest"`
	}
	if err := json.Unmarshal(out, &converted); err != nil {
		t.Fatalf("invalid conversion output: %v", err)
	}
	if converted.Manifest.MaxRuns != 3 || len(converted.Manifest.Processes) != 1 || len(converted.Manifest.Processes[0].Argv) != 3 || converted.Manifest.Processes[0].Argv[0] != "/bin/bash" {
		t.Fatalf("conversion did not produce bounded shell argv: %s", out)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("conversion evaluated cmdline, marker stat=%v", err)
	}
	if _, errOut, err := runTaskCLI(t, binary, "convert-task", "--document", document, "--max-runs", "0"); err == nil {
		t.Fatalf("unbounded conversion accepted: err=%v stderr=%s", err, errOut)
	}
}
