/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package main

import (
	agent "aurora.local/agent"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"
)

// An undrained reply pipe must not pin the control loop through cancellation.
func TestReplyCancellationClosesBlockedWriter(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- writeReply(ctx, writer, map[string]any{"ok": true}) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked reply prevented cancellation")
	}
	// Writer close is observable by the reader, so no output worker remains blocked.
	b := make([]byte, 64)
	if _, err := reader.Read(b); err != io.EOF {
		t.Fatalf("writer not closed: %v", err)
	}
}

type shortWriter struct{}

func (shortWriter) Write([]byte) (int, error) { return 0, nil }
func (shortWriter) Close() error              { return nil }

func TestReplyDoesNotReportTruncatedSuccess(t *testing.T) {
	if err := writeReply(context.Background(), shortWriter{}, map[string]any{"ok": true}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short reply was treated as success: %v", err)
	}
}

func localStore(t *testing.T) (*agent.Store, *agent.Runtime, agent.Config) {
	t.Helper()
	root := t.TempDir()
	c := agent.Config{Cluster: "lab", Incarnation: "recovery-a", Node: "agent-a", Journal: "journal-a", Boot: "boot-a", Runtime: "runtime-a", Session: "session-a", Epoch: "1", Peer: "scheduler", CPU: 1000, Memory: 1073741824}
	store, err := agent.Open(filepath.Join(root, "state.db"), c)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := agent.NewRuntime(store, agent.RuntimeOptions{Root: filepath.Join(root, "work"), Network: "agent-container"})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store, runtime, c
}

func TestLocalCancellationClosesIdleInputAndStore(t *testing.T) {
	store, runtime, c := localStore(t)
	input, sender := io.Pipe()
	outputReader, output := io.Pipe()
	defer sender.Close()
	defer outputReader.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveLocal(ctx, input, output, store, runtime, c) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle stdin prevented shutdown")
	}
	if _, err := sender.Write([]byte("x")); err != io.ErrClosedPipe {
		t.Fatalf("input not closed: %v", err)
	}
	if _, err := store.Inspect(); err == nil {
		t.Fatal("store ownership retained after shutdown")
	}
}

func TestLocalExplicitShutdownAcknowledgesClosedStore(t *testing.T) {
	store, runtime, c := localStore(t)
	input, sender := io.Pipe()
	outputReader, output := io.Pipe()
	defer sender.Close()
	defer outputReader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serveLocal(ctx, input, output, store, runtime, c) }()
	if _, err := io.WriteString(sender, "{\"action\":\"shutdown\"}\n"); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(outputReader).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var reply map[string]any
	if err := json.Unmarshal(line, &reply); err != nil || reply["ok"] != true || reply["result"] != "shutdown" {
		t.Fatalf("reply %s %v", line, err)
	}
	if _, err := store.Inspect(); err == nil {
		t.Fatal("shutdown acknowledged before store closed")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("control loop stayed alive after shutdown")
	}
}
