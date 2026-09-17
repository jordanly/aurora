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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	agent "aurora.local/agent"
	"aurora.local/agent/protocol"
)

func TestCompactCommandCreatesOfflineCopy(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	destination := filepath.Join(dir, "copy")
	configPath := filepath.Join(dir, "config.json")
	cfg := agent.Config{Cluster: "lab", Incarnation: "recovery-a", Node: "agent-a", Journal: "journal-a", Boot: "boot-a", Runtime: "runtime-a", Session: "session-a", Epoch: "1", Peer: "scheduler", CPU: 1000, Memory: 1073741824}
	s, err := agent.Open(source, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(configPath, protocol.Canonical(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	output, err := os.CreateTemp(dir, "output-")
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	savedArgs, savedStdout := os.Args, os.Stdout
	defer func() { os.Args, os.Stdout = savedArgs, savedStdout }()
	os.Args = []string{"aurora-agent", "compact", "--config", configPath, "--state", source, "--output", destination}
	os.Stdout = output
	if err = run(); err != nil {
		t.Fatal(err)
	}
	if _, err = output.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	var result agent.CompactResult
	if err = json.NewDecoder(output).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Source != source || result.Destination != destination || result.SourceBytes == 0 || result.DestinationBytes == 0 {
		t.Fatal(result)
	}
	copied, err := agent.Open(destination, cfg)
	if err != nil {
		t.Fatal(err)
	}
	copied.Close()
	if err = run(); err == nil {
		t.Fatal("CLI overwrote existing output")
	}
}
