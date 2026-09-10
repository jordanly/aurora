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
	agent "aurora.local/agent"
	"aurora.local/agent/protocol"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
)

func read(path string) ([]byte, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, 1048577))
	if len(b) > 1048576 {
		return nil, fmt.Errorf("input limit")
	}
	return b, e
}
func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("version|validate|admit|inspect")
	}
	if os.Args[1] == "version" {
		return json.NewEncoder(os.Stdout).Encode(map[string]string{"version": "native-v1alpha1-admission", "store": "bbolt-v1.5.0"})
	}
	f := flag.NewFlagSet(os.Args[1], flag.ContinueOnError)
	config := f.String("config", "", "trusted local enrollment configuration")
	state := f.String("state", "", "exclusive local state file")
	command := f.String("command", "", "Delivery JSON file")
	document := f.String("document", "", "protocol JSON file")
	if e := f.Parse(os.Args[2:]); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	if os.Args[1] == "validate" {
		b, e := read(*document)
		if e != nil {
			return e
		}
		v, e := protocol.Validate(b)
		if e != nil {
			return e
		}
		_, e = os.Stdout.Write(append(protocol.Canonical(v), '\n'))
		return e
	}
	if os.Args[1] != "admit" && os.Args[1] != "inspect" {
		return fmt.Errorf("unknown action")
	}
	b, e := read(*config)
	if e != nil {
		return e
	}
	c, e := agent.ReadConfig(b)
	if e != nil {
		return e
	}
	s, e := agent.Open(*state, c)
	if e != nil {
		return e
	}
	defer s.Close()
	var result any
	if os.Args[1] == "inspect" {
		var st agent.State
		st, e = s.Inspect()
		attempts := map[string]any{}
		for k, a := range st.Attempts {
			attempts[k] = map[string]any{"identity": a.Body["identity"], "reserved": a.Body["kind"] == "Run", "stopped": a.Stopped, "deadlineUnixMillis": a.Deadline}
		}
		result = map[string]any{"cursor": fmt.Sprint(st.Cursor), "ack": fmt.Sprint(st.Ack), "commands": st.Commands, "attempts": attempts, "observations": st.Observations}
	} else {
		b, e = read(*command)
		if e != nil {
			return e
		}
		var r agent.Result
		r, e = s.Admit(b, agent.Caller{Peer: c.Peer, Session: c.Session, Epoch: c.Epoch})
		result = r
		if e == nil && r.Outcome != "accepted" {
			json.NewEncoder(os.Stdout).Encode(r)
			return fmt.Errorf("admission rejected")
		}
	}
	if e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
func main() {
	if e := run(); e != nil {
		json.NewEncoder(os.Stderr).Encode(map[string]string{"error": e.Error()})
		os.Exit(1)
	}
}
