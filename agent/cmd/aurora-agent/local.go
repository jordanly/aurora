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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	agent "aurora.local/agent"
	"aurora.local/agent/protocol"
)

func publicState(st agent.State) map[string]any {
	attempts := map[string]any{}
	for key, a := range st.Attempts {
		attempts[key] = map[string]any{
			"identity": a.Body["identity"], "reserved": a.Reserved(),
			"stopped": a.Stopped, "deadlineUnixMillis": a.Deadline,
			"execution": publicExecution(a.Execution),
		}
	}
	return map[string]any{"cursor": fmt.Sprint(st.Cursor), "ack": fmt.Sprint(st.Ack),
		"commands": st.Commands, "attempts": attempts, "observations": st.Observations}
}

// Keep local inspection independent of future private execution metadata.
func publicExecution(e *agent.Execution) any {
	if e == nil {
		return nil
	}
	return map[string]any{
		"phase": e.Phase, "pid": e.PID, "start": e.Start,
		"outcome": e.Outcome, "cleanup": e.Cleanup, "ready": e.Ready,
		"exitCode": e.ExitCode, "signal": e.Signal,
		"stdoutBytes": e.StdoutBytes, "stderrBytes": e.StderrBytes,
		"stdoutDropped": e.StdoutDropped, "stderrDropped": e.StderrDropped,
	}
}

// A stalled operator reader must not prevent cancellation and workload cleanup.
// The CLI owns these streams; closing them must interrupt outstanding I/O.
func writeReply(ctx context.Context, output io.WriteCloser, reply any) error {
	data, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	complete := make(chan error, 1)
	go func() {
		data = append(data, '\n')
		n, err := output.Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		complete <- err
	}()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	select {
	case err := <-complete:
		return err
	case <-ctx.Done():
		output.Close()
		return ctx.Err()
	case <-deadline.C:
		output.Close()
		return errors.New("local response reader stalled")
	}
}

func runLocal(store *agent.Store, config agent.Config, options agent.RuntimeOptions) error {
	runtime, err := agent.NewRuntime(store, options)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return serveLocal(ctx, os.Stdin, os.Stdout, store, runtime, config)
}

// serveLocal is an operator-owned stdin control lane, not a network endpoint.
// Each complete line receives one response. Workloads write only bounded log files.
// EOF, a signal, or an explicit shutdown drains the runtime before releasing its lock.
func serveLocal(ctx context.Context, input io.ReadCloser, output io.WriteCloser,
	store *agent.Store, runtime *agent.Runtime, config agent.Config) (err error) {
	finished := false
	var finishErr error
	finish := func() error {
		if finished {
			return finishErr
		}
		finished = true
		shutdown, cancel := context.WithTimeout(context.Background(), 65*time.Second)
		defer cancel()
		finishErr = errors.Join(runtime.Shutdown(shutdown), runtime.Close(), store.Close())
		return finishErr
	}
	defer func() {
		input.Close()
		err = errors.Join(err, finish())
		output.Close()
	}()
	type line struct {
		data []byte
		err  error
	}
	lines := make(chan line)
	done := make(chan struct{})
	defer close(done)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(input)
		scanner.Buffer(make([]byte, 4096), 1048578)
		for scanner.Scan() {
			data := append([]byte(nil), scanner.Bytes()...)
			if len(data) > 1048576 {
				select {
				case lines <- line{err: errors.New("input limit")}:
				case <-done:
				}
				return
			}
			select {
			case lines <- line{data: data}:
			case <-done:
				return
			}
		}
		if scanner.Err() != nil {
			select {
			case lines <- line{err: errors.New("input framing failed")}:
			case <-done:
			}
		}
	}()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	caller := agent.Caller{Peer: config.Peer, Session: config.Session, Epoch: config.Epoch}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := runtime.Tick(ctx); err != nil {
				return err
			}
		case request, open := <-lines:
			if !open {
				return nil
			}
			if request.err != nil {
				return request.err
			}
			value, decodeErr := protocol.Decode(request.data)
			reply := map[string]any{"ok": false, "error": "invalid local request"}
			if decodeErr == nil {
				if value["kind"] == "Delivery" {
					result, admitErr := store.Admit(request.data, caller)
					if admitErr == nil {
						reply = map[string]any{"ok": result.Outcome == "accepted", "result": result}
					} else {
						reply["error"] = "delivery rejected"
					}
				} else if len(value) == 1 && value["action"] == "inspect" {
					state, inspectErr := store.Inspect()
					if inspectErr != nil {
						return inspectErr
					}
					reply = map[string]any{"ok": true, "result": publicState(state)}
				} else if len(value) == 1 && value["action"] == "shutdown" {
					if shutdownErr := finish(); shutdownErr != nil {
						return shutdownErr
					}
					return writeReply(ctx, output, map[string]any{"ok": true, "result": "shutdown"})
				}
			}
			if err := writeReply(ctx, output, reply); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
		}
	}
}
