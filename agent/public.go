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

package agent

import "fmt"

func PublicState(st State) map[string]any {
	attempts := map[string]any{}
	for key, a := range st.Attempts {
		attempts[key] = map[string]any{
			"identity": a.Body["identity"], "sequence": fmt.Sprint(a.Sequence), "reserved": a.Reserved(),
			"stopped": a.Stopped, "deadlineUnixMillis": a.Deadline,
			"execution": PublicExecution(a.Execution),
		}
	}
	var retention any
	if st.Retention != nil {
		retention = map[string]any{"version": st.Retention.Version, "retiredIntervals": len(st.Retention.Retired), "pendingGarbage": len(st.Retention.Garbage)}
	}
	return map[string]any{"retention": retention, "cursor": fmt.Sprint(st.Cursor), "ack": fmt.Sprint(st.Ack),
		"commands": st.Commands, "attempts": attempts, "observations": st.Observations}
}

// Keep local inspection independent of future private execution metadata.
func PublicExecution(e *Execution) any {
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

// transportState separates bounded reconciliation inventory from permanent replay
// history. Terminal facts travel in the observation journal; all reservations
// remain visible until cleanup is durably complete. Command results accompany
// their observation page and remain replayable through Admit after that page.
func transportState(st State, observations []map[string]any) map[string]any {
	attempts := make(map[string]Attempt)
	for key, attempt := range st.Attempts {
		if attempt.Reserved() {
			attempts[key] = attempt
		}
	}
	cursors := make(map[string]bool, len(observations))
	for _, observation := range observations {
		cursors[observation["cursor"].(string)] = true
	}
	commands := make(map[string]Result)
	for key, result := range st.Commands {
		if cursors[result.Cursor] {
			commands[key] = result
		}
	}
	st.Attempts, st.Commands, st.Observations = attempts, commands, observations
	return PublicState(st)
}

func reservationCount(st State) int {
	count := 0
	for _, attempt := range st.Attempts {
		if attempt.Reserved() {
			count++
		}
	}
	return count
}
