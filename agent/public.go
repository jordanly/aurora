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
	return map[string]any{"cursor": fmt.Sprint(st.Cursor), "ack": fmt.Sprint(st.Ack),
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
