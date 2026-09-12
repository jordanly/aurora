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

import "time"

type ProcessState struct {
	Runs       int       `json:"runs"`
	FailedRuns int       `json:"failedRuns"`
	Status     string    `json:"status"`
	NextStart  time.Time `json:"nextStart"`
}
type Planner struct {
	Manifest  Manifest
	States    map[string]*ProcessState
	TotalRuns int
}

func NewPlanner(m Manifest) *Planner {
	p := &Planner{Manifest: m, States: map[string]*ProcessState{}}
	for _, s := range m.Processes {
		p.States[s.Name] = &ProcessState{Status: "pending"}
	}
	return p
}
func (p *Planner) Runnable(now time.Time, final bool) []string {
	out := []string{}
	running := 0
	for _, s := range p.States {
		if s.Status == "running" {
			running++
		}
	}
	limit := p.Manifest.MaxConcurrency
	if limit == 0 {
		limit = len(p.Manifest.Processes)
	}
	for _, s := range p.Manifest.Processes {
		st := p.States[s.Name]
		if s.Finalizer != final || st.Status != "pending" || now.Before(st.NextStart) {
			continue
		}
		ready := true
		for _, dep := range s.AfterSuccess {
			if p.States[dep].Status != "succeeded" {
				ready = false
			}
		}
		if ready && len(out)+running < limit && len(out)+p.TotalRuns < p.Manifest.MaxRuns {
			out = append(out, s.Name)
		}
	}
	return out
}
func (p *Planner) Start(n string) { s := p.States[n]; s.Status = "running"; s.Runs++; p.TotalRuns++ }

// Exit's lost outcome consumes the total-run guard but not the failed-run budget.
func (p *Planner) Exit(n, outcome string, now time.Time) {
	s := p.States[n]
	var spec Process
	for _, q := range p.Manifest.Processes {
		if q.Name == n {
			spec = q
			break
		}
	}
	s.NextStart = now.Add(time.Duration(spec.RestartDelayMillis) * time.Millisecond)
	if outcome == "succeeded" && !spec.Daemon {
		s.Status = "succeeded"
		return
	}
	if outcome == "failed" {
		s.FailedRuns++
	}
	if outcome == "failed" && spec.MaxFailedRuns > 0 && s.FailedRuns >= spec.MaxFailedRuns {
		s.Status = "failed"
		if spec.Ephemeral {
			s.Status = "finished"
		}
		return
	}
	s.Status = "pending"
}

// Result is empty while required work can advance; failed predecessors never release successors.
func (p *Planner) Result(final bool) string {
	failed, pending, running := 0, false, false
	canAdvance := false
	var possible func(string) bool
	possible = func(n string) bool {
		st := p.States[n].Status
		if st == "succeeded" || st == "running" {
			return true
		}
		if st != "pending" {
			return false
		}
		for _, q := range p.Manifest.Processes {
			if q.Name == n {
				for _, dep := range q.AfterSuccess {
					if !possible(dep) {
						return false
					}
				}
			}
		}
		return true
	}
	for _, q := range p.Manifest.Processes {
		if q.Finalizer != final {
			continue
		}
		s := p.States[q.Name]
		if !final && (q.Ephemeral || q.Optional) {
			continue
		}
		switch s.Status {
		case "running":
			running = true
		case "failed":
			failed++
		case "pending":
			pending = true
			if possible(q.Name) {
				canAdvance = true
			}
		}
	}
	// Legacy task failure tolerance is a live health threshold, not merely a
	// completion check. An exhausted process must stop a running daemon sibling.
	if !final && p.Manifest.Semantics == "thermos-v1" && p.Manifest.TaskMaxFailures > 0 && failed >= p.Manifest.TaskMaxFailures {
		return "failed"
	}
	if running {
		return ""
	}
	if pending && canAdvance && p.TotalRuns < p.Manifest.MaxRuns {
		return ""
	}
	if pending {
		return "failed"
	}
	if failed > 0 && (final || p.Manifest.Semantics == "native-v1" || p.Manifest.TaskMaxFailures > 0 && failed >= p.Manifest.TaskMaxFailures) {
		return "failed"
	}
	return "succeeded"
}
