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
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
)

type Finding struct {
	Path        string `json:"path"`
	Disposition string `json:"disposition"`
	Detail      string `json:"detail"`
}
type resolvedExport struct {
	Version              string            `json:"version"`
	TrustedOfflineExport bool              `json:"trustedOfflineExport"`
	SourceDigests        []string          `json:"sourceDigests"`
	DefaultsApplied      bool              `json:"defaultsApplied"`
	Bindings             map[string]string `json:"bindings"`
	Task                 resolvedTask      `json:"task"`
}
type resolvedTask struct {
	MaxConcurrency   int               `json:"max_concurrency"`
	MaxFailures      int               `json:"max_failures"`
	FinalizationWait float64           `json:"finalization_wait"`
	Processes        []resolvedProcess `json:"processes"`
}
type resolvedProcess struct {
	Name         string            `json:"name"`
	Cmdline      string            `json:"cmdline"`
	Env          map[string]string `json:"env"`
	AfterSuccess []string          `json:"after_success"`
	Daemon       bool              `json:"daemon"`
	Ephemeral    bool              `json:"ephemeral"`
	Finalizer    bool              `json:"finalizer"`
	MaxFailures  int               `json:"max_failures"`
	MinDuration  float64           `json:"min_duration"`
}

// ConvertResolvedJSON accepts only this finite, explicitly trusted offline export
// format. It does not evaluate Python, Pystachio, bindings, profiles or imports.
// maxRuns is a mandatory operator-selected finite replacement for sys.maxsize.
func ConvertResolvedJSON(data []byte, maxRuns int) (m Manifest, findings []Finding, err error) {
	reject := func(e error) (Manifest, []Finding, error) {
		return Manifest{}, append(findings, Finding{"$", "rejected", e.Error()}), e
	}
	if len(data) > 1<<20 {
		return reject(errors.New("export exceeds 1MiB"))
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if e := unique(d); e != nil {
		return reject(e)
	}
	if _, e := d.Token(); e != io.EOF {
		return reject(errors.New("trailing JSON"))
	}
	var x resolvedExport
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if e := d.Decode(&x); e != nil {
		return reject(e)
	}
	if e := resolvedFields(data); e != nil {
		return reject(e)
	}
	if x.Version != "thermos-resolved-v1" || !x.TrustedOfflineExport || !x.DefaultsApplied || len(x.SourceDigests) == 0 || x.Bindings == nil {
		return reject(errors.New("trusted resolved export requires version, defaultsApplied, sourceDigests and explicit bindings"))
	}
	for _, s := range x.SourceDigests {
		b, e := hex.DecodeString(s)
		if e != nil || len(b) != 32 {
			return reject(errors.New("sourceDigests must contain SHA-256 hex digests"))
		}
	}
	millis := func(v float64) (int64, error) {
		if v < 0 || v > 3600 || math.Trunc(v*1000) != v*1000 {
			return 0, errors.New("duration requires exact nonnegative milliseconds within one hour")
		}
		return int64(v * 1000), nil
	}
	wait, e := millis(x.Task.FinalizationWait)
	if e != nil {
		return reject(e)
	}
	m = Manifest{Version: "task-v1alpha1", Semantics: "thermos-v1", MaxConcurrency: x.Task.MaxConcurrency, MaxRuns: maxRuns, TaskMaxFailures: x.Task.MaxFailures, FinalizationWaitMillis: wait, LogBytes: 65536}
	findings = append(findings, Finding{"task.max_failures", "retained", "task failed-process tolerance is separate from process failed-run limits"}, Finding{"task.maxRuns", "changed", "finite operator-selected guard replaces architecture-dependent total-run bound"}, Finding{"bindings", "retained", "provenance only; all commands and environment must already be resolved"})
	for i, p := range x.Task.Processes {
		delay, e := millis(p.MinDuration)
		if e != nil {
			return reject(e)
		}
		if p.Cmdline == "" || p.Env == nil || p.AfterSuccess == nil {
			return reject(fmt.Errorf("task.processes[%d] requires resolved cmdline, env and after_success", i))
		}
		m.Processes = append(m.Processes, Process{Name: p.Name, Argv: []string{"/bin/bash", "-c", p.Cmdline}, Env: p.Env, AfterSuccess: p.AfterSuccess, Daemon: p.Daemon, Ephemeral: p.Ephemeral, Finalizer: p.Finalizer, MaxFailedRuns: p.MaxFailures, RestartDelayMillis: delay})
		path := fmt.Sprintf("task.processes[%d]", i)
		findings = append(findings, Finding{path + ".cmdline", "retained", "explicit /bin/bash -c argv; no tokenization or interpolation"}, Finding{path + ".min_duration", "retained", "fixed delay measured after exit"}, Finding{path + ".max_failures", "retained", "failed-run budget; zero remains unlimited, bounded by task maxRuns"})
	}
	if e = m.Normalize(); e != nil {
		return reject(e)
	}
	return m, findings, nil
}

// Resolved exports must carry the actual defaults. In particular, an omitted
// max_failures must never silently become zero (unlimited) through Go decoding.
func resolvedFields(data []byte) error {
	fields := func(raw json.RawMessage, names ...string) (map[string]json.RawMessage, error) {
		var object map[string]json.RawMessage
		if e := json.Unmarshal(raw, &object); e != nil {
			return nil, e
		}
		for _, name := range names {
			if _, ok := object[name]; !ok {
				return nil, fmt.Errorf("resolved export missing explicit field %s", name)
			}
		}
		return object, nil
	}
	root, e := fields(data, "version", "trustedOfflineExport", "sourceDigests", "defaultsApplied", "bindings", "task")
	if e != nil {
		return e
	}
	task, e := fields(root["task"], "max_concurrency", "max_failures", "finalization_wait", "processes")
	if e != nil {
		return e
	}
	var processes []json.RawMessage
	if e = json.Unmarshal(task["processes"], &processes); e != nil {
		return e
	}
	for _, process := range processes {
		if _, e = fields(process, "name", "cmdline", "env", "after_success", "daemon", "ephemeral", "finalizer", "max_failures", "min_duration"); e != nil {
			return e
		}
	}
	return nil
}
