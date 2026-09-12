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

// Package task implements the bounded, resolved-JSON Thermos process subset.
package task

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

type Manifest struct {
	Version                string    `json:"version"`
	Semantics              string    `json:"semantics"`
	MaxConcurrency         int       `json:"maxConcurrency"`
	MaxRuns                int       `json:"maxRuns"`
	TaskMaxFailures        int       `json:"taskMaxFailures"`
	FinalizationWaitMillis int64     `json:"finalizationWaitMillis"`
	LogBytes               int64     `json:"logBytes"`
	Processes              []Process `json:"processes"`
}
type Process struct {
	Name               string            `json:"name"`
	Argv               []string          `json:"argv"`
	Env                map[string]string `json:"env"`
	AfterSuccess       []string          `json:"afterSuccess"`
	Daemon             bool              `json:"daemon"`
	Ephemeral          bool              `json:"ephemeral"`
	Optional           bool              `json:"optional"`
	Finalizer          bool              `json:"finalizer"`
	MaxFailedRuns      int               `json:"maxFailedRuns"`
	UnlimitedFailures  bool              `json:"unlimitedFailures"`
	RestartDelayMillis int64             `json:"restartDelayMillis"`
}

var nameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

// Decode rejects duplicate/unknown keys and produces a deterministic, defaulted form.
func Decode(data []byte) (Manifest, []byte, error) {
	var m Manifest
	if len(data) > 1<<20 {
		return m, nil, errors.New("task manifest exceeds 1MiB")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if e := unique(d); e != nil {
		return m, nil, e
	}
	if _, e := d.Token(); e != io.EOF {
		return m, nil, errors.New("trailing JSON")
	}
	if e := exactFields(data, reflect.TypeOf(m), false, "$"); e != nil {
		return m, nil, e
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if e := d.Decode(&m); e != nil {
		return m, nil, e
	}
	if e := m.Normalize(); e != nil {
		return m, nil, e
	}
	b, e := json.Marshal(m)
	return m, b, e
}

// encoding/json accepts case-insensitive struct field aliases even with
// DisallowUnknownFields. Validate exact JSON tags first, without restricting
// arbitrary, case-sensitive keys in environment and provenance maps.
func exactFields(data json.RawMessage, typ reflect.Type, required bool, path string) error {
	switch typ.Kind() {
	case reflect.Struct:
		var object map[string]json.RawMessage
		if e := json.Unmarshal(data, &object); e != nil {
			return e
		}
		fields := map[string]reflect.Type{}
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name != "" && name != "-" {
				fields[name] = field.Type
				if _, ok := object[name]; required && !ok {
					return fmt.Errorf("resolved export missing explicit field %s.%s", path, name)
				}
			}
		}
		for name, value := range object {
			field, ok := fields[name]
			if !ok {
				return fmt.Errorf("unknown field %s.%s", path, name)
			}
			if e := exactFields(value, field, required, path+"."+name); e != nil {
				return e
			}
		}
	case reflect.Slice:
		var values []json.RawMessage
		if e := json.Unmarshal(data, &values); e != nil {
			return e
		}
		for i, value := range values {
			if e := exactFields(value, typ.Elem(), required, fmt.Sprintf("%s[%d]", path, i)); e != nil {
				return e
			}
		}
	}
	return nil
}

func unique(d *json.Decoder) error {
	t, e := d.Token()
	if e != nil {
		return e
	}
	delim, ok := t.(json.Delim)
	if !ok {
		if t == nil {
			return errors.New("null is not accepted")
		}
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			k, e := d.Token()
			if e != nil {
				return e
			}
			s := k.(string)
			if seen[s] {
				return fmt.Errorf("duplicate key %q", s)
			}
			seen[s] = true
			if e = unique(d); e != nil {
				return e
			}
		}
	case '[':
		for d.More() {
			if e = unique(d); e != nil {
				return e
			}
		}
	default:
		return errors.New("invalid JSON")
	}
	_, e = d.Token()
	return e
}
func (m *Manifest) Normalize() error {
	if m.Version != "task-v1alpha1" {
		return errors.New("unsupported task version")
	}
	if m.Semantics != "native-v1" && m.Semantics != "thermos-v1" {
		return errors.New("unsupported semantics")
	}
	if len(m.Processes) == 0 || len(m.Processes) > 128 || m.MaxConcurrency < 0 || m.MaxConcurrency > 128 || m.MaxRuns < 1 || m.MaxRuns > 10000 {
		return errors.New("invalid process/concurrency/run bounds")
	}
	if m.TaskMaxFailures < 0 || m.TaskMaxFailures > 128 || m.Semantics == "native-v1" && m.TaskMaxFailures != 0 {
		return errors.New("task failure tolerance is compatibility-only")
	}
	if m.FinalizationWaitMillis < 0 || m.FinalizationWaitMillis > 300000 {
		return errors.New("finalization budget must be 0..300000ms")
	}
	if m.LogBytes == 0 {
		m.LogBytes = 65536
	}
	if m.LogBytes < 1024 || m.LogBytes > 1<<20 {
		return errors.New("logBytes must be 1KiB..1MiB")
	}
	names := map[string]*Process{}
	for i := range m.Processes {
		p := &m.Processes[i]
		if !nameRE.MatchString(p.Name) || names[p.Name] != nil {
			return errors.New("invalid/duplicate process name")
		}
		names[p.Name] = p
		if len(p.Argv) == 0 || len(p.Argv) > 256 || !filepath.IsAbs(p.Argv[0]) {
			return fmt.Errorf("%s requires absolute executable argv", p.Name)
		}
		for _, a := range p.Argv {
			if strings.ContainsRune(a, 0) {
				return errors.New("NUL in argv")
			}
		}
		if p.Env == nil {
			p.Env = map[string]string{}
		}
		for k, v := range p.Env {
			if k == "" || strings.ContainsAny(k, "=\x00") || strings.ContainsRune(v, 0) {
				return errors.New("invalid environment")
			}
		}
		if p.AfterSuccess == nil {
			p.AfterSuccess = []string{}
		}
		sort.Strings(p.AfterSuccess)
		if p.MaxFailedRuns < 0 || p.MaxFailedRuns > 10000 || p.RestartDelayMillis < 0 || p.RestartDelayMillis > 3600000 {
			return errors.New("invalid retry bound")
		}
		if m.Semantics == "native-v1" && p.MaxFailedRuns == 0 && !p.UnlimitedFailures {
			p.MaxFailedRuns = 1
		}
		if p.UnlimitedFailures && p.MaxFailedRuns != 0 {
			return errors.New("ambiguous failure budget")
		}
		if p.Finalizer && (p.Daemon || p.Ephemeral) {
			return errors.New("finalizers cannot be daemon/ephemeral")
		}
	}
	for _, p := range m.Processes {
		seen := map[string]bool{}
		for _, n := range p.AfterSuccess {
			q := names[n]
			if q == nil || seen[n] || q.Finalizer != p.Finalizer || q.Daemon || (!p.Ephemeral && q.Ephemeral) {
				return fmt.Errorf("invalid dependency %s after %s", p.Name, n)
			}
			seen[n] = true
		}
	}
	visiting, done := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(n string) error {
		if visiting[n] {
			return errors.New("dependency cycle")
		}
		if done[n] {
			return nil
		}
		visiting[n] = true
		for _, d := range names[n].AfterSuccess {
			if e := visit(d); e != nil {
				return e
			}
		}
		visiting[n] = false
		done[n] = true
		return nil
	}
	for n := range names {
		if e := visit(n); e != nil {
			return e
		}
	}
	sort.Slice(m.Processes, func(i, j int) bool { return m.Processes[i].Name < m.Processes[j].Name })
	b, e := json.Marshal(m)
	if e != nil {
		return e
	}
	if len(b) > 4096 {
		return errors.New("canonical task manifest exceeds 4096-byte outer argv limit")
	}
	return nil
}
