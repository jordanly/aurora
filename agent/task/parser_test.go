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
	"encoding/json"
	"strings"
	"testing"
)

func TestManifestExactFieldNames(t *testing.T) {
	good := `{"version":"task-v1alpha1","semantics":"native-v1","maxRuns":2,"processes":[{"name":"a","argv":["/bin/true"],"env":{"FOO":"upper","foo":"lower"}}]}`
	m, _, err := Decode([]byte(good))
	if err != nil {
		t.Fatal(err)
	}
	if m.LogBytes != 65536 || m.Processes[0].MaxFailedRuns != 1 || m.Processes[0].Env["FOO"] != "upper" || m.Processes[0].Env["foo"] != "lower" {
		t.Fatalf("defaults or case-sensitive environment changed: %+v", m)
	}
	for _, tc := range []struct{ name, from, to string }{
		{"root alias", `"maxRuns":2`, `"MaxRuns":2`},
		{"root override", `"maxRuns":2`, `"maxRuns":2,"MAXRUNS":10000`},
		{"process alias", `"name":"a"`, `"Name":"a"`},
		{"process override", `"name":"a"`, `"name":"a","NAME":"b"`},
		{"defaulted alias", `"name":"a"`, `"name":"a","MaxFailedRuns":0`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := Decode([]byte(strings.Replace(good, tc.from, tc.to, 1))); err == nil {
				t.Fatal("accepted nonexact schema field")
			}
		})
	}
}

func TestResolvedExportExactFieldNames(t *testing.T) {
	x := resolvedExport{
		Version: "thermos-resolved-v1", TrustedOfflineExport: true, DefaultsApplied: true,
		SourceDigests: []string{strings.Repeat("a", 64)},
		Bindings:      map[string]string{"FOO": "upper", "foo": "lower"},
		Task: resolvedTask{MaxFailures: 1, FinalizationWait: 1, Processes: []resolvedProcess{{
			Name: "a", Cmdline: "exit 0", Env: map[string]string{"FOO": "upper", "foo": "lower"},
			AfterSuccess: []string{}, MaxFailures: 1,
		}}},
	}
	data, err := json.Marshal(x)
	if err != nil {
		t.Fatal(err)
	}
	good := string(data)
	m, findings, err := ConvertResolvedJSON(data, 10)
	if err != nil {
		t.Fatal(err)
	}
	if m.Processes[0].Env["FOO"] != "upper" || m.Processes[0].Env["foo"] != "lower" {
		t.Fatal("case-sensitive map keys changed")
	}
	for _, path := range []string{"task.maxRuns", "task.finalization_wait", "task.processes.after_success"} {
		found := false
		for _, finding := range findings {
			if finding.Path == path && finding.Disposition == "changed" {
				found = true
			}
		}
		if !found {
			t.Errorf("missing changed finding for %s", path)
		}
	}
	for _, tc := range []struct{ name, from, to string }{
		{"root alias", `"version":`, `"Version":`},
		{"root override", `"version":`, `"VERSION":"thermos-resolved-v1","version":`},
		{"task alias", `"max_failures":1`, `"MAX_FAILURES":1`},
		{"task budget override", `"max_failures":1`, `"max_failures":1,"MAX_FAILURES":0`},
		{"process alias", `"name":"a"`, `"NAME":"a"`},
		{"process override", `"name":"a"`, `"name":"a","NAME":"b"`},
		{"process budget override", `"name":"a"`, `"name":"a","MAX_FAILURES":0`},
		{"process budget alias", `"max_failures":1,"min_duration"`, `"MAX_FAILURES":1,"min_duration"`},
		{"missing explicit default", `"daemon":false,`, ``},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := strings.Replace(good, tc.from, tc.to, 1)
			if bad == good {
				t.Fatal("test mutation did not match")
			}
			if _, findings, err := ConvertResolvedJSON([]byte(bad), 10); err == nil || len(findings) == 0 || findings[len(findings)-1].Disposition != "rejected" {
				t.Fatalf("nonexact/missing schema field not rejected: %v %v", err, findings)
			}
		})
	}
}
