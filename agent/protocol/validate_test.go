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

package protocol

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSharedCorpus(t *testing.T) {
	root := "../../protocol/native-v1alpha1"
	original, e := os.ReadFile(root + "/schema.json")
	if e != nil || !bytes.Equal(original, schemaBytes) {
		t.Fatal("embedded schema drift; copy authoritative protocol/native-v1alpha1/schema.json")
	}
	var gold map[string]struct {
		Canonical string
		SHA256    string
	}
	b, _ := os.ReadFile(root + "/fixtures/golden.json")
	json.Unmarshal(b, &gold)
	for _, group := range []string{"valid", "invalid", "parser-invalid"} {
		paths, _ := filepath.Glob(root + "/fixtures/" + group + "/*.json")
		if len(paths) == 0 {
			t.Fatal("missing corpus")
		}
		for _, p := range paths {
			t.Run(group+"/"+filepath.Base(p), func(t *testing.T) {
				b, _ := os.ReadFile(p)
				v, e := Validate(b)
				if group != "valid" {
					if e == nil {
						t.Fatal("invalid accepted")
					}
					return
				}
				if e != nil {
					t.Fatal(e)
				}
				g := gold[filepath.Base(p)[:len(filepath.Base(p))-5]]
				if string(Canonical(v)) != g.Canonical || Digest(v) != g.SHA256 {
					t.Fatal("canonical/hash mismatch")
				}
			})
		}
	}
}
func TestResolutionAndCapabilities(t *testing.T) {
	read := func(name string) map[string]any {
		b, e := os.ReadFile("../../protocol/native-v1alpha1/fixtures/valid/" + name + ".json")
		if e != nil {
			t.Fatal(e)
		}
		v, e := Validate(b)
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	job := read("service")
	for _, name := range []string{"service-run-a", "service-run-b"} {
		run := read(name)
		if e := RequireResolution(job, run); e != nil {
			t.Fatal(e)
		}
		run["desiredRevision"] = "999"
		if RequireResolution(job, run) == nil {
			t.Fatal("revision accepted")
		}
	}
	hard := read("batch")
	p := hard["template"].(map[string]any)
	p["resources"].(map[string]any)["memoryEnforcement"] = "hard"
	p["requiredCapabilities"] = []any{"hard-memory"}
	if _, e := Validate(Canonical(hard)); e != nil {
		t.Fatal(e)
	}
	if RequireCapabilities(hard, nil) == nil {
		t.Fatal("missing capability accepted")
	}
	if e := RequireCapabilities(hard, []string{"hard-memory"}); e != nil {
		t.Fatal(e)
	}
}

func TestSchemaInitializationFailsClosed(t *testing.T) {
	for _, data := range []string{`{`, `{"type":"not-a-json-schema-type"}`, `{"$ref":"https://example.invalid/remote"}`, `{"type":"string","pattern":"(?=x)x"}`} {
		if _, err := compileSchema([]byte(data)); err == nil {
			t.Fatalf("invalid schema compiled: %s", data)
		}
	}
	s, err := compileSchema([]byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"array","contains":{"const":"required"},"minContains":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if s.Validate([]any{"missing"}) == nil {
		t.Fatal("draft keyword ignored")
	}
	if err = s.Validate([]any{"required"}); err != nil {
		t.Fatal(err)
	}
	re, err := profileRegexp(`^[a-z]+(?![\s\S])`)
	if err != nil || !re.MatchString("abc") || re.MatchString("abc\n") {
		t.Fatal("absolute end semantics", err)
	}
}

func TestHealthBoundsAndPairedPolicy(t *testing.T) {
	raw, err := os.ReadFile("../../protocol/native-v1alpha1/fixtures/valid/service-run-a.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		startup, failures uint64
		valid             bool
	}{
		{1, 1, true}, {600000, 100, true}, {0, 1, false}, {1, 0, false},
		{600001, 1, false}, {1, 101, false},
	} {
		value, err := Validate(raw)
		if err != nil {
			t.Fatal(err)
		}
		health := value["assignment"].(map[string]any)["readiness"].(map[string]any)
		health["startupTimeoutMillis"] = item.startup
		health["failureThreshold"] = item.failures
		_, err = Validate(Canonical(value))
		if (err == nil) != item.valid {
			t.Fatalf("%+v: %v", item, err)
		}
		delete(health, "startupTimeoutMillis")
		if _, err = Validate(Canonical(value)); err == nil {
			t.Fatal("unpaired health policy accepted")
		}
	}
}
