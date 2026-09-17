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
	"crypto/sha256"
	_ "embed"
	"fmt"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

//go:embed schema.json
var schemaBytes []byte
var schema, schemaErr = compileSchema(schemaBytes)

type offlineLoader struct{}

func (offlineLoader) Load(string) (any, error) {
	return nil, fmt.Errorf("external schema loading disabled")
}

// The profile uses exactly one ECMAScript-only construct: an absolute-end
// negative lookahead. Translate only that exact suffix to Go's absolute-end
// assertion; regexp.Compile rejects other unsupported constructs.
func profileRegexp(pattern string) (jsonschema.Regexp, error) {
	const end = `(?![\s\S])`
	if strings.HasSuffix(pattern, end) {
		pattern = strings.TrimSuffix(pattern, end) + `\z`
	}
	if strings.Contains(pattern, "(?") {
		return nil, fmt.Errorf("unsupported regular expression construct")
	}
	return regexp.Compile(pattern)
}
func compileSchema(data []byte) (*jsonschema.Schema, error) {
	value, err := Decode(data)
	if err != nil {
		return nil, fmt.Errorf("embedded schema parse: %w", err)
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.UseLoader(offlineLoader{})
	c.UseRegexpEngine(profileRegexp)
	const location = "urn:aurora:native-v1alpha1"
	if err = c.AddResource(location, value); err != nil {
		return nil, err
	}
	return c.Compile(location)
}

func Digest(v any) string { return fmt.Sprintf("%x", sha256.Sum256(Canonical(v))) }
func Validate(data []byte) (map[string]any, error) {
	if schemaErr != nil {
		return nil, fmt.Errorf("embedded schema initialization: %w", schemaErr)
	}
	v, e := Decode(data)
	if e != nil {
		return nil, e
	}
	if e = schema.Validate(v); e != nil {
		return nil, e
	}
	if e = semantics(v); e != nil {
		return nil, e
	}
	return v, nil
}
func semantics(v map[string]any) error {
	var walk func(any) error
	walk = func(x any) error {
		switch m := x.(type) {
		case map[string]any:
			for k, a := range m {
				switch k {
				case "ticket", "revision", "desiredRevision", "schedulerEpoch", "sequence", "cursor", "generation", "watermark", "committedCursor":
					if _, e := strconv.ParseUint(a.(string), 10, 64); e != nil {
						return e
					}
				}
				if e := walk(a); e != nil {
					return e
				}
			}
		case []any:
			for _, a := range m {
				if e := walk(a); e != nil {
					return e
				}
			}
		}
		return nil
	}
	if e := walk(v); e != nil {
		return e
	}
	p, _ := v["template"].(map[string]any)
	if p == nil {
		p, _ = v["assignment"].(map[string]any)
	}
	if p != nil {
		args := p["argv"].([]any)
		exe, ok := args[0].(string)
		if !ok || !strings.HasPrefix(exe, "/") {
			return fmt.Errorf("absolute executable required")
		}
		caps := p["requiredCapabilities"].([]any)
		if p["resources"].(map[string]any)["memoryEnforcement"] == "hard" && len(caps) == 0 {
			return fmt.Errorf("hard memory capability required")
		}
		names := map[any]bool{}
		sockets := map[string]bool{}
		for _, x := range p["ports"].([]any) {
			port := x.(map[string]any)
			if names[port["name"]] {
				return fmt.Errorf("duplicate port")
			}
			names[port["name"]] = true
			if v["kind"] == "Run" {
				socket := fmt.Sprint(port["network"], port["family"], port["protocol"], port["number"])
				if sockets[socket] {
					return fmt.Errorf("duplicate socket")
				}
				sockets[socket] = true
			}
		}
		for _, a := range args {
			if m, ok := a.(map[string]any); ok && !names[m["portRef"]] {
				return fmt.Errorf("port reference")
			}
		}
		r := p["readiness"].(map[string]any)
		if r["kind"] == "tcp" && !names[r["port"]] {
			return fmt.Errorf("readiness port")
		}
		if v["kind"] == "Run" && v["identity"].(map[string]any)["process"] != p["process"] {
			return fmt.Errorf("process identity")
		}
	}
	if v["kind"] == "Delivery" {
		b := v["body"].(map[string]any)
		a := v["authority"].(map[string]any)
		i := b["identity"].(map[string]any)
		if a["cluster"] != i["cluster"] || a["incarnation"] != i["incarnation"] || v["bodySha256"] != Digest(b) {
			return fmt.Errorf("delivery scope or digest")
		}
		return semantics(b)
	}
	return nil
}

// RequireCapabilities checks verified agent availability separately from wire validity.
func RequireCapabilities(message map[string]any, advertised []string) error {
	if message["kind"] == "Delivery" {
		message = message["body"].(map[string]any)
	}
	p, _ := message["template"].(map[string]any)
	if p == nil {
		p, _ = message["assignment"].(map[string]any)
	}
	if p == nil {
		return fmt.Errorf("no process requirements")
	}
	have := map[string]bool{}
	for _, c := range advertised {
		have[c] = true
	}
	for _, c := range p["requiredCapabilities"].([]any) {
		if !have[c.(string)] {
			return fmt.Errorf("missing agent capability")
		}
	}
	return nil
}

// RequireResolution requires already validated Job and Run values.
func RequireResolution(job, run map[string]any) error {
	if job["kind"] != "Job" || run["kind"] != "Run" {
		return fmt.Errorf("Job and Run required")
	}
	id := run["identity"].(map[string]any)
	for _, k := range []string{"cluster", "incarnation", "jobKey"} {
		if !reflect.DeepEqual(job[k], id[k]) {
			return fmt.Errorf("job identity mismatch")
		}
	}
	if job["revision"] != run["desiredRevision"] || Digest(job["template"]) != run["templateSha256"] {
		return fmt.Errorf("revision or template mismatch")
	}
	template, _ := Decode(Canonical(job["template"]))
	assignment := run["assignment"].(map[string]any)
	ports := assignment["ports"].([]any)
	declarations := template["ports"].([]any)
	if len(ports) != len(declarations) {
		return fmt.Errorf("port count mismatch")
	}
	numbers := map[string]uint64{}
	for i, p := range ports {
		port := p.(map[string]any)
		decl := declarations[i].(map[string]any)
		for _, k := range []string{"name", "protocol", "family"} {
			if port[k] != decl[k] {
				return fmt.Errorf("port declaration mismatch")
			}
		}
		numbers[port["name"].(string)] = port["number"].(uint64)
	}
	args := template["argv"].([]any)
	for i, a := range args {
		if ref, ok := a.(map[string]any); ok {
			args[i] = strconv.FormatUint(numbers[ref["portRef"].(string)], 10)
		}
	}
	template["ports"] = ports
	if !reflect.DeepEqual(template, assignment) {
		return fmt.Errorf("assignment does not resolve template")
	}
	return nil
}
