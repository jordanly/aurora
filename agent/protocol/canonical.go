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

// Standalone canonical JSON adapter; schema/semantic validation is in check.py.
package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
)

func printableASCII(value string) error {
	for _, r := range value {
		if r < 32 || r > 126 {
			return fmt.Errorf("non-printable ASCII")
		}
	}
	return nil
}

// Token parsing rejects duplicate keys, which decoding into a map would hide.
func parse(d *json.Decoder, depth int) (interface{}, error) {
	if depth > 64 {
		return nil, fmt.Errorf("nesting limit")
	}
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch v := t.(type) {
	case json.Delim:
		if v == '{' {
			m := map[string]interface{}{}
			for d.More() {
				key, e := d.Token()
				if e != nil {
					return nil, e
				}
				s, ok := key.(string)
				if !ok {
					return nil, fmt.Errorf("object key")
				}
				if e := printableASCII(s); e != nil {
					return nil, e
				}
				if _, exists := m[s]; exists {
					return nil, fmt.Errorf("duplicate key")
				}
				item, e := parse(d, depth+1)
				if e != nil {
					return nil, e
				}
				m[s] = item
			}
			end, e := d.Token()
			if e != nil || end != json.Delim('}') {
				return nil, fmt.Errorf("object end")
			}
			return m, nil
		}
		if v == '[' {
			a := []interface{}{}
			for d.More() {
				item, e := parse(d, depth+1)
				if e != nil {
					return nil, e
				}
				a = append(a, item)
			}
			end, e := d.Token()
			if e != nil || end != json.Delim(']') {
				return nil, fmt.Errorf("array end")
			}
			return a, nil
		}
		return nil, fmt.Errorf("delimiter")
	case json.Number:
		n, e := strconv.ParseUint(string(v), 10, 64)
		if e != nil || n > 9007199254740991 {
			return nil, fmt.Errorf("noncanonical integer")
		}
		return n, nil
	case string:
		if e := printableASCII(v); e != nil {
			return nil, e
		}
		return v, nil
	default:
		return t, nil
	}
}

func Decode(data []byte) (map[string]any, error) {
	if len(data) > 1048576 {
		return nil, fmt.Errorf("input limit")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	v, e := parse(d, 0)
	if e != nil {
		return nil, e
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, fmt.Errorf("trailing input")
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("object required")
	}
	return m, nil
}
func Canonical(v any) []byte {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		panic(err)
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n"))
}
