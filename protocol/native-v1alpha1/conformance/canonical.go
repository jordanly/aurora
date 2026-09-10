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
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
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
func parse(d *json.Decoder) (interface{}, error) {
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
				item, e := parse(d)
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
				item, e := parse(d)
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
func main() {
	if len(os.Args) != 2 {
		panic("usage: canonical document.json")
	}
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	value, err := parse(d)
	if err != nil {
		panic(err)
	}
	if _, err = d.Token(); err != io.EOF {
		panic("trailing input")
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if err = encoder.Encode(value); err != nil {
		panic(err)
	}
}
