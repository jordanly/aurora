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

package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"aurora.local/agent/protocol"
	bolt "go.etcd.io/bbolt"
)

// JournalLayout is deliberately an unknown field to older readers: they must
// reject this snapshot instead of silently forgetting the separate replay ledger.
type journalSnapshot struct {
	State
	JournalLayout int       `json:"journalLayout,omitempty"`
	HistoryCounts [3]uint64 `json:"historyCounts,omitempty"`
}

var historyBuckets = []string{"commands", "attempts", "sequences"}

// Each cold value is checksummed independently. A transaction commits the hot
// snapshot and all cold writes together; no ACK or age can erase replay evidence.
func putHistory(b *bolt.Bucket, key string, value any) error {
	data := protocol.Canonical(value)
	sum := sha256.Sum256(data)
	encoded := append(sum[:], data...)
	if bytes.Equal(b.Get([]byte(key)), encoded) {
		return nil
	}
	if b.Get([]byte(key)) == nil {
		if _, err := b.NextSequence(); err != nil {
			return err
		}
	}
	return b.Put([]byte(key), encoded)
}

func decodeHistory(data []byte, out any) error {
	if len(data) < sha256.Size {
		return errors.New("missing/corrupt journal history")
	}
	sum := sha256.Sum256(data[sha256.Size:])
	if !bytes.Equal(sum[:], data[:sha256.Size]) {
		return errors.New("corrupt journal history checksum")
	}
	d := json.NewDecoder(bytes.NewReader(data[sha256.Size:]))
	d.DisallowUnknownFields()
	return d.Decode(out)
}

func save(b *bolt.Bucket, st State) error {
	buckets := make(map[string]*bolt.Bucket)
	for _, name := range historyBuckets {
		bucket, err := b.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return err
		}
		buckets[name] = bucket
	}
	hot := st
	hot.Commands = make(map[string]Result)
	hot.Attempts = make(map[string]Attempt)
	hot.Sequences = make(map[string]uint64)
	cursors := make(map[string]bool, len(st.Observations))
	for _, observation := range st.Observations {
		cursor, _ := observation["cursor"].(string)
		cursors[cursor] = true
	}
	for key, value := range st.Commands {
		if err := putHistory(buckets["commands"], key, value); err != nil {
			return err
		}
		if cursors[value.Cursor] {
			hot.Commands[key] = value
		}
	}
	for key, value := range st.Attempts {
		if err := putHistory(buckets["attempts"], key, value); err != nil {
			return err
		}
		if value.Reserved() || (value.Body["kind"] == "Stop" && value.Execution == nil) || (value.Supervisor != nil && !value.Supervisor.Acknowledged) {
			hot.Attempts[key] = value
			hot.Sequences[key] = st.Sequences[key]
		}
	}
	for key, value := range st.Sequences {
		if err := putHistory(buckets["sequences"], key, value); err != nil {
			return err
		}
	}
	var counts [3]uint64
	for i, name := range historyBuckets {
		counts[i] = buckets[name].Sequence()
	}
	data := protocol.Canonical(journalSnapshot{State: hot, JournalLayout: 1, HistoryCounts: counts})
	if err := b.Put([]byte("snapshot"), data); err != nil {
		return err
	}
	return b.Put([]byte("sha256"), []byte(fmt.Sprintf("%x", sha256.Sum256(data))))
}

func loadHistory(b *bolt.Bucket, st *State, full bool, counts [3]uint64) error {
	for i, name := range historyBuckets {
		bucket := b.Bucket([]byte(name))
		if bucket == nil {
			return errors.New("missing journal history bucket")
		}
		if bucket.Sequence() != counts[i] {
			return errors.New("corrupt journal history count")
		}
		if !full {
			continue
		}
		var count uint64
		err := bucket.ForEach(func(k, v []byte) error {
			count++
			switch name {
			case "commands":
				var value Result
				if err := decodeHistory(v, &value); err != nil {
					return err
				}
				st.Commands[string(k)] = value
			case "attempts":
				var value Attempt
				if err := decodeHistory(v, &value); err != nil {
					return err
				}
				st.Attempts[string(k)] = value
			case "sequences":
				var value uint64
				if err := decodeHistory(v, &value); err != nil {
					return err
				}
				st.Sequences[string(k)] = value
			}
			return nil
		})
		if err != nil {
			return err
		}
		if count != counts[i] {
			return errors.New("missing journal history entry")
		}
	}
	return nil
}

func loadCommand(b *bolt.Bucket, key string, st State) (Result, bool, error) {
	if value, ok := st.Commands[key]; ok {
		return value, true, nil
	}
	bucket := b.Bucket([]byte("commands"))
	if bucket == nil {
		return Result{}, false, nil
	} // Legacy, before migration.
	data := bucket.Get([]byte(key))
	if data == nil {
		return Result{}, false, nil
	}
	var value Result
	err := decodeHistory(data, &value)
	return value, true, err
}

func readForAttempt(b *bolt.Bucket, key string) (State, error) {
	st, err := readHot(b)
	if err != nil {
		return st, err
	}
	if bucket := b.Bucket([]byte("attempts")); bucket != nil {
		if data := bucket.Get([]byte(key)); data != nil {
			var value Attempt
			if err := decodeHistory(data, &value); err != nil {
				return st, err
			}
			body, err := protocol.Validate(protocol.Canonical(value.Body))
			if err != nil || (body["kind"] != "Run" && body["kind"] != "Stop") {
				return st, errors.New("corrupt historical attempt body")
			}
			identity, ok := body["identity"].(map[string]any)
			if !ok || attemptKey(identity) != key {
				return st, errors.New("corrupt historical attempt identity")
			}
			value.Body = body
			if ref := value.Supervisor; ref != nil {
				if st.FormatVersion != 3 || ref.Version != supervisorVersion || len(ref.Token) != 64 || ref.Process.PID < 0 || (ref.Process.PID > 0 && ref.Process.Start == "") {
					return st, errors.New("unsupported/corrupt historical supervisor metadata")
				}
			}
			if value.Execution != nil {
				if st.FormatVersion < 2 || st.RuntimeScope == nil {
					return st, errors.New("historical execution without runtime scope/version")
				}

				if err := validateExecution(value.Execution); err != nil {
					return st, err
				}
			}
			st.Attempts[key] = value
		}
	}
	if bucket := b.Bucket([]byte("sequences")); bucket != nil {
		if data := bucket.Get([]byte(key)); data != nil {
			var value uint64
			if err := decodeHistory(data, &value); err != nil {
				return st, err
			}
			st.Sequences[key] = value
		}
	}
	return st, nil
}

// InspectHot returns reservations, pending Stop/supervisor work, and the unpruned
// observation backlog. Permanent replay history remains available through Inspect
// or indexed InspectAttempt.
func (s *Store) InspectHot() (State, error) {
	var st State
	err := s.db.View(func(tx *bolt.Tx) error { var err error; st, err = readHot(tx.Bucket([]byte("state"))); return err })
	return st, err
}

func (s *Store) InspectAttempt(key string) (Attempt, bool, error) {
	var attempt Attempt
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		st, err := readForAttempt(tx.Bucket([]byte("state")), key)
		if err != nil {
			return err
		}
		attempt, found = st.Attempts[key]
		return nil
	})
	return attempt, found, err
}
