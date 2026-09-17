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
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"

	"aurora.local/agent/protocol"
	bolt "go.etcd.io/bbolt"
)

// MaxRetainedTickets bounds completed history plus unfinished work. A blocked
// oldest service does not pin newer retired tickets: adjacent intervals merge.
const MaxRetainedTickets = 1024
const MaxRetiredIntervals = 1024

type TicketRange struct {
	First uint64 `json:"first,string"`
	Last  uint64 `json:"last,string"`
}
type RetentionGarbage struct {
	Key     string  `json:"key"`
	Attempt Attempt `json:"attempt"`
}
type RetentionState struct {
	Version int                `json:"version"`
	Retired []TicketRange      `json:"retired"`
	Garbage []RetentionGarbage `json:"garbage"`
}
type ticketRecord struct {
	Key  string
	Run  string
	Stop string
}

func validateRetention(r *RetentionState) error {
	if r == nil {
		return nil
	}
	if r.Version != 1 || r.Retired == nil || r.Garbage == nil || len(r.Retired) > MaxRetiredIntervals {
		return errors.New("unsupported/corrupt retention state")
	}
	var previous uint64
	for i, x := range r.Retired {
		if x.First == 0 || x.Last < x.First || (i > 0 && (previous == math.MaxUint64 || x.First <= previous+1)) {
			return errors.New("corrupt retired ticket intervals")
		}
		previous = x.Last
	}
	for _, item := range r.Garbage {
		if !logAttemptKey.MatchString(item.Key) {
			return errors.New("corrupt retention garbage identity")
		}
	}
	return nil
}
func retired(r *RetentionState, ticket uint64) bool {
	if r == nil {
		return false
	}
	for _, x := range r.Retired {
		if ticket < x.First {
			return false
		}
		if ticket <= x.Last {
			return true
		}
	}
	return false
}
func retireTicket(r *RetentionState, ticket uint64) error {
	if retired(r, ticket) {
		return nil
	}
	ranges := append(append([]TicketRange{}, r.Retired...), TicketRange{ticket, ticket})
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].First < ranges[j].First })
	merged := []TicketRange{}
	for _, x := range ranges {
		n := len(merged)
		if n > 0 && (merged[n-1].Last == math.MaxUint64 || x.First <= merged[n-1].Last+1) {
			if x.Last > merged[n-1].Last {
				merged[n-1].Last = x.Last
			}
		} else {
			merged = append(merged, x)
		}
	}
	if len(merged) > MaxRetiredIntervals {
		return errors.New("retirement fragmentation capacity")
	}
	r.Retired = merged
	return nil
}
func identityTicket(body map[string]any) (uint64, error) {
	value, ok := body["identity"].(map[string]any)["ticket"]
	if !ok {
		return 0, nil
	}
	text, ok := value.(string)
	if !ok {
		return 0, errors.New("invalid ticket")
	}
	ticket, err := strconv.ParseUint(text, 10, 64)
	if err != nil || ticket == 0 || strconv.FormatUint(ticket, 10) != text {
		return 0, errors.New("invalid ticket")
	}
	return ticket, nil
}
func checkRetentionAdmission(b *bolt.Bucket, st *State, body map[string]any) (uint64, error) {
	ticket, err := identityTicket(body)
	if err != nil {
		return 0, err
	}
	if st.Retention == nil {
		if ticket != 0 {
			return 0, errors.New("retention activation required")
		}
		return 0, nil
	}
	if ticket == 0 || retired(st.Retention, ticket) {
		return 0, errors.New("legacy or retired attempt ticket")
	}
	bucket, err := b.CreateBucketIfNotExists([]byte("tickets"))
	if err != nil {
		return 0, err
	}
	key := strconv.FormatUint(ticket, 10)
	record := ticketRecord{Key: attemptKey(body["identity"].(map[string]any))}
	data := bucket.Get([]byte(key))
	if data != nil {
		if err = decodeHistory(data, &record); err != nil {
			return 0, err
		}
		if record.Key != attemptKey(body["identity"].(map[string]any)) {
			return 0, errors.New("ticket identity reuse")
		}
	} else if bucket.Sequence() >= MaxRetainedTickets || len(st.Retention.Garbage) > 0 || len(st.Observations) >= MaxObservationBacklog {
		return 0, ErrInventoryCapacity
	}
	command := body["command"].(string)
	field := &record.Run
	if body["kind"] == "Stop" {
		field = &record.Stop
	}
	if *field != "" && *field != command {
		return 0, errors.New("ticket command reuse")
	}
	*field = command
	if err = putHistory(bucket, key, record); err != nil {
		return 0, err
	}
	return ticket, nil
}
func safeToRetire(a Attempt) error {
	if a.Reserved() || (a.Supervisor != nil && !a.Supervisor.Acknowledged) {
		return errors.New("attempt cleanup or supervisor acknowledgement pending")
	}
	if a.Supervisor != nil && a.Supervisor.Process.PID > 0 {
		p, err := processInfo(a.Supervisor.Process.PID)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err == nil && p.Start == a.Supervisor.Process.Start && p.State != "Z" && p.State != "X" {
			return errors.New("supervisor still alive")
		}
	}
	return nil
}
func deleteHistory(b *bolt.Bucket, name, key string) error {
	bucket := b.Bucket([]byte(name))
	if bucket == nil || bucket.Get([]byte(key)) == nil {
		return nil
	}
	if err := bucket.Delete([]byte(key)); err != nil {
		return err
	}
	return bucket.SetSequence(bucket.Sequence() - 1)
}

// Retain is an authenticated scheduler barrier, never an age/observation ACK.
// Activation asserts the scheduler has no active, pending or uncertain legacy
// work. Retirement asserts a ticket will never again be scheduled or delivered.
// The irreversible replay fence and filesystem work list commit atomically.
func (s *Store) Retain(data []byte, caller Caller) (*RetentionState, error) {
	unlock := s.lockRetention()
	defer unlock()
	if err := s.trusted(caller); err != nil {
		return nil, err
	}
	request, err := protocol.Decode(data)
	if err != nil {
		return nil, err
	}
	if len(request) != 3 || request["journal"] != s.c.Journal {
		return nil, errors.New("retention request scope")
	}
	activate, ok := request["activate"].(bool)
	if !ok {
		return nil, errors.New("retention activation required")
	}
	values, ok := request["tickets"].([]any)
	if !ok || len(values) > 128 {
		return nil, errors.New("retirement batch limit")
	}
	tickets := []uint64{}
	for _, value := range values {
		text, ok := value.(string)
		if !ok {
			return nil, errors.New("invalid retirement ticket")
		}
		n, e := strconv.ParseUint(text, 10, 64)
		if e != nil || n == 0 || fmt.Sprint(n) != text {
			return nil, errors.New("invalid retirement ticket")
		}
		tickets = append(tickets, n)
	}
	var result *RetentionState
	err = s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		if st.Retention == nil {
			if !activate || len(tickets) > 0 {
				return errors.New("quiescent activation required")
			}
			if st.Ack != st.Cursor || len(st.Observations) > 0 {
				return errors.New("legacy observation barrier incomplete")
			}
			r := &RetentionState{Version: 1, Retired: []TicketRange{}, Garbage: []RetentionGarbage{}}
			for key, a := range st.Attempts {
				if e = safeToRetire(a); e != nil {
					return e
				}
				r.Garbage = append(r.Garbage, RetentionGarbage{key, a})
			}
			for _, name := range historyBuckets {
				if e = b.DeleteBucket([]byte(name)); e != nil {
					return e
				}
			}
			st.Commands = map[string]Result{}
			st.Attempts = map[string]Attempt{}
			st.Sequences = map[string]uint64{}
			st.Retention = r
		}
	ticketLoop:
		for _, ticket := range tickets {
			if retired(st.Retention, ticket) {
				continue
			}
			bucket := b.Bucket([]byte("tickets"))
			var record ticketRecord
			if bucket != nil && bucket.Get([]byte(fmt.Sprint(ticket))) != nil {
				if e = decodeHistory(bucket.Get([]byte(fmt.Sprint(ticket))), &record); e != nil {
					return e
				}
				if a, exists := st.Attempts[record.Key]; exists {
					if e = safeToRetire(a); e != nil {
						continue ticketLoop
					}
				}
				for _, o := range st.Observations {
					n, _ := identityTicket(map[string]any{"identity": o["identity"]})
					if n == ticket {
						continue ticketLoop
					}
				}
				for _, command := range []string{record.Run, record.Stop} {
					if v, exists := st.Commands[command]; exists {
						cursor, _ := strconv.ParseUint(v.Cursor, 10, 64)
						if cursor > st.Ack {
							continue ticketLoop
						}
						delete(st.Commands, command)
						if e = deleteHistory(b, "commands", command); e != nil {
							return e
						}
					}
				}
				if _, exists := st.Attempts[record.Key]; exists {
					st.Retention.Garbage = append(st.Retention.Garbage, RetentionGarbage{record.Key, st.Attempts[record.Key]})
				}
				delete(st.Attempts, record.Key)
				delete(st.Sequences, record.Key)
				for _, name := range []string{"attempts", "sequences"} {
					if e = deleteHistory(b, name, record.Key); e != nil {
						return e
					}
				}
				if e = deleteHistory(b, "tickets", fmt.Sprint(ticket)); e != nil {
					return e
				}
			}
			if e = retireTicket(st.Retention, ticket); e != nil {
				return e
			}
		}
		if e = save(b, st); e != nil {
			return e
		}
		if s.beforeCommit != nil {
			if e = s.beforeCommit(); e != nil {
				return e
			}
		}
		result = st.Retention
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Failure leaves the durable queue intact; admission remains backpressured.
	if err = s.collectGarbage(); err != nil {
		return nil, err
	}
	st, err := s.InspectHot()
	if err == nil {
		result = st.Retention
	}
	return result, err
}

func (s *Store) collectGarbage() error {
	if s.beforeGarbage != nil {
		if err := s.beforeGarbage(); err != nil {
			return err
		}
	}
	st, err := s.InspectHot()
	if err != nil || st.Retention == nil {
		return err
	}
	if len(st.Retention.Garbage) == 0 {
		return nil
	}
	if s.runtimeRoot == "" { // Admission-only stores have no execution artifacts.

		if st.RuntimeScope != nil {
			return errors.New("runtime required for artifact retirement")
		}
	} else {
		// os.Root confines traversal and RemoveAll refuses to follow symlinks. The
		// root is the operator-owned runtime directory, never a wire-supplied path.
		root, e := os.OpenRoot(s.runtimeRoot)
		if e != nil {
			return e
		}
		defer root.Close()
		if info, e := root.Lstat(".supervisors"); e == nil {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return errors.New("unsafe supervisor artifact directory")
			}
		} else if !os.IsNotExist(e) {
			return e
		}
		for _, item := range st.Retention.Garbage {
			key := item.Key
			if err := cleanupIsolationArtifacts(s.runtimeRoot, key, item.Attempt); err != nil {
				return err
			}
			for _, path := range []string{key, filepath.Join(".supervisors", key)} {
				info, e := root.Lstat(path)
				if os.IsNotExist(e) {
					continue
				}
				if e != nil {
					return e
				}
				if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
					return errors.New("unsafe retention artifact path")
				}
				if e = root.RemoveAll(path); e != nil {
					return e
				}
			}
		}
		// Persist both parent directory updates before forgetting the GC intent.
		// Syncing only the runtime root does not make .supervisors child removal durable.
		for _, path := range []string{".supervisors", "."} {
			dir, e := root.Open(path)
			if os.IsNotExist(e) {
				continue
			}
			if e != nil {
				return e
			}
			e = dir.Sync()
			closeErr := dir.Close()
			if e != nil {
				return e
			}
			if closeErr != nil {
				return closeErr
			}
		}
	}
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		current, e := readHot(b)
		if e != nil {
			return e
		}
		current.Retention.Garbage = []RetentionGarbage{}
		return save(b, current)
	})
}

// Startup checks the entire bounded ticket index, including entries absent from
// the hot snapshot. Missing replay evidence must never become fresh admission.
func validateTicketIndex(b *bolt.Bucket, st State) error {
	bucket := b.Bucket([]byte("tickets"))
	if st.Retention == nil {
		if bucket != nil {
			return errors.New("ticket index without retention mode")
		}
		return nil
	}
	if bucket == nil {
		for _, command := range st.Commands {
			if command.Ticket != 0 {
				return errors.New("ticket index missing")
			}
		}
		return nil
	} // Activated but no ticket has yet arrived.
	if bucket.Sequence() > MaxRetainedTickets {
		return errors.New("ticket index exceeds bound")
	}
	for _, command := range st.Commands {
		if command.Ticket == 0 || bucket.Get([]byte(fmt.Sprint(command.Ticket))) == nil {
			return errors.New("command ticket index missing")
		}
	}
	var count uint64
	err := bucket.ForEach(func(k, v []byte) error {
		count++
		ticket, e := strconv.ParseUint(string(k), 10, 64)
		if e != nil || ticket == 0 || fmt.Sprint(ticket) != string(k) || retired(st.Retention, ticket) {
			return errors.New("invalid retained ticket index")
		}
		var record ticketRecord
		if e = decodeHistory(v, &record); e != nil {
			return e
		}
		if !logAttemptKey.MatchString(record.Key) || (record.Run == "" && record.Stop == "") {
			return errors.New("invalid retained ticket identity")
		}
		for _, command := range []string{record.Run, record.Stop} {
			if command != "" {
				value, exists := st.Commands[command]
				if !exists || value.Ticket != ticket {
					return errors.New("ticket command history missing")
				}
			}
		}
		if a, ok := st.Attempts[record.Key]; ok {
			n, e := identityTicket(a.Body)
			if e != nil || n != ticket {
				return errors.New("ticket attempt history mismatch")
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if count != bucket.Sequence() {
		return errors.New("ticket index count mismatch")
	}
	return nil
}

const MaxObservationBacklog = 8192
const MaxSupervisorEvents = 256

func readinessOnly(previous, next *Execution) bool {
	if previous == nil || next == nil || previous.Phase != "released" || next.Phase != "released" || previous.Ready == next.Ready {
		return false
	}
	before := *previous
	before.Ready = next.Ready
	return reflect.DeepEqual(before, *next)
}
func appendSupervisorEvent(st *State, a *Attempt) error {
	if st.EventBase > math.MaxUint64-uint64(len(st.ExecutionEvents))-1 {
		return errors.New("supervisor event counter exhausted")
	}
	st.ExecutionEvents = append(st.ExecutionEvents, ExecutionEvent{Sequence: st.EventBase + uint64(len(st.ExecutionEvents)) + 1, Execution: *a.Execution})
	return nil
}
func recordExecutionTransition(st *State, key string, a *Attempt, previous *Execution, observe, capture bool) error {
	if a.Execution == nil {
		return nil
	}
	full := len(st.Observations) >= MaxObservationBacklog
	if capture {
		full = len(st.ExecutionEvents) >= MaxSupervisorEvents
	}
	if full && (observe || capture) && readinessOnly(previous, a.Execution) {
		// Persist the latest readiness and upstream import cursor together. No wire
		// sequence is allocated until it can be published; terminal facts supersede it.
		a.PendingReadiness = true
		return nil
	}
	if observe && !capture {
		if err := observeAttempt(st, key, a); err != nil {
			return err
		}
	}
	if capture {
		if err := appendSupervisorEvent(st, a); err != nil {
			return err
		}
	}
	if observe || capture {
		a.PendingReadiness = false
	}
	return nil
}
func (r *Runtime) flushReadiness() error {
	st, err := r.store.InspectHot()
	if err != nil {
		return err
	}
	pending := false
	for _, a := range st.Attempts {
		pending = pending || a.PendingReadiness
	}
	if !pending {
		return nil
	}
	return r.store.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		current, e := readHot(b)
		if e != nil {
			return e
		}
		for key, a := range current.Attempts {
			if !a.PendingReadiness {
				continue
			}
			full := len(current.Observations) >= MaxObservationBacklog
			if r.opts.captureEvents {
				full = len(current.ExecutionEvents) >= MaxSupervisorEvents
			}
			if full {
				break
			}
			if a.Execution == nil {
				return errors.New("pending readiness without execution")
			}
			if r.opts.captureEvents {
				e = appendSupervisorEvent(&current, &a)
			} else {
				e = observeAttempt(&current, key, &a)
			}
			if e != nil {
				return e
			}
			a.PendingReadiness = false
			current.Attempts[key] = a
		}
		return save(b, current)
	})
}
func (s *Store) ackSupervisorEvents(imported uint64) error {
	current, err := s.InspectHot()
	if err != nil {
		return err
	}
	if imported == current.EventBase && len(current.Observations) == 0 {
		return nil
	}
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := readHot(b)
		if e != nil {
			return e
		}
		if imported < st.EventBase || imported > st.EventBase+uint64(len(st.ExecutionEvents)) {
			return errors.New("supervisor ACK cursor rollback or ahead")
		}
		count := int(imported - st.EventBase)
		st.ExecutionEvents = st.ExecutionEvents[count:]
		st.EventBase = imported
		// These were redundant local observations in legacy helper journals. Only the
		// immutable event stream is consumed by the parent; helpers have no HTTP peer.
		st.Observations = []map[string]any{}
		st.Ack = st.Cursor
		return save(b, st)
	})
}
