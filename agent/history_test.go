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
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"aurora.local/agent/protocol"
	bolt "go.etcd.io/bbolt"
)

func historyBody(t *testing.T, kind string, n int) map[string]any {
	t.Helper()
	body := fixture(t, kind)
	body["command"] = fmt.Sprintf("%s-history-%d", kind, n)
	body["identity"].(map[string]any)["attempt"] = fmt.Sprintf("attempt-%d", n)
	return body
}

// Simulate old journals without iterating admission's complete snapshot rewrite.
func seedHistory(t *testing.T, s *Store, kind string, count int) {
	t.Helper()
	bodies := make([]map[string]any, count)
	for i := range bodies {
		bodies[i] = historyBody(t, kind, i)
	}
	if err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, err := read(b)
		if err != nil {
			return err
		}
		st.FormatVersion, st.RuntimeScope = 2, &RuntimeScope{}
		for _, body := range bodies {
			id := body["identity"].(map[string]any)
			key := attemptKey(id)
			st.Cursor++
			cursor := strconv.FormatUint(st.Cursor, 10)
			st.Attempts[key] = Attempt{Body: body, Stopped: kind == "stop", Sequence: 1}
			st.Sequences[key] = 1
			command := body["command"].(string)
			st.Commands[command] = Result{Command: command, Hash: protocol.Digest(body), Outcome: "accepted", Cursor: cursor}
			st.Observations = append(st.Observations, map[string]any{"version": "native-v1alpha1", "kind": "Observation", "identity": id, "source": body["target"], "sequence": "1", "cursor": cursor, "state": "unknown", "ready": false, "cleanup": "unknown"})
		}
		return save(b, st)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyHistoryPagesAndPermanentReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	s := open(t, path, config())
	seedHistory(t, s, "stop", MaxInventoryCommands+1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path, config())
	defer s.Close()
	pki := newPKI(t)
	base, clients := testTransport(t, s, pki)
	client := clients(pki.leaf(t, "scheduler", false))
	after := 0
	for after < MaxInventoryCommands+1 {
		code, page := httpJSON(t, client, "GET", base+"/v1/state?afterCursor="+strconv.Itoa(after), nil, nil)
		if code != 200 {
			t.Fatal(code, page)
		}
		state := page["state"].(map[string]any)
		observations := state["observations"].([]any)
		commands := state["commands"].(map[string]any)
		if len(state["attempts"].(map[string]any)) != 0 || len(commands) != len(observations) || len(commands) > 128 {
			t.Fatal(page)
		}
		for _, observation := range observations {
			after++
			if observation.(map[string]any)["cursor"] != strconv.Itoa(after) {
				t.Fatal("observation gap")
			}
		}
		if page["nextCursor"] != strconv.Itoa(after) {
			t.Fatal(page)
		}
	}
	ack := fixture(t, "ack")
	ack["committedCursor"] = strconv.Itoa(after)
	if err := s.Ack(protocol.Canonical(ack), caller(config()), 1); err != nil {
		t.Fatal(err)
	}
	// Bounded pruning leaves ACKed observations on disk; they cannot leak into a page.
	response := startWatch(t, client, base, config(), strconv.Itoa(after))
	f := frame(t, json.NewDecoder(response.Body))
	state := f["state"].(map[string]any)
	if len(state["attempts"].(map[string]any)) != 0 || len(state["commands"].(map[string]any)) != 0 {
		t.Fatal(f)
	}
	response.Body.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path, config())
	defer s.Close()
	replay, err := s.Admit(delivery(config(), historyBody(t, "stop", 0)), caller(config()))
	if err != nil || replay.Cursor != "1" {
		t.Fatal(replay, err)
	}
	staleRun, err := s.Admit(delivery(config(), historyBody(t, "run", 0)), caller(config()))
	if err != nil || staleRun.Outcome != "rejected-stopped" {
		t.Fatal(staleRun, err)
	}
	conflict := historyBody(t, "stop", 0)
	conflict["graceMillis"] = uint64(1)
	if _, err := s.Admit(delivery(config(), conflict), caller(config())); err == nil {
		t.Fatal("command reuse accepted")
	}
	st, err := s.Inspect()
	if err != nil || len(st.Attempts) != MaxInventoryCommands+1 || len(st.Commands) != MaxInventoryCommands+2 {
		t.Fatal("replay history lost", err)
	}
}

func TestReservationBackpressureStopAndWatchChurn(t *testing.T) {
	c := config()
	c.CPU *= 200
	c.Memory *= 200
	s := open(t, filepath.Join(t.TempDir(), "state"), c)
	defer s.Close()
	seedHistory(t, s, "run", MaxInventoryAttempts)
	before, _ := s.Inspect()
	run := historyBody(t, "run", MaxInventoryAttempts)
	if _, err := s.Admit(delivery(c, run), caller(c)); !errors.Is(err, ErrInventoryCapacity) {
		t.Fatal(err)
	}
	after, _ := s.Inspect()
	if after.Cursor != before.Cursor || len(after.Commands) != len(before.Commands) {
		t.Fatal("backpressure changed history")
	}
	pki := newPKI(t)
	base, clients := testTransport(t, s, pki)
	client := clients(pki.leaf(t, "scheduler", false))
	code, _ := httpJSON(t, client, "POST", base+"/v1/deliver", delivery(c, run), nil)
	if code != 503 {
		t.Fatal("backpressure status", code)
	}
	// Both an existing Stop and an unknown Stop tombstone remain admissible.
	for _, n := range []int{0, 1000} {
		result, err := s.Admit(delivery(c, historyBody(t, "stop", n)), caller(c))
		if err != nil || result.Outcome != "accepted" {
			t.Fatal(result, err)
		}
	}
	st, _ := s.Inspect()
	response := startWatch(t, client, base, c, strconv.FormatUint(st.Cursor, 10))
	decoder := json.NewDecoder(response.Body)
	first := frame(t, decoder)
	merged := first["state"].(map[string]any)["attempts"].(map[string]any)
	if len(merged) != MaxInventoryAttempts {
		t.Fatal("lost reservations", len(merged))
	}
	for n := 0; n < 3; n++ {
		oldKey := attemptKey(historyBody(t, "run", n)["identity"].(map[string]any))
		runtime := &Runtime{store: s}
		if err := runtime.update(oldKey, func(a *Attempt) error {
			a.Execution = &Execution{Phase: "terminal", Outcome: "stopped", Cleanup: "complete"}
			return nil
		}, true); err != nil {
			t.Fatal(err)
		}
		newRun := historyBody(t, "run", MaxInventoryAttempts+n)
		result, err := s.Admit(delivery(c, newRun), caller(c))
		if err != nil || result.Outcome != "accepted" {
			t.Fatal(result, err)
		}
		newKey := attemptKey(newRun["identity"].(map[string]any))
		for {
			f := frame(t, decoder)
			if f["kind"] == "heartbeat" {
				continue
			}
			changes := f["state"].(map[string]any)["attempts"].(map[string]any)
			if f["kind"] == "snapshot" {
				merged = changes
			} else {
				for key, value := range changes {
					merged[key] = value
				}
			}
			if len(merged) > MaxInventoryAttempts {
				t.Fatal("watch merge exceeded inventory bound")
			}
			if _, ok := merged[newKey]; ok {
				if _, old := merged[oldKey]; old {
					t.Fatal("retired reservation survived snapshot")
				}
				break
			}
		}
	}
	response.Body.Close()
	path := s.db.Path()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path, c)
	defer s.Close()
	result, err := s.Admit(delivery(c, historyBody(t, "run", 0)), caller(c))
	if err != nil || result.Outcome != "accepted" || result.Cursor != "1" {
		t.Fatal("Run replay changed after cleanup/restart", result, err)
	}
	oldRun := historyBody(t, "run", 0)
	oldRun["command"] = "new-command-old-run"
	result, err = s.Admit(delivery(c, oldRun), caller(c))
	if err != nil || result.Outcome != "rejected-stopped" {
		t.Fatal("retired Run resurrected", result, err)
	}

}

func TestLegacyExcessReservationsFailClosedButStopWorks(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "state"), config())
	defer s.Close()
	seedHistory(t, s, "run", MaxInventoryAttempts+1)
	pki := newPKI(t)
	base, clients := testTransport(t, s, pki)
	client := clients(pki.leaf(t, "scheduler", false))
	code, _ := httpJSON(t, client, "GET", base+"/v1/state", nil, nil)
	if code != 503 {
		t.Fatal("partial reservations published", code)
	}
	result, err := s.Admit(delivery(config(), historyBody(t, "stop", 0)), caller(config()))
	if err != nil || result.Outcome != "accepted" {
		t.Fatal(result, err)
	}
	st, _ := s.Inspect()
	if reservationCount(st) != MaxInventoryAttempts+1 {
		t.Fatal("Stop discarded reservation before cleanup")
	}
	key := attemptKey(historyBody(t, "run", 0)["identity"].(map[string]any))
	runtime := &Runtime{store: s}
	if err := runtime.update(key, func(a *Attempt) error {
		a.Execution = &Execution{Phase: "terminal", Outcome: "stopped", Cleanup: "complete"}
		return nil
	}, true); err != nil {
		t.Fatal(err)
	}
	code, page := httpJSON(t, client, "GET", base+"/v1/state", nil, nil)
	if code != 200 || len(page["state"].(map[string]any)["attempts"].(map[string]any)) != MaxInventoryAttempts {
		t.Fatal("cleanup failed to restore complete inventory", code, page)
	}

}
