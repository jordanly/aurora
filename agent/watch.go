/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"time"

	bolt "go.etcd.io/bbolt"
)

// update publishes only after bbolt's durable commit has succeeded. The channel
// is a coalescing wakeup, not an event queue: observations remain in the journal
// until the scheduler acknowledges committed receipt.
func (s *Store) update(fn func(*bolt.Tx) error) error {
	changed := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		var before []byte
		if b != nil {
			before = bytes.Clone(b.Get([]byte("snapshot")))
		}
		if err := fn(tx); err != nil {
			return err
		}
		b = tx.Bucket([]byte("state"))
		changed = b != nil && !bytes.Equal(before, b.Get([]byte("snapshot")))
		return nil
	})
	if err == nil && changed {
		s.watchMu.Lock()
		if !s.closed {
			close(s.changed)
			s.changed = make(chan struct{})
		}
		s.watchMu.Unlock()
	}
	return err
}

// Register BEFORE reading the snapshot. A concurrent commit is either present
// in the read or closes this channel (possibly both), so no wakeup can be lost.
func (s *Store) watchSignal() (<-chan struct{}, bool) {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	return s.changed, s.closed
}

type watchTiming struct {
	heartbeat, write time.Duration
	reconcile        func() time.Duration
}

var defaultWatchTiming = watchTiming{30 * time.Second, 5 * time.Second, func() time.Duration {
	return 270*time.Second + time.Duration(rand.Int64N(int64(60*time.Second)))
}}

func watchQuery(r *http.Request) (uint64, uint64, error) {
	if r.ContentLength > 0 || len(r.TransferEncoding) != 0 {
		return 0, 0, errors.New("GET body rejected")
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return 0, 0, err
	}
	for k, v := range q {
		if (k != "afterCursor" && k != "limit") || len(v) != 1 {
			return 0, 0, errors.New("invalid query")
		}
	}
	after, limit := uint64(0), uint64(128)
	if v, ok := q["afterCursor"]; ok {
		after, err = counter(v[0])
		if err != nil {
			return 0, 0, err
		}
	}
	if v, ok := q["limit"]; ok {
		limit, err = counter(v[0])
		if err != nil || limit < 1 || limit > 128 {
			return 0, 0, errors.New("invalid page limit")
		}
	}
	return after, limit, nil
}

func serveWatch(w http.ResponseWriter, r *http.Request, store *Store, timing watchTiming) {
	after, limit, err := watchQuery(r)
	if err != nil {
		transportError(w, 400, "invalid watch query or body")
		return
	}
	authority := store.CurrentConfig()
	if len(r.Header.Values("X-Aurora-Epoch")) != 1 || len(r.Header.Values("X-Aurora-Session")) != 1 || r.Header.Get("X-Aurora-Epoch") != authority.Epoch || r.Header.Get("X-Aurora-Session") != authority.Session {
		transportError(w, 409, "stale watch authority")
		return
	}
	controller := http.NewResponseController(w)
	started := false
	write := func(frame any) bool {
		if r.Context().Err() != nil || store.CurrentConfig() != authority {
			return false
		}
		data, err := json.Marshal(frame)
		if err != nil || len(data)+1 > MaxTransportBytes {
			if !started {
				transportError(w, 503, "response profile limit")
			}
			return false
		}
		// Override the server's short request WriteTimeout for each frame; never
		// leave a deadline spanning the idle interval between frames.
		if controller.SetWriteDeadline(time.Now().Add(timing.write)) != nil {
			return false
		}
		if !started {
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("X-Accel-Buffering", "no")
			started = true
		}
		if _, err = w.Write(append(data, '\n')); err != nil {
			return false
		}
		if controller.Flush() != nil {
			return false
		}
		return controller.SetWriteDeadline(time.Time{}) == nil
	}
	heartbeat := time.NewTicker(timing.heartbeat)
	defer heartbeat.Stop()
	reconcile := time.NewTimer(timing.reconcile())
	defer reconcile.Stop()
	full := true
	var previous map[string]any
	for {
		signal, closed := store.watchSignal()
		if closed {
			return
		}
		st, err := store.InspectHot()
		if err != nil {
			if !started {
				transportError(w, 503, "state unavailable")
			}
			return
		}
		if st.Config != authority || after < st.Ack || after > st.Cursor {
			if !started {
				transportError(w, 409, "cursor scope, retention gap or stale authority")
			}
			return
		}
		if reservationCount(st) > MaxInventoryAttempts {
			if !started {
				transportError(w, 503, "inventory profile limit")
			}
			return
		}
		observations := []map[string]any{}
		next, more := after, false
		for _, o := range st.Observations {
			cursor, err := counter(o["cursor"].(string))
			if err != nil {
				return
			}
			if cursor <= after {
				continue
			}
			if uint64(len(observations)) == limit {
				more = true
				break
			}
			observations = append(observations, o)
			next = cursor
		}
		current := transportState(st, observations)
		inventory := current["attempts"].(map[string]any)
		// Deltas cannot remove keys. Publish a replacement snapshot on cleanup,
		// including when a new reservation replaces one between watch reads.
		if !full {
			for key := range previous {
				if _, exists := inventory[key]; !exists {
					full = true
					break
				}
			}
		}
		attempts := inventory
		if !full {
			attempts = map[string]any{}
			for key, value := range inventory {
				if !reflect.DeepEqual(previous[key], value) {
					attempts[key] = value
				}
			}
		}
		current["attempts"] = attempts
		changed := full || len(observations) > 0 || len(attempts) > 0
		if changed {
			kind := "delta"
			if full {
				kind = "snapshot"
			}
			if !write(map[string]any{"kind": kind, "config": st.Config, "state": current, "nextCursor": strconv.FormatUint(next, 10), "hasMore": more}) {
				return
			}
			after = next
		}
		previous = inventory
		full = false
		if more {
			// A continuously growing observation backlog must not postpone
			// the independent full reconciliation deadline indefinitely.
			select {
			case <-reconcile.C:
				full = true
				reconcile.Reset(timing.reconcile())
			default:
			}
			continue
		}
		// Heartbeats use only authority and the last SENT observation cursor. They
		// never inspect inventory, advance receipt, or trigger reconciliation.
	wait:
		for {
			select {
			case <-r.Context().Done():
				return
			case <-signal:
				break wait
			case <-heartbeat.C:
				if !write(map[string]any{"kind": "heartbeat", "config": authority, "nextCursor": strconv.FormatUint(after, 10)}) {
					return
				}
			case <-reconcile.C:
				full = true
				reconcile.Reset(timing.reconcile())
				break wait
			}
		}
	}
}
