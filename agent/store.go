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

// Package agent provides durable admission and optional trusted Linux process execution.
package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"time"

	"aurora.local/agent/protocol"
	bolt "go.etcd.io/bbolt"
)

type Config struct {
	Cluster     string `json:"cluster"`
	Incarnation string `json:"incarnation"`
	Node        string `json:"node"`
	Journal     string `json:"journal"`
	Boot        string `json:"boot"`
	Runtime     string `json:"runtime"`
	Session     string `json:"session"`
	Epoch       string `json:"schedulerEpoch"`
	Peer        string `json:"peer"`
	CPU         uint64 `json:"cpuMillis"`
	Memory      uint64 `json:"memoryBytes"`
}

func ReadConfig(data []byte) (Config, error) {
	var c Config
	m, e := protocol.Decode(data)
	if e != nil {
		return c, e
	}
	if len(m) != 11 {
		return c, errors.New("config fields required")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if e = d.Decode(&c); e != nil {
		return c, e
	}
	for _, s := range []string{c.Cluster, c.Incarnation, c.Node, c.Journal, c.Boot, c.Runtime, c.Session, c.Peer} {
		if len(s) == 0 || len(s) > 64 {
			return c, errors.New("config identity required")
		}
		for i, ch := range s {
			if !(ch >= 'a' && ch <= 'z' || i > 0 && (ch >= '0' && ch <= '9' || ch == '-')) {
				return c, errors.New("config identity")
			}
		}
	}
	n, e := strconv.ParseUint(c.Epoch, 10, 64)
	if e != nil || strconv.FormatUint(n, 10) != c.Epoch || c.CPU == 0 || c.Memory == 0 || c.CPU > 2147483647 || c.Memory > 9007199254740991 {
		return c, errors.New("config capacity or epoch")
	}
	return c, nil
}

// Caller must be constructed by the embedding authenticated transport, never from wire JSON.
type Caller struct{ Peer, Session, Epoch string }
type Result struct {
	Command  string `json:"command"`
	Hash     string `json:"bodySha256"`
	Outcome  string `json:"outcome"`
	Cursor   string `json:"cursor"`
	Deadline int64  `json:"deadlineUnixMillis,omitempty"`
}
type Attempt struct {
	DeadlineMono int64          `json:"deadlineMono,omitempty"`
	Body         map[string]any `json:"body"`
	Stopped      bool           `json:"stopped"`
	Deadline     int64          `json:"deadlineUnixMillis"`
	Sequence     uint64         `json:"sequence"`
	Execution    *Execution     `json:"execution,omitempty"`
	Supervisor   *SupervisorRef `json:"supervisor,omitempty"`
}
type State struct {
	FormatVersion   int                `json:"formatVersion"`
	ExecutionEvents []ExecutionEvent   `json:"executionEvents,omitempty"`
	StopMono        int64              `json:"stopMono,omitempty"`
	RuntimeScope    *RuntimeScope      `json:"runtimeScope,omitempty"`
	Config          Config             `json:"config"`
	Cursor          uint64             `json:"cursor,string"`
	Ack             uint64             `json:"ack,string"`
	Commands        map[string]Result  `json:"commands"`
	Attempts        map[string]Attempt `json:"attempts"`
	Sequences       map[string]uint64  `json:"sequences"`
	Observations    []map[string]any   `json:"observations"`
}

// ErrInventoryCapacity is retryable after an existing reservation completes cleanup.
var ErrInventoryCapacity = errors.New("reservation inventory full")

type Store struct {
	db              *bolt.DB
	c               Config
	beforeCommit    func() error
	effectMu        sync.Mutex
	runtimeActive   bool
	runtimeDraining bool
	watchMu         sync.Mutex
	changed         chan struct{}
	closed          bool
}

func Open(path string, c Config) (*Store, error) { return openStore(path, c, false) }

// OpenServer restores durable session authority after daemon restart; enrollment
// and capacity must still match the operator-owned config exactly.
func OpenServer(path string, c Config) (*Store, error) { return openStore(path, c, true) }
func openStore(path string, c Config, restoreAuthority bool) (s *Store, err error) {
	if _, e := ReadConfig(protocol.Canonical(c)); e != nil {
		return nil, e
	}
	if fi, e := os.Lstat(path); e == nil && fi.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("state path must not be symlink")
	}
	_, statErr := os.Stat(path)
	existed := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return nil, statErr
	}
	markerPresent, e := checkMarker(path+".owner", c)
	if e != nil {
		return nil, e
	}
	if markerPresent && !existed {
		return nil, errors.New("enrolled journal state is missing")
	}

	if f, e := os.Stat(path); e == nil && f.Size() == 0 {
		return nil, errors.New("empty existing state")
	}
	db, e := bolt.Open(path, 0600, &bolt.Options{Timeout: 100 * time.Millisecond, OpenFile: func(path string, flag int, mode os.FileMode) (*os.File, error) {
		return os.OpenFile(path, flag|unix.O_NOFOLLOW, mode)
	}})
	if e != nil {
		return nil, e
	}
	s = &Store{db: db, c: c, changed: make(chan struct{})}
	defer func() {
		if err != nil {
			db.Close()
		}
	}()
	markerPresent, err = checkMarker(path+".owner", c)
	if err != nil {
		return s, err
	}
	fresh := false
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		if b == nil {
			if existed || markerPresent {
				return errors.New("missing state bucket")
			}
			fresh = true
			var e error
			b, e = tx.CreateBucket([]byte("state"))
			if e != nil {
				return e
			}
			return save(b, State{FormatVersion: 1, Config: c, Commands: map[string]Result{}, Attempts: map[string]Attempt{}, Sequences: map[string]uint64{}, Observations: []map[string]any{}})
		}
		if !markerPresent {
			return errors.New("enrollment marker missing for existing state")
		}
		st, e := read(b)
		if e != nil {
			return e
		}
		if restoreAuthority {
			c.Session = st.Config.Session
			c.Epoch = st.Config.Epoch
		}
		old := st.Config
		old.Session = c.Session
		old.Epoch = c.Epoch
		if !reflect.DeepEqual(old, c) {
			return errors.New("store enrollment/config mismatch")
		}
		prior, _ := strconv.ParseUint(st.Config.Epoch, 10, 64)
		next, _ := strconv.ParseUint(c.Epoch, 10, 64)
		if next < prior {
			return errors.New("authority epoch rollback")
		}
		st.Config = c
		return save(b, st)
	})
	if err == nil && fresh {
		err = createMarker(path+".owner", c)
	}
	if err == nil {
		dir, e := os.Open(filepath.Dir(path))
		if e != nil {
			err = e
		} else {
			err = dir.Sync()
			dir.Close()
		}
	}
	if err == nil {
		s.c = c
	}
	return s, err
}
func (s *Store) Close() error {
	s.effectMu.Lock()
	defer s.effectMu.Unlock()
	if s.runtimeActive {
		return errors.New("runtime must close before store")
	}
	s.watchMu.Lock()
	if !s.closed {
		s.closed = true
		close(s.changed)
	}
	s.watchMu.Unlock()
	return s.db.Close()
}
func save(b *bolt.Bucket, st State) error {
	data := protocol.Canonical(st)
	if e := b.Put([]byte("snapshot"), data); e != nil {
		return e
	}
	return b.Put([]byte("sha256"), []byte(fmt.Sprintf("%x", sha256.Sum256(data))))
}
func read(b *bolt.Bucket) (State, error) {
	var st State
	if b == nil {
		return st, errors.New("missing state")
	}
	data := b.Get([]byte("snapshot"))
	var value any
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if e := d.Decode(&value); e != nil {
		return st, e
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != string(b.Get([]byte("sha256"))) {
		return st, errors.New("corrupt state checksum")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if e := d.Decode(&st); e != nil {
		return st, e
	}
	if st.FormatVersion != 1 && st.FormatVersion != 2 && st.FormatVersion != 3 {
		return st, errors.New("unsupported store format")
	}
	if st.Sequences == nil || st.Commands == nil || st.Attempts == nil || st.Observations == nil || st.Ack > st.Cursor {
		return st, errors.New("corrupt state invariants")
	}
	if (len(st.ExecutionEvents) > 0 || st.StopMono != 0) && st.FormatVersion != 3 {
		return st, errors.New("supervisor metadata without format3")
	}
	for i, event := range st.ExecutionEvents {
		if event.Sequence != uint64(i+1) {
			return st, errors.New("supervisor event sequence corruption")
		}
		if err := validateExecution(&event.Execution); err != nil {
			return st, err
		}
	}
	for k, a := range st.Attempts {
		v, e := protocol.Validate(protocol.Canonical(a.Body))
		if e != nil || (v["kind"] != "Run" && v["kind"] != "Stop") {
			return st, errors.New("corrupt attempt kind")
		}
		id, ok := v["identity"].(map[string]any)
		if !ok || attemptKey(id) != k {
			return st, errors.New("corrupt attempt")
		}
		a.Body = v
		if a.Supervisor != nil {
			ref := a.Supervisor
			if st.FormatVersion != 3 || ref.Version != supervisorVersion || len(ref.Token) != 64 || ref.Process.PID < 0 || (ref.Process.PID > 0 && ref.Process.Start == "") {
				return st, errors.New("unsupported/corrupt supervisor metadata")
			}
		}
		if a.Execution != nil {
			if st.FormatVersion < 2 || st.RuntimeScope == nil {
				return st, errors.New("execution without runtime scope/version")
			}
			if e := validateExecution(a.Execution); e != nil {
				return st, e
			}
		}
		st.Attempts[k] = a
	}
	for i, o := range st.Observations {
		v, e := protocol.Validate(protocol.Canonical(o))
		if e != nil {
			return st, e
		}
		st.Observations[i] = v
	}
	return st, nil
}
func (s *Store) trusted(c Caller) error {
	if c.Peer != s.c.Peer || c.Session != s.c.Session || c.Epoch != s.c.Epoch {
		return errors.New("untrusted caller or stale authority")
	}
	return nil
}
func (s *Store) Inspect() (State, error) {
	var st State
	e := s.db.View(func(tx *bolt.Tx) error { var e error; st, e = read(tx.Bucket([]byte("state"))); return e })
	return st, e
}
func (s *Store) Admit(data []byte, caller Caller) (Result, error) {
	s.effectMu.Lock()
	defer s.effectMu.Unlock()
	var out Result
	if e := s.trusted(caller); e != nil {
		return out, e
	}
	v, e := protocol.Validate(data)
	if e != nil {
		return out, e
	}
	if v["kind"] != "Delivery" {
		return out, errors.New("Delivery required")
	}
	auth := v["authority"].(map[string]any)
	body := v["body"].(map[string]any)
	id := body["identity"].(map[string]any)
	target := body["target"].(map[string]any)
	if auth["session"] != s.c.Session || auth["schedulerEpoch"] != s.c.Epoch || id["cluster"] != s.c.Cluster || id["incarnation"] != s.c.Incarnation || target["node"] != s.c.Node || target["journal"] != s.c.Journal || target["boot"] != s.c.Boot || target["runtime"] != s.c.Runtime {
		return out, errors.New("authority or enrollment mismatch")
	}
	e = s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		command := body["command"].(string)
		hash := protocol.Digest(body)
		if prior, ok := st.Commands[command]; ok {
			if prior.Hash != hash {
				return errors.New("conflicting command reuse")
			}
			out = prior
			return nil
		}
		key := attemptKey(id)
		attempt, exists := st.Attempts[key]
		if exists && protocol.Digest(attempt.Body["identity"]) != protocol.Digest(id) {
			return errors.New("conflicting attempt identity")
		}
		out = Result{Command: command, Hash: hash, Outcome: "accepted"}
		kind := body["kind"]
		if kind == "Run" && s.runtimeDraining {
			return errors.New("runtime shutting down")
		}
		if kind == "Run" {
			if exists {
				if attempt.Stopped {
					out.Outcome = "rejected-stopped"
				} else {
					out.Outcome = "rejected-attempt-exists"
				}
			} else {
				if reservationCount(st) >= MaxInventoryAttempts {
					return ErrInventoryCapacity
				}
				p := body["assignment"].(map[string]any)
				r := p["resources"].(map[string]any)
				cpu, mem := r["cpuMillis"].(uint64), r["memoryBytes"].(uint64)
				sockets := map[string]bool{}
				for _, a := range st.Attempts {
					if a.Body["kind"] != "Run" || (a.Execution != nil && a.Execution.Cleanup == "complete") {
						continue
					}
					ap := a.Body["assignment"].(map[string]any)
					ar := ap["resources"].(map[string]any)
					cpu += ar["cpuMillis"].(uint64)
					mem += ar["memoryBytes"].(uint64)
					for _, p := range ap["ports"].([]any) {
						sockets[socket(p)] = true
					}
				}
				if cpu > s.c.CPU || mem > s.c.Memory {
					out.Outcome = "rejected-capacity"
				}
				if len(p["requiredCapabilities"].([]any)) != 0 {
					out.Outcome = "rejected-capability"
				}
				for _, port := range p["ports"].([]any) {
					if sockets[socket(port)] {
						out.Outcome = "rejected-socket"
					}
				}
				if out.Outcome == "accepted" {
					attempt = Attempt{Body: body}
					exists = true
					st.Attempts[key] = attempt
				}
			}
		} else {
			deadline := time.Now().UnixMilli() + int64(body["graceMillis"].(uint64))
			if !exists {
				attempt = Attempt{Body: body}
			}
			if attempt.Deadline == 0 || deadline < attempt.Deadline {
				attempt.Deadline = deadline
			}
			if st.FormatVersion == 3 {
				deadlineMono := monoMillis() + int64(body["graceMillis"].(uint64))
				if attempt.DeadlineMono == 0 || deadlineMono < attempt.DeadlineMono {
					attempt.DeadlineMono = deadlineMono
				}
			}
			attempt.Stopped = true
			if attempt.Execution != nil {
				attempt.Execution.Ready = false
			}
			exists = true
			st.Attempts[key] = attempt
			out.Deadline = attempt.Deadline
		}
		if st.Cursor == math.MaxUint64 {
			return errors.New("cursor exhausted")
		}
		st.Cursor++
		out.Cursor = strconv.FormatUint(st.Cursor, 10)
		seq := st.Sequences[key]
		if seq == math.MaxUint64 {
			return errors.New("sequence exhausted")
		}
		seq++
		st.Sequences[key] = seq
		if exists {
			attempt = st.Attempts[key]
			attempt.Sequence = seq
			st.Attempts[key] = attempt
		}
		obs := map[string]any{"version": "native-v1alpha1", "kind": "Observation", "identity": id, "source": target, "sequence": strconv.FormatUint(seq, 10), "cursor": out.Cursor, "state": "unknown", "ready": false, "cleanup": "unknown"}
		// Admission records command disposition without regressing already observed
		// execution facts, including a completed attempt receiving a late Stop.
		if exists && attempt.Execution != nil {
			obs["state"] = attempt.Execution.Outcome
			obs["ready"] = attempt.Execution.Ready
			obs["cleanup"] = attempt.Execution.Cleanup
		}
		st.Observations = append(st.Observations, obs)
		st.Commands[command] = out
		if e = save(b, st); e != nil {
			return e
		}
		if s.beforeCommit != nil {
			return s.beforeCommit()
		}
		return nil
	})
	return out, e
}
func socket(x any) string {
	p := x.(map[string]any)
	return fmt.Sprintf("%s/%s/%s/%v", p["network"], p["family"], p["protocol"], p["number"])
}

// Ack trusts the authenticated scheduler's assertion of contiguous committed receipt.
// At most limit observations are deleted; command dedupe and tombstones remain permanent.
func (s *Store) Ack(data []byte, c Caller, limit int) error {
	s.effectMu.Lock()
	defer s.effectMu.Unlock()
	if e := s.trusted(c); e != nil {
		return e
	}
	if limit < 1 || limit > 1024 {
		return errors.New("prune limit")
	}
	v, e := protocol.Validate(data)
	if e != nil {
		return e
	}
	if v["kind"] != "ObservationAck" || v["cluster"] != s.c.Cluster || v["incarnation"] != s.c.Incarnation || v["node"] != s.c.Node || v["journal"] != s.c.Journal {
		return errors.New("ACK scope")
	}
	n, _ := strconv.ParseUint(v["committedCursor"].(string), 10, 64)
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		if n > st.Cursor || n < st.Ack {
			return errors.New("ACK cursor")
		}
		st.Ack = n
		i := 0
		for i < len(st.Observations) && i < limit {
			cursor, _ := strconv.ParseUint(st.Observations[i]["cursor"].(string), 10, 64)
			if cursor > n {
				break
			}
			i++
		}
		st.Observations = st.Observations[i:]
		return save(b, st)
	})
}

func attemptKey(id map[string]any) string {
	return protocol.Digest(map[string]any{"cluster": id["cluster"], "incarnation": id["incarnation"], "jobKey": id["jobKey"], "instance": id["instance"], "attempt": id["attempt"]})
}

// The enrollment marker detects loss of the database without silently resetting
// the enrolled journal. Its path must stay alongside the database during moves.
func marker(c Config) map[string]any {
	return map[string]any{"formatVersion": uint64(1), "cluster": c.Cluster, "incarnation": c.Incarnation, "node": c.Node, "journal": c.Journal}
}
func checkMarker(path string, c Config) (bool, error) {
	f, e := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if os.IsNotExist(e) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return false, e
	}
	if !info.Mode().IsRegular() {
		return false, errors.New("invalid enrollment marker file")
	}
	data, e := io.ReadAll(io.LimitReader(f, 4097))
	if e != nil {
		return false, e
	}
	if len(data) > 4096 {
		return false, errors.New("enrollment marker too large")
	}
	m, e := protocol.Decode(data)
	if e != nil || !reflect.DeepEqual(m, marker(c)) {
		return false, errors.New("corrupt or mismatched enrollment marker")
	}
	return true, nil
}
func createMarker(path string, c Config) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".aurora-owner-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	data := protocol.Canonical(marker(c))
	if _, e = f.Write(data); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	// Linking publishes the already fsynced contents atomically without replacing
	// a concurrent or unexpected enrollment marker. Open still owns the DB lock.
	return os.Link(f.Name(), path)
}

// Reserved is true until cleanup of an admitted Run is durably confirmed.
func (a Attempt) Reserved() bool {
	return a.Body["kind"] == "Run" && (a.Execution == nil || a.Execution.Cleanup != "complete")
}

// CurrentConfig returns the authority snapshot under the same lock as admission.
func (s *Store) CurrentConfig() Config { s.effectMu.Lock(); defer s.effectMu.Unlock(); return s.c }
func (s *Store) RefreshSession(peer, epoch, session string) (Config, error) {
	s.effectMu.Lock()
	defer s.effectMu.Unlock()
	c := s.c
	if peer != c.Peer {
		return c, errors.New("session peer rejected")
	}
	next := c
	next.Epoch = epoch
	next.Session = session
	if _, e := ReadConfig(protocol.Canonical(next)); e != nil {
		return c, e
	}
	oldEpoch, _ := strconv.ParseUint(c.Epoch, 10, 64)
	newEpoch, _ := strconv.ParseUint(epoch, 10, 64)
	if newEpoch < oldEpoch || newEpoch == oldEpoch && session != c.Session {
		return c, errors.New("stale or conflicting session")
	}
	if next == c {
		return c, nil
	}
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, e := read(b)
		if e != nil {
			return e
		}
		st.Config = next
		return save(b, st)
	})
	if err != nil {
		return c, err
	}
	s.c = next
	return next, nil
}
