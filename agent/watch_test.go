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
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func watchClient(t *testing.T, s *Store, timing watchTiming) (*http.Client, string) {
	t.Helper()
	return watchClientWithDeadline(t, s, timing, 0)
}
func watchClientWithDeadline(t *testing.T, s *Store, timing watchTiming, deadline time.Duration) (*http.Client, string) {
	t.Helper()
	pki := newPKI(t)
	handler := transportHandler(s, timing)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if deadline > 0 {
			// Apply the short response deadline after TLS negotiation. A global
			// WriteTimeout also bounds the handshake, which this test does not measure.
			if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(deadline)); err != nil {
				t.Error(err)
				http.Error(w, "test deadline unavailable", http.StatusInternalServerError)
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	server.Config.WriteTimeout = 5 * time.Second
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pki.leaf(t, "agent-a", true)}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.pool, MinVersion: tls.VersionTLS13}
	server.StartTLS()
	t.Cleanup(server.Close)
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pki.pool, Certificates: []tls.Certificate{pki.leaf(t, "scheduler", false)}, MinVersion: tls.VersionTLS13}}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 3 * time.Second}, server.URL
}
func startWatch(t *testing.T, c *http.Client, base string, cfg Config, after string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("GET", base+"/v1/watch?afterCursor="+after+"&limit=1", nil)
	req.Header.Set("X-Aurora-Epoch", cfg.Epoch)
	req.Header.Set("X-Aurora-Session", cfg.Session)
	// Streaming reconciliation intentionally outlives the generic RPC client's
	// short deadline, particularly while the churn test commits many journals.
	// Keep a bounded stream lifetime without changing server/per-frame deadlines.
	streamClient := *c
	streamClient.Timeout = 30 * time.Second
	resp, err := streamClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		t.Fatalf("watch HTTP %d", resp.StatusCode)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}
func frame(t *testing.T, d *json.Decoder) map[string]any {
	t.Helper()
	var f map[string]any
	if err := d.Decode(&f); err != nil {
		t.Fatal(err)
	}
	return f
}
func TestWatchCommitDeltaReplayAndFence(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "state"), config())
	defer s.Close()
	client, base := watchClient(t, s, defaultWatchTiming)
	response := startWatch(t, client, base, config(), "0")
	d := json.NewDecoder(response.Body)
	if f := frame(t, d); f["kind"] != "snapshot" || f["nextCursor"] != "0" {
		t.Fatal(f)
	}
	if _, err := s.Admit(delivery(config(), fixture(t, "run")), caller(config())); err != nil {
		t.Fatal(err)
	}
	f := frame(t, d)
	state := f["state"].(map[string]any)
	if f["kind"] != "delta" || f["nextCursor"] != "1" || len(state["attempts"].(map[string]any)) != 1 || len(state["commands"].(map[string]any)) != 1 {
		t.Fatal(f)
	}
	// A public change without an observation cursor (e.g. cleanup metadata or
	// shutdown stop intent) must also trigger an inventory delta after commit.
	st, _ := s.Inspect()
	var key string
	for k := range st.Attempts {
		key = k
	}
	runtime := &Runtime{store: s}
	if err := runtime.update(key, func(a *Attempt) error { a.Deadline = 123; return nil }, false); err != nil {
		t.Fatal(err)
	}
	f = frame(t, d)
	state = f["state"].(map[string]any)
	if f["nextCursor"] != "1" || len(state["observations"].([]any)) != 0 || len(state["commands"].(map[string]any)) != 0 || len(state["attempts"].(map[string]any)) != 1 {
		t.Fatal(f)
	}
	response.Body.Close()
	// Delivery was received but never acknowledged: reconnect replays it with
	// full inventory instead of advancing from a volatile stream cursor.
	response = startWatch(t, client, base, config(), "0")
	d = json.NewDecoder(response.Body)
	f = frame(t, d)
	if f["kind"] != "snapshot" || f["nextCursor"] != "1" {
		t.Fatal(f)
	}
	epoch, _ := counter(config().Epoch)
	if _, err := s.RefreshSession(config().Peer, strconv.FormatUint(epoch+1, 10), "session-b"); err != nil {
		t.Fatal(err)
	}
	if err := d.Decode(&f); err != io.EOF {
		t.Fatalf("fenced watch remained open: %v", err)
	}
}
func TestWatchHeartbeatPeriodicSnapshotAndAckSilence(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "state"), config())
	defer s.Close()
	timing := watchTiming{40 * time.Millisecond, time.Second, func() time.Duration { return 150 * time.Millisecond }}
	client, base := watchClientWithDeadline(t, s, timing, 20*time.Millisecond)
	if _, err := s.Admit(delivery(config(), fixture(t, "run")), caller(config())); err != nil {
		t.Fatal(err)
	}
	response := startWatch(t, client, base, config(), "0")
	d := json.NewDecoder(response.Body)
	frame(t, d)
	signal, _ := s.watchSignal()
	ack := fixture(t, "ack")
	ack["committedCursor"] = "1"
	if err := s.Ack(protocolBytes(ack), caller(config()), 128); err != nil {
		t.Fatal(err)
	}
	select {
	case <-signal:
		t.Fatal("ACK woke watch")
	default:
	}
	f := frame(t, d)
	if f["kind"] != "heartbeat" || f["nextCursor"] != "1" || f["state"] != nil || len(f) != 3 {
		t.Fatal(f)
	}
	// Heartbeat survives the initial 20ms response write timeout, and eventually
	// a jitter timer, not polling, produces a new full inventory.
	for f["kind"] == "heartbeat" {
		f = frame(t, d)
	}
	if f["kind"] != "snapshot" || len(f["state"].(map[string]any)["attempts"].(map[string]any)) != 1 {
		t.Fatal(f)
	}
}
func protocolBytes(v any) []byte { b, _ := json.Marshal(v); return b }
func TestWatchNotificationCommitFailureAndRegistration(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "state"), config())
	defer s.Close()
	signal, _ := s.watchSignal()
	s.beforeCommit = func() error { return errors.New("disk failure") }
	if _, err := s.Admit(delivery(config(), fixture(t, "run")), caller(config())); err == nil {
		t.Fatal("expected rollback")
	}
	select {
	case <-signal:
		t.Fatal("uncommitted event notified")
	default:
	}
	s.beforeCommit = nil
	if _, err := s.Admit(delivery(config(), fixture(t, "run")), caller(config())); err != nil {
		t.Fatal(err)
	}
	st, err := s.Inspect()
	if err != nil || st.Cursor != 1 {
		t.Fatal(st, err)
	}
	select {
	case <-signal:
	default:
		t.Fatal("commit between registration and snapshot lost")
	}
	next, _ := s.watchSignal()
	if _, err := s.Admit(delivery(config(), fixture(t, "run")), caller(config())); err != nil {
		t.Fatal(err)
	}
	select {
	case <-next:
		t.Fatal("duplicate no-op admission woke watch")
	default:
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-next:
	default:
		t.Fatal("store close did not wake watcher")
	}
}
func TestWatchSlotsDoNotBlockDeliveryAndCancel(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "state"), config())
	defer s.Close()
	client, base := watchClient(t, s, defaultWatchTiming)
	first := startWatch(t, client, base, config(), "0")
	frame(t, json.NewDecoder(first.Body))
	second := startWatch(t, client, base, config(), "0")
	frame(t, json.NewDecoder(second.Body))
	req, _ := http.NewRequestWithContext(context.Background(), "GET", base+"/v1/watch", nil)
	req.Header.Set("X-Aurora-Epoch", config().Epoch)
	req.Header.Set("X-Aurora-Session", config().Session)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatal(resp.StatusCode)
	}
	status, _ := httpJSON(t, client, "GET", base+"/v1/state", nil, nil)
	if status != 200 {
		t.Fatal(status)
	}
	first.Body.Close()
	second.Body.Close()
}

func TestWatchPagedSnapshotDoesNotAdvanceReceipt(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "state"), config())
	defer s.Close()
	for _, name := range []string{"run", "stop"} {
		if _, err := s.Admit(delivery(config(), fixture(t, name)), caller(config())); err != nil {
			t.Fatal(err)
		}
	}
	client, base := watchClient(t, s, defaultWatchTiming)
	response := startWatch(t, client, base, config(), "0")
	d := json.NewDecoder(response.Body)
	first := frame(t, d)
	second := frame(t, d)
	if first["kind"] != "snapshot" || first["nextCursor"] != "1" || first["hasMore"] != true || first["state"].(map[string]any)["cursor"] != "2" {
		t.Fatal(first)
	}
	secondState := second["state"].(map[string]any)
	if second["kind"] != "delta" || second["nextCursor"] != "2" || second["hasMore"] != false || len(secondState["commands"].(map[string]any)) != 1 || len(secondState["attempts"].(map[string]any)) != 0 {
		t.Fatal(second)
	}
	st, _ := s.Inspect()
	if st.Ack != 0 || len(st.Observations) != 2 {
		t.Fatal("stream advanced durable receipt", st)
	}
	ack := fixture(t, "ack")
	ack["committedCursor"] = "1"
	if err := s.Ack(protocolBytes(ack), caller(config()), 128); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	response = startWatch(t, client, base, config(), "1")
	replay := frame(t, json.NewDecoder(response.Body))
	if replay["kind"] != "snapshot" || replay["nextCursor"] != "2" || len(replay["state"].(map[string]any)["commands"].(map[string]any)) != 1 {
		t.Fatal(replay)
	}
}
