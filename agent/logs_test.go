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
 */

package agent

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

func seededLogs(t *testing.T) (*Store, Config, string) {
	t.Helper()
	c := config()
	s := open(t, filepath.Join(t.TempDir(), "state"), c)
	b := fixture(t, "run")
	key := attemptKey(b["identity"].(map[string]any))
	if result, err := s.Admit(delivery(c, b), caller(c)); err != nil || result.Outcome != "accepted" {
		t.Fatalf("admit: %+v %v", result, err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		st, err := readHot(tx.Bucket([]byte("state")))
		if err != nil {
			return err
		}
		a := st.Attempts[key]
		st.FormatVersion = 2
		st.RuntimeScope = &RuntimeScope{}
		a.Execution = &Execution{Phase: "terminal", Cleanup: "complete", Outcome: "succeeded"}
		st.Attempts[key] = a
		return save(tx.Bucket([]byte("state")), st)
	}); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "runtime")
	if err := os.MkdirAll(filepath.Join(root, key), 0700); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.InspectAttempt(key); err != nil || !found {
		t.Fatalf("seeded attempt unavailable: found=%v err=%v", found, err)
	}
	if err := os.WriteFile(filepath.Join(root, key, "stdout.log"), []byte("stdout-0123456789"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, key, "stderr.log"), []byte("stderr-output"), 0600); err != nil {
		t.Fatal(err)
	}
	s.effectMu.Lock()
	s.runtimeRoot = root
	s.effectMu.Unlock()
	t.Cleanup(func() { s.Close() })
	return s, c, key
}

func logRequest(t *testing.T, s *Store, c Config, rawQuery string) (int, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/v1/logs?"+rawQuery, nil)
	r.Header.Set("X-Aurora-Epoch", c.Epoch)
	r.Header.Set("X-Aurora-Session", c.Session)
	w := httptest.NewRecorder()
	serveLogs(w, r, s)
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON: %s", w.Body.Bytes())
	}
	return w.Code, body
}

func TestServeLogsPaginationAndBothStreams(t *testing.T) {
	s, c, key := seededLogs(t)
	code, body := logRequest(t, s, c, "attempt="+key+"&stream=stdout&offset=0&limit=8")
	if code != 200 || body["data"] != "stdout-0" || body["nextOffset"] != float64(8) || body["hasMore"] != true || body["truncated"] != false || body["complete"] != true {
		t.Fatalf("unexpected stdout page: %d %+v", code, body)
	}
	code, body = logRequest(t, s, c, "attempt="+key+"&stream=stderr&offset=0&limit=65536")
	if code != 200 || body["data"] != "stderr-output" {
		t.Fatalf("unexpected stderr page: %d %+v", code, body)
	}
}

func TestServeLogsRejectsStaleAuthorityAndMalformedQueries(t *testing.T) {
	s, c, key := seededLogs(t)
	r := httptest.NewRequest(http.MethodGet, "/v1/logs?attempt="+key+"&stream=stdout&offset=0&limit=8", nil)
	r.Header.Set("X-Aurora-Epoch", "old")
	r.Header.Set("X-Aurora-Session", c.Session)
	w := httptest.NewRecorder()
	serveLogs(w, r, s)
	if w.Code != 409 {
		t.Fatalf("stale authority status %d", w.Code)
	}
	for _, query := range []string{
		"attempt=" + key + "&stream=stdout&offset=0&limit=8&extra=x",
		"attempt=" + key + "&stream=stdout&offset=0&limit=8&limit=8",
		"attempt=../bad&stream=stdout&offset=0&limit=8",
		"attempt=" + key + "&stream=stdout&offset=0&limit=65537",
	} {
		code, _ := logRequest(t, s, c, query)
		if code != 400 {
			t.Errorf("query %q status %d", query, code)
		}
	}
}

func TestServeLogsRejectsSymlinkHardlinkAndFIFO(t *testing.T) {
	s, c, key := seededLogs(t)
	root := s.runtimeRoot
	stdout := filepath.Join(root, key, "stdout.log")
	if err := os.Remove(stdout); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("stderr.log", stdout); err != nil {
		t.Fatal(err)
	}
	code, _ := logRequest(t, s, c, "attempt="+key+"&stream=stdout&offset=0&limit=8")
	if code != 503 {
		t.Fatalf("log symlink status %d", code)
	}
	if err := os.Remove(stdout); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(stdout, 0600); err != nil {
		t.Fatal(err)
	}
	code, _ = logRequest(t, s, c, "attempt="+key+"&stream=stdout&offset=0&limit=8")
	if code != 503 {
		t.Fatalf("FIFO status %d", code)
	}
	// A separately linked regular file is rejected by the singly-linked-file check.
	if err := os.Remove(stdout); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stdout, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(stdout, filepath.Join(root, key, "copy.log")); err != nil {
		t.Fatal(err)
	}
	code, _ = logRequest(t, s, c, "attempt="+key+"&stream=stdout&offset=0&limit=8")
	if code != 503 {
		t.Fatalf("hardlink status %d", code)
	}
}

func TestServeLogsRejectsAttemptDirectorySymlink(t *testing.T) {
	s, c, key := seededLogs(t)
	root := s.runtimeRoot
	if err := os.Rename(filepath.Join(root, key), filepath.Join(root, key+"-real")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(key+"-real", filepath.Join(root, key)); err != nil {
		t.Fatal(err)
	}
	code, _ := logRequest(t, s, c, "attempt="+key+"&stream=stdout&offset=0&limit=8")
	if code != 503 {
		t.Fatalf("attempt directory symlink status %d", code)
	}
}

func TestServeLogsOutputIsLiteral(t *testing.T) {
	s, c, key := seededLogs(t)
	contents := "<script>alert('literal')</script> λ\n"
	if err := os.WriteFile(filepath.Join(s.runtimeRoot, key, "stdout.log"), []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	status, page := logRequest(t, s, c, "attempt="+key+"&stream=stdout&offset=0&limit=65536")
	if status != 200 || page["data"] != contents {
		t.Fatalf("transformed output: %d %+v", status, page)
	}
}

func TestLogTransportRequiresPeerAndCurrentAuthority(t *testing.T) {
	s, c, key := seededLogs(t)
	pki := newPKI(t)
	base, client := testTransport(t, s, pki)
	path := base + "/v1/logs?attempt=" + key + "&stream=stdout&offset=0&limit=65536"
	headers := map[string]string{"X-Aurora-Epoch": c.Epoch, "X-Aurora-Session": c.Session}
	wrongPeer := client(pki.leaf(t, "other-scheduler", false))
	if status, _ := httpJSON(t, wrongPeer, "GET", path, nil, headers); status != 403 {
		t.Fatalf("wrong peer HTTP%d", status)
	}
	scheduler := client(pki.leaf(t, c.Peer, false))
	if status, _ := httpJSON(t, scheduler, "GET", path, nil, nil); status != 409 {
		t.Fatalf("missing authority HTTP%d", status)
	}
	if status, page := httpJSON(t, scheduler, "GET", path, nil, headers); status != 200 || page["data"] != "stdout-0123456789" {
		t.Fatalf("valid log request: HTTP%d %+v", status, page)
	}
	if status, _ := httpJSON(t, scheduler, "POST", path, nil, headers); status != 405 {
		t.Fatalf("log POST HTTP%d", status)
	}
}

func TestServeLogsRetentionAndOffsetStates(t *testing.T) {
	s, c, key := seededLogs(t)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		st, err := readForAttempt(b, key)
		if err != nil {
			return err
		}
		a := st.Attempts[key]
		a.Execution.StdoutDropped = 3
		st.Attempts[key] = a
		return save(b, st)
	}); err != nil {
		t.Fatal(err)
	}
	status, page := logRequest(t, s, c, "attempt="+key+"&stream=stdout&offset=17&limit=8")
	if status != 200 || page["data"] != "" || page["hasMore"] != false || page["truncated"] != true || page["nextOffset"] != float64(17) {
		t.Fatalf("invalid end page: %d %+v", status, page)
	}
	if status, _ := logRequest(t, s, c, "attempt="+key+"&stream=stdout&offset=18&limit=8"); status != 416 {
		t.Fatalf("past-end offset: %d", status)
	}
	if status, _ := logRequest(t, s, c, "attempt="+strings.Repeat("f", 64)+"&stream=stdout&offset=0&limit=8"); status != 404 {
		t.Fatalf("unknown attempt: %d", status)
	}
	if err := os.Remove(filepath.Join(s.runtimeRoot, key, "stdout.log")); err != nil {
		t.Fatal(err)
	}
	if status, _ := logRequest(t, s, c, "attempt="+key+"&stream=stdout&offset=0&limit=8"); status != 404 {
		t.Fatalf("removed log: %d", status)
	}
}

func TestServeLogsPreservesTextPageBoundary(t *testing.T) {
	s, c, key := seededLogs(t)
	contents := strings.Repeat("x", 65535) + "λdone"
	if err := os.WriteFile(filepath.Join(s.runtimeRoot, key, "stdout.log"), []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	status, page := logRequest(t, s, c, "attempt="+key+"&stream=stdout&offset=0&limit=65536")
	if status != 200 || page["nextOffset"] != float64(65535) || page["data"] != strings.Repeat("x", 65535) {
		t.Fatalf("split text page: %d %+v", status, page)
	}
	status, page = logRequest(t, s, c, "attempt="+key+"&stream=stdout&offset=65535&limit=65536")
	if status != 200 || page["data"] != "λdone" || page["hasMore"] != false {
		t.Fatalf("invalid next text page: %d %+v", status, page)
	}
}
