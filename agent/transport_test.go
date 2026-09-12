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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"aurora.local/agent/protocol"
	bolt "go.etcd.io/bbolt"
)

type testPKI struct {
	ca   *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newPKI(t *testing.T) testPKI {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, e := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	parsed, e := x509.ParseCertificate(der)
	if e != nil {
		t.Fatal(e)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return testPKI{parsed, key, pool}
}
func (p testPKI) leaf(t *testing.T, name string, server bool) tls.Certificate {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	serial, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if e != nil {
		t.Fatal(e)
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "scheduler"}, DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	if server {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, e := x509.CreateCertificate(rand.Reader, template, p.ca, &key.PublicKey, p.key)
	if e != nil {
		t.Fatal(e)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
func testTransport(t *testing.T, s *Store, pki testPKI) (string, func(tls.Certificate) *http.Client) {
	t.Helper()
	server := httptest.NewUnstartedServer(TransportHandler(s))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pki.leaf(t, "agent-a", true)}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.pool, MinVersion: tls.VersionTLS13}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server.URL, func(cert tls.Certificate) *http.Client {
		cfg := &tls.Config{RootCAs: pki.pool, MinVersion: tls.VersionTLS13}
		if len(cert.Certificate) > 0 {
			cfg.Certificates = []tls.Certificate{cert}
		}
		transport := &http.Transport{TLSClientConfig: cfg}
		t.Cleanup(transport.CloseIdleConnections)
		return &http.Client{Transport: transport, Timeout: 3 * time.Second}
	}
}
func httpJSON(t *testing.T, client *http.Client, method, url string, body []byte, headers map[string]string) (int, map[string]any) {
	t.Helper()
	req, e := http.NewRequest(method, url, bytes.NewReader(body))
	if e != nil {
		t.Fatal(e)
	}
	if method == "POST" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, e := client.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	raw, e := io.ReadAll(resp.Body)
	if e != nil {
		t.Fatal(e)
	}
	var value map[string]any
	if e = json.Unmarshal(raw, &value); e != nil {
		t.Fatalf("HTTP%d invalid JSON: %s", resp.StatusCode, raw)
	}
	return resp.StatusCode, value
}
func TestTransportRequiresVerifiedExactSchedulerSAN(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "state"), config())
	defer s.Close()
	pki := newPKI(t)
	base, client := testTransport(t, s, pki)
	for name, cert := range map[string]tls.Certificate{"absent": {}, "untrusted": newPKI(t).leaf(t, "scheduler", false)} {
		t.Run(name, func(t *testing.T) {
			resp, e := client(cert).Get(base + "/v1/state")
			if e == nil {
				resp.Body.Close()
				t.Fatal("unverified client accepted")
			}
		})
	}
	for _, name := range []string{"scheduler-other", "*.scheduler"} {
		code, _ := httpJSON(t, client(pki.leaf(t, name, false)), "GET", base+"/v1/state", nil, nil)
		if code != 403 {
			t.Fatal("nonexact SAN authorized", code)
		}
	}
	code, _ := httpJSON(t, client(pki.leaf(t, "scheduler", false)), "GET", base+"/v1/state", nil, nil)
	if code != 200 {
		t.Fatal(code)
	}
	request := httptest.NewRequest("GET", "https://example/v1/state", nil)
	record := httptest.NewRecorder()
	TransportHandler(s).ServeHTTP(record, request)
	if record.Code != 403 {
		t.Fatal("plaintext handler authorized")
	}
}
func TestTransportSessionReplayPagingAndACK(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	c := config()
	s := open(t, path, c)
	pki := newPKI(t)
	base, clients := testTransport(t, s, pki)
	client := clients(pki.leaf(t, "scheduler", false))
	body := fixture(t, "run")
	d := delivery(c, body)
	code, original := httpJSON(t, client, "POST", base+"/v1/deliver", d, nil)
	if code != 200 {
		t.Fatal(code, original)
	}
	next := c
	next.Epoch = "18446744073709551615"
	next.Session = "scheduler-max"
	code, updated := httpJSON(t, client, "POST", base+"/v1/session", protocol.Canonical(map[string]any{"schedulerEpoch": next.Epoch, "session": next.Session}), nil)
	if code != 200 || updated["schedulerEpoch"] != next.Epoch {
		t.Fatal(code, updated)
	}
	for _, pair := range []map[string]any{{"schedulerEpoch": c.Epoch, "session": c.Session}, {"schedulerEpoch": next.Epoch, "session": "conflict"}} {
		code, _ = httpJSON(t, client, "POST", base+"/v1/session", protocol.Canonical(pair), nil)
		if code != 409 {
			t.Fatal("stale session accepted", code)
		}
	}
	code, _ = httpJSON(t, client, "POST", base+"/v1/deliver", d, nil)
	if code != 409 {
		t.Fatal("old delivery session accepted")
	}
	code, replay := httpJSON(t, client, "POST", base+"/v1/deliver", delivery(next, body), nil)
	if code != 200 || protocol.Digest(original) != protocol.Digest(replay) {
		t.Fatal("refresh replay mismatch", code, replay)
	}
	stop := fixture(t, "stop")
	code, _ = httpJSON(t, client, "POST", base+"/v1/deliver", delivery(next, stop), nil)
	if code != 200 {
		t.Fatal(code)
	}
	code, page := httpJSON(t, client, "GET", base+"/v1/state?afterCursor=0&limit=1", nil, nil)
	if code != 200 || page["hasMore"] != true || page["nextCursor"] != "1" {
		t.Fatal(code, page)
	}
	state := page["state"].(map[string]any)
	raw := string(protocol.Canonical(state))
	if strings.Contains(raw, "FIXTURE_TEXT") || strings.Contains(raw, "argv") || strings.Contains(raw, `"env":`) {
		t.Fatal("private assignment leaked")
	}
	for _, a := range state["attempts"].(map[string]any) {
		if _, ok := a.(map[string]any)["sequence"].(string); !ok {
			t.Fatal("attempt sequence not string")
		}
	}
	code, page = httpJSON(t, client, "GET", base+"/v1/state?afterCursor=1&limit=1", nil, nil)
	if code != 200 || page["nextCursor"] != "2" || page["hasMore"] != false {
		t.Fatal(code, page)
	}
	ack := map[string]any{"version": "native-v1alpha1", "kind": "ObservationAck", "cluster": c.Cluster, "incarnation": c.Incarnation, "node": c.Node, "journal": c.Journal, "committedCursor": "2"}
	oldHeaders := map[string]string{"X-Aurora-Epoch": c.Epoch, "X-Aurora-Session": c.Session}
	code, _ = httpJSON(t, client, "POST", base+"/v1/ack", protocol.Canonical(ack), oldHeaders)
	if code != 409 {
		t.Fatal("stale ACK accepted")
	}
	headers := map[string]string{"X-Aurora-Epoch": next.Epoch, "X-Aurora-Session": next.Session}
	ack["committedCursor"] = "3"
	code, _ = httpJSON(t, client, "POST", base+"/v1/ack", protocol.Canonical(ack), headers)
	if code != 409 {
		t.Fatal("future ACK accepted")
	}
	ack["committedCursor"] = "2"
	code, _ = httpJSON(t, client, "POST", base+"/v1/ack", protocol.Canonical(ack), headers)
	if code != 200 {
		t.Fatal(code)
	}
	code, _ = httpJSON(t, client, "GET", base+"/v1/state?afterCursor=0", nil, nil)
	if code != 409 {
		t.Fatal("retention gap not explicit")
	}
	code, page = httpJSON(t, client, "GET", base+"/v1/state?afterCursor=2", nil, nil)
	if code != 200 || page["nextCursor"] != "2" {
		t.Fatal(code, page)
	}
	s.Close()
	s, e := OpenServer(path, c)
	if e != nil {
		t.Fatal("daemon reconnect with static enrollment", e)
	}
	defer s.Close()
	if s.CurrentConfig().Epoch != next.Epoch || s.CurrentConfig().Session != next.Session {
		t.Fatal("durable authority lost")
	}
	r, e := s.Admit(delivery(next, body), caller(next))
	if e != nil || r.Hash != original["bodySha256"] {
		t.Fatal(r, e)
	}
}
func TestTransportStrictBoundsAndSnapshotLimit(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "state"), config())
	defer s.Close()
	pki := newPKI(t)
	base, clients := testTransport(t, s, pki)
	client := clients(pki.leaf(t, "scheduler", false))
	for _, query := range []string{"?limit=0", "?limit=129", "?limit=1&limit=2", "?afterCursor=-0", "?unknown=1", "?limit=%ZZ"} {
		code, _ := httpJSON(t, client, "GET", base+"/v1/state"+query, nil, nil)
		if code != 400 {
			t.Fatal(query, code)
		}
	}
	code, _ := httpJSON(t, client, "PUT", base+"/v1/state", nil, nil)
	if code != 405 {
		t.Fatal(code)
	}
	for _, body := range []string{`{"schedulerEpoch":"1","schedulerEpoch":"2","session":"x"}`, `{"schedulerEpoch":"1","session":"x","extra":true}`, `[]`} {
		code, _ = httpJSON(t, client, "POST", base+"/v1/session", []byte(body), nil)
		if code != 400 {
			t.Fatal(code, body)
		}
	}
	code, _ = httpJSON(t, client, "POST", base+"/v1/session", bytes.Repeat([]byte{' '}, MaxTransportBytes+1), nil)
	if code != 413 {
		t.Fatal("oversize body", code)
	}
	s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("state"))
		st, e := read(bucket)
		if e != nil {
			return e
		}
		for i := 0; i <= MaxInventoryCommands; i++ {
			st.Commands[strconv.Itoa(i)] = Result{}
		}
		return save(bucket, st)
	})
	code, value := httpJSON(t, client, "GET", base+"/v1/state", nil, nil)
	if code != 503 || value["state"] != nil {
		t.Fatal("partial oversized inventory served", code, value)
	}
}
func TestTLSConfigFilesEnforceMutualTLS(t *testing.T) {
	pki := newPKI(t)
	cert := pki.leaf(t, "agent-a", true)
	dir := t.TempDir()
	options := HTTPSOptions{CertFile: filepath.Join(dir, "cert"), KeyFile: filepath.Join(dir, "key"), CAFile: filepath.Join(dir, "ca")}
	key, e := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if e != nil {
		t.Fatal(e)
	}
	os.WriteFile(options.CertFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0600)
	os.WriteFile(options.KeyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600)
	os.WriteFile(options.CAFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pki.ca.Raw}), 0600)
	cfg, e := serverTLS(options)
	if e != nil || cfg.MinVersion != tls.VersionTLS13 || cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatal(cfg, e)
	}
	os.WriteFile(options.CAFile, []byte("not a CA"), 0600)
	if _, e = serverTLS(options); e == nil {
		t.Fatal("invalid trust roots accepted")
	}
}

func TestServeHTTPSExecutesDurablyAndReopensWithStaticEnrollment(t *testing.T) {
	pki := newPKI(t)
	cert := pki.leaf(t, "agent-a", true)
	dir := t.TempDir()
	options := HTTPSOptions{Listen: fmt.Sprintf("127.0.0.1:%d", freePort(t)), CertFile: filepath.Join(dir, "cert"), KeyFile: filepath.Join(dir, "key"), CAFile: filepath.Join(dir, "ca")}
	key, e := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if e != nil {
		t.Fatal(e)
	}
	os.WriteFile(options.CertFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0600)
	os.WriteFile(options.KeyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600)
	os.WriteFile(options.CAFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pki.ca.Raw}), 0600)
	cfg := config()
	next := cfg
	next.Epoch = "18446744073709551615"
	next.Session = "scheduler-max"
	path := filepath.Join(dir, "state")
	body := runtimeBody(t, "batch", filepath.Join(dir, "effects"), 0)
	base := "https://" + options.Listen
	for generation := 0; generation < 2; generation++ {
		s, e := OpenServer(path, cfg)
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- ServeHTTPS(ctx, s, testRuntimeOpts(t, filepath.Join(dir, "work")), options) }()
		transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pki.pool, Certificates: []tls.Certificate{pki.leaf(t, "scheduler", false)}, MinVersion: tls.VersionTLS13}}
		client := &http.Client{Transport: transport, Timeout: time.Second}
		finish := func() {
			transport.CloseIdleConnections()
			cancel()
			select {
			case e := <-done:
				if e != nil {
					t.Error(e)
				}
			case <-time.After(3 * time.Second):
				t.Error("HTTPS shutdown timeout")
			}
			if e := s.Close(); e != nil {
				t.Error(e)
			}
		}
		until := time.Now().Add(3 * time.Second)
		started := false
		for time.Now().Before(until) {
			response, e := client.Get(base + "/v1/state")
			if e == nil {
				response.Body.Close()
				if response.StatusCode == 200 {
					started = true
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !started {
			finish()
			t.Fatal("HTTPS did not start")
		}
		if generation == 0 {
			code, _ := httpJSON(t, client, "POST", base+"/v1/session", protocol.Canonical(map[string]any{"schedulerEpoch": next.Epoch, "session": next.Session}), nil)
			if code != 200 {
				finish()
				t.Fatal(code)
			}
		}
		code, page := httpJSON(t, client, "GET", base+"/v1/state", nil, nil)
		if code != 200 || page["config"].(map[string]any)["schedulerEpoch"] != next.Epoch {
			finish()
			t.Fatal("authority not restored", code, page)
		}
		code, _ = httpJSON(t, client, "POST", base+"/v1/deliver", delivery(next, body), nil)
		if code != 200 {
			finish()
			t.Fatal(code)
		}
		complete := false
		until = time.Now().Add(3 * time.Second)
		for time.Now().Before(until) {
			code, page = httpJSON(t, client, "GET", base+"/v1/state", nil, nil)
			if code != 200 {
				break
			}
			attempts := page["state"].(map[string]any)["attempts"].(map[string]any)
			for _, raw := range attempts {
				a := raw.(map[string]any)
				x, _ := a["execution"].(map[string]any)
				if x["outcome"] == "succeeded" && x["cleanup"] == "complete" && a["reserved"] == false {
					complete = true
				}
			}
			if complete {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		finish()
		if !complete {
			t.Fatal("HTTPS workload did not complete", page)
		}
	}
	effects, e := os.ReadFile(filepath.Join(dir, "effects"))
	if e != nil || string(effects) != "launch\n" {
		t.Fatal("HTTPS reconnect duplicated execution", string(effects), e)
	}
}

func TestServeHTTPSBindFailurePreservesSupervisedWork(t *testing.T) {
	root := t.TempDir()
	c := config()
	s := open(t, filepath.Join(root, "state"), c)
	defer s.Close()
	opts := supervisorOpts(t, filepath.Join(root, "work"))
	b := runtimeBody(t, "sleep", filepath.Join(root, "marker"), 0)
	p := b["assignment"].(map[string]any)
	p["ports"] = []any{}
	p["readiness"] = map[string]any{"kind": "none"}
	if _, err := s.Admit(delivery(c, b), caller(c)); err != nil {
		t.Fatal(err)
	}
	r, err := NewRuntime(s, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if r.closed {
			reopened, reopenErr := NewRuntime(s, opts)
			if reopenErr != nil {
				t.Error(reopenErr)
				return
			}
			r = reopened
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err = r.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}()
	runUntil(t, r, func(st State) bool { return onlyAttempt(st).Execution.Outcome == "running" })
	before, err := s.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	pki := newPKI(t)
	cert := pki.leaf(t, "agent-a", true)
	options := HTTPSOptions{Listen: occupied.Addr().String(), CertFile: filepath.Join(root, "cert"), KeyFile: filepath.Join(root, "key"), CAFile: filepath.Join(root, "ca")}
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	for path, data := range map[string][]byte{
		options.CertFile: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}),
		options.KeyFile:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}),
		options.CAFile:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pki.ca.Raw}),
	} {
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err = ServeHTTPS(context.Background(), s, opts, options); err == nil {
		t.Fatal("occupied listener accepted")
	}
	after, err := s.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	a := onlyAttempt(after)
	old := onlyAttempt(before)
	if a.Stopped || !a.Reserved() || a.Execution.PID != old.Execution.PID || a.Execution.Start != old.Execution.Start || a.Execution.ExitCode != nil {
		t.Fatalf("bind failure drained task: %+v", a)
	}
	process, err := processInfo(a.Execution.PID)
	if err != nil || process.Start != a.Execution.Start || process.State == "Z" || process.State == "X" {
		t.Fatalf("workload did not survive bind failure: %+v %v", process, err)
	}
	if err = occupied.Close(); err != nil {
		t.Fatal(err)
	}
	// An explicit expired context is operator shutdown, unlike the bind fault.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err = ServeHTTPS(ctx, s, opts, options); err != nil {
		t.Fatal(err)
	}
	after, err = s.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	a = onlyAttempt(after)
	if !a.Stopped || a.Reserved() || a.Execution.Outcome != "stopped" {
		t.Fatalf("explicit context deadline did not drain: %+v", a)
	}
}
