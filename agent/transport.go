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
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"aurora.local/agent/protocol"
)

const MaxTransportBytes = 1 << 20
const MaxInventoryAttempts = 128
const MaxInventoryCommands = 1024

type HTTPSOptions struct{ Listen, CertFile, KeyFile, CAFile string }

// TransportHandler requires a verified TLS connection and exact leaf DNS SAN.
// It never accepts a peer or caller authority assertion from command JSON.
func TransportHandler(store *Store) http.Handler {
	return transportHandler(store, defaultWatchTiming)
}

func transportHandler(store *Store, timing watchTiming) http.Handler {
	slots := make(chan struct{}, 8)
	watchSlots := make(chan struct{}, 2)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestSlots := slots
		if r.URL.Path == "/v1/watch" {
			requestSlots = watchSlots
		}
		select {
		case requestSlots <- struct{}{}:
			defer func() { <-requestSlots }()
		default:
			transportError(w, 503, "server busy")
			return
		}
		cfg := store.CurrentConfig()
		if !authenticatedPeer(r, cfg.Peer) {
			transportError(w, 403, "peer rejected")
			return
		}
		if r.URL.Path != "/v1/watch" && r.URL.Path != "/v1/state" && r.URL.Path != "/v1/session" && r.URL.Path != "/v1/deliver" && r.URL.Path != "/v1/ack" {
			transportError(w, 404, "unknown endpoint")
			return
		}
		if r.URL.Path == "/v1/state" || r.URL.Path == "/v1/watch" {
			if r.Method != "GET" {
				transportError(w, 405, "method rejected")
				return
			}
			if r.URL.Path == "/v1/watch" {
				serveWatch(w, r, store, timing)
			} else {
				serveState(w, r, store)
			}
			return
		}
		if r.Method != "POST" {
			transportError(w, 405, "method rejected")
			return
		}
		if r.URL.RawQuery != "" {
			transportError(w, 400, "query rejected")
			return
		}
		media, _, e := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if e != nil || media != "application/json" {
			transportError(w, 415, "application/json required")
			return
		}
		data, e := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxTransportBytes))
		if e != nil {
			transportError(w, 413, "body limit or read failure")
			return
		}
		switch r.URL.Path {
		case "/v1/session":
			value, e := protocol.Decode(data)
			if e != nil || len(value) != 2 {
				transportError(w, 400, "invalid session request")
				return
			}
			epoch, okEpoch := value["schedulerEpoch"].(string)
			session, okSession := value["session"].(string)
			if !okEpoch || !okSession {
				transportError(w, 400, "invalid session request")
				return
			}
			current, e := store.RefreshSession(cfg.Peer, epoch, session)
			if e != nil {
				transportError(w, 409, "session rejected")
				return
			}
			transportJSON(w, 200, current)
		case "/v1/deliver":
			result, e := store.Admit(data, Caller{Peer: cfg.Peer, Epoch: cfg.Epoch, Session: cfg.Session})
			if e != nil {
				transportError(w, 409, "delivery rejected")
				return
			}
			status := 200
			if result.Outcome != "accepted" {
				status = 409
			}
			transportJSON(w, status, result)
		case "/v1/ack":
			if len(r.Header.Values("X-Aurora-Epoch")) != 1 || len(r.Header.Values("X-Aurora-Session")) != 1 || r.Header.Get("X-Aurora-Epoch") != cfg.Epoch || r.Header.Get("X-Aurora-Session") != cfg.Session {
				transportError(w, 409, "stale ACK authority")
				return
			}
			if e := store.Ack(data, Caller{Peer: cfg.Peer, Epoch: cfg.Epoch, Session: cfg.Session}, 128); e != nil {
				transportError(w, 409, "ACK rejected")
				return
			}
			transportJSON(w, 200, map[string]bool{"ok": true})
		}
	})
}
func authenticatedPeer(r *http.Request, peer string) bool {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
		return false
	}
	for _, name := range r.TLS.PeerCertificates[0].DNSNames {
		if name == peer {
			return true
		}
	}
	return false
}
func counter(value string) (uint64, error) {
	n, e := strconv.ParseUint(value, 10, 64)
	if e != nil || strconv.FormatUint(n, 10) != value {
		return 0, errors.New("invalid counter")
	}
	return n, nil
}
func serveState(w http.ResponseWriter, r *http.Request, store *Store) {
	if r.ContentLength > 0 || len(r.TransferEncoding) != 0 {
		transportError(w, 400, "GET body rejected")
		return
	}
	q, e := url.ParseQuery(r.URL.RawQuery)
	if e != nil {
		transportError(w, 400, "invalid query")
		return
	}
	for k, v := range q {
		if (k != "afterCursor" && k != "limit") || len(v) != 1 {
			transportError(w, 400, "invalid query")
			return
		}
	}
	after := uint64(0)
	if value, ok := q["afterCursor"]; ok {
		after, e = counter(value[0])
		if e != nil {
			transportError(w, 400, "invalid cursor")
			return
		}
	}
	limit := uint64(128)
	if value, ok := q["limit"]; ok {
		limit, e = counter(value[0])
		if e != nil || limit < 1 || limit > 128 {
			transportError(w, 400, "invalid page limit")
			return
		}
	}
	st, e := store.Inspect()
	if e != nil {
		transportError(w, 503, "state unavailable")
		return
	}
	if after < st.Ack || after > st.Cursor {
		transportError(w, 409, "cursor scope or retention gap")
		return
	}
	if len(st.Attempts) > MaxInventoryAttempts || len(st.Commands) > MaxInventoryCommands {
		transportError(w, 503, "inventory profile limit")
		return
	}
	observations := []map[string]any{}
	next := after
	more := false
	for _, o := range st.Observations {
		cursor, e := counter(o["cursor"].(string))
		if e != nil {
			transportError(w, 503, "state unavailable")
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
	st.Observations = observations
	transportJSON(w, 200, map[string]any{"config": st.Config, "state": PublicState(st), "nextCursor": strconv.FormatUint(next, 10), "hasMore": more})
}
func transportError(w http.ResponseWriter, status int, message string) {
	transportJSON(w, status, map[string]string{"error": message})
}
func transportJSON(w http.ResponseWriter, status int, value any) {
	data, e := json.Marshal(value)
	if e != nil || len(data) > MaxTransportBytes {
		status = 503
		data = []byte(`{"error":"response profile limit"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(append(data, '\n'))
}
func readTransportFile(path string) ([]byte, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, errors.New("TLS CA unavailable")
	}
	defer f.Close()
	data, e := io.ReadAll(io.LimitReader(f, MaxTransportBytes+1))
	if e != nil || len(data) > MaxTransportBytes {
		return nil, errors.New("TLS CA read limit")
	}
	return data, nil
}
func serverTLS(options HTTPSOptions) (*tls.Config, error) {
	cert, e := tls.LoadX509KeyPair(options.CertFile, options.KeyFile)
	if e != nil {
		return nil, errors.New("TLS certificate/key unavailable")
	}
	data, e := readTransportFile(options.CAFile)
	if e != nil {
		return nil, e
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, errors.New("invalid TLS client CA")
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots, MinVersion: tls.VersionTLS13, NextProtos: []string{"http/1.1"}}, nil
}

// ServeHTTPS owns runtime ticking and graceful drain. Transport loss does not
// stop workloads; daemon shutdown drains them using the existing runtime policy.
func ServeHTTPS(ctx context.Context, store *Store, runtimeOptions RuntimeOptions, options HTTPSOptions) (err error) {
	if options.Listen == "" || options.CertFile == "" || options.KeyFile == "" || options.CAFile == "" {
		return errors.New("listen and TLS cert/key/CA required")
	}
	tlsConfig, e := serverTLS(options)
	if e != nil {
		return e
	}
	runtime, e := NewRuntime(store, runtimeOptions)
	if e != nil {
		return e
	}
	defer func() {
		drain, cancel := context.WithTimeout(context.Background(), 65*time.Second)
		defer cancel()
		err = errors.Join(err, runtime.Shutdown(drain), runtime.Close())
	}()
	listener, e := net.Listen("tcp", options.Listen)
	if e != nil {
		return e
	}
	serverCtx, cancelServer := context.WithCancel(ctx)
	defer cancelServer()
	server := &http.Server{BaseContext: func(net.Listener) context.Context { return serverCtx }, Handler: TransportHandler(store), TLSConfig: tlsConfig, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(tls.NewListener(listener, tlsConfig)) }()
	defer server.Close()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return server.Shutdown(shutdown)
		case e := <-serveResult:
			if errors.Is(e, http.ErrServerClosed) {
				return nil
			}
			return e
		case <-ticker.C:
			if e = runtime.Tick(ctx); e != nil {
				if ctx.Err() != nil && errors.Is(e, context.Canceled) {
					return nil
				}
				return e
			}
		}
	}
}
