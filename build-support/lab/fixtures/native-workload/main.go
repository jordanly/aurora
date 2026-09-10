/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type options struct {
	mode, identity, evidence, address   string
	port, readyDelay, exitCode, logSpam int
	refuseTerm                          bool
}

func bounded(name, value string, min, max int) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%s out of bounds", name)
	}
	return n, nil
}

func parse() (options, error) {
	var o options
	flag.StringVar(&o.mode, "mode", "", "batch, service, probe, or port-check")
	flag.StringVar(&o.identity, "identity", "", "explicit workload identity")
	flag.StringVar(&o.evidence, "evidence", "", "explicit durable evidence file")
	flag.StringVar(&o.address, "address", "127.0.0.1", "exact IPv4 bind address")
	var port, delay, exit, spam string
	flag.StringVar(&port, "port", "0", "exact TCP4 port")
	flag.StringVar(&delay, "ready-delay-ms", "0", "bounded readiness delay")
	flag.StringVar(&exit, "exit-code", "0", "bounded batch exit code")
	flag.StringVar(&spam, "log-spam-lines", "0", "bounded stderr lines")
	flag.BoolVar(&o.refuseTerm, "refuse-sigterm", false, "ignore SIGTERM until externally escalated")
	flag.Parse()
	if flag.NArg() != 0 {
		return o, errors.New("unexpected arguments")
	}
	if o.mode != "batch" && o.mode != "service" && o.mode != "probe" && o.mode != "port-check" {
		return o, errors.New("mode must be batch, service, probe, or port-check")
	}
	if o.mode != "probe" && o.mode != "port-check" && (o.identity == "" || len(o.identity) > 128 || strings.ContainsAny(o.identity, "\r\n\x00")) {
		return o, errors.New("identity is missing or invalid")
	}
	for _, r := range o.identity {
		if r < 0x20 || r > 0x7e {
			return o, errors.New("identity must be printable ASCII")
		}
	}
	if o.mode != "probe" && o.mode != "port-check" && (o.evidence == "" || !filepath.IsAbs(o.evidence)) {
		return o, errors.New("evidence must be an absolute path")
	}
	var err error
	if o.port, err = bounded("port", port, 1, 65535); err != nil && o.mode == "service" {
		return o, err
	}
	if o.mode == "batch" || o.mode == "probe" {
		o.port = 0
	}
	if o.readyDelay, err = bounded("ready-delay-ms", delay, 0, 30000); err != nil {
		return o, err
	}
	if o.exitCode, err = bounded("exit-code", exit, 0, 125); err != nil {
		return o, err
	}
	if o.logSpam, err = bounded("log-spam-lines", spam, 0, 1000); err != nil {
		return o, err
	}
	if (o.mode == "service" || o.mode == "probe" || o.mode == "port-check") && (net.ParseIP(o.address) == nil || net.ParseIP(o.address).To4() == nil) {
		return o, errors.New("address must be an IPv4 address")
	}
	if o.mode == "probe" || o.mode == "port-check" {
		o.port, err = bounded("port", port, 1, 65535)
		if err != nil {
			return o, err
		}
	}
	return o, nil
}

func appendEvidence(path string, event map[string]any) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("evidence path must be absolute without traversal")
	}
	for _, ch := range path {
		if ch < 32 || ch > 126 {
			return errors.New("evidence path must be printable ASCII")
		}
	}
	parent := filepath.Dir(path)
	for p := parent; p != filepath.Dir(p); p = filepath.Dir(p) {
		st, err := os.Lstat(p)
		if err != nil || st.Mode()&os.ModeSymlink != 0 || !st.IsDir() {
			return errors.New("evidence parent must be real non-symlink directories")
		}
	}
	if st, err := os.Lstat(path); err == nil && (st.Mode()&os.ModeSymlink != 0 || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0) {
		return errors.New("evidence must be a private regular file")
	}
	b, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if len(b)+1 > 1<<20 {
		return errors.New("evidence event too large")
	}
	fd, err := syscall.Open(path, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_APPEND|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		syscall.Close(fd)
		return errors.New("evidence open failed")
	}
	defer f.Close()
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size()+int64(len(b)+1) > 1<<20 {
		return errors.New("evidence must be a bounded regular file")
	}
	if _, err = f.Write(append(b, '\n')); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if st, err := os.Open(parent); err == nil {
		defer st.Close()
		return st.Sync()
	}
	return errors.New("evidence parent sync failed")
}

func logSpam(n int) {
	for i := 0; i < n; i++ {
		fmt.Fprintf(os.Stderr, "fixture-log-%04d\n", i)
	}
}

func main() {
	o, err := parse()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(64)
	}
	if o.mode == "port-check" {
		listener, err := net.Listen("tcp4", net.JoinHostPort(o.address, strconv.Itoa(o.port)))
		if err != nil {
			fmt.Fprintln(os.Stderr, "exact port unavailable")
			os.Exit(98)
		}
		defer listener.Close()
		if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"available": true, "port": o.port}); err != nil {
			os.Exit(74)
		}
		return
	}
	if o.mode == "probe" {
		probe(o)
		return
	}
	start := time.Now().UTC().Format(time.RFC3339Nano)
	event := map[string]any{"event": "launch", "mode": o.mode, "identity": o.identity, "startedAt": start}
	if err := appendEvidence(o.evidence, event); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(74)
	}
	logSpam(o.logSpam)
	if o.mode == "batch" {
		os.Exit(o.exitCode)
	}
	ln, err := net.Listen("tcp4", net.JoinHostPort(o.address, strconv.Itoa(o.port)))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(98)
	}
	defer ln.Close()
	ready := make(chan struct{})
	go func() { time.Sleep(time.Duration(o.readyDelay) * time.Millisecond); close(ready) }()
	mux := http.NewServeMux()
	mux.HandleFunc("/identity", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, o.identity+"\n") })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		select {
		case <-ready:
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "ready\n")
		default:
			http.Error(w, "not ready\n", http.StatusServiceUnavailable)
		}
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second, IdleTimeout: 2 * time.Second}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		for sig := range sigs {
			if err := appendEvidence(o.evidence, map[string]any{
				"event": "signal", "identity": o.identity, "signal": sig.String(),
			}); err != nil {
				fmt.Fprintln(os.Stderr, "signal evidence write failed")
				os.Exit(74)
			}
			if o.refuseTerm && sig == syscall.SIGTERM {
				continue
			}
			_ = srv.Close()
			return
		}
	}()
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(98)
	}
}

func probe(o options) {
	client := &http.Client{Timeout: time.Second,
		Transport:     &http.Transport{DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	base := "http://" + net.JoinHostPort(o.address, strconv.Itoa(o.port))
	identity, err := client.Get(base + "/identity")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(98)
	}
	b, err := io.ReadAll(io.LimitReader(identity.Body, 130))
	identity.Body.Close()
	if err != nil || len(b) > 129 || identity.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "identity probe failed")
		os.Exit(98)
	}
	ready, err := client.Get(base + "/ready")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(98)
	}
	rb, err := io.ReadAll(io.LimitReader(ready.Body, 129))
	ready.Body.Close()
	if err != nil || len(rb) > 128 || (ready.StatusCode != http.StatusOK && ready.StatusCode != http.StatusServiceUnavailable) {
		fmt.Fprintln(os.Stderr, "ready probe failed")
		os.Exit(98)
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"identity": strings.TrimSpace(string(b)), "ready": ready.StatusCode == http.StatusOK})
}
