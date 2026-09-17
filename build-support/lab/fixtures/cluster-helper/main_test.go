/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package main

import (
	"bytes"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func startHelper(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	encoded, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(), "CLUSTER_HELPER_ARGS="+string(encoded))
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-waited:
			if err != nil {
				t.Errorf("helper failed: %v: %s", err, output.String())
			}
		case <-time.After(7 * time.Second):
			_ = cmd.Process.Kill()
			select {
			case <-waited:
			case <-time.After(time.Second):
				t.Error("helper failed to reap")
			}
			t.Error("helper did not shut down")
		}
	})
	return cmd
}
func until(t *testing.T, condition func() bool) {
	t.Helper()
	end := time.Now().Add(4 * time.Second)
	for time.Now().Before(end) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("bounded condition timed out")
}
func state(path string) map[string]any {
	b, e := os.ReadFile(filepath.Join(path, "daemon.json"))
	if e != nil {
		return nil
	}
	var v map[string]any
	if json.Unmarshal(b, &v) != nil {
		return nil
	}
	return v
}
func generation(path string) int {
	s := state(path)
	if s == nil {
		return 0
	}
	return int(s["generation"].(float64))
}
func pid(path string) int {
	s := state(path)
	if s == nil {
		return 0
	}
	return int(s["pid"].(float64))
}
func action(t *testing.T, dir, name string) {
	t.Helper()
	if e := os.WriteFile(filepath.Join(dir, name), []byte("1"), 0600); e != nil {
		t.Fatal(e)
	}
}
func gone(id int) bool { return id > 0 && errors.Is(syscall.Kill(id, 0), syscall.ESRCH) }
func TestKeeperCrashPauseResume(t *testing.T) {
	d := t.TempDir()
	if e := os.Chmod(d, 0700); e != nil {
		t.Fatal(e)
	}
	startHelper(t, "keeper", "--control", d, "--", "/bin/sleep", "30")
	until(t, func() bool { return generation(d) == 1 })
	first := pid(d)
	if e := syscall.Kill(first, 0); e != nil {
		t.Fatal("daemon not running", e)
	}
	time.Sleep(100 * time.Millisecond)
	if generation(d) != 1 {
		t.Fatal("sleep child exited unexpectedly")
	}
	action(t, d, "crash")
	until(t, func() bool { return generation(d) == 2 })
	until(t, func() bool { return gone(first) })
	second := pid(d)
	if second == first {
		t.Fatal("daemon identity reused")
	}
	action(t, d, "pause")
	until(t, func() bool { s := state(d); return s != nil && s["paused"] == true && pid(d) == 0 })
	until(t, func() bool { return gone(second) })
	time.Sleep(150 * time.Millisecond)
	if generation(d) != 2 {
		t.Fatal("paused daemon restarted")
	}
	action(t, d, "resume")
	until(t, func() bool { return generation(d) == 3 && pid(d) > 0 })
	if state(d)["paused"] != false {
		t.Fatal("resume not recorded")
	}
	events, e := os.ReadFile(filepath.Join(d, "events.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	starts, exits := 0, 0
	for _, line := range bytes.Split(bytes.TrimSpace(events), []byte{'\n'}) {
		var event map[string]any
		if json.Unmarshal(line, &event) != nil {
			t.Fatal("invalid event")
		}
		if event["event"] == "start" {
			starts++
		}
		if event["event"] == "exit" {
			exits++
		}
		if starts-exits < 0 || starts-exits > 1 {
			t.Fatal("daemon generations overlap")
		}
	}
	if starts != 3 || exits != 2 {
		t.Fatalf("expected 3starts/2exits, got %d/%d", starts, exits)
	}
}
func TestKeeperNaturalExit(t *testing.T) {
	d := t.TempDir()
	if e := os.Chmod(d, 0700); e != nil {
		t.Fatal(e)
	}
	startHelper(t, "keeper", "--control", d, "--", "/bin/true")
	until(t, func() bool { return generation(d) >= 3 })
	action(t, d, "pause")
	until(t, func() bool { s := state(d); return s != nil && s["paused"] == true })
	stable := generation(d)
	time.Sleep(150 * time.Millisecond)
	if generation(d) != stable {
		t.Fatal("natural exit loop ignored pause")
	}
}
func TestCertgenIdentitiesAndNoOverwrite(t *testing.T) {
	d := filepath.Join(t.TempDir(), "certs")
	if e := certgen([]string{"--out", d}); e != nil {
		t.Fatal(e)
	}
	caPEM, e := os.ReadFile(filepath.Join(d, "ca.crt"))
	if e != nil {
		t.Fatal(e)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("CA invalid")
	}
	for _, name := range []string{"ca", "scheduler", "operator", "agent-a", "agent-b"} {
		b, e := os.ReadFile(filepath.Join(d, name+".crt"))
		if e != nil {
			t.Fatal(e)
		}
		block, _ := pem.Decode(b)
		if block == nil {
			t.Fatal("no certificate")
		}
		cert, e := x509.ParseCertificate(block.Bytes)
		if e != nil {
			t.Fatal(e)
		}
		if name != "ca" {
			if len(cert.AuthorityKeyId) == 0 {
				t.Fatal("missing issuer authority key identifier")
			}
			if len(cert.DNSNames) != 1 || cert.DNSNames[0] != name {
				t.Fatal("wrong DNS SAN")
			}
			if _, e = cert.Verify(x509.VerifyOptions{Roots: roots, DNSName: name, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); e != nil {
				t.Fatal(e)
			}
		} else if !cert.IsCA {
			t.Fatal("CA constraints")
		}
		st, e := os.Stat(filepath.Join(d, name+".key"))
		if e != nil || st.Mode().Perm() != 0600 {
			t.Fatal("key mode", e)
		}
	}
	st, e := os.Stat(d)
	if e != nil || st.Mode().Perm() != 0700 {
		t.Fatal("directory mode")
	}
	if e = certgen([]string{"--out", d}); e == nil {
		t.Fatal("reused certificate directory accepted")
	}
	after, _ := os.ReadFile(filepath.Join(d, "ca.crt"))
	if !bytes.Equal(caPEM, after) {
		t.Fatal("existing CA overwritten")
	}
	if e = writePEM(filepath.Join(d, "ca.crt"), "CERTIFICATE", []byte("bad"), 0600); e == nil {
		t.Fatal("certificate overwrite allowed")
	}
}
func TestProxyBlockClosesBothSidesAndRecovers(t *testing.T) {
	target, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { target.Close() })
	closed := make(chan struct{}, 10)
	go func() {
		for {
			c, e := target.Accept()
			if e != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c); closed <- struct{}{} }()
		}
	}()
	ephemeral, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	address := ephemeral.Addr().String()
	ephemeral.Close()
	d := t.TempDir()
	if e := os.Chmod(d, 0700); e != nil {
		t.Fatal(e)
	}
	startHelper(t, "proxy", "--listen", address, "--target", target.Addr().String(), "--control", d)
	var client net.Conn
	until(t, func() bool { client, e = net.DialTimeout("tcp4", address, 100*time.Millisecond); return e == nil })
	defer client.Close()
	echo := func(c net.Conn) {
		t.Helper()
		c.SetDeadline(time.Now().Add(time.Second))
		if _, e := c.Write([]byte("ping")); e != nil {
			t.Fatal(e)
		}
		b := make([]byte, 4)
		if _, e := io.ReadFull(c, b); e != nil || string(b) != "ping" {
			t.Fatal("proxy forwarding failed", e)
		}
	}
	echo(client)
	action(t, d, "blocked")
	client.SetReadDeadline(time.Now().Add(700 * time.Millisecond))
	var b [1]byte
	_, e = client.Read(b[:])
	if e == nil {
		t.Fatal("blocked connection survived")
	}
	if ne, ok := e.(net.Error); ok && ne.Timeout() {
		t.Fatal("read timed out; connection was not closed")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("upstream connection leaked")
	}
	blocked, e := net.DialTimeout("tcp4", address, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	blocked.SetDeadline(time.Now().Add(700 * time.Millisecond))
	_, e = blocked.Read(b[:])
	blocked.Close()
	if e == nil {
		t.Fatal("blocked new connection admitted")
	}
	if ne, ok := e.(net.Error); ok && ne.Timeout() {
		t.Fatal("blocked new connection stalled")
	}
	if e = os.Remove(filepath.Join(d, "blocked")); e != nil {
		t.Fatal(e)
	}
	recovered, e := net.DialTimeout("tcp4", address, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	defer recovered.Close()
	echo(recovered)
}
func TestHelperProcess(t *testing.T) {
	encoded := os.Getenv("CLUSTER_HELPER_ARGS")
	if encoded == "" {
		return
	}
	var args []string
	if e := json.Unmarshal([]byte(encoded), &args); e != nil {
		os.Exit(2)
	}
	var err error
	switch args[0] {
	case "health":
		err = health(args[1:])
	case "keeper":
		err = keeper(args[1:])
	case "proxy":
		err = proxy(args[1:])
	default:
		err = errors.New("unknown helper")
	}
	if err != nil {
		os.Stderr.WriteString(strconv.Quote(err.Error()))
		os.Exit(2)
	}
	os.Exit(0)
}

func TestHealthListenerLossKeepsProcessAlive(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	cmd := startHelper(t, "health", "--port", strconv.Itoa(port), "--delay-ms", "100", "--close-after-ms", "300", "--log-bytes", "128")
	connects := func() bool {
		connection, err := net.DialTimeout("tcp4", address, 30*time.Millisecond)
		if err != nil {
			return false
		}
		connection.Close()
		return true
	}
	until(t, connects)
	until(t, func() bool { return !connects() })
	if err = cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatal("listener loss exited workload:", err)
	}
}

func TestHealthBounds(t *testing.T) {
	for _, args := range [][]string{{"--port", "0"}, {"--port", "65536"}, {"--delay-ms", "600001"}, {"--close-after-ms", "-1"}, {"--log-bytes", "2097153"}, {"extra"}} {
		if err := health(args); err == nil {
			t.Fatalf("invalid accepted: %v", args)
		}
	}
}
