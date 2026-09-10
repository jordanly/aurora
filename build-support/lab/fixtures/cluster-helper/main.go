/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		die("keeper, proxy, or certgen")
	}
	var err error
	switch os.Args[1] {
	case "keeper":
		err = keeper(os.Args[2:])
	case "proxy":
		err = proxy(os.Args[2:])
	case "certgen":
		err = certgen(os.Args[2:])
	default:
		err = errors.New("unknown subcommand")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
func die(s string) { fmt.Fprintln(os.Stderr, s); os.Exit(2) }
func atomicJSON(path string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	t := path + ".tmp-" + strconv.Itoa(os.Getpid())
	if e = os.WriteFile(t, append(b, '\n'), 0600); e != nil {
		return e
	}
	return os.Rename(t, path)
}
func event(root string, v any) {
	p := filepath.Join(root, "events.jsonl")
	b, e := json.Marshal(v)
	if e != nil {
		return
	}
	f, e := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		return
	}
	defer f.Close()
	if st, _ := f.Stat(); st != nil && st.Size()+int64(len(b)+1) > 1<<20 {
		return
	}
	_, _ = f.Write(append(b, '\n'))
	_ = f.Sync()
}

// Keeper is a lab-only daemon parent. Workload containment stays in the agent.
func keeper(a []string) (err error) {
	fs := flag.NewFlagSet("keeper", flag.ContinueOnError)
	control := fs.String("control", "/control", "private control directory")
	if err = fs.Parse(a); err != nil {
		return err
	}
	args := fs.Args()
	if len(args) == 0 {
		return errors.New("keeper -- command [args]")
	}
	if err = privateDirectory(*control, false); err != nil {
		return err
	}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigs)
	var cmd *exec.Cmd
	var exited chan error
	generation, paused := 0, false
	record := func() error {
		pid := 0
		if cmd != nil {
			pid = cmd.Process.Pid
		}
		return atomicJSON(filepath.Join(*control, "daemon.json"), map[string]any{
			"pid": pid, "generation": generation, "paused": paused, "startedAt": time.Now().UTC().Format(time.RFC3339Nano)})
	}
	stop := func(sig os.Signal) {
		if cmd == nil {
			return
		}
		_ = cmd.Process.Signal(sig)
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
		}
		event(*control, map[string]any{"event": "exit", "pid": cmd.Process.Pid, "generation": generation})
		cmd, exited = nil, nil
	}
	defer func() { stop(syscall.SIGKILL) }()
	start := func() error {
		if paused || cmd != nil {
			return nil
		}
		next := exec.Command(args[0], args[1:]...)
		next.Stdout, next.Stderr = os.Stdout, os.Stderr
		if e := next.Start(); e != nil {
			return e
		}
		cmd = next
		generation++
		completed := make(chan error, 1)
		exited = completed
		go func() { completed <- next.Wait() }()
		event(*control, map[string]any{"event": "start", "pid": cmd.Process.Pid, "generation": generation})
		return record()
	}
	if err = start(); err != nil {
		return err
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case s := <-sigs:
			stop(s)
			return record()
		case <-exited:
			event(*control, map[string]any{"event": "exit", "pid": cmd.Process.Pid, "generation": generation})
			cmd, exited = nil, nil
			// Restart only on the next tick, bounding a failing daemon to 20 starts/sec.
		case <-ticker.C:
			for _, name := range []string{"restart", "crash", "pause", "resume"} {
				path := filepath.Join(*control, name)
				info, e := os.Lstat(path)
				if os.IsNotExist(e) {
					continue
				}
				if e != nil || !info.Mode().IsRegular() {
					return errors.New("invalid control action file")
				}
				switch name {
				case "restart":
					stop(syscall.SIGTERM)
					paused = false
				case "crash":
					stop(syscall.SIGKILL)
					paused = false
				case "pause":
					stop(syscall.SIGKILL)
					paused = true
				case "resume":
					paused = false
				}
				if e = record(); e != nil {
					return e
				}
				event(*control, map[string]any{"action": name, "generation": generation, "at": time.Now().UTC().Format(time.RFC3339Nano)})
				if e = os.Remove(path); e != nil {
					return e
				}
			}
			if err = start(); err != nil {
				return err
			}
		}
	}
}

type proxyPair struct {
	client net.Conn
	target net.Conn
}

func (p *proxyPair) close() {
	p.client.Close()
	if p.target != nil {
		p.target.Close()
	}
}

func proxy(a []string) error {
	fs := flag.NewFlagSet("proxy", flag.ContinueOnError)
	listen := fs.String("listen", ":9443", "")
	target := fs.String("target", "", "")
	control := fs.String("control", "/control", "")
	if e := fs.Parse(a); e != nil {
		return e
	}
	if *target == "" || fs.NArg() != 0 {
		return errors.New("target required; no positional args")
	}
	if e := privateDirectory(*control, false); e != nil {
		return e
	}
	ln, e := net.Listen("tcp4", *listen)
	if e != nil {
		return e
	}
	defer ln.Close()
	var mu sync.Mutex
	pairs := map[*proxyPair]bool{}
	var workers sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigs)
	isBlocked := func() bool {
		_, e := os.Lstat(filepath.Join(*control, "blocked"))
		return !os.IsNotExist(e) // Invalid/unreadable control state fails closed.
	}
	closeAll := func() {
		mu.Lock()
		defer mu.Unlock()
		for p := range pairs {
			p.close()
		}
	}
	defer func() { cancel(); ln.Close(); closeAll(); workers.Wait() }()
	go func() {
		select {
		case <-sigs:
			cancel()
			ln.Close()
			closeAll()
		case <-ctx.Done():
		}
	}()
	workers.Add(1)
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if isBlocked() {
					closeAll()
				}
			}
		}
	}()
	for {
		client, e := ln.Accept()
		if e != nil {
			if ctx.Err() != nil {
				return nil
			}
			return e
		}
		mu.Lock()
		if len(pairs) >= 64 || isBlocked() {
			mu.Unlock()
			client.Close()
			continue
		}
		pair := &proxyPair{client: client}
		pairs[pair] = true
		mu.Unlock()
		workers.Add(1)
		go func(p *proxyPair) {
			defer workers.Done()
			defer func() { mu.Lock(); p.close(); delete(pairs, p); mu.Unlock() }()
			remote, e := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp4", *target)
			if e != nil {
				return
			}
			mu.Lock()
			p.target = remote
			blocked := isBlocked() || ctx.Err() != nil
			mu.Unlock()
			if blocked {
				return
			}
			completed := make(chan struct{}, 2)
			copyLane := func(dst, src net.Conn) {
				_, copyErr := io.Copy(dst, src)
				if copyErr == nil {
					if tcp, ok := dst.(*net.TCPConn); ok {
						_ = tcp.CloseWrite()
					}
				}
				completed <- struct{}{}
			}
			go copyLane(remote, client)
			go copyLane(client, remote)
			<-completed
			// Give normal half-close a short drain; never leak an idle upstream peer.
			select {
			case <-completed:
				return
			case <-time.After(250 * time.Millisecond):
			case <-ctx.Done():
			}
			mu.Lock()
			p.close()
			mu.Unlock()
			<-completed
		}(pair)
	}
}

func privateDirectory(path string, fresh bool) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("absolute clean private directory required")
	}
	for p := path; ; p = filepath.Dir(p) {
		st, e := os.Lstat(p)
		if e == nil && st.Mode()&os.ModeSymlink != 0 {
			return errors.New("symlink directory rejected")
		}
		if e != nil && !os.IsNotExist(e) {
			return e
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	if fresh {
		if e := os.Mkdir(path, 0700); e != nil {
			return e
		}
	} else {
		if e := os.MkdirAll(path, 0700); e != nil {
			return e
		}
	}
	st, e := os.Stat(path)
	if e != nil {
		return e
	}
	if !st.IsDir() || st.Mode().Perm()&0077 != 0 || st.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return errors.New("directory must be private and owned by invoking user")
	}
	return nil
}

func certgen(a []string) error {
	fs := flag.NewFlagSet("certgen", flag.ContinueOnError)
	out := fs.String("out", "", "")
	if e := fs.Parse(a); e != nil {
		return e
	}
	if !filepath.IsAbs(*out) || *out == "" {
		return errors.New("--out must be absolute")
	}
	if e := privateDirectory(*out, true); e != nil {
		return e
	}
	caKey, e := rsa.GenerateKey(rand.Reader, 2048)
	if e != nil {
		return e
	}
	caSerial, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if e != nil {
		return e
	}
	caT := &x509.Certificate{SerialNumber: caSerial, Subject: pkix.Name{CommonName: "Aurora local CA"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(7 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature}
	caDER, e := x509.CreateCertificate(rand.Reader, caT, caT, &caKey.PublicKey, caKey)
	if e != nil {
		return e
	}
	caParent, e := x509.ParseCertificate(caDER)
	if e != nil {
		return e
	}
	if e = writePEM(filepath.Join(*out, "ca.crt"), "CERTIFICATE", caDER, 0644); e != nil {
		return e
	}
	if e = writeKey(filepath.Join(*out, "ca.key"), caKey); e != nil {
		return e
	}
	for _, name := range []string{"scheduler", "operator", "agent-a", "agent-b"} {
		k, e := rsa.GenerateKey(rand.Reader, 2048)
		if e != nil {
			return e
		}
		serial, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
		if e != nil {
			return e
		}
		t := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(7 * 24 * time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment}
		der, e := x509.CreateCertificate(rand.Reader, t, caParent, &k.PublicKey, caKey)
		if e != nil {
			return e
		}
		if e = writePEM(filepath.Join(*out, name+".crt"), "CERTIFICATE", der, 0644); e != nil {
			return e
		}
		if e = writeKey(filepath.Join(*out, name+".key"), k); e != nil {
			return e
		}
	}
	return nil
}

func writePEM(path, kind string, b []byte, mode os.FileMode) error {
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if e != nil {
		return e
	}
	if e = pem.Encode(f, &pem.Block{Type: kind, Bytes: b}); e == nil {
		e = f.Sync()
	}
	return errors.Join(e, f.Close())
}
func writeKey(path string, k *rsa.PrivateKey) error {
	b, e := x509.MarshalPKCS8PrivateKey(k)
	if e != nil {
		return e
	}
	return writePEM(path, "PRIVATE KEY", b, 0600)
}
