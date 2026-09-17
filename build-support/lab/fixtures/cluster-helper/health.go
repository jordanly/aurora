/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// health is a bounded lab workload, independent of keeper and daemon supervision.
func health(args []string) error {
	flags := flag.NewFlagSet("health", flag.ContinueOnError)
	port := flags.Int("port", 18080, "IPv4 loopback TCP port")
	delay := flags.Int("delay-ms", 0, "delay before binding")
	closeAfter := flags.Int("close-after-ms", 0, "close listener but retain process after this time")
	logBytes := flags.Int("log-bytes", 0, "extra bytes per output stream")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *port < 1 || *port > 65535 || *delay < 0 || *delay > 600000 || *closeAfter < 0 || *closeAfter > 600000 || *logBytes < 0 || *logBytes > 2<<20 {
		return errors.New("health requires port1..65535, delay/close-after0..600000 ms, log-bytes0..2097152")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	fmt.Fprintln(os.Stdout, "health-stdout-marker")
	fmt.Fprintln(os.Stderr, "health-stderr-marker")
	for _, stream := range []*os.File{os.Stdout, os.Stderr} {
		if _, err := stream.WriteString(strings.Repeat("x", *logBytes)); err != nil {
			return err
		}
	}
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(time.Duration(*delay) * time.Millisecond):
	}
	listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		return err
	}
	defer listener.Close()
	go func() {
		if *closeAfter > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(time.Duration(*closeAfter) * time.Millisecond):
			}
		} else {
			<-ctx.Done()
		}
		listener.Close()
	}()
	for {
		connection, err := listener.Accept()
		if err != nil {
			break
		}
		connection.Close()
	}
	// A closed listener must fail health while this process is still alive.
	<-ctx.Done()
	return nil
}
