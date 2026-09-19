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
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

const maxLogPageBytes = 65536
const maxLogFileBytes = 16 << 20

var logAttemptKey = regexp.MustCompile(`^[a-f0-9]{64}$`)

// Log reads use the enrolled journal identity and current authority, never a client path.
func serveLogs(w http.ResponseWriter, r *http.Request, store *Store) {
	cfg := store.CurrentConfig()
	if len(r.Header.Values("X-Aurora-Epoch")) != 1 || len(r.Header.Values("X-Aurora-Session")) != 1 || r.Header.Get("X-Aurora-Epoch") != cfg.Epoch || r.Header.Get("X-Aurora-Session") != cfg.Session {
		transportError(w, 409, "stale log authority")
		return
	}
	if r.ContentLength > 0 || len(r.TransferEncoding) != 0 {
		transportError(w, 400, "GET body rejected")
		return
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(q) != 4 {
		transportError(w, 400, "invalid log query")
		return
	}
	for key, values := range q {
		if (key != "attempt" && key != "stream" && key != "offset" && key != "limit") || len(values) != 1 {
			transportError(w, 400, "invalid log query")
			return
		}
	}
	key, stream := q.Get("attempt"), q.Get("stream")
	offset, offsetErr := counter(q.Get("offset"))
	limit, limitErr := counter(q.Get("limit"))
	if !logAttemptKey.MatchString(key) || (stream != "stdout" && stream != "stderr") || offsetErr != nil || limitErr != nil || offset > maxLogFileBytes || limit == 0 || limit > maxLogPageBytes {
		transportError(w, 400, "invalid log bounds or stream")
		return
	}
	a, found, err := store.InspectAttempt(key)
	if err != nil {
		transportError(w, 503, "journal unavailable")
		return
	}
	if !found || a.Body["kind"] != "Run" || a.Execution == nil {
		transportError(w, 404, "attempt logs unavailable")
		return
	}
	store.effectMu.Lock()
	root := store.runtimeRoot
	store.effectMu.Unlock()
	if root == "" {
		transportError(w, 503, "runtime unavailable")
		return
	}
	f, err := openAttemptLog(root, key, stream)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			transportError(w, 404, "log not retained or not created")
		} else {
			transportError(w, 503, "log unavailable")
		}
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxLogFileBytes {
		transportError(w, 503, "invalid retained log")
		return
	}
	if offset > uint64(info.Size()) {
		transportError(w, 416, "offset exceeds retained log")
		return
	}
	count := min(limit, uint64(info.Size())-offset)
	data := make([]byte, int(count))
	n, err := f.ReadAt(data, int64(offset))
	if err != nil && err != io.EOF {
		transportError(w, 503, "log read failed")
		return
	}
	// Preserve UTF-8 boundaries for normal text pages. Tiny byte limits or an
	// explicitly unaligned starting offset still use JSON's replacement decoding.
	if offset+uint64(n) < uint64(info.Size()) {
		for start := max(0, n-3); start < n; start++ {
			if start > 0 && utf8.RuneStart(data[start]) && !utf8.FullRune(data[start:n]) {
				n = start
				break
			}
		}
	}
	dropped := a.Execution.StdoutDropped
	if stream == "stderr" {
		dropped = a.Execution.StderrDropped
	}
	next := offset + uint64(n)
	transportJSON(w, 200, map[string]any{
		"attempt": key, "stream": stream, "offset": offset, "nextOffset": next,
		"hasMore": next < uint64(info.Size()), "truncated": dropped > 0 || a.Execution.OutputIncomplete,
		"complete": a.Execution.Phase == "terminal" && a.Execution.Cleanup == "complete",
		"data":     string(data[:n]),
	})
}

// Open every mutable path component without following links. Nonblocking opens avoid
// hanging on a FIFO substituted for a log; only a regular, singly linked file is read.
func openAttemptLog(root, key, stream string) (*os.File, error) {
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(rootFD)
	dirFD, err := unix.Openat(rootFD, key, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(dirFD)
	fd, err := unix.Openat(dirFD, stream+".log", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), stream+".log")
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 {
		f.Close()
		return nil, errors.New("invalid log file")
	}
	return f, nil
}
