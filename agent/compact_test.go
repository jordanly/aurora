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
	"crypto/sha256"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"aurora.local/agent/protocol"
	bolt "go.etcd.io/bbolt"
)

func TestCompactJournalReclaimsPagesAndPreservesReplay(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	destination := filepath.Join(dir, "compacted")
	s := open(t, source, config())
	seedHistory(t, s, "stop", 100)
	ackAllHistory(t, s)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("state")).Put([]byte("reclaimable-padding"), make([]byte, 8<<20))
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("state")).Delete([]byte("reclaimable-padding")) }); err != nil {
		t.Fatal(err)
	}
	before, err := s.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	sourceBytes, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	sourceOwner, err := os.ReadFile(source + ".owner")
	if err != nil {
		t.Fatal(err)
	}
	// A config carrying stale scheduler authority cannot mutate the offline copy.
	cfg := config()
	cfg.Epoch = "1"
	cfg.Session = "stale-session"
	result, err := CompactJournal(source, destination, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.SourceBytes != int64(len(sourceBytes)) || result.DestinationBytes >= result.SourceBytes {
		t.Fatalf("free pages not reclaimed: %+v", result)
	}
	unchanged, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(unchanged) != sha256.Sum256(sourceBytes) {
		t.Fatal("compaction modified source bytes")
	}
	owner, err := os.ReadFile(source + ".owner")
	if err != nil || !bytes.Equal(sourceOwner, owner) {
		t.Fatal("compaction modified source marker", err)
	}
	compacted, err := OpenServer(destination, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer compacted.Close()
	after, err := compacted.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(protocol.Canonical(before), protocol.Canonical(after)) {
		t.Fatal("compacted state changed identity, history or counters")
	}
	prior, err := compacted.Admit(delivery(config(), historyBody(t, "stop", 0)), caller(config()))
	if err != nil || prior.Cursor != "1" {
		t.Fatal(prior, err)
	}
	run := historyBody(t, "run", 0)
	resultRun, err := compacted.Admit(delivery(config(), run), caller(config()))
	if err != nil || resultRun.Outcome != "rejected-stopped" {
		t.Fatal(resultRun, err)
	}
}

func TestCompactJournalRefusesBusyAndExistingPaths(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	destination := filepath.Join(dir, "destination")
	s := open(t, source, config())
	if _, err := CompactJournal(source, destination, config()); err == nil {
		t.Fatal("compacted a locked journal")
	}
	if _, err := os.Lstat(destination); !os.IsNotExist(err) {
		t.Fatal("busy source published destination", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"same", "file", "marker", "symlink", "source-symlink"} {
		t.Run(mode, func(t *testing.T) {
			dest := filepath.Join(dir, mode)
			src := source
			switch mode {
			case "same":
				dest = source
			case "file":
				if err := os.WriteFile(dest, []byte("untouched"), 0600); err != nil {
					t.Fatal(err)
				}
			case "marker":
				if err := os.WriteFile(dest+".owner", []byte("untouched"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(source, dest); err != nil {
					t.Fatal(err)
				}
			case "source-symlink":
				src = filepath.Join(dir, "source-link")
				if err := os.Symlink(source, src); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := CompactJournal(src, dest, config()); err == nil {
				t.Fatal("unsafe path accepted")
			}
			if mode == "file" || mode == "marker" {
				p := dest
				if mode == "marker" {
					p += ".owner"
				}
				data, err := os.ReadFile(p)
				if err != nil || string(data) != "untouched" {
					t.Fatal("existing output replaced", err)
				}
			}
		})
	}
	after, err := os.ReadFile(source)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed compaction changed source", err)
	}
}

func TestCompactJournalInterruptedPublicationFailsClosed(t *testing.T) {
	for _, stage := range []string{"copied", "marker"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			source := filepath.Join(dir, "source")
			destination := filepath.Join(dir, "destination")
			s := open(t, source, config())
			s.Close()
			before, err := os.ReadFile(source)
			if err != nil {
				t.Fatal(err)
			}
			sentinel := errors.New("injected interrupted compaction")
			_, err = compactJournal(source, destination, config(), func(at string) error {
				if at == stage {
					return sentinel
				}
				return nil
			})
			if !errors.Is(err, sentinel) {
				t.Fatal(err)
			}
			after, err := os.ReadFile(source)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("failed copy changed source", err)
			}
			if _, err := os.Stat(destination); !os.IsNotExist(err) {
				t.Fatal("incomplete data published", err)
			}
			if stage == "marker" {
				if reopened, err := Open(destination, config()); err == nil {
					reopened.Close()
					t.Fatal("incomplete copy silently re-enrolled")
				}
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if entry.IsDir() {
					t.Fatal("staging directory leaked after returned failure")
				}
			}
		})
	}
}

func TestCompactJournalRejectsLostOrCorruptHistory(t *testing.T) {
	for _, mode := range []string{"missing", "empty", "marker", "history", "enrollment"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			source := filepath.Join(dir, "source")
			destination := filepath.Join(dir, "destination")
			s := open(t, source, config())
			seedHistory(t, s, "stop", 1)
			if mode == "history" {
				if err := s.db.Update(func(tx *bolt.Tx) error {
					return tx.Bucket([]byte("state")).Bucket([]byte("commands")).Delete([]byte("stop-history-0"))
				}); err != nil {
					t.Fatal(err)
				}
			}
			s.Close()
			cfg := config()
			switch mode {
			case "missing":
				os.Remove(source)
			case "empty":
				os.WriteFile(source, nil, 0600)
			case "marker":
				os.Remove(source + ".owner")
			case "enrollment":
				cfg.CPU++
			}
			if _, err := CompactJournal(source, destination, cfg); err == nil {
				t.Fatal("invalid source compacted")
			}
			if _, err := os.Stat(destination); !os.IsNotExist(err) {
				t.Fatal("invalid source published", err)
			}
			if mode == "missing" {
				if _, err := os.Stat(source); !os.IsNotExist(err) {
					t.Fatal("missing source recreated", err)
				}
			}
		})
	}
}

func TestCompactJournalDoesNotOverwriteRacingDestination(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	destination := filepath.Join(dir, "destination")
	s := open(t, source, config())
	s.Close()
	_, err := compactJournal(source, destination, config(), func(stage string) error {
		if stage == "marker" {
			return os.WriteFile(destination, []byte("operator-file"), 0600)
		}
		return nil
	})
	if err == nil {
		t.Fatal("racing destination replaced")
	}
	data, err := os.ReadFile(destination)
	if err != nil || string(data) != "operator-file" {
		t.Fatal("racing operator file removed", err)
	}
}

func TestCompactCrashHelper(t *testing.T) {
	stage := os.Getenv("AURORA_TEST_COMPACT_CRASH")
	if stage == "" {
		return
	}
	_, err := compactJournal(os.Getenv("AURORA_TEST_COMPACT_SOURCE"), os.Getenv("AURORA_TEST_COMPACT_DESTINATION"), config(), func(at string) error {
		if at == stage {
			os.Exit(73)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	os.Exit(74)
}

func TestCompactJournalCrashPublication(t *testing.T) {
	for _, stage := range []string{"copied", "marker", "after"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			source := filepath.Join(dir, "source")
			destination := filepath.Join(dir, "destination")
			s := open(t, source, config())
			seedHistory(t, s, "stop", 1)
			s.Close()
			before, err := os.ReadFile(source)
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestCompactCrashHelper$")
			cmd.Env = append(os.Environ(), "AURORA_TEST_COMPACT_CRASH="+stage, "AURORA_TEST_COMPACT_SOURCE="+source, "AURORA_TEST_COMPACT_DESTINATION="+destination)
			err = cmd.Run()
			var exitErr *exec.ExitError
			want := 73
			if stage == "after" {
				want = 74
			}
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != want {
				t.Fatal("unexpected child exit", err)
			}
			after, err := os.ReadFile(source)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("abrupt compaction changed source", err)
			}
			restored := open(t, source, config())
			restored.Close()
			if stage == "after" {
				copied := open(t, destination, config())
				defer copied.Close()
				r, err := copied.Admit(delivery(config(), historyBody(t, "stop", 0)), caller(config()))
				if err != nil || r.Cursor != "1" {
					t.Fatal("published copy lost replay", r, err)
				}
			} else {
				if _, err := os.Stat(destination); !os.IsNotExist(err) {
					t.Fatal("partial database published", err)
				}
				if stage == "marker" {
					if copied, err := Open(destination, config()); err == nil {
						copied.Close()
						t.Fatal("marker-only crash output re-enrolled")
					}
				}
			}
		})
	}
}

func TestCompactJournalLeavesUnsyncedFreelistSourceUnchanged(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	destination := filepath.Join(dir, "destination")
	s := open(t, source, config())
	s.Close()
	// This is a valid bbolt journal whose freelist would normally be rewritten by
	// a default writable Open, before our own code starts any transactions.
	db, err := bolt.Open(source, 0600, &bolt.Options{NoFreelistSync: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("state")).Put([]byte("unsynced-freelist-test"), []byte("retained"))
	}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = CompactJournal(source, destination, config()); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(source)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("opening compaction source rewrote unsynced freelist", err)
	}
	copied := open(t, destination, config())
	copied.Close()
}
