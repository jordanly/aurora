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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"aurora.local/agent/protocol"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

// CompactResult describes an offline copy; no source path or runtime is switched.
type CompactResult struct {
	Source           string `json:"source"`
	Destination      string `json:"destination"`
	SourceBytes      int64  `json:"sourceBytes"`
	DestinationBytes int64  `json:"destinationBytes"`
}

// CompactJournal exclusively locks an existing, stopped journal and copies every
// retained entry to a new compact database. The source is never written. The
// caller must explicitly choose the destination for a later daemon start.
func CompactJournal(source, destination string, config Config) (CompactResult, error) {
	return compactJournal(source, destination, config, nil)
}

func compactJournal(source, destination string, config Config, checkpoint func(string) error) (result CompactResult, err error) {
	if _, err = ReadConfig(protocol.Canonical(config)); err != nil {
		return result, err
	}
	if source == "" || destination == "" {
		return result, errors.New("source and destination required")
	}
	source, err = filepath.Abs(source)
	if err != nil {
		return result, err
	}
	destination, err = filepath.Abs(destination)
	if err != nil {
		return result, err
	}
	if source == destination {
		return result, errors.New("compaction requires a new destination")
	}
	for _, path := range []string{destination, destination + ".owner"} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			if err != nil {
				return result, err
			}
			return result, fmt.Errorf("compaction destination already exists: %s", path)
		}
	}
	info, err := os.Lstat(source)
	if err != nil {
		return result, err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return result, errors.New("source journal must be a nonempty regular file")
	}
	present, err := checkMarker(source+".owner", config)
	if err != nil {
		return result, err
	}
	if !present {
		return result, errors.New("source enrollment marker missing")
	}
	// ReadOnly would take a shared lock. Use a writer lock but never a write
	// transaction. NoFreelistSync prevents Open itself migrating an unsynced freelist.
	// Remove O_CREATE so a concurrently missing source stays lost.
	var sourceFile *os.File
	src, err := bolt.Open(source, 0600, &bolt.Options{Timeout: 100 * time.Millisecond, NoFreelistSync: true, OpenFile: func(path string, flag int, mode os.FileMode) (*os.File, error) {
		f, err := os.OpenFile(path, (flag&^os.O_CREATE)|unix.O_NOFOLLOW|unix.O_NONBLOCK, mode)
		if err != nil {
			return nil, err
		}
		opened, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		if !opened.Mode().IsRegular() || opened.Size() == 0 || !os.SameFile(info, opened) {
			f.Close()
			return nil, errors.New("source journal changed while opening")
		}
		sourceFile = f
		return f, nil
	}})
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, src.Close()) }()
	info, err = sourceFile.Stat()
	if err != nil {
		return result, err
	}
	present, err = checkMarker(source+".owner", config)
	if err != nil {
		return result, err
	}
	if !present {
		return result, errors.New("source enrollment marker missing")
	}
	var original State
	err = src.View(func(tx *bolt.Tx) error { var err error; original, err = read(tx.Bucket([]byte("state"))); return err })
	if err != nil {
		return result, err
	}
	if _, err = ReadConfig(protocol.Canonical(original.Config)); err != nil {
		return result, err
	}
	enrollment := original.Config
	enrollment.Session, enrollment.Epoch = config.Session, config.Epoch
	if !reflect.DeepEqual(enrollment, config) {
		return result, errors.New("source enrollment/config mismatch")
	}
	originalData := protocol.Canonical(original)
	parent := filepath.Dir(destination)
	staging, err := os.MkdirTemp(parent, ".aurora-compact-")
	if err != nil {
		return result, err
	}
	defer os.RemoveAll(staging)
	staged := filepath.Join(staging, "journal")
	dst, err := bolt.Open(staged, 0600, nil)
	if err != nil {
		return result, err
	}
	copyErr := bolt.Compact(dst, src, 16<<20)
	if copyErr == nil {
		copyErr = dst.View(func(tx *bolt.Tx) error {
			copied, err := read(tx.Bucket([]byte("state")))
			if err != nil {
				return err
			}
			if !bytes.Equal(originalData, protocol.Canonical(copied)) {
				return errors.New("compacted journal state mismatch")
			}
			return nil
		})
	}
	if copyErr == nil {
		copyErr = dst.Sync()
	}
	if err = errors.Join(copyErr, dst.Close()); err != nil {
		return result, err
	}
	if err = createMarker(staged+".owner", original.Config); err != nil {
		return result, err
	}
	if err = syncDir(staging); err != nil {
		return result, err
	}
	if checkpoint != nil {
		if err = checkpoint("copied"); err != nil {
			return result, err
		}
	}
	compactInfo, err := os.Stat(staged)
	if err != nil {
		return result, err
	}
	result = CompactResult{Source: source, Destination: destination, SourceBytes: info.Size(), DestinationBytes: compactInfo.Size()}
	// Each hard link atomically publishes a complete file and refuses overwrite.
	// Persist the marker first: interruption before data publication fails closed
	// instead of allowing an empty destination to be silently enrolled later.
	if err = os.Link(staged+".owner", destination+".owner"); err != nil {
		return result, err
	}
	if err = syncDir(parent); err != nil {
		return result, err
	}
	if checkpoint != nil {
		if err = checkpoint("marker"); err != nil {
			return result, err
		}
	}
	if err = os.Link(staged, destination); err != nil {
		return result, err
	}
	if err = syncDir(parent); err != nil {
		return result, err
	}
	return result, nil
}
