// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bootstrap

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const maxMemberBytes int64 = 512 << 20
const maxExtractedBytes int64 = 2 << 30
const maxMembers = 200000

type extraction struct {
	root    string
	seen    map[string]bool
	total   int64
	members int
}
type archiveLink struct {
	target, name string
	hard         bool
}

func MemberPath(root, name string) (string, error) {
	if name == "" || strings.HasPrefix(name, "/") || strings.Contains(name, "\\") {
		return "", errors.New("unsafe archive member")
	}
	for _, part := range strings.Split(name, "/") {
		if part == ".." {
			return "", errors.New("archive traversal rejected")
		}
	}
	target, err := CheckPath(filepath.Join(root, filepath.FromSlash(name)))
	if err != nil {
		return "", err
	}
	if target == root && !strings.HasSuffix(name, "/") && name != "." {
		return "", errors.New("unsafe archive root")
	}
	return target, nil
}
func (e *extraction) reserve(name string, size int64) (string, error) {
	target, err := MemberPath(e.root, name)
	if err != nil {
		return "", err
	}
	if e.seen[target] {
		return "", errors.New("duplicate archive member")
	}
	e.seen[target] = true
	e.members++
	if e.members > maxMembers {
		return "", errors.New("archive entry count limit")
	}
	if err = e.account(size); err != nil {
		return "", err
	}
	return target, nil
}
func (e *extraction) account(size int64) error {
	if size < 0 || size > maxMemberBytes || e.total > maxExtractedBytes-size {
		return errors.New("archive extraction size limit")
	}
	e.total += size
	return nil
}
func writeMember(path string, input io.Reader, size int64, mode os.FileMode, mtime time.Time) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	permissions := os.FileMode(0644)
	if mode&0111 != 0 {
		permissions = 0755
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, permissions)
	if err != nil {
		return err
	}
	defer f.Close()
	copied, err := io.Copy(f, io.LimitReader(input, size+1))
	if err != nil {
		return err
	}
	if copied != size {
		return errors.New("archive member size mismatch")
	}
	if err = f.Close(); err != nil {
		return err
	}
	if !mtime.IsZero() {
		return os.Chtimes(path, mtime, mtime)
	}
	return nil
}
func Extract(ctx context.Context, archive, destination string, restoreTimes bool) error {
	checkedArchive, err := CheckPath(archive)
	if err != nil {
		return err
	}
	archive = checkedArchive
	clean, err := CheckPath(destination)
	if err != nil {
		return err
	}
	if err = os.Mkdir(clean, 0700); err != nil {
		return fmt.Errorf("fresh extraction destination: %w", err)
	}
	e := extraction{root: clean, seen: map[string]bool{}}
	if strings.HasSuffix(archive, ".zip") {
		return e.zip(archive, restoreTimes)
	}
	file, err := os.OpenFile(archive, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("archive must be a regular file")
	}
	var input io.Reader = file
	var decompressor *exec.Cmd
	if strings.HasSuffix(archive, ".gz") {
		compressed, err := gzip.NewReader(file)
		if err != nil {
			return err
		}
		defer compressed.Close()
		input = compressed
	} else if strings.HasSuffix(archive, ".xz") {
		// XZ is a host decompressor only; Go still validates every tar entry and
		// applies all bounds. No archive-provided executable is invoked here.
		decompressor = exec.CommandContext(ctx, "xz", "--decompress", "--stdout", "--", archive)
		stdout, err := decompressor.StdoutPipe()
		if err != nil {
			return err
		}
		if err = decompressor.Start(); err != nil {
			return err
		}
		input = stdout
		defer func() {
			stdout.Close()
			if decompressor.ProcessState == nil {
				decompressor.Process.Kill()
				decompressor.Wait()
			}
		}()
	}
	reader := tar.NewReader(input)
	links := []archiveLink{}
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		target, err := e.reserve(header.Name, header.Size)
		if err != nil {
			return err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err = os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			mtime := time.Time{}
			if restoreTimes {
				mtime = header.ModTime
			}
			if err = writeMember(target, reader, header.Size, os.FileMode(header.Mode), mtime); err != nil {
				return err
			}
		case tar.TypeSymlink, tar.TypeLink:
			links = append(links, archiveLink{target, header.Linkname, header.Typeflag == tar.TypeLink})
		default:
			return errors.New("archive special file rejected")
		}
	}
	// Drain compressed trailing padding within the same decompression budget;
	// otherwise an XZ child can block on a full pipe after tar's end marker.
	remaining, err := io.Copy(io.Discard, io.LimitReader(input, maxExtractedBytes-e.total+1))
	if err != nil {
		return err
	}
	if remaining > maxExtractedBytes-e.total {
		return errors.New("archive trailing decompression limit")
	}
	if decompressor != nil {
		if err = decompressor.Wait(); err != nil {
			return err
		}
	}
	for _, link := range links {
		if err = e.materialize(link); err != nil {
			return err
		}
	}
	return nil
}
func (e *extraction) zip(archive string, restoreTimes bool) error {
	reader, err := zip.OpenReader(archive)
	if err != nil {
		return err
	}
	defer reader.Close()
	for _, item := range reader.File {
		if item.UncompressedSize64 > uint64(maxMemberBytes) {
			return errors.New("zip member size limit")
		}
		target, err := e.reserve(item.Name, int64(item.UncompressedSize64))
		if err != nil {
			return err
		}
		mode := item.Mode()
		if !mode.IsRegular() && !mode.IsDir() {
			return errors.New("ZIP special file or symlink rejected")
		}
		if mode.IsDir() {
			if err = os.MkdirAll(target, 0755); err != nil {
				return err
			}
			continue
		}
		source, err := item.Open()
		if err != nil {
			return err
		}
		mtime := time.Time{}
		if restoreTimes {
			mtime = item.Modified
		}
		err = writeMember(target, source, int64(item.UncompressedSize64), mode, mtime)
		source.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
func (e *extraction) materialize(link archiveLink) error {
	if _, err := os.Lstat(link.target); !os.IsNotExist(err) {
		return errors.New("archive link target already exists")
	}
	if link.name == "" || filepath.IsAbs(link.name) || strings.Contains(link.name, "\\") {
		return errors.New("unsafe archive link")
	}
	base := filepath.Dir(link.target)
	if link.hard {
		base = e.root
	}
	source := filepath.Clean(filepath.Join(base, link.name))
	if source == e.root || !strings.HasPrefix(source, e.root+string(os.PathSeparator)) {
		return errors.New("escaping archive link")
	}
	if _, err := CheckPath(source); err != nil {
		return err
	}
	info, err := os.Stat(source)
	if err != nil {
		return errors.New("dangling archive link")
	}
	if info.IsDir() {
		if link.hard || link.target == source || strings.HasPrefix(link.target, source+string(os.PathSeparator)) {
			return errors.New("cyclic/directory hard link rejected")
		}
		return filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			relative, err := filepath.Rel(source, path)
			if err != nil {
				return err
			}
			target := filepath.Join(link.target, relative)
			if entry.IsDir() {
				return os.MkdirAll(target, 0755)
			}
			stat, err := entry.Info()
			if err != nil {
				return err
			}
			if !stat.Mode().IsRegular() {
				return errors.New("special file in directory link")
			}
			return e.copyLinked(path, target, stat)
		})
	}
	if !info.Mode().IsRegular() {
		return errors.New("archive link source not regular")
	}
	return e.copyLinked(source, link.target, info)
}
func (e *extraction) copyLinked(source, target string, info os.FileInfo) error {
	if err := e.account(info.Size()); err != nil {
		return err
	}
	e.members++
	if e.members > maxMembers {
		return errors.New("archive linked entry count limit")
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	return writeMember(target, input, info.Size(), info.Mode(), info.ModTime())
}
