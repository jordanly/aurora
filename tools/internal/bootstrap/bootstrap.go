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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

const MaxArchiveBytes int64 = 1 << 30

type Pin struct {
	File      string `json:"file"`
	URL       string `json:"url"`
	SHA256    string `json:"sha256"`
	Directory string `json:"directory"`
	Version   string `json:"version,omitempty"`
}
type Manifest struct {
	Schema   int            `json:"schema"`
	Platform string         `json:"platform"`
	Tools    map[string]Pin `json:"tools"`
}

func ReadJSON(path string, value any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 1<<20+1))
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return errors.New("manifest size limit")
	}
	// Reject duplicate keys even in metadata fields that this command does not use.
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	if err := uniqueJSON(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing manifest data")
	}
	return json.Unmarshal(data, value)
}
func uniqueJSON(d *json.Decoder) error {
	t, err := d.Token()
	if err != nil {
		return err
	}
	if delim, ok := t.(json.Delim); ok {
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				s, ok := key.(string)
				if !ok || seen[s] {
					return errors.New("duplicate JSON key")
				}
				seen[s] = true
				if err := uniqueJSON(d); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := uniqueJSON(d); err != nil {
					return err
				}
			}
		default:
			return errors.New("invalid JSON delimiter")
		}
		_, err = d.Token()
		return err
	}
	return nil
}
func LoadManifest(path string) (Manifest, error) {
	var m Manifest
	if err := ReadJSON(path, &m); err != nil {
		return m, err
	}
	if m.Schema != 1 || m.Platform != runtime.GOOS+"/"+runtime.GOARCH || len(m.Tools) == 0 {
		return m, errors.New("unsupported tool manifest schema or host platform")
	}
	for name, pin := range m.Tools {
		if !safeComponent(name) {
			return m, errors.New("unsafe tool name")
		}
		if err := pin.Validate(); err != nil {
			return m, fmt.Errorf("%s: %w", name, err)
		}
	}
	return m, nil
}
func safeComponent(value string) bool {
	return value != "" && value != "." && value != ".." && !strings.ContainsAny(value, "/\\")
}
func (p Pin) Validate() error {
	if !safeComponent(p.File) {
		return errors.New("unsafe archive filename")
	}
	if len(p.SHA256) != 64 || strings.ToLower(p.SHA256) != p.SHA256 {
		return errors.New("invalid SHA256 pin")
	}
	if _, err := hex.DecodeString(p.SHA256); err != nil {
		return err
	}
	u, err := url.Parse(p.URL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
		return errors.New("tool archives require HTTPS")
	}
	if p.Directory != "" {
		if _, err := MemberPath("/private", p.Directory); err != nil {
			return err
		}
	}
	return nil
}

// CheckPath examines components before cleaning, including a symlink followed
// by /..; normalization must not hide a symlink the caller supplied.
func CheckPath(path string) (string, error) {
	if !filepath.IsAbs(path) {
		cwd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		path = cwd + "/" + path
	}
	current := "/"
	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("symlink path rejected: %s", current)
		}
	}
	return filepath.Clean(path), nil
}
func PrivateCache(path string) (string, error) {
	clean, err := CheckPath(path)
	if err != nil {
		return "", err
	}
	if err = os.MkdirAll(clean, 0700); err != nil {
		return "", err
	}
	info, err := os.Stat(clean)
	if err != nil {
		return "", err
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(owner.Uid) != os.Getuid() || !info.IsDir() {
		return "", errors.New("cache must be a directory owned by this user")
	}
	if err = os.Chmod(clean, 0700); err != nil {
		return "", err
	}
	return clean, nil
}
func Lock(path string) (func(), error) {
	clean, err := CheckPath(path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(clean, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, errors.New("lock must be a regular file")
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return func() { syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
}
func Digest(path string) (string, error) {
	clean, err := CheckPath(path)
	if err != nil {
		return "", err
	}
	f, err := os.OpenFile(clean, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("digest requires regular file")
	}
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func verify(path, digest string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > MaxArchiveBytes {
		return errors.New("archive type or size limit")
	}
	actual, err := Digest(path)
	if err != nil {
		return err
	}
	if actual != digest {
		return fmt.Errorf("archive checksum mismatch: %s", path)
	}
	return nil
}

func EnsureArchive(ctx context.Context, pin Pin, cache, seed string, offline bool) (string, error) {
	if err := pin.Validate(); err != nil {
		return "", err
	}
	archives, err := PrivateCache(filepath.Join(cache, "archives"))
	if err != nil {
		return "", err
	}
	path, err := CheckPath(filepath.Join(archives, pin.File))
	if err != nil {
		return "", err
	}
	if _, err = os.Lstat(path); err == nil {
		return path, verify(path, pin.SHA256)
	} else if !os.IsNotExist(err) {
		return "", err
	}
	temporary := path + ".part"
	if _, err = CheckPath(temporary); err != nil {
		return "", err
	}
	target, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return "", err
	}
	defer os.Remove(temporary)
	defer target.Close()
	var input io.ReadCloser
	if seed != "" {
		candidate, err := CheckPath(filepath.Join(seed, pin.File))
		if err != nil {
			return "", err
		}
		if _, err = os.Lstat(candidate); err == nil {
			if err = verify(candidate, pin.SHA256); err != nil {
				return "", err
			}
			input, err = os.OpenFile(candidate, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
			if err != nil {
				return "", err
			}
		} else if !os.IsNotExist(err) {
			return "", err
		}
	}
	if input == nil {
		if offline {
			return "", fmt.Errorf("offline tool archive missing: %s", pin.File)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, pin.URL, nil)
		if err != nil {
			return "", err
		}
		client := &http.Client{Timeout: 60 * time.Second, CheckRedirect: func(r *http.Request, via []*http.Request) error {
			if r.URL.Scheme != "https" || len(via) > 10 {
				return errors.New("insecure/excessive tool redirect")
			}
			return nil
		}}
		response, err := client.Do(request)
		if err != nil {
			return "", err
		}
		if response.StatusCode != 200 {
			response.Body.Close()
			return "", fmt.Errorf("tool download HTTP %d", response.StatusCode)
		}
		input = response.Body
	}
	defer input.Close()
	copied, err := io.Copy(target, io.LimitReader(input, MaxArchiveBytes+1))
	if err != nil {
		return "", err
	}
	if copied > MaxArchiveBytes {
		return "", errors.New("tool download size limit")
	}
	if err = target.Sync(); err != nil {
		return "", err
	}
	if err = target.Close(); err != nil {
		return "", err
	}
	if err = verify(temporary, pin.SHA256); err != nil {
		return "", err
	}
	if err = os.Link(temporary, path); err != nil {
		return "", err
	}
	return path, nil
}
func Tools(ctx context.Context, m Manifest, cache, work, seed string, offline bool) (map[string]string, error) {
	result := map[string]string{}
	for name, pin := range m.Tools {
		archive, err := EnsureArchive(ctx, pin, cache, seed, offline)
		if err != nil {
			return nil, err
		}
		destination := filepath.Join(work, name)
		if err = Extract(ctx, archive, destination, false); err != nil {
			return nil, err
		}
		tool := filepath.Join(destination, pin.Directory)
		info, err := os.Stat(tool)
		if err != nil || !info.IsDir() {
			return nil, fmt.Errorf("tool archive layout differs: %s", name)
		}
		result[name] = tool
	}
	return result, nil
}
