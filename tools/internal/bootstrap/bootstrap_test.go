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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type member struct {
	name, link, body string
	kind             byte
	size             int64
}

func tarFixture(t *testing.T, members ...member) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.tar")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := tar.NewWriter(f)
	for _, m := range members {
		size := m.size
		if size == 0 {
			size = int64(len(m.body))
		}
		if m.kind == 0 {
			m.kind = tar.TypeReg
		}
		if err := writer.WriteHeader(&tar.Header{Name: m.name, Typeflag: m.kind, Linkname: m.link, Size: size, Mode: 0755, ModTime: time.Unix(123456789, 0)}); err != nil {
			t.Fatal(err)
		}
		if m.body != "" {
			writer.Write([]byte(m.body))
		}
	}
	writer.Close()
	f.Close()
	return path
}
func TestArchiveRejectsTraversalDuplicatesSpecialAndUnsafeLinks(t *testing.T) {
	for name, members := range map[string][]member{
		"parent": {{name: "../outside", body: "x"}}, "absolute": {{name: "/outside", body: "x"}}, "backslash": {{name: "a\\b", body: "x"}},
		"duplicate": {{name: "a", body: "x"}, {name: "a", body: "y"}}, "device": {{name: "dev", kind: tar.TypeChar}},
		"escape-link": {{name: "a", kind: tar.TypeSymlink, link: "../../outside"}}, "absolute-link": {{name: "a", kind: tar.TypeSymlink, link: "/etc/passwd"}},
		"dangling": {{name: "a", kind: tar.TypeSymlink, link: "missing"}}, "too-large": {{name: "huge", size: maxMemberBytes + 1}},
		"cycle":           {{name: "dir/", kind: tar.TypeDir}, {name: "dir/inside", kind: tar.TypeSymlink, link: "../dir"}},
		"implicit-target": {{name: "dir/file", body: "x"}, {name: "dir", kind: tar.TypeSymlink, link: "other"}, {name: "other/file", body: "y"}},
	} {
		t.Run(name, func(t *testing.T) {
			archive := tarFixture(t, members...)
			if err := Extract(context.Background(), archive, filepath.Join(t.TempDir(), "out"), false); err == nil {
				t.Fatal("unsafe archive accepted")
			}
		})
	}
}
func TestArchiveMaterializesSafeLinksAndNeverReusesExtraction(t *testing.T) {
	archive := tarFixture(t, member{name: "pkg/file", body: "original"}, member{name: "pkg/copy", kind: tar.TypeSymlink, link: "file"}, member{name: "pkg/hard", kind: tar.TypeLink, link: "pkg/file"})
	destination := filepath.Join(t.TempDir(), "out")
	if err := Extract(context.Background(), archive, destination, false); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"file", "copy", "hard"} {
		path := filepath.Join(destination, "pkg", name)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatal(info, err)
		}
		data, _ := os.ReadFile(path)
		if string(data) != "original" {
			t.Fatal(string(data))
		}
	}
	if err := Extract(context.Background(), archive, destination, false); err == nil {
		t.Fatal("reused mutable extraction")
	}
}
func TestZIPRejectsSymlinkAndTraversal(t *testing.T) {
	for _, name := range []string{"../outside", "link"} {
		t.Run(name, func(t *testing.T) {
			var data bytes.Buffer
			z := zip.NewWriter(&data)
			header := &zip.FileHeader{Name: name}
			if name == "link" {
				header.SetMode(os.ModeSymlink | 0777)
			}
			file, err := z.CreateHeader(header)
			if err != nil {
				t.Fatal(err)
			}
			file.Write([]byte("target"))
			z.Close()
			archive := filepath.Join(t.TempDir(), "tool.zip")
			os.WriteFile(archive, data.Bytes(), 0600)
			if err := Extract(context.Background(), archive, filepath.Join(t.TempDir(), "out"), false); err == nil {
				t.Fatal("unsafe ZIP accepted")
			}
		})
	}
}
func TestSymlinkBeforeParentTraversalIsRejected(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckPath(root + "/alias/../cache"); err == nil {
		t.Fatal("normalization concealed symlink")
	}
	if _, err := PrivateCache(filepath.Join(root, "alias")); err == nil {
		t.Fatal("symlink cache accepted")
	}
}
func pinFor(t *testing.T, archive string) Pin {
	t.Helper()
	data, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	return Pin{File: "tool.tar", URL: "https://invalid.example/tool.tar", SHA256: hex.EncodeToString(hash[:]), Directory: "pkg"}
}
func TestVerifiedSeedFreshExtractionAndCorruptCacheRefusal(t *testing.T) {
	source := tarFixture(t, member{name: "pkg/tool", body: "verified"})
	pin := pinFor(t, source)
	seed := t.TempDir()
	data, _ := os.ReadFile(source)
	os.WriteFile(filepath.Join(seed, pin.File), data, 0600)
	cache := t.TempDir()
	manifest := Manifest{Schema: 1, Platform: runtime.GOOS + "/" + runtime.GOARCH, Tools: map[string]Pin{"tool": pin}}
	first, err := Tools(context.Background(), manifest, cache, t.TempDir(), seed, true)
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(first["tool"], "tool"), []byte("tampered"), 0600)
	second, err := Tools(context.Background(), manifest, cache, t.TempDir(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	actual, _ := os.ReadFile(filepath.Join(second["tool"], "tool"))
	if string(actual) != "verified" {
		t.Fatal("trusted existing executable directory")
	}
	os.WriteFile(filepath.Join(cache, "archives", pin.File), []byte("corrupt"), 0600)
	if _, err := Tools(context.Background(), manifest, cache, t.TempDir(), seed, true); err == nil {
		t.Fatal("corrupt cache was used or silently replaced")
	}
}
func TestOfflineMissingMismatchedSeedAndSymlinkSeedFailClosed(t *testing.T) {
	source := tarFixture(t, member{name: "pkg/tool", body: "verified"})
	pin := pinFor(t, source)
	if _, err := EnsureArchive(context.Background(), pin, t.TempDir(), "", true); err == nil {
		t.Fatal("offline download allowed")
	}
	seed := t.TempDir()
	os.WriteFile(filepath.Join(seed, pin.File), []byte("corrupt"), 0600)
	cache := t.TempDir()
	if _, err := EnsureArchive(context.Background(), pin, cache, seed, true); err == nil {
		t.Fatal("unverified seed used")
	}
	if _, err := os.Stat(filepath.Join(cache, "archives", pin.File)); !os.IsNotExist(err) {
		t.Fatal("bad archive published")
	}
	if _, err := os.Stat(filepath.Join(cache, "archives", pin.File+".part")); !os.IsNotExist(err) {
		t.Fatal("partial archive retained")
	}
	os.Remove(filepath.Join(seed, pin.File))
	os.Symlink(source, filepath.Join(seed, pin.File))
	if _, err := EnsureArchive(context.Background(), pin, t.TempDir(), seed, true); err == nil {
		t.Fatal("symlink seed accepted")
	}
}
func TestManifestRejectsDuplicatesAndInvalidPins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	os.WriteFile(path, []byte(`{"schema":1,"schema":1}`), 0600)
	var m Manifest
	if err := ReadJSON(path, &m); err == nil {
		t.Fatal("duplicate JSON keys accepted")
	}
	pin := Pin{File: "x.tar", URL: "http://invalid/x", SHA256: strings.Repeat("0", 64)}
	if err := pin.Validate(); err == nil {
		t.Fatal("insecure URL accepted")
	}
	pin.URL = "https://invalid/x"
	pin.File = "../x"
	if err := pin.Validate(); err == nil {
		t.Fatal("unsafe filename accepted")
	}
}
func TestInitialShellPinMatchesJSON(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	var m Manifest
	if err := ReadJSON(filepath.Join(root, "tools/go-tools.json"), &m); err != nil {
		t.Fatal(err)
	}
	shell, err := os.ReadFile(filepath.Join(root, "build-support/bootstrap-go"))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{"bootstrap_platform": m.Platform, "bootstrap_file": m.Tools["go"].File, "bootstrap_url": m.Tools["go"].URL, "bootstrap_sha": m.Tools["go"].SHA256} {
		if !strings.Contains(string(shell), "\n"+key+"="+value+"\n") {
			t.Fatalf("shell pin drift: %s", key)
		}
	}
}

func TestCompilerSourceExtractionRestoresArchiveMtime(t *testing.T) {
	archive := tarFixture(t, member{name: "pkg/generated.cpp", body: "source"})
	destination := filepath.Join(t.TempDir(), "out")
	if err := Extract(context.Background(), archive, destination, true); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(destination, "pkg/generated.cpp"))
	if err != nil {
		t.Fatal(err)
	}
	if info.ModTime().Unix() != 123456789 {
		t.Fatalf("autotools source timestamp changed: %v", info.ModTime())
	}
}
