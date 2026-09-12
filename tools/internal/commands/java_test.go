// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"aurora.local/tools/internal/bootstrap"
)

func TestStageJavaVerifiedFreshAndJavaOnly(t *testing.T) {
	root := t.TempDir()
	seed := filepath.Join(root, "seed")
	if err := os.Mkdir(seed, 0700); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(seed, "java.tar.gz")
	file, err := os.Create(archive)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(file)
	tw := tar.NewWriter(gz)
	for _, name := range []string{"bin/java", "bin/keytool", "release"} {
		body := "fixture " + name
		if err = tw.WriteHeader(&tar.Header{Name: "jdk/" + name, Mode: 0755, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err = tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err = tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err = gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	digest, err := bootstrap.Digest(archive)
	if err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(root, "manifest.json")
	pins := bootstrap.Manifest{Schema: 1, Platform: "linux/arm64", Tools: map[string]bootstrap.Pin{
		"java":   {File: "java.tar.gz", URL: "https://example.invalid/java.tar.gz", SHA256: digest, Directory: "jdk"},
		"gradle": {File: "absent.zip", URL: "https://example.invalid/absent.zip", SHA256: digest, Directory: "gradle"},
	}}
	if err = writeJSON(manifest, pins); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "staged")
	if err = stageJava(context.Background(), manifest, output, filepath.Join(root, "cache"), seed, true); err != nil {
		t.Fatal(err)
	}
	var receipt struct {
		TreeSHA256 string        `json:"treeSha256"`
		Archive    bootstrap.Pin `json:"archive"`
	}
	data, err := os.ReadFile(output + ".provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	tree, err := TreeDigest(output)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.TreeSHA256 != tree || receipt.Archive.SHA256 != digest {
		t.Fatal("provenance mismatch")
	}
	if err = stageJava(context.Background(), manifest, output, filepath.Join(root, "cache"), seed, true); err == nil {
		t.Fatal("existing output accepted")
	}
	alias := filepath.Join(root, "alias")
	if err = os.Symlink(output, alias); err != nil {
		t.Fatal(err)
	}
	if err = stageJava(context.Background(), manifest, alias, filepath.Join(root, "cache"), seed, true); err == nil {
		t.Fatal("symlink output accepted")
	}
}

func TestJavaDirectoryPublicationNeverReplaces(t *testing.T) {
	root := t.TempDir()
	from := filepath.Join(root, "from")
	to := filepath.Join(root, "to")
	for _, path := range []string{from, to} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	before, err := os.Stat(to)
	if err != nil {
		t.Fatal(err)
	}
	if err = renameNewDirectory(from, to); err == nil {
		t.Fatal("replaced existing empty directory")
	}
	after, err := os.Stat(to)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("destination changed")
	}
}
