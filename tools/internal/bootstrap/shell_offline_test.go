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
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestInitialShellOfflineColdCache(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "arm64" {
		t.Skip("initial SDK bootstrap supports linux/arm64")
	}
	shell, err := filepath.Abs(filepath.Join("..", "..", "..", "build-support/bootstrap-go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, env string
		args      []string
		offline   bool
	}{
		{"check-tools-shared-env", "AURORA_INPLACE_OFFLINE=1", []string{"check-tools"}, true},
		{"check-tools-go-env", "AURORA_INPLACE_GO_OFFLINE=1", []string{"check-tools"}, true},
		{"build-client-shared-env", "AURORA_INPLACE_OFFLINE=1", []string{"build-client", "--output", "unused"}, true},
		{"build-client-go-env", "AURORA_INPLACE_GO_OFFLINE=1", []string{"build-client", "--output", "unused"}, true},
		{"stage-java-shared-env", "AURORA_INPLACE_OFFLINE=1", []string{"stage-java", "--output", "unused"}, true},
		{"explicit-flag", "", []string{"check-tools", "--offline"}, true},
		{"explicit-true", "", []string{"check-tools", "--offline=true"}, true},
		{"unset-env-control", "", []string{"check-tools"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			bin := filepath.Join(root, "bin")
			seed := filepath.Join(root, "seed")
			marker := filepath.Join(root, "curl-marker")
			for _, path := range []string{bin, seed} {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			// Fake curl cannot contact the network or create an executable archive.
			if err := os.WriteFile(filepath.Join(bin, "curl"), []byte("#!/bin/sh\nprintf attempted > \"$AURORA_OFFLINE_TEST_MARKER\"\nexit 97\n"), 0700); err != nil {
				t.Fatal(err)
			}
			env := []string{}
			for _, entry := range os.Environ() {
				key, _, _ := strings.Cut(entry, "=")
				if key == "PATH" || key == "AURORA_OFFLINE_TEST_MARKER" || strings.HasPrefix(key, "AURORA_") {
					continue
				}
				env = append(env, entry)
			}
			env = append(env, "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "AURORA_OFFLINE_TEST_MARKER="+marker, "AURORA_BOOTSTRAP_GO_CACHE="+filepath.Join(root, "cache"), "AURORA_INPLACE_GO_SEED_ARCHIVES="+seed, "AURORA_INPLACE_SEED_ARCHIVES="+seed)
			if tc.env != "" {
				env = append(env, tc.env)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "/bin/sh", append([]string{shell}, tc.args...)...)
			cmd.Env = env
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatalf("expected controlled failure, got %v: %s", err, output)
			}
			_, markerErr := os.Stat(marker)
			if tc.offline {
				if exit.ExitCode() != 1 || !os.IsNotExist(markerErr) || !strings.Contains(string(output), "offline pinned Go archive missing") {
					t.Fatalf("offline bootstrap attempted download or failed unexpectedly: exit=%d marker=%v output=%s", exit.ExitCode(), markerErr, output)
				}
			} else if exit.ExitCode() != 97 || markerErr != nil {
				t.Fatalf("fake curl control did not run: exit=%d marker=%v output=%s", exit.ExitCode(), markerErr, output)
			}
		})
	}
}
