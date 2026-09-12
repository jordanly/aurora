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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"aurora.local/tools/internal/bootstrap"
)

var agentBuild = []string{"CGO_ENABLED=0", "GOOS=linux", "GOARCH=arm64", "GOTOOLCHAIN=local"}

func TreeDigest(root string) (string, error) {
	if _, err := bootstrap.CheckPath(root); err != nil {
		return "", err
	}
	paths := []string{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("source symlink rejected")
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("source special file rejected")
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(paths)
	hash := sha256.New()
	for _, path := range paths {
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return "", err
		}
		digest, err := bootstrap.Digest(path)
		if err != nil {
			return "", err
		}
		decoded, _ := hex.DecodeString(digest)
		hash.Write([]byte(filepath.ToSlash(relative) + "\x00"))
		hash.Write(decoded)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
func goEnvironment(cache, goTool string, offline bool) ([]string, error) {
	buildCache, err := bootstrap.PrivateCache(envPath("GOCACHE", filepath.Join(cache, "gocache")))
	if err != nil {
		return nil, err
	}
	moduleCache, err := bootstrap.PrivateCache(envPath("GOMODCACHE", filepath.Join(cache, "gomodcache")))
	if err != nil {
		return nil, err
	}
	values := map[string]string{"CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "arm64", "GOTOOLCHAIN": "local", "GOENV": "off", "GOWORK": "off", "GOFLAGS": "", "GOROOT": filepath.Dir(filepath.Dir(goTool)), "GOCACHE": buildCache, "GOMODCACHE": moduleCache}
	if offline || os.Getenv("AURORA_INPLACE_GO_OFFLINE") == "1" {
		values["GOPROXY"] = "off"
	}
	return environment(values), nil
}
func Agents(ctx context.Context, root string, args []string) error {
	flags := flag.NewFlagSet("agents", flag.ContinueOnError)
	offline := flags.Bool("offline", false, "disable tool and module downloads")
	check := flags.Bool("check", false, "run uncached tests and vet before building")
	cachePath := flags.String("cache", filepath.Join(root, ".cache/inplace-go"), "private build cache")
	seed := flags.String("seed-archives", filepath.Join(root, ".pi-tools/downloads"), "verified offline archive directory")
	outputPath := flags.String("output", filepath.Join(root, ".pi-tools/agent-original-integration"), "artifact directory")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected agents argument")
	}
	cache, err := bootstrap.PrivateCache(*cachePath)
	if err != nil {
		return err
	}
	output, err := bootstrap.PrivateCache(*outputPath)
	if err != nil {
		return err
	}
	unlock, err := bootstrap.Lock(filepath.Join(cache, "agents.lock"))
	if err != nil {
		return err
	}
	defer unlock()
	pins, err := bootstrap.LoadManifest(filepath.Join(root, "tools/go-tools.json"))
	if err != nil {
		return err
	}
	workspace, err := os.MkdirTemp(cache, "tool-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(workspace)
	tools, err := bootstrap.Tools(ctx, pins, cache, workspace, *seed, *offline)
	if err != nil {
		return err
	}
	goTool := filepath.Join(tools["go"], "bin/go")
	env, err := goEnvironment(cache, goTool, *offline)
	if err != nil {
		return err
	}
	// Version is a consistency check only, after verified fresh archive extraction.
	got, err := goVersion(ctx, goTool, env)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(got, "go version go1.27.1 ") {
		return errors.New("verified Go archive version mismatch")
	}
	targets := []struct{ source, name, pkg string }{{filepath.Join(root, "agent"), "aurora-agent", "./cmd/aurora-agent"}, {filepath.Join(root, "build-support/lab/fixtures/cluster-helper"), "cluster-helper", "./"}}
	if *check {
		for _, target := range targets {
			for _, command := range [][]string{{goTool, "-C", target.source, "test", "-count=1", "./..."}, {goTool, "-C", target.source, "vet", "./..."}} {
				if err = run(ctx, command, root, env, false); err != nil {
					return err
				}
			}
		}
	}
	for _, target := range targets {
		// Record source before and after compilation so a changing tree cannot get
		// a misleading receipt for a binary built from different inputs.
		before, err := TreeDigest(target.source)
		if err != nil {
			return err
		}
		binary := filepath.Join(output, target.name)
		if _, err = bootstrap.CheckPath(binary); err != nil {
			return err
		}
		staging, err := os.MkdirTemp(output, ".build-")
		if err != nil {
			return err
		}
		temporary := filepath.Join(staging, target.name)
		err = run(ctx, []string{goTool, "-C", target.source, "build", "-trimpath", "-buildvcs=false", "-ldflags=-s -w", "-o", temporary, target.pkg}, root, env, false)
		if err != nil {
			os.RemoveAll(staging)
			return err
		}
		after, err := TreeDigest(target.source)
		if err != nil {
			os.RemoveAll(staging)
			return err
		}
		if before != after {
			os.RemoveAll(staging)
			return errors.New("source tree changed during build")
		}
		if err = os.Rename(temporary, binary); err != nil {
			os.RemoveAll(staging)
			return err
		}
		os.RemoveAll(staging)
		digest, err := bootstrap.Digest(binary)
		if err != nil {
			return err
		}
		record := map[string]any{"schema": 1, "sourceSha256": after, "binarySha256": digest, "goVersion": got, "build": append(append([]string{}, agentBuild...), "-trimpath", "-buildvcs=false", "-ldflags=-s -w")}
		if err = writeJSON(binary+".provenance.json", record); err != nil {
			return err
		}
		fmt.Println(binary)
	}
	return nil
}
