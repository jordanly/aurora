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
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"aurora.local/tools/internal/bootstrap"
)

// DeveloperGo always extracts a freshly verified SDK. Environment variables are
// configuration inputs, never evidence that an ambient Go executable is trusted.
func DeveloperGo(ctx context.Context, root, mode string, args []string) error {
	if mode != "check-tools" && mode != "build-client" {
		return errors.New("unsupported Go developer command")
	}
	flags := flag.NewFlagSet(mode, flag.ContinueOnError)
	offline := flags.Bool("offline", os.Getenv("AURORA_INPLACE_GO_OFFLINE") == "1" || os.Getenv("AURORA_INPLACE_OFFLINE") == "1", "disable archive and module downloads")
	cachePath := flags.String("cache", envPath("AURORA_INPLACE_GO_CACHE", filepath.Join(root, ".cache/inplace-go")), "private SDK and build cache")
	seed := flags.String("seed-archives", envPath("AURORA_INPLACE_GO_SEED_ARCHIVES", envPath("AURORA_INPLACE_SEED_ARCHIVES", filepath.Join(root, ".pi-tools/downloads"))), "verified offline archive directory")
	var output string
	if mode == "build-client" {
		flags.StringVar(&output, "output", "", "standalone client output file (required)")
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected Go developer command argument")
	}
	source := filepath.Join(root, "tools")
	if mode == "build-client" {
		if output == "" {
			return errors.New("build-client requires --output FILE")
		}
		var err error
		output, err = bootstrap.CheckPath(output)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, output)
		if err != nil {
			return err
		}
		if relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return errors.New("client output must be outside its source tree")
		}
		if _, err = bootstrap.PrivateCache(filepath.Dir(output)); err != nil {
			return err
		}
		if _, err = bootstrap.CheckPath(output + ".provenance.json"); err != nil {
			return err
		}
	}
	cache, err := bootstrap.PrivateCache(*cachePath)
	if err != nil {
		return err
	}
	unlock, err := bootstrap.Lock(filepath.Join(cache, "developer-go.lock"))
	if err != nil {
		return err
	}
	defer unlock()
	pins, err := bootstrap.LoadManifest(filepath.Join(source, "go-tools.json"))
	if err != nil {
		return err
	}
	work, err := os.MkdirTemp(cache, "tool-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(work)
	extracted, err := bootstrap.Tools(ctx, pins, cache, work, *seed, *offline)
	if err != nil {
		return err
	}
	goTool := filepath.Join(extracted["go"], "bin/go")
	env, err := goEnvironment(cache, goTool, *offline)
	if err != nil {
		return err
	}
	version, err := goVersion(ctx, goTool, env)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(version, "go version go1.27.1 ") {
		return errors.New("verified Go archive version mismatch")
	}
	if mode == "check-tools" {
		for _, step := range []string{"test", "vet"} {
			command := []string{goTool, "-C", source, step}
			if step == "test" {
				command = append(command, "-count=1")
			}
			command = append(command, "./...")
			if err = run(ctx, command, root, env, false); err != nil {
				return err
			}
		}
		return nil
	}
	before, err := TreeDigest(source)
	if err != nil {
		return err
	}
	staging, err := os.MkdirTemp(filepath.Dir(output), ".client-build-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	temporary := filepath.Join(staging, "aurora")
	if err = run(ctx, []string{goTool, "-C", source, "build", "-trimpath", "-buildvcs=false", "-ldflags=-s -w", "-o", temporary, "./cmd/aurora"}, root, env, false); err != nil {
		return err
	}
	after, err := TreeDigest(source)
	if err != nil {
		return err
	}
	if after != before {
		return errors.New("source tree changed during client build")
	}
	digest, err := bootstrap.Digest(temporary)
	if err != nil {
		return err
	}
	receipt := map[string]any{"schema": 1, "sourceSha256": after, "binarySha256": digest, "goVersion": version, "goArchiveSha256": pins.Tools["go"].SHA256, "build": append(append([]string{}, agentBuild...), "-trimpath", "-buildvcs=false", "-ldflags=-s -w"), "package": "./cmd/aurora"}
	if err = writeJSON(temporary+".provenance.json", receipt); err != nil {
		return err
	}
	if _, err = bootstrap.CheckPath(output); err != nil {
		return err
	}
	if err = os.Rename(temporary, output); err != nil {
		return err
	}
	if err = os.Rename(temporary+".provenance.json", output+".provenance.json"); err != nil {
		return err
	}
	fmt.Println(output)
	return nil
}
