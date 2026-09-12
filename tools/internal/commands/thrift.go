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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"

	"aurora.local/tools/internal/bootstrap"
)

const extractionRecipe = "Go bounded TAR/ZIP extraction with regular-file archive mtime restoration"

type thriftPins struct {
	Source    bootstrap.Pin `json:"source"`
	Compiler  string        `json:"compiler"`
	Configure []string      `json:"configure"`
}
type compilerReceipt struct {
	SourceVersion string `json:"source_version"`
	SourceURL     string `json:"source_url"`
	SourceSHA     string `json:"source_sha256"`
	Compiler      string `json:"compiler"`
	RecipeSHA     string `json:"recipe_sha256"`
	CompilerSHA   string `json:"compiler_sha256"`
	Version       string `json:"version"`
}

func ThriftRecipeHash(compiler string, configure []string) string {
	data, _ := json.Marshal(map[string]any{"compiler": compiler, "configure": configure, "make": []string{"make", "-C", "compiler/cpp", "-j1"}, "extraction": extractionRecipe})
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
func version(ctx context.Context, compiler string) (string, error) {
	command := exec.CommandContext(ctx, compiler, "--version")
	data, err := command.CombinedOutput()
	return strings.TrimSpace(string(data)), err
}
func matchingReceipt(ctx context.Context, path, sourceDir, compiler string, pins thriftPins) bool {
	for _, path := range []string{path, sourceDir, compiler} {
		if _, err := bootstrap.CheckPath(path); err != nil {
			return false
		}
	}
	var actual compilerReceipt
	if err := bootstrap.ReadJSON(path, &actual); err != nil {
		return false
	}
	digest, err := bootstrap.Digest(compiler)
	if err != nil {
		return false
	}
	expected := compilerReceipt{pins.Source.Version, pins.Source.URL, pins.Source.SHA256, pins.Compiler, ThriftRecipeHash(pins.Compiler, pins.Configure), digest, "Thrift version " + pins.Source.Version}
	if actual != expected {
		return false
	}
	got, err := version(ctx, compiler)
	return err == nil && got == expected.Version
}
func Thrift(ctx context.Context, root string, args []string) error {
	for _, arg := range args {
		if arg != "--offline" {
			return fmt.Errorf("unsupported thrift bootstrap argument: %s", arg)
		}
	}
	var pins thriftPins
	if err := bootstrap.ReadJSON(filepath.Join(root, "build-support/java/thrift-tools.json"), &pins); err != nil {
		return err
	}
	if err := pins.Source.Validate(); err != nil {
		return err
	}
	if pins.Source.Version != "0.10.0" || pins.Compiler != "compiler/cpp/thrift" || !reflect.DeepEqual(pins.Configure, []string{"--without-libs", "--without-tests", "--without-tutorial", "--disable-plugin"}) {
		return errors.New("unsupported Thrift compiler-only recipe")
	}
	cache, err := bootstrap.PrivateCache(envPath("AURORA_INPLACE_THRIFT_CACHE", filepath.Join(root, ".cache/inplace-thrift")))
	if err != nil {
		return err
	}
	unlock, err := bootstrap.Lock(filepath.Join(cache, "thrift.lock"))
	if err != nil {
		return err
	}
	defer unlock()
	seed := seedPath("AURORA_INPLACE_THRIFT_SEED_ARCHIVES", root)
	archive, err := bootstrap.EnsureArchive(ctx, pins.Source, cache, seed, has(args, "--offline"))
	if err != nil {
		return err
	}
	source := filepath.Join(cache, "source")
	compiler := filepath.Join(source, pins.Compiler)
	receipt := filepath.Join(cache, "compiler-receipt.json")
	if matchingReceipt(ctx, receipt, source, compiler, pins) {
		fmt.Println(compiler)
		return nil
	}
	if _, err = bootstrap.CheckPath(source); err != nil {
		return err
	}
	if err = os.RemoveAll(source); err != nil {
		return err
	}
	workspace, err := os.MkdirTemp(cache, "extract-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(workspace)
	extracted := filepath.Join(workspace, "out")
	if err = bootstrap.Extract(ctx, archive, extracted, true); err != nil {
		return err
	}
	if err = os.Rename(filepath.Join(extracted, "thrift-"+pins.Source.Version), source); err != nil {
		return err
	}
	for _, tool := range []string{"make", "g++"} {
		if _, err = exec.LookPath(tool); err != nil {
			return fmt.Errorf("Thrift compiler requires %s", tool)
		}
	}
	for _, command := range [][]string{append([]string{"./configure"}, pins.Configure...), {"make", "-C", "compiler/cpp", "-j1"}} {
		fmt.Fprintln(os.Stderr, "+", strings.Join(command, " "))
		if err = run(ctx, command, source, os.Environ(), true); err != nil {
			return err
		}
	}
	got, err := version(ctx, compiler)
	if err != nil {
		return err
	}
	if got != "Thrift version "+pins.Source.Version {
		return errors.New("built Thrift compiler version mismatch")
	}
	digest, err := bootstrap.Digest(compiler)
	if err != nil {
		return err
	}
	record := compilerReceipt{pins.Source.Version, pins.Source.URL, pins.Source.SHA256, pins.Compiler, ThriftRecipeHash(pins.Compiler, pins.Configure), digest, got}
	if err = writeJSON(receipt, record); err != nil {
		return err
	}
	fmt.Println(compiler)
	return nil
}
func writeJSON(path string, value any) error {
	if _, err := bootstrap.CheckPath(path); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	file, err := os.CreateTemp(filepath.Dir(path), ".json-")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	defer file.Close()
	if _, err = file.Write(data); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}
