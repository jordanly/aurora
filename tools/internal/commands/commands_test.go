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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestGradleArgumentsArePreservedAndManagedOptionsRefused(t *testing.T) {
	args := []string{"test", "--tests", "org.example.Test", "-Pmessage=a b", "--offline"}
	command, _ := GradleInvocation(args, map[string]string{"java": "/java", "gradle": "/gradle"}, "/cache")
	if !reflect.DeepEqual(command[len(command)-len(args):], args) {
		t.Fatal(command)
	}
	for _, args := range [][]string{{"-Dorg.gradle.java.home=/bad"}, {"--system-prop", "org.gradle.jvmargs=-javaagent:bad"}, {"--project-prop=org.gradle.java.installations.paths=/bad"}, {"-PjavaVersion=8"}, {"-D", "org.gradle.java.installations.auto-detect=true"}} {
		if err := ValidateGradleArguments(args); err == nil {
			t.Fatal("unmanaged toolchain option accepted", args)
		}
	}
}
func TestUIUsesPinnedNodeRealNPMAndPrivateCache(t *testing.T) {
	t.Setenv("NPM_CONFIG_CACHE", "/untrusted")
	args := []string{"ci", "--offline"}
	command, env := UIInvocation(args, map[string]string{"node": "/node"}, "/cache")
	if !reflect.DeepEqual(command, []string{"/node/bin/node", "/node/lib/node_modules/npm/bin/npm-cli.js", "ci", "--offline"}) {
		t.Fatal(command)
	}
	found := false
	for _, entry := range env {
		if strings.HasPrefix(entry, "NPM_CONFIG_CACHE=") {
			t.Fatal(entry)
		}
		if entry == "npm_config_cache=/cache/npm" {
			found = true
		}
	}
	if !found {
		t.Fatal(env)
	}
}
func TestThriftRecipeIncludesPluginAndExtractionPolicy(t *testing.T) {
	flags := []string{"--without-libs", "--without-tests", "--without-tutorial", "--disable-plugin"}
	expected := ThriftRecipeHash("compiler/cpp/thrift", flags)
	if expected == ThriftRecipeHash("compiler/cpp/thrift", flags[:3]) {
		t.Fatal("plugin recipe omitted from provenance")
	}
	if expected == ThriftRecipeHash("other", flags) {
		t.Fatal("compiler layout omitted from provenance")
	}
}
func TestReceiptDoesNotTrustCompilerVersionAlone(t *testing.T) {
	dir := t.TempDir()
	compiler := filepath.Join(dir, "thrift")
	os.WriteFile(compiler, []byte("#!/bin/sh\necho 'Thrift version 0.10.0'\n"), 0700)
	pins := thriftPins{Compiler: "thrift"}
	pins.Source.Version = "0.10.0"
	if matchingReceipt(context.Background(), filepath.Join(dir, "missing.json"), dir, compiler, pins) {
		t.Fatal("unproven executable accepted")
	}
}
func TestTreeDigestIncludesNamesContentsAndRejectsSymlinks(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "a"), []byte("same"), 0600)
	first, err := TreeDigest(root)
	if err != nil {
		t.Fatal(err)
	}
	os.Rename(filepath.Join(root, "a"), filepath.Join(root, "b"))
	second, err := TreeDigest(root)
	if err != nil || first == second {
		t.Fatal(first, second, err)
	}
	os.Symlink(filepath.Join(root, "b"), filepath.Join(root, "alias"))
	if _, err := TreeDigest(root); err == nil {
		t.Fatal("symlink source provenance accepted")
	}
}

type usageError struct{}

func (usageError) Error() string { return "usage" }
func (usageError) ExitCode() int { return 2 }
func TestExitCodePreservesWrappedUsageAndSignal(t *testing.T) {
	if got := ExitCode(fmt.Errorf("wrapped: %w", usageError{})); got != 2 {
		t.Fatal(got)
	}
	err := exec.Command("/bin/sh", "-c", "kill -TERM $$").Run()
	if got := ExitCode(err); got != 143 {
		t.Fatal(got, err)
	}
}
