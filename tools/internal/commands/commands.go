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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"aurora.local/tools/internal/bootstrap"
)

func environment(values map[string]string, remove ...string) []string {
	result := []string{}
	skip := map[string]bool{}
	for k := range values {
		skip[k] = true
	}
	for _, key := range remove {
		skip[key] = true
	}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if !skip[key] {
			result = append(result, entry)
		}
	}
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	return result
}
func envPath(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func seedPath(key, root string) string {
	return envPath(key, os.Getenv("AURORA_INPLACE_SEED_ARCHIVES"))
}
func has(arguments []string, value string) bool {
	for _, arg := range arguments {
		if arg == value {
			return true
		}
	}
	return false
}
func run(ctx context.Context, command []string, cwd string, env []string, stderrOnly bool) error {
	if len(command) == 0 {
		return errors.New("empty command")
	}
	child := exec.CommandContext(ctx, command[0], command[1:]...)
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	child.Cancel = func() error { return syscall.Kill(-child.Process.Pid, syscall.SIGKILL) }
	child.Dir = cwd
	child.Env = env
	child.Stdin = os.Stdin
	child.Stderr = os.Stderr
	child.Stdout = os.Stdout
	if stderrOnly {
		child.Stdout = os.Stderr
	}
	return child.Run()
}

func ValidateGradleArguments(arguments []string) error {
	for index, arg := range arguments {
		value := arg
		if (arg == "-D" || arg == "-P" || arg == "--system-prop" || arg == "--project-prop") && index+1 < len(arguments) {
			value = arguments[index+1]
		}
		for _, prefix := range []string{"--system-prop=", "-D", "--project-prop=", "-P"} {
			value = strings.TrimPrefix(value, prefix)
		}
		for _, prefix := range []string{"org.gradle.java.home", "org.gradle.java.installations.", "org.gradle.jvmargs"} {
			if strings.HasPrefix(value, prefix) {
				return errors.New("toolchain/JVM options are managed by the pinned launcher")
			}
		}
		for _, prefix := range []string{"javaVersion=", "sourceCompatibility=", "targetCompatibility=", "release="} {
			if strings.HasPrefix(value, prefix) {
				_, version, _ := strings.Cut(value, "=")
				if version == "8" || version == "1.8" {
					return errors.New("Java 8 rejected; this build requires Java 25")
				}
			}
		}
	}
	return nil
}
func GradleInvocation(arguments []string, tools map[string]string, cache string) ([]string, []string) {
	java := tools["java"]
	command := []string{filepath.Join(tools["gradle"], "bin/gradle"), "--no-daemon", "--max-workers=2", "--warning-mode=fail",
		"-Dorg.gradle.java.installations.auto-detect=false", "-Dorg.gradle.java.installations.auto-download=false",
		"-Dorg.gradle.java.installations.paths=" + java, "-Dorg.gradle.java.home=" + java,
		"-Dorg.gradle.jvmargs=-Xmx256m -XX:MaxMetaspaceSize=192m -XX:ActiveProcessorCount=2 -Dfile.encoding=UTF-8 --enable-native-access=ALL-UNNAMED",
		"--project-cache-dir", filepath.Join(cache, "project"), "-PauroraBuildRoot=" + filepath.Join(cache, "build")}
	return append(command, arguments...), environment(map[string]string{"JAVA_HOME": java, "GRADLE_USER_HOME": filepath.Join(cache, "gradle"), "PATH": filepath.Join(java, "bin") + string(os.PathListSeparator) + os.Getenv("PATH")})
}
func UIInvocation(arguments []string, tools map[string]string, cache string) ([]string, []string) {
	node := tools["node"]
	command := []string{filepath.Join(node, "bin/node"), filepath.Join(node, "lib/node_modules/npm/bin/npm-cli.js")}
	return append(command, arguments...), environment(map[string]string{"PATH": filepath.Join(node, "bin") + string(os.PathListSeparator) + os.Getenv("PATH"), "npm_config_cache": filepath.Join(cache, "npm"), "npm_config_update_notifier": "false"}, "NPM_CONFIG_CACHE")
}
func Gradle(ctx context.Context, root string, args []string) error {
	if err := ValidateGradleArguments(args); err != nil {
		return err
	}
	return withTools(ctx, root, "toolchains.json", "AURORA_INPLACE_CACHE", "inplace-build", "build.lock", seedPath("AURORA_INPLACE_SEED_ARCHIVES", root), args, func(tools map[string]string, cache string) error {
		command, env := GradleInvocation(args, tools, cache)
		return run(ctx, command, root, env, false)
	})
}
func UI(ctx context.Context, root string, args []string) error {
	return withTools(ctx, root, "ui-tools.json", "AURORA_INPLACE_UI_CACHE", "inplace-ui", "ui.lock", seedPath("AURORA_INPLACE_UI_SEED_ARCHIVES", root), args, func(tools map[string]string, cache string) error {
		command, env := UIInvocation(args, tools, cache)
		for _, file := range command[:2] {
			info, err := os.Stat(file)
			if err != nil || !info.Mode().IsRegular() {
				return errors.New("pinned Node archive layout differs")
			}
		}
		return run(ctx, command, filepath.Join(root, "ui"), env, false)
	})
}
func withTools(ctx context.Context, root, manifest, cacheEnv, defaultCache, lockName, seed string, args []string, action func(map[string]string, string) error) error {
	pins, err := bootstrap.LoadManifest(filepath.Join(root, "build-support/java", manifest))
	if err != nil {
		return err
	}
	cache, err := bootstrap.PrivateCache(envPath(cacheEnv, filepath.Join(root, ".cache", defaultCache)))
	if err != nil {
		return err
	}
	unlock, err := bootstrap.Lock(filepath.Join(cache, lockName))
	if err != nil {
		return err
	}
	defer unlock()
	work, err := os.MkdirTemp(cache, "tools-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(work)
	tools, err := bootstrap.Tools(ctx, pins, cache, work, seed, has(args, "--offline"))
	if err != nil {
		return err
	}
	return action(tools, cache)
}
func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	fmt.Fprintln(os.Stderr, "Aurora tools:", err)
	var status interface{ ExitCode() int }
	if errors.As(err, &status) {
		code := status.ExitCode()
		if code >= 0 && code <= 255 {
			return code
		}
		var child *exec.ExitError
		if errors.As(err, &child) {
			if signal, ok := child.Sys().(syscall.WaitStatus); ok && signal.Signaled() {
				return 128 + int(signal.Signal())
			}
		}
	}
	return 1
}
