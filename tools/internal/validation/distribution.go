// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package validation

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

func require(ok bool, message string) error {
	if !ok {
		return fmt.Errorf("%s", message)
	}
	return nil
}

func runCaptured(ctx context.Context, command []string, environment []string, directory string, timeout time.Duration) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	child := exec.CommandContext(ctx, command[0], command[1:]...)
	child.Env = environment
	child.Dir = directory
	child.WaitDelay = time.Second
	output, err := child.CombinedOutput()
	if ctx.Err() != nil {
		return nil, fmt.Errorf("command exceeded timeout: %w", ctx.Err())
	}
	code := 0
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			code = exit.ExitCode()
		} else {
			return nil, err
		}
	}
	text := string(bytes.Runes(output))
	return map[string]any{"command": command, "exitCode": code, "output": text, "outputSha256": digest([]byte(text))}, nil
}

func verifyHelp(result map[string]any, mainClass string, requiredOptions []string) error {
	if result["exitCode"] != 1 {
		return fmt.Errorf("%s: expected usage exit 1, got %v", mainClass, result["exitCode"])
	}
	output := result["output"].(string)
	if !regexp.MustCompile(`Usage:\s*` + regexp.QuoteMeta(mainClass)).MatchString(output) {
		return fmt.Errorf("%s: expected original usage banner", mainClass)
	}
	for _, option := range requiredOptions {
		if !strings.Contains(output, option) {
			return fmt.Errorf("%s: usage missing %s", mainClass, option)
		}
	}
	for _, bad := range []string{"NoClassDefFoundError", "ClassNotFoundException", "Could not find or load main class", "UnsatisfiedLinkError", "Recovering from"} {
		if strings.Contains(output, bad) {
			return fmt.Errorf("%s: unexpected startup failure/action: %s", mainClass, bad)
		}
	}
	result["expectedExitCode"] = 1
	result["usageVerified"] = true
	return nil
}

func verifyClasspath(script, distribution string) ([]string, error) {
	matches := regexp.MustCompile(`\$APP_HOME/lib/([^:\s"\n]+)`).FindAllStringSubmatch(script, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("installed launcher has no explicit runtime classpath")
	}
	references := []string{}
	seen := map[string]bool{}
	for _, match := range matches {
		entry := match[1]
		if seen[entry] {
			return nil, fmt.Errorf("installed launcher has duplicate classpath entries")
		}
		seen[entry] = true
		if filepath.Base(entry) != entry || strings.Contains(entry, "\\") {
			return nil, fmt.Errorf("unsafe runtime classpath entry: %s", entry)
		}
		info, err := os.Stat(filepath.Join(distribution, "lib", entry))
		if err != nil || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("installed launcher references missing runtime jar: %s", entry)
		}
		if !strings.HasSuffix(entry, ".jar") {
			return nil, fmt.Errorf("installed runtime classpath must use packaged jars: %s", entry)
		}
		references = append(references, entry)
	}
	jars, err := filepath.Glob(filepath.Join(distribution, "lib", "*.jar"))
	if err != nil {
		return nil, err
	}
	if len(jars) != len(seen) {
		return nil, fmt.Errorf("installed runtime jars and launcher classpath differ")
	}
	for _, jar := range jars {
		if !seen[filepath.Base(jar)] {
			return nil, fmt.Errorf("installed runtime jars and launcher classpath differ")
		}
	}
	err = filepath.WalkDir(filepath.Join(distribution, "lib"), func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasSuffix(name, ".class") {
			return fmt.Errorf("distribution contains unpackaged generated classes")
		}
		return nil
	})
	return references, err
}

const schedulerClass = "org/apache/aurora/scheduler/app/SchedulerMain.class"
const apiClass = "org/apache/aurora/gen/ReadOnlyScheduler.class"
const commonsClass = "org/apache/aurora/common/application/Lifecycle.class"

var schedulerEntries = []string{
	"org/apache/aurora/scheduler/storage/durability/RecoveryTool.class",
	"scheduler/assets/scheduler/index.html", "scheduler/assets/js/bundle.js",
	"scheduler/assets/js/bundle.js.map", "scheduler/assets/js/thrift.js",
	"scheduler/assets/bower_components/jquery/dist/jquery.min.js",
}
var apiEntries = []string{
	"org/apache/aurora/gen/AuroraAdmin.class", "org/apache/aurora/scheduler/storage/entities/IScheduledTask.class",
	"org/apache/aurora/scheduler/gen/client/ReadOnlyScheduler.js", "org/apache/aurora/scheduler/gen/client/AuroraAdmin.js",
	"org/apache/aurora/scheduler/gen/client/api_types.js", "org/apache/aurora/scheduler/gen/client/api.html",
}
var dependencyEntries = []string{
	"org/sqlite/JDBC.class", "com/google/inject/Guice.class", "com/google/common/collect/ImmutableList.class",
	"org/apache/thrift/TBase.class", "org/apache/zookeeper/ZooKeeper.class", "org/eclipse/jetty/server/Server.class",
	"com/google/gson/Gson.class", "javax/xml/bind/annotation/XmlElement.class",
}

func inspectJars(distribution string) (map[string]map[string]any, map[string]string, error) {
	jars, err := filepath.Glob(filepath.Join(distribution, "lib", "*.jar"))
	if err != nil {
		return nil, nil, err
	}
	if len(jars) == 0 {
		return nil, nil, fmt.Errorf("no installed runtime jars")
	}
	sort.Strings(jars)
	members := map[string]map[string]*zip.File{}
	artifacts := map[string]map[string]any{}
	opened := []*zip.ReadCloser{}
	defer func() {
		for _, archive := range opened {
			archive.Close()
		}
	}()
	for _, jar := range jars {
		archive, err := zip.OpenReader(jar)
		if err != nil {
			return nil, nil, err
		}
		opened = append(opened, archive)
		entries := map[string]*zip.File{}
		for _, file := range archive.File {
			if entries[file.Name] != nil {
				return nil, nil, fmt.Errorf("duplicate jar entries: %s", filepath.Base(jar))
			}
			name := strings.TrimSuffix(file.Name, "/")
			if path.IsAbs(name) || path.Clean(name) != name || strings.Contains(name, "\\") || name == ".." || strings.HasPrefix(name, "../") {
				return nil, nil, fmt.Errorf("unsafe jar entry: %s", file.Name)
			}
			for _, prefix := range []string{"org/apache/mesos/", "org/apache/aurora/scheduler/mesos/", "org/apache/aurora/scheduler/log/mesos/"} {
				if strings.HasPrefix(file.Name, prefix) {
					return nil, nil, fmt.Errorf("Mesos classes remain in %s", filepath.Base(jar))
				}
			}
			entries[file.Name] = file
		}
		members[jar] = entries
		data, err := os.ReadFile(jar)
		if err != nil {
			return nil, nil, err
		}
		artifacts[filepath.Base(jar)] = map[string]any{"sha256": digest(data), "size": len(data)}
	}
	containing := func(entry string) (string, error) {
		found := ""
		for jar, entries := range members {
			if entries[entry] != nil {
				if found != "" {
					return "", fmt.Errorf("expected exactly one runtime jar containing %s", entry)
				}
				found = jar
			}
		}
		if found == "" {
			return "", fmt.Errorf("expected exactly one runtime jar containing %s", entry)
		}
		return found, nil
	}
	scheduler, err := containing(schedulerClass)
	if err != nil {
		return nil, nil, err
	}
	api, err := containing(apiClass)
	if err != nil {
		return nil, nil, err
	}
	commons, err := containing(commonsClass)
	if err != nil {
		return nil, nil, err
	}
	for jar, required := range map[string][]string{scheduler: schedulerEntries, api: apiEntries} {
		for _, entry := range required {
			member := members[jar][entry]
			if member == nil {
				return nil, nil, fmt.Errorf("%s: missing %s", filepath.Base(jar), entry)
			}
			if member.UncompressedSize64 == 0 {
				return nil, nil, fmt.Errorf("%s: empty %s", filepath.Base(jar), entry)
			}
		}
		artifacts[filepath.Base(jar)]["requiredEntries"] = required
	}
	for _, jar := range []string{scheduler, api, commons} {
		count := 0
		for name, member := range members[jar] {
			if strings.HasSuffix(name, ".py") || strings.HasSuffix(name, ".pex") {
				return nil, nil, fmt.Errorf("retired Python artifact in %s: %s", filepath.Base(jar), name)
			}
			if !strings.HasSuffix(name, ".class") {
				continue
			}
			count++
			if member.UncompressedSize64 > 64*1024*1024 {
				return nil, nil, fmt.Errorf("class exceeds 64 MiB bound: %s", name)
			}
			stream, err := member.Open()
			if err != nil {
				return nil, nil, err
			}
			data, readErr := io.ReadAll(io.LimitReader(stream, 64*1024*1024+1))
			stream.Close()
			if readErr != nil {
				return nil, nil, readErr
			}
			if len(data) > 64*1024*1024 || len(data) < 8 || !bytes.Equal(data[:4], []byte{0xca, 0xfe, 0xba, 0xbe}) {
				return nil, nil, fmt.Errorf("invalid class header: %s", name)
			}
			major := binary.BigEndian.Uint16(data[6:8])
			if major != 69 {
				return nil, nil, fmt.Errorf("%s: %s has bytecode %d, expected 69", filepath.Base(jar), name, major)
			}
		}
		if count == 0 {
			return nil, nil, fmt.Errorf("no Aurora classes in %s", filepath.Base(jar))
		}
		artifacts[filepath.Base(jar)]["auroraClassCount"] = count
		artifacts[filepath.Base(jar)]["bytecodeMajor"] = 69
	}
	dependencies := map[string]string{}
	for _, entry := range dependencyEntries {
		jar, err := containing(entry)
		if err != nil {
			return nil, nil, err
		}
		dependencies[entry] = filepath.Base(jar)
	}
	return artifacts, dependencies, nil
}

func cleanJavaEnvironment(home string) []string {
	remove := map[string]bool{"JAVA_OPTS": true, "JAVA_TOOL_OPTIONS": true, "_JAVA_OPTIONS": true, "JDK_JAVA_OPTIONS": true, "AURORA_SCHEDULER_OPTS": true, "RECOVERY_TOOL_OPTS": true, "CLASSPATH": true, "JAVA_HOME": true, "PATH": true}
	result := []string{}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if !remove[key] {
			result = append(result, entry)
		}
	}
	return append(result, "JAVA_HOME="+home, "PATH="+filepath.Join(home, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func executable(name string) bool {
	info, err := os.Stat(name)
	return err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0
}

func verifyDistribution(ctx context.Context, distribution string, receipt map[string]any, timeout time.Duration) error {
	info, err := os.Stat(distribution)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("installed distribution does not exist: %s", distribution)
	}
	home := os.Getenv("JAVA_HOME")
	if home == "" {
		return fmt.Errorf("JAVA_HOME must select the pinned Java runtime")
	}
	home, err = filepath.Abs(home)
	if err != nil {
		return err
	}
	java := filepath.Join(home, "bin/java")
	if !executable(java) {
		return fmt.Errorf("JAVA_HOME does not contain executable java")
	}
	environment := cleanJavaEnvironment(home)
	directory, err := os.MkdirTemp("", "aurora-distribution-check-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	runtime, err := runCaptured(ctx, []string{java, "--version"}, environment, directory, timeout)
	if err != nil {
		return err
	}
	receipt["java"] = runtime
	if runtime["exitCode"] != 0 {
		return fmt.Errorf("Java runtime version command failed")
	}
	match := regexp.MustCompile(`(?m)^(?:openjdk|java) (\d+)`).FindStringSubmatch(runtime["output"].(string))
	version := 0
	if len(match) == 2 {
		version, _ = strconv.Atoi(match[1])
	}
	if version < 25 {
		return fmt.Errorf("installed launcher requires Java 25 or newer")
	}
	runtime["javaHome"] = home
	artifacts, dependencies, err := inspectJars(distribution)
	if err != nil {
		return err
	}
	receipt["artifacts"] = artifacts
	receipt["runtimeDependencies"] = dependencies
	launchers := map[string]any{}
	receipt["launchers"] = launchers
	for _, launcher := range []struct {
		name, main, help string
		options          []string
	}{
		{"aurora-scheduler", "org.apache.aurora.scheduler.app.SchedulerMain", "-help", []string{"-cluster_name", "-serverset_path"}},
		{"recovery-tool", "org.apache.aurora.scheduler.storage.durability.RecoveryTool", "--help", []string{"-from", "-to", "--help"}},
	} {
		script := filepath.Join(distribution, "bin", launcher.name)
		windows := script + ".bat"
		if !executable(script) {
			return fmt.Errorf("installed Unix launcher is missing or not executable: %s", script)
		}
		windowsData, err := os.ReadFile(windows)
		if err != nil || len(windowsData) == 0 {
			return fmt.Errorf("installed Windows launcher is missing: %s", windows)
		}
		scriptData, err := os.ReadFile(script)
		if err != nil {
			return err
		}
		if !strings.Contains(string(scriptData), launcher.main) {
			return fmt.Errorf("installed launcher has wrong entrypoint: %s", launcher.name)
		}
		references, err := verifyClasspath(string(scriptData), distribution)
		if err != nil {
			return err
		}
		result, err := runCaptured(ctx, []string{script, launcher.help}, environment, directory, timeout)
		if err != nil {
			return err
		}
		launchers[launcher.name] = result
		result["unixSha256"] = digest(scriptData)
		result["windowsSha256"] = digest(windowsData)
		result["classpathJars"] = references
		if err := verifyHelp(result, launcher.main, launcher.options); err != nil {
			return err
		}
	}
	receipt["status"] = "passed"
	return nil
}

func distribution(ctx context.Context, args []string) error {
	flags, positional, err := options(args, "--receipt", "--timeout")
	if err != nil {
		return err
	}
	if len(positional) != 1 || flags["--receipt"] == "" {
		return usage("verify-distribution requires <distribution> --receipt <path> [--timeout seconds]")
	}
	path, err := filepath.Abs(positional[0])
	if err != nil {
		return err
	}
	receipt := map[string]any{"schema": 1, "distribution": path, "status": "failed", "checkedAt": time.Now().UTC().Format(time.RFC3339Nano)}
	timeout := 30.0
	if value := flags["--timeout"]; value != "" {
		timeout, err = strconv.ParseFloat(value, 64)
	}
	if err == nil && !(timeout > 0 && timeout < 1e9) {
		err = fmt.Errorf("timeout must be positive and finite")
	}
	if err == nil {
		err = verifyDistribution(ctx, path, receipt, time.Duration(timeout*float64(time.Second)))
	}
	if err != nil {
		receipt["error"] = err.Error()
	}
	if writeErr := writeJSON(flags["--receipt"], receipt); writeErr != nil {
		return writeErr
	}
	if err != nil {
		return fmt.Errorf("distribution verification failed: %w", err)
	}
	fmt.Printf("Installed distribution verified; receipt: %s\n", flags["--receipt"])
	return nil
}
