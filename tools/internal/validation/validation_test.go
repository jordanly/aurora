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
	"context"
	"encoding/json"
	"encoding/xml"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func write(t *testing.T, path, content string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}
func script(t *testing.T, path, content string) string {
	t.Helper()
	write(t, path, "#!/bin/sh\n"+content)
	if err := os.Chmod(path, 0755); err != nil {
		t.Fatal(err)
	}
	return path
}
func expectError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected contract violation")
	}
}
func expectCode(t *testing.T, err error, code int) {
	t.Helper()
	failure, ok := err.(interface{ ExitCode() int })
	if !ok || failure.ExitCode() != code {
		t.Fatalf("expected exit %d, got %v", code, err)
	}
}

func TestLicenseLeadingCommentContract(t *testing.T) {
	data, err := os.ReadFile("../../../config/checkstyle/apache.header")
	if err != nil {
		t.Fatal(err)
	}
	expected := normalized(strings.Split(string(data), "\n"))
	lineHeader := "// " + strings.Join(expected, "\n// ") + "\n"
	blockHeader := "/**\n * " + strings.Join(expected, "\n * ") + "\n */\n"
	for name, fixture := range map[string]struct {
		source string
		valid  bool
	}{
		"block CRLF":    {strings.ReplaceAll(blockHeader+"class Good {}\n", "\n", "\r\n"), true},
		"shebang hash":  {"#!/usr/bin/env sh\n# " + strings.Join(expected, "\n# ") + "\n", true},
		"BOM lines":     {"\ufeff" + lineHeader + "class Good {}", true},
		"after source":  {"class Bad {}\n" + blockHeader, false},
		"wrong":         {"/* Licensed under another license. */\n", false},
		"missing":       {"package example;\n", false},
		"leading blank": {"\n" + lineHeader, false},
		"unclosed":      {"/*\n" + strings.Join(expected, "\n"), false},
		"extra words":   {lineHeader + "// Extra header text.\n", false},
		"invalid UTF8":  {lineHeader + string([]byte{0xff}), false},
	} {
		t.Run(name, func(t *testing.T) {
			file := write(t, filepath.Join(t.TempDir(), "Test.java"), fixture.source)
			if hasLicense(file, expected) != fixture.valid {
				t.Fatalf("wrong result for %s", name)
			}
		})
	}
}

func TestLicenseScopeCustomHeaderAndSymlinks(t *testing.T) {
	dir := t.TempDir()
	header := write(t, filepath.Join(dir, "custom.header"), "Custom license text.\n")
	good := write(t, filepath.Join(dir, "Good.java"), "// Custom license text.\nclass Good {}\n")
	write(t, filepath.Join(dir, "build/Bad.java"), "bad")
	write(t, filepath.Join(dir, ".cache/Bad.java"), "bad")
	if err := os.Symlink("missing", filepath.Join(dir, "Link.java")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(dir, filepath.Join(dir, "cycle")); err != nil {
		t.Fatal(err)
	}
	files, err := sourceFiles([]string{dir}, []string{".java"})
	if err != nil || !reflect.DeepEqual(files, []string{good}) {
		t.Fatal(files, err)
	}
	if err := licenses(".", []string{dir, "--header", header, "--extensions", ".java"}); err != nil {
		t.Fatal(err)
	}
	write(t, good, "// Wrong\n")
	expectCode(t, licenses(".", []string{dir, "--header", header, "--extensions", ".java"}), 1)
	expectCode(t, licenses(".", []string{filepath.Join(dir, "cycle"), "--header", header}), 2)
	expectCode(t, licenses(".", []string{dir, "--header", header, "--extensions", ".not-source"}), 2)
	expectCode(t, licenses(".", []string{filepath.Join(dir, "missing"), "--header", header}), 2)
	write(t, header, "\n")
	expectCode(t, licenses(".", []string{dir, "--header", header}), 2)
}

func TestOriginalUsageContract(t *testing.T) {
	good := map[string]any{"exitCode": 1, "output": "Usage: example.Main [options]\n--help"}
	if err := verifyHelp(good, "example.Main", []string{"--help"}); err != nil || good["usageVerified"] != true {
		t.Fatal(err, good)
	}
	for _, fixture := range []struct {
		code   int
		output string
	}{
		{1, "failed"}, {0, "Usage: example.Main --help"}, {2, "Usage: example.Main --help"},
		{1, "Usage: wrong.Main --help"}, {1, "Usage: example.Main"},
		{1, "Usage: example.Main --help\nNoClassDefFoundError: missing"},
		{1, "Usage: example.Main --help\nClassNotFoundException"},
		{1, "Usage: example.Main --help\nUnsatisfiedLinkError"},
		{1, "Usage: example.Main --help\nRecovering from SNAPSHOT"},
	} {
		expectError(t, verifyHelp(map[string]any{"exitCode": fixture.code, "output": fixture.output}, "example.Main", []string{"--help"}))
	}
}

func TestPackagedClasspathAndPathRejection(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "lib/app.jar"), "")
	base := "CLASSPATH=$APP_HOME/lib/app.jar\n"
	refs, err := verifyClasspath(base, dir)
	if err != nil || !reflect.DeepEqual(refs, []string{"app.jar"}) {
		t.Fatal(refs, err)
	}
	for _, invalid := range []string{strings.TrimSpace(base) + ":$APP_HOME/lib/classes\n", strings.TrimSpace(base) + ":$APP_HOME/lib/app.jar\n", "CLASSPATH=$APP_HOME/lib/missing.jar\n", "CLASSPATH=$APP_HOME/lib/../outside.jar\n", "CLASSPATH=$APP_HOME/lib/sub\\app.jar\n", "CLASSPATH=none\n"} {
		_, err := verifyClasspath(invalid, dir)
		expectError(t, err)
	}
	extra := write(t, filepath.Join(dir, "lib/extra.jar"), "")
	_, err = verifyClasspath(base, dir)
	expectError(t, err)
	os.Remove(extra)
	write(t, filepath.Join(dir, "lib/generated/Generated.class"), "")
	_, err = verifyClasspath(base, dir)
	expectError(t, err)
}

func jar(t *testing.T, path string, entries map[string]string, duplicates ...string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	archive := zip.NewWriter(file)
	names := []string{}
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	names = append(names, duplicates...)
	for _, name := range names {
		entry, err := archive.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write([]byte(entries[name])); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}
func class() string { return string([]byte{0xca, 0xfe, 0xba, 0xbe, 0, 0, 0, 69}) }
func packaged(t *testing.T) (string, map[string]map[string]string) {
	t.Helper()
	dir := t.TempDir()
	contents := map[string]map[string]string{"scheduler.jar": {schedulerClass: class()}, "api.jar": {apiClass: class()}, "commons.jar": {commonsClass: class()}, "dependencies.jar": {}}
	for _, entry := range schedulerEntries {
		value := "asset"
		if strings.HasSuffix(entry, ".class") {
			value = class()
		}
		contents["scheduler.jar"][entry] = value
	}
	for _, entry := range apiEntries {
		value := "asset"
		if strings.HasSuffix(entry, ".class") {
			value = class()
		}
		contents["api.jar"][entry] = value
	}
	for _, entry := range dependencyEntries {
		contents["dependencies.jar"][entry] = "dependency"
	}
	for name, entries := range contents {
		jar(t, filepath.Join(dir, "lib", name), entries)
	}
	return dir, contents
}

func TestJarArtifactsDependenciesAndBytecode(t *testing.T) {
	dir, _ := packaged(t)
	artifacts, deps, err := inspectJars(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 4 || len(deps) != 8 || artifacts["scheduler.jar"]["auroraClassCount"] != 2 || artifacts["api.jar"]["bytecodeMajor"] != 69 {
		t.Fatal(artifacts, deps)
	}
	data, err := os.ReadFile(filepath.Join(dir, "lib/api.jar"))
	if err != nil {
		t.Fatal(err)
	}
	if artifacts["api.jar"]["sha256"] != digest(data) || artifacts["api.jar"]["size"] != len(data) {
		t.Fatal("artifact digest differs")
	}
}

func TestMalformedAndForbiddenJarsFail(t *testing.T) {
	for _, name := range []string{"missing asset", "empty asset", "duplicate entry", "duplicate owner", "invalid magic", "old bytecode", "bad zip", "mesos", "scheduler mesos", "log mesos", "traversal", "absolute", "backslash"} {
		t.Run(name, func(t *testing.T) {
			dir, contents := packaged(t)
			target := "scheduler.jar"
			entries := contents[target]
			switch name {
			case "missing asset":
				delete(entries, schedulerEntries[1])
			case "empty asset":
				entries[schedulerEntries[1]] = ""
			case "duplicate entry":
				jar(t, filepath.Join(dir, "lib/scheduler.jar"), entries, schedulerClass)
				_, _, err := inspectJars(dir)
				expectError(t, err)
				return
			case "duplicate owner":
				target = "duplicate.jar"
				entries = map[string]string{schedulerClass: class()}
			case "invalid magic":
				entries[schedulerClass] = "notclass"
			case "old bytecode":
				entries[schedulerClass] = string([]byte{0xca, 0xfe, 0xba, 0xbe, 0, 0, 0, 52})
			case "bad zip":
				write(t, filepath.Join(dir, "lib/bad.jar"), "broken")
				_, _, err := inspectJars(dir)
				expectError(t, err)
				return
			case "mesos":
				entries["org/apache/mesos/Scheduler.class"] = class()
			case "scheduler mesos":
				entries["org/apache/aurora/scheduler/mesos/Driver.class"] = class()
			case "log mesos":
				entries["org/apache/aurora/scheduler/log/mesos/Log.class"] = class()
			case "traversal":
				entries["../Outside.class"] = class()
			case "absolute":
				entries["/Outside.class"] = class()
			case "backslash":
				entries["..\\Outside.class"] = class()
			}
			jar(t, filepath.Join(dir, "lib", target), entries)
			_, _, err := inspectJars(dir)
			expectError(t, err)
		})
	}
}

func TestRetiredPythonArtifactsRejectedInAuroraJars(t *testing.T) {
	for _, target := range []string{"scheduler.jar", "api.jar", "commons.jar"} {
		for _, artifact := range []string{"bootstrap/testinfra.py", "thermos.pex"} {
			t.Run(target+"/"+artifact, func(t *testing.T) {
				dir, contents := packaged(t)
				if _, _, err := inspectJars(dir); err != nil {
					t.Fatal("baseline distribution invalid", err)
				}
				contents[target][artifact] = "retired executable"
				jar(t, filepath.Join(dir, "lib", target), contents[target])
				_, _, err := inspectJars(dir)
				if err == nil || !strings.Contains(err.Error(), "retired Python artifact") ||
					!strings.Contains(err.Error(), target) || !strings.Contains(err.Error(), artifact) {
					t.Fatalf("expected precise retired artifact rejection, got %v", err)
				}
			})
		}
	}
}

func TestDistributionEndToEndAndFailedReceipt(t *testing.T) {
	dir, _ := packaged(t)
	home := t.TempDir()
	script(t, filepath.Join(home, "bin/java"), "echo 'openjdk 25.0.1'\n")
	t.Setenv("JAVA_HOME", home)
	t.Setenv("JAVA_OPTS", "must-be-cleared")
	t.Setenv("JAVA_TOOL_OPTIONS", "must-be-cleared")
	classpath := "# CLASSPATH=$APP_HOME/lib/api.jar:$APP_HOME/lib/commons.jar:$APP_HOME/lib/dependencies.jar:$APP_HOME/lib/scheduler.jar\n"
	for _, launcher := range []struct{ name, main, options string }{{"aurora-scheduler", "org.apache.aurora.scheduler.app.SchedulerMain", "-cluster_name -serverset_path"}, {"recovery-tool", "org.apache.aurora.scheduler.storage.durability.RecoveryTool", "-from -to --help"}} {
		script(t, filepath.Join(dir, "bin", launcher.name), classpath+"test -z \"$JAVA_OPTS$JAVA_TOOL_OPTIONS\" || exit 2\necho 'Usage: "+launcher.main+" "+launcher.options+"'\nexit 1\n")
		write(t, filepath.Join(dir, "bin", launcher.name+".bat"), "windows launcher")
	}
	receipt := filepath.Join(t.TempDir(), "receipt.json")
	if err := distribution(context.Background(), []string{dir, "--receipt", receipt}); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(receipt)
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result["status"] != "passed" {
		t.Fatal(result)
	}
	expectError(t, distribution(context.Background(), []string{dir, "--receipt", receipt, "--timeout", "0"}))
	data, _ = os.ReadFile(receipt)
	json.Unmarshal(data, &result)
	if result["status"] != "failed" || result["error"] == nil {
		t.Fatal(result)
	}
}

func TestCapturedCommandDeadline(t *testing.T) {
	path := script(t, filepath.Join(t.TempDir(), "wait"), "exec sleep 10\n")
	start := time.Now()
	_, err := runCaptured(context.Background(), []string{path}, os.Environ(), t.TempDir(), 20*time.Millisecond)
	expectError(t, err)
	if time.Since(start) > 2*time.Second {
		t.Fatal("command deadline was not bounded")
	}
}

const originalPath = "src/main/java/org/apache/aurora/scheduler/resources/AcceptedOffer.java"
const currentPath = "src/main/java/org/apache/aurora/scheduler/mesos/AcceptedOffer.java"

func TestInventoryRelocationsAndGitBlobIdentity(t *testing.T) {
	renames := map[string]map[string]any{originalPath: {"originalPath": originalPath, "currentPath": currentPath, "reason": "test relocation"}}
	for _, state := range []string{"target-missing", "relocated", "ambiguous", "original-present"} {
		t.Run(state, func(t *testing.T) {
			dir := t.TempDir()
			if state == "relocated" || state == "ambiguous" {
				write(t, filepath.Join(dir, currentPath), "")
			}
			if state == "ambiguous" || state == "original-present" {
				write(t, filepath.Join(dir, originalPath), "")
			}
			tree := []byte("100644 blob e69de29bb2d1d6434b8b29ae775ad8c2e48c5391\t" + originalPath + "\x00")
			result, err := inspectInventory(dir, "test-commit", tree, renames)
			if err != nil {
				t.Fatal(err)
			}
			item := result.Components["scheduler_java"].Files[0]
			if item["relocation"].(map[string]any)["status"] != state || item["currentPath"] != currentPath {
				t.Fatal(item)
			}
			if (len(result.Missing) == 1) != (state == "target-missing") || (len(result.Ambiguous) == 1) != (state == "ambiguous") || len(result.Modified) != 0 {
				t.Fatal(result)
			}
			if _, ok := renames[originalPath]["status"]; ok {
				t.Fatal("mutated rename input")
			}
		})
	}
}

func TestInventorySymlinksExclusionsAndChanges(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, originalPath), "changed")
	link := "src/main/java/Link.java"
	if err := os.Symlink("missing-target", filepath.Join(dir, link)); err != nil {
		t.Fatal(err)
	}
	tree := []byte("100644 blob 0000000000000000000000000000000000000000\t" + originalPath + "\x00" + "120000 blob 0000000000000000000000000000000000000000\t" + link + "\x00" + "100644 blob zero\tREADME.md\x00" + "160000 commit zero\tsrc/main/java/submodule\x00")
	result, err := inspectInventory(dir, "test", tree, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Missing) != 0 || len(result.Modified) != 2 || result.Components["scheduler_java"].FileCount != 2 {
		t.Fatal(result)
	}
	items := result.Components["scheduler_java"].Files
	if items[0]["currentIsSymlink"] != false || items[1]["currentIsSymlink"] != true {
		t.Fatal(items)
	}
	// Git blob identity of a symlink hashes its link text, not its missing target.
	write(t, filepath.Join(dir, link+".copy"), "missing-target")
	copyTree := []byte("100644 blob zero\t" + link + ".copy\x00")
	copyResult, err := inspectInventory(dir, "test", copyTree, nil)
	if err != nil || copyResult.Components["scheduler_java"].Files[0]["currentGitBlob"] != items[1]["currentGitBlob"] {
		t.Fatal(copyResult, err)
	}
}

func TestRenameMapRejectsMalformedPathsAndDuplicates(t *testing.T) {
	for _, data := range []string{`{}`, `{"renames":{}}`, `{"renames":[{}]}`, `{"renames":[{"originalPath":"../a","currentPath":"b"}]}`, `{"renames":[{"originalPath":"a","currentPath":"/b"}]}`, `{"renames":[{"originalPath":"a","currentPath":"b"},{"originalPath":"a","currentPath":"c"}]}`, `{"renames":[{"originalPath":"a","currentPath":"b"},{"originalPath":"c","currentPath":"b"}]}`} {
		_, err := loadRenames(write(t, filepath.Join(t.TempDir(), "renames.json"), data))
		expectError(t, err)
	}
}

func TestCheckstyleConfigurationAndReportContracts(t *testing.T) {
	source, err := os.ReadFile("../../../config/checkstyle/checkstyle.xml")
	if err != nil {
		t.Fatal(err)
	}
	for _, historical := range []bool{true, false} {
		data, err := checkstyleConfiguration(source, historical)
		if err != nil {
			t.Fatal(err)
		}
		var root xmlElement
		if err := xml.Unmarshal(data, &root); err != nil {
			t.Fatal(err)
		}
		walker, err := findModule(root, "TreeWalker")
		if err != nil {
			t.Fatal(err)
		}
		javadoc, _ := findModule(walker, "JavadocVariable")
		if historical && (len(javadoc.Children) != 1 || javadoc.Children[0].attribute("name") != "excludeScope" || javadoc.Children[0].attribute("value") != "private") {
			t.Fatal(javadoc)
		}
		imports, _ := findModule(walker, "ImportOrder")
		static := false
		for _, property := range imports.Children {
			if property.attribute("name") == "staticGroups" {
				static = true
			}
		}
		if static == historical {
			t.Fatal("static grouping migration differs")
		}
		_, filterErr := findModule(root, "SuppressionSingleFilter")
		if (filterErr == nil) == historical {
			t.Fatal("suppression migration differs")
		}
	}
	found, err := checkstyleViolations([]byte(`<checkstyle><file name="/tmp/Good.java"/><file name="/tmp/Bad.java"><error/><error/></file></checkstyle>`))
	if err != nil || !reflect.DeepEqual(found, map[string]int{"Good": 0, "Bad": 2}) {
		t.Fatal(found, err)
	}
	_, err = checkstyleViolations([]byte(`<checkstyle><file name="A.java"/><file name="A.java"/></checkstyle>`))
	expectError(t, err)
	var fixtures map[string]checkstyleFixture
	if err := json.Unmarshal(checkstyleFixtureJSON, &fixtures); err != nil {
		t.Fatal(err)
	}
	modern := 0
	for _, fixture := range fixtures {
		if fixture.ModernOnly {
			modern++
		}
	}
	if len(fixtures) != 21 || modern != 3 {
		t.Fatal(len(fixtures), modern)
	}
}
