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
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

const baselineCommit = "11ebaeeb071cb182c388a40755e84f60dda32260"

var components = []struct{ name, root string }{
	{"scheduler_java", "src/main/java/"}, {"scheduler_java_tests", "src/test/java/"},
	{"benchmarks", "src/jmh/"}, {"commons_java", "commons/src/main/java/"},
	{"commons_java_tests", "commons/src/test/java/"}, {"api_schemas", "api/src/"},
	{"runtime_resources", "src/main/resources/"}, {"test_resources", "src/test/resources/"},
	{"python_runtime_and_tools", "src/main/python/"}, {"python_tests", "src/test/python/"},
	{"ui", "ui/"}, {"build_plugins", "buildSrc/"},
}

func loadRenames(path string) (map[string]map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		return nil, err
	}
	entries, ok := document["renames"].([]any)
	if !ok {
		return nil, fmt.Errorf("rename map must contain a renames list")
	}
	result := map[string]map[string]any{}
	targets := map[string]bool{}
	for _, entry := range entries {
		rename, ok := entry.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("rename entries must be objects")
		}
		original, okOriginal := rename["originalPath"].(string)
		current, okCurrent := rename["currentPath"].(string)
		if !okOriginal || !okCurrent {
			return nil, fmt.Errorf("rename entries require originalPath and currentPath")
		}
		if result[original] != nil || targets[current] {
			return nil, fmt.Errorf("rename map contains duplicate paths")
		}
		for _, path := range []string{original, current} {
			if filepath.IsAbs(path) {
				return nil, fmt.Errorf("rename paths must be relative")
			}
			for _, part := range strings.Split(filepath.ToSlash(path), "/") {
				if part == ".." {
					return nil, fmt.Errorf("rename paths must be relative")
				}
			}
		}
		result[original] = rename
		targets[current] = true
	}
	return result, nil
}

type componentInventory struct {
	Root            string           `json:"root"`
	Files           []map[string]any `json:"files"`
	FileCount       int              `json:"fileCount"`
	JavaSourceCount int              `json:"javaSourceCount"`
}
type inventoryReceipt struct {
	BaselineCommit string                         `json:"baselineCommit"`
	Scope          string                         `json:"scope"`
	Components     map[string]*componentInventory `json:"components"`
	Missing        []string                       `json:"missing"`
	Ambiguous      []string                       `json:"ambiguous"`
	Modified       []string                       `json:"modified"`
}

func present(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && (info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0)
}

func inspectInventory(root, commit string, tree []byte, renames map[string]map[string]any) (*inventoryReceipt, error) {
	result := &inventoryReceipt{BaselineCommit: commit,
		Scope:      "Original tracked source, schema, resource, UI and build-plugin inputs; counts are files, not executed tests.",
		Components: map[string]*componentInventory{}, Missing: []string{}, Ambiguous: []string{}, Modified: []string{}}
	for _, component := range components {
		result.Components[component.name] = &componentInventory{Root: component.root, Files: []map[string]any{}}
	}
	for _, entry := range bytes.Split(tree, []byte{0}) {
		if len(entry) == 0 {
			continue
		}
		metadata, rawPath, found := bytes.Cut(entry, []byte{'\t'})
		fields := strings.Fields(string(metadata))
		if !found || len(fields) != 3 || !utf8.Valid(rawPath) {
			return nil, fmt.Errorf("invalid Git tree entry")
		}
		path := string(rawPath)
		var group *componentInventory
		for _, component := range components {
			if strings.HasPrefix(path, component.root) {
				group = result.Components[component.name]
				break
			}
		}
		if group == nil || fields[1] != "blob" {
			continue
		}
		item := map[string]any{"path": path, "baselineGitBlob": fields[2]}
		current := filepath.Join(root, filepath.FromSlash(path))
		if rename := renames[path]; rename != nil {
			relocation := map[string]any{}
			for key, value := range rename {
				relocation[key] = value
			}
			mapped := filepath.Join(root, filepath.FromSlash(rename["currentPath"].(string)))
			item["currentPath"] = rename["currentPath"]
			item["relocation"] = relocation
			switch {
			case present(current) && present(mapped):
				item["ambiguous"] = true
				relocation["status"] = "ambiguous"
				result.Ambiguous = append(result.Ambiguous, path)
			case !present(current) && present(mapped):
				relocation["status"] = "relocated"
				current = mapped
			case present(current):
				relocation["status"] = "original-present"
			default:
				relocation["status"] = "target-missing"
			}
		}
		if present(current) {
			info, err := os.Lstat(current)
			if err != nil {
				return nil, err
			}
			symlink := info.Mode()&os.ModeSymlink != 0
			var data []byte
			if symlink {
				target, err := os.Readlink(current)
				if err != nil {
					return nil, err
				}
				data = []byte(target)
			} else {
				data, err = os.ReadFile(current)
				if err != nil {
					return nil, err
				}
			}
			hash := sha1.New()
			fmt.Fprintf(hash, "blob %d\x00", len(data))
			hash.Write(data)
			blob := hex.EncodeToString(hash.Sum(nil))
			if blob != fields[2] || (fields[0] == "120000") != symlink {
				item["currentGitBlob"] = blob
				item["currentIsSymlink"] = symlink
				result.Modified = append(result.Modified, path)
			}
		} else {
			item["missing"] = true
			result.Missing = append(result.Missing, path)
		}
		group.Files = append(group.Files, item)
	}
	for _, group := range result.Components {
		group.FileCount = len(group.Files)
		for _, item := range group.Files {
			if strings.HasSuffix(item["path"].(string), ".java") {
				group.JavaSourceCount++
			}
		}
	}
	return result, nil
}

func inventory(ctx context.Context, root, baseline string, renames map[string]map[string]any) (*inventoryReceipt, error) {
	run := func(args ...string) ([]byte, error) {
		command := exec.CommandContext(ctx, "git", args...)
		command.Dir = root
		return command.Output()
	}
	commit, err := run("rev-parse", "--verify", "--end-of-options", baseline+"^{commit}")
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(string(commit))
	tree, err := run("ls-tree", "-rz", name)
	if err != nil {
		return nil, err
	}
	return inspectInventory(root, name, tree, renames)
}

func sourceInventory(ctx context.Context, root string, args []string) error {
	flags, positional, err := options(args, "--baseline", "--output")
	if err != nil {
		return err
	}
	if len(positional) != 0 || flags["--output"] == "" {
		return usage("source-inventory requires --output, with optional --baseline")
	}
	baseline := flags["--baseline"]
	if baseline == "" {
		baseline = baselineCommit
	}
	renames, err := loadRenames(filepath.Join(root, "build-support/java/source-inventory-renames.json"))
	if err != nil {
		return err
	}
	result, err := inventory(ctx, root, baseline, renames)
	if err != nil {
		return err
	}
	if err := writeJSON(flags["--output"], result); err != nil {
		return err
	}
	counts := map[string]int{}
	for name, group := range result.Components {
		counts[name] = group.FileCount
	}
	summary, err := json.Marshal(map[string]any{"output": flags["--output"], "missing": result.Missing, "ambiguous": result.Ambiguous, "modified": result.Modified, "counts": counts})
	if err != nil {
		return err
	}
	fmt.Println(string(summary))
	if len(result.Missing) > 0 || len(result.Ambiguous) > 0 {
		return failure{1, "source inventory contains missing or ambiguous original inputs"}
	}
	return nil
}
