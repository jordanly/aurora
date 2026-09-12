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
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

var defaultExtensions = []string{".gradle", ".groovy", ".java", ".js", ".proto", ".py", ".sh", ".thrift"}
var skippedDirectories = map[string]bool{
	".cache": true, ".git": true, ".gradle": true, ".idea": true, ".pytest_cache": true,
	"__pycache__": true, "build": true, "dist": true, "node_modules": true,
}

func normalized(lines []string) []string {
	result := []string{}
	for _, line := range lines {
		if text := strings.TrimSpace(line); text != "" {
			result = append(result, text)
		}
	}
	return result
}

func commentText(content string) []string {
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	lines[0] = strings.TrimPrefix(lines[0], "\ufeff")
	index := 0
	if strings.HasPrefix(lines[0], "#!") {
		index++
	}
	if index >= len(lines) {
		return nil
	}
	first := strings.TrimLeftFunc(lines[index], unicode.IsSpace)
	var text []string
	if strings.HasPrefix(first, "/*") {
		for _, line := range lines[index:] {
			stripped := strings.TrimSpace(line)
			if strings.HasPrefix(stripped, "/*") {
				stripped = stripped[2:]
			} else if strings.HasSuffix(stripped, "*/") {
				text = append(text, strings.TrimLeft(stripped[:len(stripped)-2], " *"))
				return text
			}
			text = append(text, strings.TrimLeft(stripped, " *"))
		}
		return nil
	}
	prefix := ""
	if strings.HasPrefix(first, "#") {
		prefix = "#"
	} else if strings.HasPrefix(first, "//") {
		prefix = "//"
	}
	if prefix == "" {
		return nil
	}
	for _, line := range lines[index:] {
		stripped := strings.TrimLeftFunc(line, unicode.IsSpace)
		if !strings.HasPrefix(stripped, prefix) {
			break
		}
		text = append(text, strings.TrimLeftFunc(stripped[len(prefix):], unicode.IsSpace))
	}
	return text
}

func hasLicense(path string, expected []string) bool {
	data, err := os.ReadFile(path)
	return err == nil && utf8.Valid(data) && reflect.DeepEqual(normalized(commentText(string(data))), expected)
}

func sourceFiles(roots, extensions []string) ([]string, error) {
	allowed := map[string]bool{}
	for _, extension := range extensions {
		allowed[extension] = true
	}
	roots = append([]string(nil), roots...)
	sort.Strings(roots)
	files := []string{}
	for _, root := range roots {
		info, err := os.Lstat(root)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		if info.Mode().IsRegular() {
			if allowed[filepath.Ext(root)] {
				files = append(files, root)
			}
			continue
		}
		// os.walk visits a directory's files before recursing into its sorted children.
		var walk func(string) error
		walk = func(directory string) error {
			entries, err := os.ReadDir(directory)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if entry.Type().IsRegular() && allowed[filepath.Ext(entry.Name())] {
					files = append(files, filepath.Join(directory, entry.Name()))
				}
			}
			for _, entry := range entries {
				if entry.IsDir() && !skippedDirectories[entry.Name()] {
					if err := walk(filepath.Join(directory, entry.Name())); err != nil {
						return err
					}
				}
			}
			return nil
		}
		if err := walk(root); err != nil {
			return nil, err
		}
	}
	return files, nil
}

func licenses(root string, args []string) error {
	extensions := append([]string(nil), defaultExtensions...)
	filtered := []string{}
	for i := 0; i < len(args); i++ {
		if args[i] != "--extensions" {
			filtered = append(filtered, args[i])
			continue
		}
		extensions = nil
		for i+1 < len(args) && !strings.HasPrefix(args[i+1], "--") {
			i++
			extensions = append(extensions, args[i])
		}
		if len(extensions) == 0 {
			return usage("--extensions requires at least one extension")
		}
	}
	flags, roots, err := options(filtered, "--header")
	if err != nil {
		return err
	}
	if len(roots) == 0 {
		return usage("licenses requires source roots")
	}
	for _, path := range roots {
		info, err := os.Lstat(path)
		if err != nil {
			return usage("source root does not exist: " + path)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return usage("source roots must not be symlinks")
		}
	}
	header := flags["--header"]
	if header == "" {
		header = filepath.Join(root, "config/checkstyle/apache.header")
	}
	data, err := os.ReadFile(header)
	if err != nil {
		return err
	}
	if !utf8.Valid(data) {
		return usage("canonical license header is not UTF-8")
	}
	expected := normalized(strings.Split(string(data), "\n"))
	if len(expected) == 0 {
		return usage("canonical license header is empty")
	}
	files, err := sourceFiles(roots, extensions)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return usage("no matching source files found")
	}
	bad := 0
	for _, path := range files {
		if !hasLicense(path, expected) {
			fmt.Fprintln(os.Stderr, path)
			bad++
		}
	}
	if bad > 0 {
		return failure{1, fmt.Sprintf("license header check failed: %d file(s)", bad)}
	}
	fmt.Printf("Verified license headers in %d source files\n", len(files))
	return nil
}
