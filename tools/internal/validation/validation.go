// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package validation implements the build's source and distribution contracts.
package validation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type failure struct {
	code    int
	message string
}

func (f failure) Error() string  { return f.message }
func (f failure) ExitCode() int  { return f.code }
func usage(message string) error { return failure{2, message} }

func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}

// options accepts flags before or after positionals, as the former argparse CLIs did.
func options(args []string, names ...string) (map[string]string, []string, error) {
	allowed := map[string]bool{}
	for _, name := range names {
		allowed[name] = true
	}
	values := map[string]string{}
	var positional []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(arg, "--") {
			positional = append(positional, arg)
			continue
		}
		name, value, equal := strings.Cut(arg, "=")
		if !allowed[name] {
			return nil, nil, usage("unknown option: " + name)
		}
		if !equal {
			i++
			if i >= len(args) {
				return nil, nil, usage("missing value for " + name)
			}
			value = args[i]
		}
		values[name] = value
	}
	return values, positional, nil
}

// Run dispatches one validation command; usage errors retain exit status 2.
func Run(ctx context.Context, root, mode string, args []string) error {
	switch mode {
	case "licenses":
		return licenses(root, args)
	case "verify-distribution":
		return distribution(ctx, args)
	case "source-inventory":
		return sourceInventory(ctx, root, args)
	case "checkstyle-migration":
		return checkstyleMigration(ctx, root, args)
	default:
		return usage(fmt.Sprintf("unknown validation command %q", mode))
	}
}
