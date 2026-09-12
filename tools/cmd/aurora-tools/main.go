// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"aurora.local/tools/internal/bootstrap"
	"aurora.local/tools/internal/client"
	"aurora.local/tools/internal/commands"
	"aurora.local/tools/internal/entities"
	"aurora.local/tools/internal/lab"
	"aurora.local/tools/internal/validation"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(commands.ExitCode(dispatch(ctx, os.Args[1:])))
}
func dispatch(ctx context.Context, args []string) error {
	if original := os.Getenv("AURORA_TOOLS_CWD"); original != "" {
		checked, err := bootstrap.CheckPath(original)
		if err != nil {
			return err
		}
		if err = os.Chdir(checked); err != nil {
			return err
		}
	}
	if len(args) == 0 || args[0] == "--help" || args[0] == "help" {
		fmt.Println("usage: bootstrap-go <gradle|ui|thrift|stage-java|agents|check-tools|build-client|client|entities|inplace-cluster|inplace-check|licenses|verify-distribution|source-inventory|checkstyle-migration> [arguments...]")
		return nil
	}
	root := os.Getenv("AURORA_TOOLS_ROOT")
	if root == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		root = cwd
		for {
			if _, err = os.Stat(filepath.Join(root, "tools/go.mod")); err == nil {
				break
			}
			parent := filepath.Dir(root)
			if parent == root {
				return errors.New("cannot find Aurora tools root")
			}
			root = parent
		}
	}
	var err error
	root, err = bootstrap.CheckPath(root)
	if err != nil {
		return err
	}
	if digest := os.Getenv("AURORA_BOOTSTRAP_GO_ARCHIVE_SHA256"); digest != "" {
		pins, err := bootstrap.LoadManifest(filepath.Join(root, "tools/go-tools.json"))
		if err != nil {
			return err
		}
		if pins.Tools["go"].SHA256 != digest {
			return errors.New("shell bootstrap and Go JSON pin differ")
		}
	}
	switch args[0] {
	case "stage-java":
		return commands.StageJava(ctx, root, args[1:])
	case "check-tools", "build-client":
		return commands.DeveloperGo(ctx, root, args[0], args[1:])
	case "client":
		return client.Run(ctx, args[1:], os.Stdin, os.Stdout, os.Stderr)
	case "inplace-cluster":
		return lab.Cluster(ctx, root, args[1:])
	case "inplace-check":
		return lab.Check(ctx, root, args[1:])
	case "licenses", "verify-distribution", "source-inventory", "checkstyle-migration":
		return validation.Run(ctx, root, args[0], args[1:])
	case "entities":
		if len(args) != 4 {
			return errors.New("usage: entities <schema> <java-out> <resources-out>")
		}
		return entities.Generate(args[1], args[2], args[3])
	case "gradle":
		return commands.Gradle(ctx, root, args[1:])
	case "ui":
		return commands.UI(ctx, root, args[1:])
	case "thrift":
		return commands.Thrift(ctx, root, args[1:])
	case "agents":
		return commands.Agents(ctx, root, args[1:])
	default:
		return fmt.Errorf("unknown tools command %q", args[0])
	}
}
