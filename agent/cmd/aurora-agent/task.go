// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"aurora.local/agent/task"
)

// Task execution is an ordinary immutable Run beneath the surviving supervisor.
// Its default private journal is relative to the attempt's owned working directory.
func runTask(action string, args []string) error {
	if action == "task-child" {
		if len(args) != 0 {
			return errors.New("unexpected helper arguments")
		}
		return task.ChildHelper()
	}
	if action == "convert-task" {
		f := flag.NewFlagSet(action, flag.ContinueOnError)
		document := f.String("document", "", "trusted resolved JSON export; never evaluates Python")
		maxRuns := f.Int("max-runs", 0, "required finite task-wide run limit")
		if e := f.Parse(args); e != nil {
			return e
		}
		if f.NArg() != 0 || *document == "" {
			return errors.New("convert-task requires --document and --max-runs")
		}
		data, e := read(*document)
		if e != nil {
			return e
		}
		manifest, findings, e := task.ConvertResolvedJSON(data, *maxRuns)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(struct {
			Manifest task.Manifest  `json:"manifest"`
			Findings []task.Finding `json:"findings"`
		}{manifest, findings})
	}
	f := flag.NewFlagSet(action, flag.ContinueOnError)
	manifest := f.String("manifest-json", "", "canonical resolved task JSON, bound by the outer Run hash")
	stateDir := f.String("state-dir", "", "absolute private state directory; defaults to task-state under attempt cwd")
	if e := f.Parse(args); e != nil {
		return e
	}
	if f.NArg() != 0 || *manifest == "" {
		return errors.New("execute-task requires --manifest-json and no positional arguments")
	}
	m, _, e := task.Decode([]byte(*manifest))
	if e != nil {
		return e
	}
	if *stateDir == "" {
		cwd, e := os.Getwd()
		if e != nil {
			return e
		}
		*stateDir = filepath.Join(cwd, "task-state")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	result, runErr := task.Execute(ctx, m, task.Options{StateDir: *stateDir})
	writeErr := json.NewEncoder(os.Stdout).Encode(result)
	if runErr != nil || writeErr != nil {
		return errors.Join(runErr, writeErr)
	}
	if result.PrimaryResult != "succeeded" || result.Cleanup != "complete" {
		return fmt.Errorf("task primary result %s; cleanup %s", result.PrimaryResult, result.Cleanup)
	}
	return nil
}
