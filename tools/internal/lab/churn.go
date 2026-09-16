// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lab

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

const churnBatches = 130

func spreadJob(name string, service bool, duration int, cpu float64) Object {
	config := job(name, service, duration, "")
	taskConfig := get(config, 6).(map[string]any)
	taskConfig["32"] = field("set", resources(cpu))
	hostLimit := Object{"1": field("str", "host"), "2": field("rec", Object{"2": field("rec", Object{"1": field("i32", 1)})})}
	taskConfig["20"] = field("set", []any{"rec", 1, hostLimit})
	return config
}

// Health requires owned, running containers and live keeper-managed daemons.
// Completed batches additionally prove agent delivery, execution and observation
// reconciliation; the scheduler endpoint alone cannot establish agent health.
func (c *check) churnHealth() (map[string]daemonState, error) {
	result := map[string]daemonState{}
	for _, role := range roles {
		item, err := Inspect(c.ctx, c.runner, &c.data, role, false)
		if err != nil {
			return nil, err
		}
		if !item.State.Running {
			return nil, fmt.Errorf("%s container stopped", role)
		}
		var state daemonState
		if err := readJSON(filepath.Join(c.root, role, "control/daemon.json"), &state); err != nil {
			return nil, err
		}
		if state.PID <= 0 || state.Generation <= 0 || state.Paused {
			return nil, fmt.Errorf("%s daemon unavailable", role)
		}
		result[role] = state
	}
	request, err := http.NewRequestWithContext(c.ctx, http.MethodGet, strings.TrimSuffix(c.url, "/api")+"/health", nil)
	if err != nil {
		return nil, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1025))
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK || strings.TrimSpace(string(body)) != "OK" {
		return nil, fmt.Errorf("scheduler health HTTP%d: %q", response.StatusCode, body)
	}
	return result, nil
}

// Record backup publication without opening a live SQLite database. This is
// evidence for separate backup/restore qualification, not a restore check.
func (c *check) churnBackups() ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(c.root, "scheduler/state/backups"))
	if errors.Is(err, os.ErrNotExist) {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}
	names := []string{}
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasPrefix(entry.Name(), "backup-") && strings.HasSuffix(entry.Name(), ".db") {
			names = append(names, entry.Name())
		}
	}
	return names, nil
}

func (c *check) churn() (err error) {
	name := c.prefix + "-churn-service"
	const duration = 3603
	pending := ""
	// Failed batches and the service are cleaned up with the caller's context,
	// after the phase deadline has been removed. Successful batches are terminal.
	defer func() {
		if pending != "" {
			err = errors.Join(err, c.kill(pending))
		}
	}()
	if _, err = c.rpc("createJob", Object{"1": field("rec", spreadJob(name, true, duration, 0.1))}); err != nil {
		return err
	}
	return c.withService(name, func() error {
		originalContext := c.ctx
		phaseContext, cancel := context.WithTimeout(originalContext, 30*time.Minute)
		c.ctx = phaseContext
		defer func() { c.ctx = originalContext; cancel() }()
		original, err := c.wait(name, func(xs []Task) bool { return len(xs) == 2 && all(xs, 2) }, 90*time.Second)
		if err != nil {
			return err
		}
		if hosts(original) != 2 {
			return errors.New("churn service must use both agents")
		}
		physical, err := c.physical(duration)
		if err != nil {
			return err
		}
		daemons, err := c.churnHealth()
		if err != nil {
			return err
		}
		backupsBefore, err := c.churnBackups()
		if err != nil {
			return err
		}
		started := c.now()
		completed := []Task{}
		ids := map[string]bool{}
		perHost := map[string]int{"agent-a": 0, "agent-b": 0}
		for n := 0; n < churnBatches; n++ {
			pending = fmt.Sprintf("%s-churn-batch-%d", c.prefix, n)
			// Keep both instances alive long enough for placement, and require host
			// diversity independently of the scheduler constraint in the receipt.
			if _, err = c.rpc("createJob", Object{"1": field("rec", spreadJob(pending, false, 3, 0.6))}); err != nil {
				return err
			}
			done, err := c.wait(pending, func(xs []Task) bool { return len(xs) == 2 && all(xs, 3) }, 90*time.Second)
			if err != nil {
				return err
			}
			seenHosts := map[string]bool{}
			for _, task := range done {
				host, ok := task.Host.(string)
				if !ok || (host != "agent-a" && host != "agent-b") || seenHosts[host] || ids[task.ID] {
					return errors.New("churn requires distinct FINISHED task IDs and one instance per enrolled host")
				}
				seenHosts[host], ids[task.ID] = true, true
				perHost[host]++
			}
			completed = append(completed, done...)
			pending = ""
			current, err := c.tasks(name)
			if err != nil {
				return err
			}
			if !sameTasks(current, original) {
				return errors.New("churn service identity/state changed")
			}
			currentPhysical, err := c.physical(duration)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(currentPhysical, physical) {
				return errors.New("churn workload PID changed")
			}
		}
		currentDaemons, err := c.churnHealth()
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(currentDaemons, daemons) {
			return errors.New("churn daemon restarted or changed")
		}
		if len(ids) != 2*churnBatches || perHost["agent-a"] != churnBatches || perHost["agent-b"] != churnBatches {
			return errors.New("incomplete per-agent history churn")
		}
		backupsAfter, err := c.churnBackups()
		if err != nil {
			return err
		}
		c.add(Object{"backupFilesBefore": backupsBefore, "backupFilesAfter": backupsAfter, "backupCountBefore": len(backupsBefore), "backupCountAfter": len(backupsAfter), "name": "retained-history-churn", "batches": churnBatches, "finishedTasks": completed, "finishedPerHost": perHost, "serviceTasks": original, "physical": physical, "daemons": daemons, "samples": churnBatches, "elapsedSeconds": c.now().Sub(started).Seconds()})
		return nil
	})
}
