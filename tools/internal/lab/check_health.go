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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func healthTask(name string, delay, closeAfter, logBytes int) Object {
	config := task(name, true, 0)
	// Both fit one CPU reservation; the shared health port forces two hosts.
	config["32"] = field("set", resources(0.2))
	process := Object{"version": "aurora-process-v1", "argv": []string{
		"/opt/bin/cluster-helper", "health", "--port", "18080",
		"--delay-ms", strconv.Itoa(delay), "--close-after-ms", strconv.Itoa(closeAfter),
		"--log-bytes", strconv.Itoa(logBytes)}, "env": Object{}, "graceMillis": 1000,
		"health": Object{"kind": "tcp", "port": 18080, "network": "agent-container",
			"intervalMillis": 100, "timeoutMillis": 100, "startupTimeoutMillis": 2000,
			"failureThreshold": 3}}
	encoded, _ := json.Marshal(process)
	config["25"] = field("rec", Object{"1": field("str", "go-process"), "2": field("str", string(encoded))})
	return config
}

func (c *check) createHealth(name string, config Object) error {
	value := job(name, true, 0, "")
	value["6"] = field("rec", config)
	_, err := c.rpc("createJob", Object{"1": field("rec", value)})
	return err
}

func healthFailure(values []any, reason string, original map[string]bool, mustHaveRun bool) (string, error) {
	for _, value := range values {
		id := text(get(get(value, 1), 1))
		if number(get(value, 2)) != 4 || original != nil && !original[id] {
			continue
		}
		events, err := collection(get(value, 4), "rec")
		if err != nil {
			return "", err
		}
		ran, diagnosed := false, false
		for _, event := range events {
			ran = ran || number(get(event, 2)) == 2
			diagnosed = diagnosed || number(get(event, 2)) == 4 && strings.Contains(text(get(event, 3)), reason)
		}
		if diagnosed {
			if ran != mustHaveRun {
				return "", fmt.Errorf("health failure %s has unexpected RUNNING history: %v", id, ran)
			}
			return id, nil
		}
	}
	return "", fmt.Errorf("missing FAILED task event for %s", reason)
}

func (c *check) health() error {
	name := c.prefix + "-health"
	baseline := healthTask(name, 0, 0, 128)
	if err := c.createHealth(name, baseline); err != nil {
		return err
	}
	if err := c.withService(name, func() error {
		initial, err := c.wait(name, func(tasks []Task) bool { return len(tasks) == 2 && all(tasks, 2) }, 90*time.Second)
		if err != nil {
			return err
		}
		if hosts(initial) != 2 {
			return errors.New("fixed TCP health port failed to spread 200m services across agents")
		}
		c.add(Object{"name": "health-readiness-two-agents", "tasks": initial})
		update, err := c.startUpdate(healthTask(name, 30000, 0, 128), "health startup failure must roll back")
		if err != nil {
			return err
		}
		if err = c.waitUpdate(update, 5, 120*time.Second); err != nil {
			return err
		}
		restored, err := c.wait(name, func(tasks []Task) bool { return len(running(tasks)) == 2 }, 90*time.Second)
		if err != nil {
			return err
		}
		scheduled, err := c.scheduled(name)
		if err != nil {
			return err
		}
		failed, err := healthFailure(scheduled, "health-startup-timeout", nil, false)
		if err != nil {
			return err
		}
		for _, value := range scheduled {
			if number(get(value, 2)) == 2 && text(get(get(get(get(value, 1), 4), 25), 2)) != text(get(get(baseline, 25), 2)) {
				return errors.New("health rollback did not restore exact original executor configuration")
			}
		}
		c.add(Object{"name": "health-startup-rollback", "failedTask": failed, "tasks": restored, "updateStatus": 5, "executorRestored": true})
		return nil
	}); err != nil {
		return err
	}
	loss := c.prefix + "-health-loss"
	if err := c.createHealth(loss, healthTask(loss, 0, 8000, 128)); err != nil {
		return err
	}
	return c.withService(loss, func() error {
		initial, err := c.wait(loss, func(tasks []Task) bool { return len(running(tasks)) == 2 }, 90*time.Second)
		if err != nil {
			return err
		}
		originals := map[string]bool{}
		for _, task := range running(initial) {
			originals[task.ID] = true
		}
		terminal, err := c.wait(loss, func(tasks []Task) bool {
			for _, task := range tasks {
				if originals[task.ID] && task.Status == 4 {
					return true
				}
			}
			return false
		}, 90*time.Second)
		if err != nil {
			return err
		}
		scheduled, err := c.scheduled(loss)
		if err != nil {
			return err
		}
		failed, err := healthFailure(scheduled, "health-check-failed", originals, true)
		if err != nil {
			return err
		}
		c.add(Object{"name": "health-listener-loss", "failedTask": failed, "originalTasks": initial, "tasks": terminal})
		return nil
	})
}
