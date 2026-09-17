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
	"path/filepath"
	"reflect"
	"time"
)

func (c *check) withService(name string, action func() error) (err error) {
	defer func() { err = errors.Join(err, c.kill(name)) }()
	return action()
}
func (c *check) smoke() error {
	batch := c.prefix + "-batch"
	if _, err := c.rpc("createJob", Object{"1": field("rec", job(batch, false, 0, ""))}); err != nil {
		return err
	}
	done, err := c.wait(batch, func(xs []Task) bool { return len(xs) == 2 && all(xs, 3) }, 90*time.Second)
	if err != nil {
		return err
	}
	c.add(Object{"name": "batch-finished", "tasks": done})
	service := c.prefix + "-service"
	if _, err = c.rpc("createJob", Object{"1": field("rec", job(service, true, 300, ""))}); err != nil {
		return err
	}
	err = c.withService(service, func() error {
		live, err := c.wait(service, func(xs []Task) bool { return len(xs) == 2 && all(xs, 2) }, 90*time.Second)
		if err != nil {
			return err
		}
		if hosts(live) != 2 {
			return errors.New("600m tasks did not use both agents")
		}
		physical, err := c.physical(300)
		if err != nil {
			return err
		}
		c.add(Object{"name": "two-agent-service", "tasks": live, "physical": physical})
		updateKey, err := c.startUpdate(task(service, true, 301), "isolated acceptance update")
		if err != nil {
			return err
		}
		old := map[string]bool{}
		for _, x := range live {
			old[x.ID] = true
		}
		updated, err := c.wait(service, func(xs []Task) bool {
			count := 0
			for _, x := range xs {
				if x.Status == 2 && !old[x.ID] {
					count++
				}
			}
			return count == 2
		}, 90*time.Second)
		if err != nil {
			return err
		}
		if err = c.waitUpdate(updateKey, 4, 90*time.Second); err != nil {
			return err
		}
		c.add(Object{"name": "rolling-update", "tasks": updated, "updateStatus": 4})
		return nil
	})
	if err != nil {
		return err
	}
	c.add(Object{"name": "service-killed"})
	if err = c.quota(); err != nil {
		return err
	}
	if _, err = c.rpc("getQuota", Object{"1": field("str", "fixtures")}); err != nil {
		return err
	}
	cron := c.prefix + "-cron"
	if _, err = c.rpc("scheduleCronJob", Object{"1": field("rec", job(cron, false, 0, "0 0 1 1 *"))}); err != nil {
		return err
	}
	if _, err = c.rpc("descheduleCronJob", Object{"4": field("rec", key(cron))}); err != nil {
		return err
	}
	c.add(Object{"name": "quota-and-cron-api"})
	hostSet := Object{"1": field("set", []any{"str", 2, "agent-a", "agent-b"})}
	if _, err = c.rpc("drainHosts", Object{"1": field("rec", hostSet)}); err != nil {
		return err
	}
	if _, err = c.rpc("endMaintenance", Object{"1": field("rec", hostSet)}); err != nil {
		return err
	}
	c.add(Object{"name": "drain-empty-hosts-api"})
	return nil
}
func (c *check) ownedAction(role, action string) error {
	unlock, err := lock(c.root)
	if err != nil {
		return err
	}
	defer unlock()
	data, err := load(c.root)
	if err != nil {
		return err
	}
	if _, err = Inspect(c.ctx, c.runner, &data, role, false); err != nil {
		return err
	}
	if err = request(c.root, role, action, "owned request\n"); err != nil {
		return err
	}
	for role := range data.Containers {
		if _, err = Inspect(c.ctx, c.runner, &data, role, false); err != nil {
			return err
		}
	}
	return nil
}
func (c *check) recovery() error {
	name := c.prefix + "-recovery"
	duration := 3600
	if _, err := c.rpc("createJob", Object{"1": field("rec", job(name, true, duration, ""))}); err != nil {
		return err
	}
	return c.withService(name, func() error {
		original, err := c.wait(name, func(xs []Task) bool { return len(xs) == 2 && all(xs, 2) }, 90*time.Second)
		if err != nil {
			return err
		}
		if hosts(original) != 2 {
			return errors.New("recovery must use both agents")
		}
		physical, err := c.physical(duration)
		if err != nil {
			return err
		}
		for round := 0; round < c.rounds; round++ {
			for _, action := range []string{"restart-scheduler", "crash-scheduler", "crash-agent"} {
				node := "agent-a"
				if round%2 == 1 {
					node = "agent-b"
				}
				role := node
				keeperAction := "crash"
				if action != "crash-agent" {
					role = "scheduler"
				}
				if action == "restart-scheduler" {
					keeperAction = "restart"
				}
				keeper := filepath.Join(c.root, role, "control/daemon.json")
				var before daemonState
				if err = readJSON(keeper, &before); err != nil {
					return err
				}
				if err = c.ownedAction(role, keeperAction); err != nil {
					return err
				}
				after, err := waitDaemon(c.ctx, keeper, 90*time.Second, func(state daemonState) bool { return state.Generation > before.Generation && state.PID > 0 })
				if err != nil {
					return err
				}
				if err = c.pause(3 * time.Second); err != nil {
					return err
				}
				deadline := time.Now().Add(daemonRecoveryTimeout)
				var current []Task
				for {
					current, err = c.wait(name, func(xs []Task) bool { return len(xs) == 2 && all(xs, 2) }, 5*time.Second)
					if err == nil {
						break
					}
					if !time.Now().Before(deadline) {
						return err
					}
					if err = c.pause(time.Second); err != nil {
						return err
					}
				}
				if !sameTasks(original, current) {
					return fmt.Errorf("task IDs/hosts/status changed after %s", action)
				}
				currentPhysical, err := c.physical(duration)
				if err != nil {
					return err
				}
				if !reflect.DeepEqual(currentPhysical, physical) {
					return fmt.Errorf("physical workload PID changed after %s", action)
				}
				c.add(Object{"name": action, "round": round + 1, "node": role, "beforeGeneration": before.Generation, "afterGeneration": after.Generation, "tasks": current, "physical": physical})
			}
		}
		return nil
	})
}
func (c *check) policy() error {
	name := c.prefix + "-policy-service"
	baseline := task(name, true, 3602)
	if _, err := c.rpc("createJob", Object{"1": field("rec", job(name, true, 3602, ""))}); err != nil {
		return err
	}
	var hostSet Object
	err := c.withService(name, func() (err error) {
		defer func() {
			if hostSet != nil {
				_, cleanup := c.rpc("endMaintenance", Object{"1": field("rec", hostSet)})
				err = errors.Join(err, cleanup)
			}
		}()
		original, err := c.wait(name, func(xs []Task) bool { return len(xs) == 2 && all(xs, 2) }, 90*time.Second)
		if err != nil {
			return err
		}
		if hosts(original) != 2 {
			return errors.New("policy service must use both agents")
		}
		failing := task(name, true, 3602)
		executor := get(failing, 25).(map[string]any)
		var process Object
		if err = decodeJSON([]byte(text(get(executor, 2))), &process); err != nil {
			return err
		}
		process["argv"] = []string{"/bin/false"}
		encoded, _ := json.Marshal(process)
		executor["2"] = field("str", string(encoded))
		updateKey, err := c.startUpdate(failing, "intentional failing update for rollback")
		if err != nil {
			return err
		}
		if err = c.waitUpdate(updateKey, 5, 120*time.Second); err != nil {
			return err
		}
		restored, err := c.wait(name, func(xs []Task) bool { return len(running(xs)) == 2 }, 90*time.Second)
		if err != nil {
			return err
		}
		scheduled, err := c.scheduled(name)
		if err != nil {
			return err
		}
		var expected Object
		if err = decodeJSON([]byte(text(get(get(baseline, 25), 2))), &expected); err != nil {
			return err
		}
		count := 0
		failed := false
		for _, value := range scheduled {
			status := number(get(value, 2))
			if status == 4 {
				failed = true
			}
			if status == 2 {
				count++
				var actual Object
				if err = decodeJSON([]byte(text(get(get(get(get(value, 1), 4), 25), 2))), &actual); err != nil {
					return err
				}
				if !reflect.DeepEqual(expected, actual) {
					return errors.New("rollback did not restore exact original executor configuration")
				}
			}
		}
		if count != 2 || !failed {
			return errors.New("rollback did not restore two instances or observe a FAILED task")
		}
		c.add(Object{"name": "automatic-rollback", "updateStatus": 5, "tasks": restored, "executorRestored": true})
		target := running(restored)[0]
		hostSet = Object{"1": field("set", []any{"str", 1, target.Host})}
		if _, err = c.rpc("drainHosts", Object{"1": field("rec", hostSet)}); err != nil {
			return err
		}
		deadline := time.Now().Add(120 * time.Second)
		for {
			response, err := c.rpc("maintenanceStatus", Object{"1": field("rec", hostSet)})
			if err != nil {
				return err
			}
			statuses, err := collection(get(get(get(response, 3), 10), 1), "rec")
			if err != nil {
				return err
			}
			if len(statuses) == 1 && get(statuses[0], 1) == target.Host && number(get(statuses[0], 2)) == 4 {
				break
			}
			if !time.Now().Before(deadline) {
				return errors.New("occupied host did not become DRAINED")
			}
			if err = c.pause(time.Second); err != nil {
				return err
			}
		}
		removed := func(xs []Task) bool {
			for _, x := range xs {
				if x.ID == target.ID && x.Status == 2 {
					return false
				}
			}
			return true
		}
		drained, err := c.wait(name, func(xs []Task) bool {
			pending := false
			for _, x := range xs {
				if x.Status == 0 {
					pending = true
				}
			}
			return len(running(xs)) == 1 && pending && removed(xs)
		}, 90*time.Second)
		if err != nil {
			return err
		}
		if _, err = c.rpc("endMaintenance", Object{"1": field("rec", hostSet)}); err != nil {
			return err
		}
		hostSet = nil
		replaced, err := c.wait(name, func(xs []Task) bool { return len(running(xs)) == 2 && removed(xs) }, 90*time.Second)
		if err != nil {
			return err
		}
		c.add(Object{"name": "active-drain-replacement", "host": target.Host, "drainedTasks": drained, "replacedTasks": replaced})
		return nil
	})
	if err != nil {
		return err
	}
	cron := c.prefix + "-policy-cron"
	if _, err = c.rpc("scheduleCronJob", Object{"1": field("rec", job(cron, false, 0, "0 0 1 1 *"))}); err != nil {
		return err
	}
	return func() (err error) {
		defer func() {
			_, cleanup := c.rpc("descheduleCronJob", Object{"4": field("rec", key(cron))})
			err = errors.Join(err, cleanup)
		}()
		if _, err = c.rpc("startCronJob", Object{"4": field("rec", key(cron))}); err != nil {
			return err
		}
		finished, err := c.wait(cron, func(xs []Task) bool { return len(xs) == 2 && all(xs, 3) }, 90*time.Second)
		if err != nil {
			return err
		}
		c.add(Object{"name": "manual-cron-finished", "tasks": finished})
		return nil
	}()
}
func (c *check) soak() error {
	name := c.prefix + "-soak-service"
	duration := 3601
	config := job(name, true, duration, "")
	taskConfig := get(config, 6).(map[string]any)
	taskConfig["32"] = field("set", resources(0.1))
	hostLimit := Object{"1": field("str", "host"), "2": field("rec", Object{"2": field("rec", Object{"1": field("i32", 1)})})}
	taskConfig["20"] = field("set", []any{"rec", 1, hostLimit})
	if _, err := c.rpc("createJob", Object{"1": field("rec", config)}); err != nil {
		return err
	}
	return c.withService(name, func() error {
		original, err := c.wait(name, func(xs []Task) bool { return len(xs) == 2 && all(xs, 2) }, 90*time.Second)
		if err != nil {
			return err
		}
		if hosts(original) != 2 {
			return errors.New("soak service must use both agents")
		}
		physical, err := c.physical(duration)
		if err != nil {
			return err
		}
		started := c.now()
		samples := 0
		completed := []Task{}
		for n := 0; n < 20; n++ {
			batch := fmt.Sprintf("%s-soak-batch-%d", c.prefix, n)
			config := job(batch, false, 0, "")
			get(config, 6).(map[string]any)["32"] = field("set", resources(0.1))
			if _, err = c.rpc("createJob", Object{"1": field("rec", config)}); err != nil {
				return err
			}
			done, err := c.wait(batch, func(xs []Task) bool { return len(xs) == 2 && all(xs, 3) }, 90*time.Second)
			if err != nil {
				return err
			}
			completed = append(completed, done...)
			until := started.Add(time.Duration(n+1) * 30 * time.Second)
			for {
				current, err := c.tasks(name)
				if err != nil {
					return err
				}
				if !sameTasks(current, original) {
					return errors.New("soak service identity/state changed")
				}
				currentPhysical, err := c.physical(duration)
				if err != nil {
					return err
				}
				if !reflect.DeepEqual(currentPhysical, physical) {
					return errors.New("soak workload PID changed")
				}
				samples++
				remaining := until.Sub(c.now())
				if remaining <= 0 {
					break
				}
				if remaining > 5*time.Second {
					remaining = 5 * time.Second
				}
				if err = c.pause(remaining); err != nil {
					return err
				}
			}
		}
		elapsed := c.now().Sub(started).Seconds()
		ids := map[string]bool{}
		for _, task := range completed {
			ids[task.ID] = true
		}
		if elapsed < 600 || len(completed) != 40 || len(ids) != 40 {
			return errors.New("incomplete mixed soak")
		}
		c.add(Object{"name": "mixed-soak", "elapsedSeconds": elapsed, "batches": 20, "finishedTasks": completed, "serviceTasks": original, "physical": physical, "samples": samples})
		return nil
	})
}
