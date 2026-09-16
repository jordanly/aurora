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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Object = map[string]any

func field(kind string, value any) Object { return Object{kind: value} }
func get(value any, number int) any {
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	wrapped, ok := object[strconv.Itoa(number)].(map[string]any)
	if !ok || len(wrapped) != 1 {
		return nil
	}
	for _, v := range wrapped {
		return v
	}
	return nil
}
func integer(value any) (int64, bool) {
	switch n := value.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		if n == float64(int64(n)) {
			return int64(n), true
		}
	case json.Number:
		v, err := n.Int64()
		return v, err == nil
	}
	return 0, false
}
func number(value any) int64 {
	n, ok := integer(value)
	if !ok {
		return -1
	}
	return n
}
func text(value any) string { s, _ := value.(string); return s }
func key(name string) Object {
	return Object{"1": field("str", "fixtures"), "2": field("str", "test"), "3": field("str", name)}
}
func resource(n int, kind string, value any) Object {
	return Object{strconv.Itoa(n): field(kind, value)}
}
func resources(cpu float64) []any {
	return []any{"rec", 3, resource(1, "dbl", cpu), resource(2, "i64", 32), resource(3, "i64", 32)}
}
func task(name string, service bool, duration int) Object {
	argv := []string{"/bin/true"}
	if duration > 0 {
		argv = []string{"/bin/sleep", strconv.Itoa(duration)}
	}
	process, _ := json.Marshal(Object{"version": "aurora-process-v1", "argv": argv, "env": Object{}, "graceMillis": 1000})
	enabled := 0
	if service {
		enabled = 1
	}
	return Object{"28": field("rec", key(name)), "17": field("rec", Object{"2": field("str", "fixture")}), "7": field("tf", enabled), "11": field("i32", 0), "13": field("i32", 1), "30": field("str", "preferred"), "32": field("set", resources(0.6)), "20": field("set", []any{"rec", 0}), "25": field("rec", Object{"1": field("str", "go-process"), "2": field("str", string(process))}), "34": field("rec", Object{"1": field("tf", 0)}), "29": field("rec", Object{"1": field("rec", Object{"2": field("lst", []any{"rec", 0})})})}
}
func job(name string, service bool, duration int, cron string) Object {
	value := Object{"9": field("rec", key(name)), "7": field("rec", Object{"2": field("str", "fixture")}), "5": field("i32", 0), "6": field("rec", task(name, service, duration)), "8": field("i32", 2)}
	if cron != "" {
		value["4"] = field("str", cron)
	}
	return value
}
func collection(value any, kind string) ([]any, error) {
	items, ok := value.([]any)
	if !ok || len(items) < 2 || items[0] != kind || number(items[1]) != int64(len(items)-2) {
		return nil, errors.New("malformed Thrift collection")
	}
	return items[2:], nil
}
func decodeJSON(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return errors.New("trailing JSON data")
	}
	return nil
}
func parseReply(raw []byte, method string, sequence int64) (Object, error) {
	if len(raw) > 1<<20 {
		return nil, errors.New("oversize API response")
	}
	var wire []any
	if err := decodeJSON(raw, &wire); err != nil {
		return nil, err
	}
	if len(wire) != 5 || number(wire[0]) != 1 || wire[1] != method || number(wire[2]) != 2 || number(wire[3]) != sequence {
		return nil, errors.New("Thrift reply mismatch")
	}
	result, ok := get(wire[4], 0).(map[string]any)
	if !ok {
		return nil, errors.New("missing Thrift success result")
	}
	return result, nil
}

type Task struct {
	ID     string `json:"id"`
	Host   any    `json:"host"`
	Status int64  `json:"status"`
}
type check struct {
	ctx                                    context.Context
	root, repo, url, prefix, phase, output string
	data                                   Data
	runner                                 Runner
	client                                 *http.Client
	sequence                               int64
	rounds                                 int
	report                                 Object
	responseBytes                          int64
	nowFn                                  func() time.Time
	pauseFn                                func(context.Context, time.Duration) error
}

func newCheck(ctx context.Context, root, repo, phase string, rounds int, runner Runner) (*check, error) {
	data, err := load(root)
	if err != nil {
		return nil, err
	}
	scheduler, err := Inspect(ctx, runner, &data, "scheduler", false)
	if err != nil {
		return nil, err
	}
	network, ok := scheduler.NetworkSettings.Networks[data.Network.Name]
	if !ok || net.ParseIP(network.IPAddress) == nil {
		return nil, errors.New("missing valid scheduler network address")
	}
	token, err := randomHex(6)
	if err != nil {
		return nil, err
	}
	prefix := "check-" + strconv.FormatInt(time.Now().Unix(), 10) + "-" + token
	c := &check{ctx: ctx, root: root, repo: repo, data: data, runner: runner, client: &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, url: "http://" + net.JoinHostPort(network.IPAddress, "8081") + "/api", prefix: prefix, phase: phase, rounds: rounds, output: filepath.Join(root, "check-"+phase+"-"+strconv.FormatInt(time.Now().UnixNano(), 10)+".json")}
	c.report = Object{"ok": false, "phase": phase, "prefix": prefix, "cases": []any{}, "rpc": []any{}, "recoveryRounds": nil}
	if phase == "recovery" {
		c.report["recoveryRounds"] = rounds
	}
	return c, nil
}
func (c *check) rpc(method string, args Object) (Object, error) {
	c.sequence++
	body, err := json.Marshal([]any{1, method, 1, c.sequence, args})
	if err != nil {
		return nil, err
	}
	if len(body) > 1<<20 {
		return nil, errors.New("oversize fixture request")
	}
	request, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/x-thrift")
	response, err := c.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("API HTTP %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	result, err := parseReply(raw, method, c.sequence)
	if err != nil {
		return nil, err
	}
	c.responseBytes += int64(len(raw))
	if c.responseBytes > 64<<20 {
		return nil, errors.New("acceptance report response budget exceeded")
	}
	c.report["rpc"] = append(c.report["rpc"].([]any), Object{"method": method, "response": result})
	if number(get(result, 1)) != 1 {
		detail, _ := json.Marshal(get(result, 6))
		return nil, fmt.Errorf("%s rejected: %s", method, detail)
	}
	return result, nil
}
func (c *check) add(value Object) { c.report["cases"] = append(c.report["cases"].([]any), value) }
func (c *check) scheduled(name string) ([]any, error) {
	result, err := c.rpc("getTasksStatus", Object{"1": field("rec", Object{"11": field("set", []any{"rec", 1, key(name)})})})
	if err != nil {
		return nil, err
	}
	return collection(get(get(get(result, 3), 3), 1), "rec")
}
func (c *check) tasks(name string) ([]Task, error) {
	values, err := c.scheduled(name)
	if err != nil {
		return nil, err
	}
	tasks := []Task{}
	for _, v := range values {
		assigned := get(v, 1)
		id, host, status := text(get(assigned, 1)), get(assigned, 3), number(get(v, 2))
		if host != nil {
			if _, ok := host.(string); !ok {
				return nil, errors.New("malformed task host")
			}
		}
		if id == "" || status < 0 {
			return nil, errors.New("malformed scheduled task")
		}
		tasks = append(tasks, Task{id, host, status})
	}
	return tasks, nil
}
func (c *check) wait(name string, predicate func([]Task) bool, duration time.Duration) ([]Task, error) {
	deadline := time.Now().Add(duration)
	var values []Task
	for time.Now().Before(deadline) {
		var err error
		values, err = c.tasks(name)
		if err != nil {
			return nil, err
		}
		if predicate(values) {
			return values, nil
		}
		if err = c.pause(time.Second); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("timed out waiting for %s: %v", name, values)
}
func all(tasks []Task, status int64) bool {
	if len(tasks) == 0 {
		return false
	}
	for _, task := range tasks {
		if task.Status != status {
			return false
		}
	}
	return true
}
func running(tasks []Task) []Task {
	result := []Task{}
	for _, task := range tasks {
		if task.Status == 2 {
			result = append(result, task)
		}
	}
	return result
}
func hosts(tasks []Task) int {
	set := map[any]bool{}
	for _, task := range tasks {
		set[task.Host] = true
	}
	return len(set)
}
func sameTasks(a, b []Task) bool {
	a = append([]Task{}, a...)
	b = append([]Task{}, b...)
	sort.Slice(a, func(i, j int) bool { return a[i].ID < a[j].ID })
	sort.Slice(b, func(i, j int) bool { return b[i].ID < b[j].ID })
	return reflect.DeepEqual(a, b)
}
func (c *check) kill(name string) error {
	if _, err := c.rpc("killTasks", Object{"4": field("rec", key(name)), "5": field("set", []any{"i32", 0}), "6": field("str", "isolated acceptance cleanup")}); err != nil {
		return err
	}
	_, err := c.wait(name, func(xs []Task) bool {
		if len(xs) == 0 {
			return false
		}
		for _, x := range xs {
			if x.Status != 3 && x.Status != 4 && x.Status != 5 && x.Status != 7 {
				return false
			}
		}
		return true
	}, 90*time.Second)
	return err
}
func (c *check) physical(duration int) (map[string][]string, error) {
	result := map[string][]string{}
	for _, node := range roles[:2] {
		item, err := Inspect(c.ctx, c.runner, &c.data, node, false)
		if err != nil {
			return nil, err
		}
		raw, err := c.runner(c.ctx, "docker", "top", item.ID, "-eo", "pid,args")
		if err != nil {
			return nil, err
		}
		lines := strings.Split(raw, "\n")
		matches := []string{}
		for _, line := range lines[1:] {
			if strings.Contains(line, "/bin/sleep "+strconv.Itoa(duration)) && !strings.Contains(line, "aurora-agent") {
				matches = append(matches, strings.TrimSpace(line))
			}
		}
		sort.Strings(matches)
		if len(matches) != 1 {
			return nil, fmt.Errorf("expected one physical sleep%d on %s", duration, node)
		}
		result[node] = matches
	}
	return result, nil
}
func (c *check) quota() error {
	quota := []any{"rec", 3, resource(1, "dbl", 4.0), resource(2, "i64", 1024), resource(3, "i64", 1024)}
	_, err := c.rpc("setQuota", Object{"1": field("str", "fixtures"), "2": field("rec", Object{"4": field("set", quota)})})
	return err
}
func settings() Object {
	return Object{"1": field("i32", 1), "2": field("i32", 0), "3": field("i32", 0), "5": field("i32", 1000), "6": field("tf", 1), "7": field("set", []any{"rec", 0}), "8": field("tf", 1)}
}
func (c *check) startUpdate(config Object, message string) (any, error) {
	result, err := c.rpc("startJobUpdate", Object{"1": field("rec", Object{"1": field("rec", config), "2": field("i32", 2), "3": field("rec", settings())}), "3": field("str", message)})
	if err != nil {
		return nil, err
	}
	key := get(get(get(result, 3), 22), 1)
	if key == nil {
		return nil, errors.New("missing update key")
	}
	return key, nil
}
func (c *check) waitUpdate(key any, expected int64, duration time.Duration) error {
	deadline := time.Now().Add(duration)
	for {
		result, err := c.rpc("getJobUpdateDetails", Object{"1": field("rec", key)})
		if err != nil {
			return err
		}
		details := get(get(get(result, 3), 24), 1)
		if details == nil {
			return errors.New("missing update details")
		}
		status := number(get(get(get(get(details, 1), 1), 4), 1))
		if status == expected {
			return nil
		}
		if status == 4 || status == 5 || status == 6 || status == 7 || status == 8 {
			return fmt.Errorf("unexpected update terminal status %d", status)
		}
		if !time.Now().Before(deadline) {
			return errors.New("job update did not reach expected terminal status")
		}
		if err = c.pause(time.Second); err != nil {
			return err
		}
	}
}
func Check(ctx context.Context, repo string, args []string) error {
	flags := flag.NewFlagSet("inplace-check", flag.ContinueOnError)
	root := flags.String("root", "", "absolute owned lab root")
	phase := flags.String("phase", "", "smoke, recovery, policy, soak or churn")
	rounds := flags.Int("rounds", 3, "recovery rounds1 through3")
	dryRun := flags.Bool("dry-run", false, "render fixture scope without network calls or writes")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || (*phase != "smoke" && *phase != "recovery" && *phase != "policy" && *phase != "soak" && *phase != "churn") || *rounds < 1 || *rounds > 3 {
		return errors.New("supported phase and rounds1 through3 required")
	}
	checked, err := Safe(*root)
	if err != nil {
		return err
	}
	if *dryRun {
		encoded, _ := json.MarshalIndent(Object{"dryRun": true, "root": checked, "phase": *phase, "rounds": *rounds, "jobRole": "fixtures", "jobEnvironment": "test"}, "", "  ")
		fmt.Println(string(encoded))
		return nil
	}
	c, err := newCheck(ctx, checked, repo, *phase, *rounds, run)
	if err != nil {
		return err
	}
	defer c.client.CloseIdleConnections()
	err = c.quota()
	if err == nil {
		switch *phase {
		case "smoke":
			err = c.smoke()
		case "recovery":
			err = c.recovery()
		case "policy":
			err = c.policy()
		case "soak":
			err = c.soak()
		case "churn":
			err = c.churn()
		}
	}
	c.report["ok"] = err == nil
	if err != nil {
		c.report["error"] = err.Error()
	}
	writeErr := saveJSON(c.output, c.report)
	if err != nil {
		return errors.Join(err, writeErr)
	}
	if writeErr != nil {
		return writeErr
	}
	encoded, _ := json.Marshal(Object{"ok": true, "report": c.output})
	fmt.Println(string(encoded))
	return nil
}

func (c *check) now() time.Time {
	if c.nowFn != nil {
		return c.nowFn()
	}
	return time.Now()
}
func (c *check) pause(duration time.Duration) error {
	if c.pauseFn != nil {
		return c.pauseFn(c.ctx, duration)
	}
	return pause(c.ctx, duration)
}
