// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

const Usage = `usage: aurora [options] <group> <operation> [arguments]

Options precede the command:
  --scheduler URL     original scheduler leader URL (or AURORA_SCHEDULER)
  --timeout 15s       bounded HTTP request timeout
  --ca FILE          custom HTTPS trust roots
  --cert FILE --key FILE  HTTPS client certificate
  --message TEXT     audit message for job/update mutations
  --offline          prevent the repository bootstrap from downloading Go

Commands:
  job validate|render FILE       local validation or Thrift job rendering
  job check|create FILE          server validation or job creation
  job list [ROLE]               role summary or jobs for a role
  job status|pending KEY        task status or pending placement reasons
  job kill KEY [IDS]            stop all instances, or comma-separated IDs
  job restart KEY IDS           restart explicit comma-separated instances
  job add KEY INSTANCE COUNT    add instances using an existing task template
  update start|diff FILE        service rolling update or configuration diff
  update list KEY               update summaries for a job
  update status|pause|resume|abort|rollback|pulse KEY UPDATE_ID
  cron schedule|replace FILE    register or replace a cron template
  cron start|delete KEY         run now or deschedule
  quota get ROLE                role quota
  quota set ROLE FILE           resources JSON: cpuMillis, ramMb, diskMb
  hosts start|drain|status|end HOST...
  rpc METHOD FILE               original Thrift JSON argument struct

KEY is role/environment/name. FILE may be - for stdin. Job documents use
aurora-job-v1; executable Python .aurora files are not interpreted. All commands
emit JSON. Mutations are never automatically retried after an uncertain reply.
Job/resource JSON field names are case-sensitive. Omit optional fields to use
defaults; explicit null values are rejected in these typed documents.
`

// Run is shared by the standalone Go CLI and the pinned repository launcher.
func Run(ctx context.Context, arguments []string, input io.Reader, output, diagnostic io.Writer) error {
	flags := flag.NewFlagSet("aurora", flag.ContinueOnError)
	flags.SetOutput(diagnostic)
	flags.Usage = func() { fmt.Fprint(diagnostic, Usage) }
	var connection Connection
	flags.StringVar(&connection.URL, "scheduler", os.Getenv("AURORA_SCHEDULER"), "scheduler leader URL")
	flags.StringVar(&connection.CAFile, "ca", "", "HTTPS CA certificates")
	flags.StringVar(&connection.CertificateFile, "cert", "", "HTTPS client certificate")
	flags.StringVar(&connection.KeyFile, "key", "", "HTTPS client key")
	flags.DurationVar(&connection.Timeout, "timeout", 15*time.Second, "HTTP timeout")
	message := flags.String("message", "Aurora Go client", "audit message")
	flags.Bool("offline", false, "disable bootstrap downloads")
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	args := flags.Args()
	if len(args) == 0 || args[0] == "help" {
		_, err := fmt.Fprint(output, Usage)
		return err
	}
	if len(args) < 2 {
		return errors.New("expected a command group and operation; see --help")
	}
	group, operation, args := args[0], args[1], args[2:]
	method, wireArgs, local, err := command(group, operation, args, *message, input)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if method == "" {
		return encoder.Encode(local)
	}
	if connection.URL == "" {
		return errors.New("--scheduler or AURORA_SCHEDULER is required")
	}
	c, err := New(connection)
	if err != nil {
		return err
	}
	defer c.http.CloseIdleConnections()
	response, err := c.Call(ctx, method, wireArgs)
	if response != nil {
		var value any = response
		if method == "getTasksStatus" && group == "job" && err == nil {
			value, err = taskSummary(response)
			if err != nil {
				return err
			}
		}
		if writeErr := encoder.Encode(value); writeErr != nil {
			return writeErr
		}
	}
	return err
}

func command(group, operation string, args []string, message string, input io.Reader) (string, Struct, any, error) {
	fail := func(text string) (string, Struct, any, error) { return "", nil, nil, errors.New(text) }
	call := func(method string, fields Struct) (string, Struct, any, error) { return method, fields, nil, nil }
	jobFile := group == "job" && contains(operation, "validate", "render", "check", "create") ||
		group == "update" && contains(operation, "start", "diff") ||
		group == "cron" && contains(operation, "schedule", "replace")
	if jobFile {
		if len(args) != 1 {
			return fail("expected one JSON job file")
		}
		job, err := LoadJob(args[0], input)
		if err != nil {
			return "", nil, nil, err
		}
		if group == "job" {
			switch operation {
			case "validate":
				return "", nil, job, nil
			case "render":
				return "", nil, job.Wire(), nil
			case "check":
				return call("populateJobConfig", Struct{"1": Field("rec", job.Wire())})
			case "create":
				if job.CronSchedule != "" {
					return fail("use cron schedule for cron jobs")
				}
				return call("createJob", Struct{"1": Field("rec", job.Wire())})
			}
		}
		if group == "cron" {
			if job.CronSchedule == "" {
				return fail("cronSchedule is required")
			}
			method := "scheduleCronJob"
			if operation == "replace" {
				method = "replaceCronTemplate"
			}
			return call(method, Struct{"1": Field("rec", job.Wire())})
		}
		if !job.Service || job.CronSchedule != "" {
			return fail("rolling updates require a service job without cronSchedule")
		}
		if operation == "diff" {
			return call("getJobUpdateDiff", Struct{"1": Field("rec", job.UpdateRequest())})
		}
		return call("startJobUpdate", Struct{"1": Field("rec", job.UpdateRequest()), "3": Field("str", message)})
	}
	if group == "rpc" {
		if len(args) != 1 {
			return fail("rpc requires a method and one Thrift JSON argument file")
		}
		fields := Struct{}
		if err := ReadJSON(args[0], input, &fields); err != nil {
			return "", nil, nil, err
		}
		if fields == nil {
			return fail("RPC arguments must be an object")
		}
		return call(operation, fields)
	}
	if group == "job" && operation == "list" {
		if len(args) == 0 {
			return call("getRoleSummary", Struct{})
		}
		if len(args) != 1 || args[0] == "" {
			return fail("job list accepts zero or one role")
		}
		return call("getJobs", Struct{"1": Field("str", args[0])})
	}
	if group == "quota" {
		if len(args) == 1 && operation == "get" {
			return call("getQuota", Struct{"1": Field("str", args[0])})
		}
		if len(args) != 2 || operation != "set" {
			return fail("quota get ROLE or quota set ROLE FILE")
		}
		var resources Resources
		if err := ReadJSON(args[1], input, &resources); err != nil {
			return "", nil, nil, err
		}
		if err := resources.Validate(true); err != nil {
			return "", nil, nil, err
		}
		return call("setQuota", Struct{"1": Field("str", args[0]), "2": Field("rec", Struct{"4": Field("set", resources.Wire())})})
	}
	if group == "hosts" {
		method := map[string]string{"start": "startMaintenance", "drain": "drainHosts", "status": "maintenanceStatus", "end": "endMaintenance"}[operation]
		if method == "" || len(args) == 0 {
			return fail("hosts start|drain|status|end requires explicit host names")
		}
		values, seen := []any{}, map[string]bool{}
		for _, host := range args {
			if host == "" || seen[host] {
				return fail("hosts must be unique and nonempty")
			}
			seen[host] = true
			values = append(values, host)
		}
		return call(method, Struct{"1": Field("rec", Struct{"1": Field("set", Set("str", values...))})})
	}
	if len(args) == 0 {
		return fail("expected a role/environment/name job key")
	}
	key, err := ParseKey(args[0])
	if err != nil {
		return "", nil, nil, err
	}
	if group == "job" {
		switch operation {
		case "status", "pending":
			if len(args) != 1 {
				return fail("expected one job key")
			}
			method := "getTasksStatus"
			if operation == "pending" {
				method = "getPendingReason"
			}
			return call(method, Struct{"1": Field("rec", Struct{"11": Field("set", Set("rec", key.Wire()))})})
		case "kill", "restart":
			if len(args) > 2 || operation == "restart" && len(args) != 2 {
				return fail("expected job key and comma-separated instance IDs (required for restart)")
			}
			ids := []any{}
			if len(args) == 2 {
				ids, err = instances(args[1])
				if err != nil {
					return "", nil, nil, err
				}
			}
			if operation == "restart" {
				return call("restartShards", Struct{"5": Field("rec", key.Wire()), "3": Field("set", Set("i32", ids...))})
			}
			return call("killTasks", Struct{"4": Field("rec", key.Wire()), "5": Field("set", Set("i32", ids...)), "6": Field("str", message)})
		case "add":
			if len(args) != 3 {
				return fail("job add requires KEY INSTANCE COUNT")
			}
			instance, e1 := strconv.ParseInt(args[1], 10, 32)
			count, e2 := strconv.ParseInt(args[2], 10, 32)
			if e1 != nil || e2 != nil || instance < 0 || count <= 0 {
				return fail("instance must be nonnegative and count positive")
			}
			return call("addInstances", Struct{"3": Field("rec", Struct{"1": Field("rec", key.Wire()), "2": Field("i32", instance)}), "4": Field("i32", count)})
		}
	}
	if group == "cron" && contains(operation, "start", "delete") && len(args) == 1 {
		method := "startCronJob"
		if operation == "delete" {
			method = "descheduleCronJob"
		}
		return call(method, Struct{"4": Field("rec", key.Wire())})
	}
	if group == "update" {
		if operation == "list" && len(args) == 1 {
			return call("getJobUpdateSummaries", Struct{"1": Field("rec", Struct{"3": Field("rec", key.Wire()), "6": Field("i32", 0), "7": Field("i32", 100)})})
		}
		method := map[string]string{"status": "getJobUpdateDetails", "pause": "pauseJobUpdate", "resume": "resumeJobUpdate", "abort": "abortJobUpdate", "rollback": "rollbackJobUpdate", "pulse": "pulseJobUpdate"}[operation]
		if method == "" || len(args) != 2 || args[1] == "" {
			return fail("update operation requires KEY UPDATE_ID")
		}
		fields := Struct{"1": Field("rec", Struct{"1": Field("rec", key.Wire()), "2": Field("str", args[1])})}
		if operation == "rollback" {
			fields["2"] = Field("str", message)
		}
		if contains(operation, "pause", "resume", "abort") {
			fields["3"] = Field("str", message)
		}
		return call(method, fields)
	}
	return fail("unknown command; see --help")
}

func contains(value string, choices ...string) bool {
	for _, choice := range choices {
		if value == choice {
			return true
		}
	}
	return false
}
func instances(text string) ([]any, error) {
	result, seen := []any{}, map[int64]bool{}
	for _, part := range strings.Split(text, ",") {
		value, err := strconv.ParseInt(part, 10, 32)
		if err != nil || value < 0 || seen[value] {
			return nil, errors.New("instance IDs must be unique nonnegative i32 integers")
		}
		seen[value] = true
		result = append(result, value)
	}
	return result, nil
}

func taskSummary(response Struct) ([]any, error) {
	payload, err := wireField(response, 3, "rec")
	if err != nil {
		return nil, err
	}
	statusResult, err := wireField(payload, 3, "rec")
	if err != nil {
		return nil, err
	}
	collection, err := wireField(statusResult, 1, "lst")
	if err != nil {
		return nil, err
	}
	values, ok := collection.([]any)
	if !ok || len(values) < 2 || values[0] != "rec" {
		return nil, errors.New("invalid task status collection")
	}
	count, err := wireInteger(values[1])
	if err != nil || count < 0 || int64(len(values)-2) != count {
		return nil, errors.New("invalid task status collection count")
	}
	result := []any{}
	for _, task := range values[2:] {
		assigned, err := wireField(task, 1, "rec")
		if err != nil {
			return nil, err
		}
		taskID, err := wireField(assigned, 1, "str")
		if text, ok := taskID.(string); err != nil || !ok || text == "" {
			return nil, errors.New("invalid task status task ID")
		}
		instance, err := wireField(assigned, 6, "i32")
		instanceID, numberErr := wireInteger(instance)
		if err != nil || numberErr != nil || instanceID < 0 {
			return nil, errors.New("invalid task status instance ID")
		}
		status, err := wireField(task, 2, "i32")
		statusID, numberErr := wireInteger(status)
		if err != nil || numberErr != nil {
			return nil, errors.New("invalid task status enum")
		}
		switch statusID {
		case 0, 1, 2, 3, 4, 5, 6, 7, 9, 11, 12, 13, 16, 17, 18:
		default:
			return nil, errors.New("unknown task status enum")
		}
		// Pending tasks have no assigned host. A present host must still be a string.
		var host any
		if _, present := wireObject(assigned)["3"]; present {
			host, err = wireField(assigned, 3, "str")
			if _, ok := host.(string); err != nil || !ok {
				return nil, errors.New("invalid task status host")
			}
		}
		result = append(result, map[string]any{"taskId": taskID, "host": host,
			"instance": instance, "status": status})
	}
	return result, nil
}

func wireObject(value any) map[string]any {
	if object, ok := value.(Struct); ok {
		return object
	}
	object, _ := value.(map[string]any)
	return object
}

func wireField(value any, number int, kind string) (any, error) {
	field := wireObject(wireObject(value)[strconv.Itoa(number)])
	result, ok := field[kind]
	if len(field) != 1 || !ok || result == nil || kind == "rec" && wireObject(result) == nil {
		return nil, fmt.Errorf("invalid task status field %d (%s)", number, kind)
	}
	return result, nil
}

func wireInteger(value any) (int64, error) {
	switch number := value.(type) {
	case json.Number:
		return strconv.ParseInt(string(number), 10, 32)
	case int:
		return strconv.ParseInt(strconv.Itoa(number), 10, 32)
	case int32:
		return int64(number), nil
	default:
		return 0, errors.New("expected Thrift i32 integer")
	}
}
