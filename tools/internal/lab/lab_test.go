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
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func fixtureData(t *testing.T) (string, Data) {
	t.Helper()
	root := t.TempDir()
	os.Chmod(root, 0700)
	networkID := strings.Repeat("f", 64)
	data := Data{Schema: 1, Token: "0123456789abcdef", UID: os.Getuid(), GID: os.Getgid(), Prefix: "aurora-inplace-0123456789abcdef", Image: "sha256:pinned", Containers: map[string]*ContainerRecord{}, Network: &NetworkRecord{Name: "aurora-inplace-0123456789abcdef-network", ID: &networkID}}
	for index, role := range roles {
		record, _ := containerPlan(root, &data, role)
		id := strings.Repeat(string(rune('a'+index)), 64)
		record.ID = &id
		data.Containers[role] = record
		os.MkdirAll(filepath.Join(root, role, "control"), 0700)
		if err := saveJSON(filepath.Join(root, role, "control/daemon.json"), daemonState{Generation: 1, PID: 100 + index}); err != nil {
			t.Fatal(err)
		}
	}
	if err := save(root, &data); err != nil {
		t.Fatal(err)
	}
	return root, data
}
func inspected(data *Data, role string) Object {
	record := data.Containers[role]
	mounts := []any{}
	for _, m := range record.Mounts {
		mounts = append(mounts, Object{"Type": "bind", "Source": m.Source, "Destination": m.Destination, "RW": m.RW})
	}
	return Object{"Id": *record.ID, "Name": "/" + record.Name, "Image": data.Image, "Config": Object{"Labels": map[string]string{Label: data.Token, Label + ".role": role}, "User": fmtUser(data), "Entrypoint": []string{"/opt/bin/cluster-helper"}, "Cmd": record.Args}, "HostConfig": Object{"Privileged": false, "ReadonlyRootfs": true, "CapDrop": []string{"ALL"}, "NetworkMode": data.Network.Name, "PortBindings": nil, "PidMode": "", "SecurityOpt": []string{"no-new-privileges:true"}}, "Mounts": mounts, "State": Object{"Running": true}, "NetworkSettings": Object{"Networks": Object{data.Network.Name: Object{"IPAddress": "172.25.0.3"}}}}
}
func fmtUser(data *Data) string {
	return strings.Join([]string{strconvI(data.UID), strconvI(data.GID)}, ":")
}
func strconvI(n int) string     { b, _ := json.Marshal(n); return string(b) }
func jsonText(value any) string { data, _ := json.Marshal(value); return string(data) }
func TestOwnedInspectionRefusesChangesBeforeRemoval(t *testing.T) {
	for _, change := range []string{"label", "image", "mount", "command", "privileged", "missing-privileged", "network", "id"} {
		t.Run(change, func(t *testing.T) {
			root, data := fixtureData(t)
			item := inspected(&data, "scheduler")
			switch change {
			case "label":
				item["Config"].(Object)["Labels"] = map[string]string{Label: "foreign", Label + ".role": "scheduler"}
			case "image":
				item["Image"] = "other"
			case "mount":
				item["Mounts"].([]any)[0].(Object)["RW"] = true
			case "command":
				item["Config"].(Object)["Cmd"] = []string{"other"}
			case "privileged":
				item["HostConfig"].(Object)["Privileged"] = true
			case "missing-privileged":
				delete(item["HostConfig"].(Object), "Privileged")
			case "network":
				item["HostConfig"].(Object)["NetworkMode"] = "host"
			case "id":
				item["Id"] = strings.Repeat("e", 64)
			}
			calls := 0
			runner := func(_ context.Context, args ...string) (string, error) {
				calls++
				if !reflect.DeepEqual(args, []string{"docker", "inspect", *data.Containers["scheduler"].ID}) {
					t.Fatalf("mutation before ownership check: %v", args)
				}
				return jsonText([]any{item}), nil
			}
			if err := down(context.Background(), root, &data, runner); err == nil {
				t.Fatal("foreign resource removal accepted")
			}
			if calls != 1 || len(data.Containers) != 3 {
				t.Fatal("ownership rejection changed records")
			}
		})
	}
}
func TestUnknownCreationIDNeverUsesNameForRemoval(t *testing.T) {
	root, data := fixtureData(t)
	data.Containers["scheduler"].ID = nil
	runner := func(context.Context, ...string) (string, error) {
		t.Fatal("looked up unknown resource by name")
		return "", nil
	}
	if err := down(context.Background(), root, &data, runner); err == nil {
		t.Fatal("unknown resource accepted")
	}
}
func TestPathsAndDryRunNeverTouchDockerOrExistingLabs(t *testing.T) {
	root := t.TempDir()
	os.Symlink(t.TempDir(), filepath.Join(root, "link"))
	for _, path := range []string{"relative", root + "/../other", root + ",source=bad", filepath.Join(root, "link/x")} {
		if _, err := Safe(path); err == nil {
			t.Fatal("unsafe path accepted", path)
		}
	}
	untouched := filepath.Join(root, "not-created")
	runner := func(context.Context, ...string) (string, error) { t.Fatal("dry-run contacted Docker"); return "", nil }
	if err := cluster(context.Background(), root, []string{"up", "--root", untouched, "--dry-run"}, runner); err != nil {
		t.Fatal(err)
	}
	if err := Check(context.Background(), root, []string{"--root", untouched, "--phase", "smoke", "--dry-run"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(untouched); !os.IsNotExist(err) {
		t.Fatal("dry-run created lab files")
	}
}
func TestContainerPlanRetainsExactSandboxAndEntrypoint(t *testing.T) {
	root, data := fixtureData(t)
	for _, role := range roles {
		record, args := containerPlan(root, &data, role)
		if record.Args[0] != "keeper" || record.Args[1] != "--" {
			t.Fatal(record.Args)
		}
		joined := strings.Join(args, "\n")
		if role == "scheduler" && (!strings.Contains(joined, "-backup_interval=30secs") || !strings.Contains(joined, "-max_saved_backups=3")) {
			t.Fatal("missing lab backup schedule/retention")
		}
		for _, required := range []string{"--read-only", "--cap-drop\nALL", "--security-opt\nno-new-privileges:true", "--pull\nnever", "--entrypoint\n/opt/bin/cluster-helper"} {
			if !strings.Contains(joined, required) {
				t.Fatal("missing boundary", required)
			}
		}
		for _, mount := range record.Mounts {
			if (mount.Destination == "/tls" || mount.Destination == "/config" || strings.HasPrefix(mount.Destination, "/opt/")) && mount.RW {
				t.Fatal("mutable artifact/config mount")
			}
		}
	}
}
func TestDistributionRequiresOriginalMainAndRejectsReplacement(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		root := t.TempDir()
		os.MkdirAll(filepath.Join(root, "bin"), 0700)
		os.Mkdir(filepath.Join(root, "lib"), 0700)
		os.WriteFile(filepath.Join(root, "bin/aurora-scheduler"), []byte("launcher"), 0700)
		file, _ := os.Create(filepath.Join(root, "lib/scheduler.jar"))
		archive := zip.NewWriter(file)
		archive.Create("org/apache/aurora/scheduler/app/SchedulerMain.class")
		if replacement {
			archive.Create("x/NativeEngine.class")
		}
		archive.Close()
		file.Close()
		err := validateDistribution(root)
		if (err != nil) != replacement {
			t.Fatal(replacement, err)
		}
	}
}
func TestThriftFixtureShapeAndReplyFencing(t *testing.T) {
	config := task("check-test", true, 300)
	if text(get(get(config, 25), 1)) != "go-process" || number(get(config, 7)) != 1 || number(get(get(config, 34), 1)) != 0 {
		t.Fatal(config)
	}
	values, err := collection(get(config, 32), "rec")
	if err != nil || len(values) != 3 || get(values[0], 1) != 0.6 || number(get(values[1], 2)) != 32 {
		t.Fatal(values, err)
	}
	var process Object
	if err = decodeJSON([]byte(text(get(get(config, 25), 2))), &process); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(process["argv"], []any{"/bin/sleep", "300"}) {
		t.Fatal(process)
	}
	result := Object{"1": field("i32", 1)}
	good := []any{1, "createJob", 2, 7, Object{"0": field("rec", result)}}
	raw, _ := json.Marshal(good)
	if _, err = parseReply(raw, "createJob", 7); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]any{{1, "other", 2, 7, good[4]}, {1, "createJob", 2, 8, good[4]}, {1, "createJob", 3, 7, good[4]}, {1, "createJob", 2, 7, Object{}}} {
		raw, _ := json.Marshal(bad)
		if _, err = parseReply(raw, "createJob", 7); err == nil {
			t.Fatal("unfenced response accepted", bad)
		}
	}
	if _, err = parseReply(bytes.Repeat([]byte("x"), (1<<20)+1), "createJob", 7); err == nil {
		t.Fatal("oversize reply accepted")
	}
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type apiFixture struct {
	t                                              *testing.T
	phase                                          string
	calls                                          []string
	updating, killed, draining, ended, cronStarted bool
	prefix                                         string
	churnFault                                     string
	churnBatchReads                                int
}

func responseResult(fieldID int, value any) Object {
	return Object{"1": field("i32", 1), "3": field("rec", Object{strconvI(fieldID): field("rec", value)})}
}
func taskRecord(id, host string, status int64, config Object) Object {
	assigned := Object{"1": field("str", id), "4": field("rec", config)}
	if host != "" {
		assigned["3"] = field("str", host)
	}
	return Object{"1": field("rec", assigned), "2": field("i32", status)}
}
func (f *apiFixture) serve(request *http.Request) (*http.Response, error) {
	if request.Method == "GET" && request.URL.Path == "/health" {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("OK")), Header: http.Header{}}, nil
	}
	if request.Method != "POST" || request.Header.Get("Content-Type") != "application/x-thrift" {
		f.t.Fatal("wrong Thrift transport")
	}
	data, _ := io.ReadAll(request.Body)
	var wire []any
	if err := decodeJSON(data, &wire); err != nil {
		f.t.Fatal(err)
	}
	if len(wire) != 5 || number(wire[0]) != 1 || number(wire[2]) != 1 {
		f.t.Fatal(wire)
	}
	method := text(wire[1])
	f.calls = append(f.calls, method)
	result := Object{"1": field("i32", 1)}
	args := wire[4]
	switch method {
	case "createJob":
		if f.phase == "churn" {
			config := get(get(args, 1), 6)
			constraints, err := collection(get(config, 20), "rec")
			if err != nil || len(constraints) != 1 || get(constraints[0], 1) != "host" || number(get(get(get(constraints[0], 2), 2), 1)) != 1 {
				f.t.Fatal("churn job did not constrain one instance per host", config)
			}
			resources, err := collection(get(config, 32), "rec")
			cpu := "0.6"
			if number(get(config, 7)) == 1 {
				cpu = "0.1"
			}
			if err != nil || len(resources) != 3 || jsonText(get(resources[0], 1)) != cpu {
				f.t.Fatal("churn CPU reservation differs", config)
			}
		}
		f.killed = false
	case "startJobUpdate":
		f.updating = true
		result = responseResult(22, Object{"1": field("rec", Object{"1": field("rec", key(f.prefix+"-service")), "2": field("str", "update")})})
	case "getJobUpdateDetails":
		status := 4
		if f.phase == "policy" {
			status = 5
		}
		details := Object{"1": field("rec", Object{"1": field("rec", Object{"4": field("rec", Object{"1": field("i32", status)})})})}
		result = responseResult(24, Object{"1": field("rec", details)})
	case "killTasks":
		f.killed = true
	case "drainHosts":
		f.draining = true
	case "endMaintenance":
		f.draining = false
		f.ended = true
	case "maintenanceStatus":
		result = responseResult(10, Object{"1": field("set", []any{"rec", 1, Object{"1": field("str", "agent-a"), "2": field("i32", 4)}})})
	case "startCronJob":
		f.cronStarted = true
	case "getTasksStatus":
		keys, err := collection(get(get(args, 1), 11), "rec")
		if err != nil || len(keys) != 1 {
			f.t.Fatal("invalid fixture scope", args)
		}
		name := text(get(keys[0], 3))
		if text(get(keys[0], 1)) != "fixtures" || text(get(keys[0], 2)) != "test" || !strings.HasPrefix(name, f.prefix+"-") {
			f.t.Fatal("fixture escaped its namespace", name)
		}
		status := int64(2)
		duration := 300
		idA, idB := "old-a", "old-b"
		if strings.Contains(name, "batch") || strings.Contains(name, "cron") {
			status = 3
			duration = 0
			idA = name + "-a"
			idB = name + "-b"
		}
		if strings.Contains(name, "churn-service") {
			duration = 3603
			if f.churnFault == "service-id" && f.churnBatchReads > 0 {
				idA = "replacement-service"
			}
		}
		if strings.Contains(name, "churn-batch") {
			f.churnBatchReads++
			if f.churnFault == "reused-id" {
				idA = "reused-batch-id"
			}
		}
		if strings.Contains(name, "soak-service") {
			duration = 3601
		}
		if f.phase == "recovery" {
			duration = 3600
		}
		if f.phase == "policy" && !strings.Contains(name, "cron") {
			duration = 3602
		}
		config := task(name, duration > 0, duration)
		records := []any{}
		if f.killed && duration > 0 {
			status = 5
		}
		if f.phase == "smoke" && f.updating && duration > 0 {
			oldStatus := int64(5)
			records = append(records, taskRecord("old-a", "agent-a", oldStatus, config), taskRecord("old-b", "agent-b", oldStatus, config))
			idA, idB = "new-a", "new-b"
		}
		if f.phase == "policy" && f.updating && duration > 0 {
			records = append(records, taskRecord("failed", "agent-a", 4, config))
		}
		if f.phase == "policy" && f.draining && !f.killed && duration > 0 {
			records = append(records, taskRecord(idA, "agent-a", 5, config), taskRecord("pending", "", 0, config))
			idA = "omit"
		} else if f.phase == "policy" && f.ended && duration > 0 {
			idA = "replacement"
		}
		if idA != "omit" {
			records = append(records, taskRecord(idA, "agent-a", status, config))
		}
		hostB := "agent-b"
		if f.churnFault == "same-host" && strings.Contains(name, "churn-batch") {
			hostB = "agent-a"
		}
		records = append(records, taskRecord(idB, hostB, status, config))
		collection := append([]any{"rec", len(records)}, records...)
		result = responseResult(3, Object{"1": field("set", collection)})
	case "setQuota", "getQuota", "scheduleCronJob", "descheduleCronJob":
	default:
		return nil, errors.New("unexpected fixture method " + method)
	}
	response, _ := json.Marshal([]any{1, method, 2, wire[3], Object{"0": field("rec", result)}})
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(response)), Header: http.Header{}}, nil
}
func fixtureCheck(t *testing.T, phase string) (*check, *apiFixture) {
	root, data := fixtureData(t)
	now := time.Unix(10000, 0)
	runner := func(_ context.Context, args ...string) (string, error) {
		if len(args) < 3 || args[0] != "docker" {
			t.Fatalf("fixture invoked external command: %v", args)
		}
		role := ""
		for name, record := range data.Containers {
			if *record.ID == args[2] {
				role = name
			}
		}
		if role == "" {
			t.Fatal("fixture inspected unowned ID", args)
		}
		if args[1] == "inspect" {
			for _, action := range []string{"restart", "crash"} {
				request := filepath.Join(root, role, "control", action)
				if _, err := os.Stat(request); err == nil {
					var state daemonState
					readJSON(filepath.Join(root, role, "control/daemon.json"), &state)
					state.Generation++
					state.PID++
					saveJSON(filepath.Join(root, role, "control/daemon.json"), state)
					os.Remove(request)
				}
			}
			return jsonText([]any{inspected(&data, role)}), nil
		}
		if args[1] == "top" {
			duration := 300
			switch phase {
			case "policy":
				duration = 3602
			case "recovery":
				duration = 3600
			case "soak":
				duration = 3601
			case "churn":
				duration = 3603
			}
			return "PID ARGS\n100 /bin/sleep " + strconvI(duration), nil
		}
		t.Fatal("mutating Docker request", args)
		return "", nil
	}
	c, err := newCheck(context.Background(), root, root, phase, 1, runner)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &apiFixture{t: t, phase: phase, prefix: c.prefix}
	c.client = &http.Client{Transport: transportFunc(fixture.serve)}
	c.nowFn = func() time.Time { return now }
	c.pauseFn = func(_ context.Context, duration time.Duration) error { now = now.Add(duration); return nil }
	return c, fixture
}
func TestSmokeAssertionsAgainstThriftFixtures(t *testing.T) {
	c, f := fixtureCheck(t, "smoke")
	if err := c.quota(); err != nil {
		t.Fatal(err)
	}
	if err := c.smoke(); err != nil {
		t.Fatal(err)
	}
	expected := []string{"batch-finished", "two-agent-service", "rolling-update", "service-killed", "quota-and-cron-api", "drain-empty-hosts-api"}
	cases := c.report["cases"].([]any)
	if len(cases) != len(expected) {
		t.Fatal(cases)
	}
	for i, name := range expected {
		if cases[i].(Object)["name"] != name {
			t.Fatal(cases)
		}
	}
	if f.calls[0] != "setQuota" || !f.killed {
		t.Fatal(f.calls)
	}
}
func TestRecoveryKeepsTaskAndPhysicalIdentityAcrossKeeperActions(t *testing.T) {
	c, _ := fixtureCheck(t, "recovery")
	if err := c.recovery(); err != nil {
		t.Fatal(err)
	}
	cases := c.report["cases"].([]any)
	if len(cases) != 3 {
		t.Fatal(cases)
	}
	for _, v := range cases {
		record := v.(Object)
		if record["afterGeneration"].(int64) <= record["beforeGeneration"].(int64) {
			t.Fatal(record)
		}
	}
}
func TestPolicyRetainsRollbackDrainAndManualCronAssertions(t *testing.T) {
	c, _ := fixtureCheck(t, "policy")
	if err := c.policy(); err != nil {
		t.Fatal(err)
	}
	cases := c.report["cases"].([]any)
	if len(cases) != 3 || cases[0].(Object)["executorRestored"] != true || cases[1].(Object)["name"] != "active-drain-replacement" || cases[2].(Object)["name"] != "manual-cron-finished" {
		t.Fatal(cases)
	}
}
func TestSoakRequiresFortyDistinctTasksAndTenMinutesOfSamples(t *testing.T) {
	c, _ := fixtureCheck(t, "soak")
	if err := c.soak(); err != nil {
		t.Fatal(err)
	}
	cases := c.report["cases"].([]any)
	if len(cases) != 1 {
		t.Fatal(cases)
	}
	record := cases[0].(Object)
	if record["elapsedSeconds"].(float64) != 600 || len(record["finishedTasks"].([]Task)) != 40 || record["samples"].(int) < 120 {
		t.Fatal(record)
	}
}

func TestChurnPhaseParsesWithoutMutatingLab(t *testing.T) {
	root := filepath.Join(t.TempDir(), "not-created")
	if err := Check(context.Background(), root, []string{"--root", root, "--phase", "churn", "--dry-run"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("dry run created lab", err)
	}
}

func TestChurnCrossesHistoryBoundaryOnBothAgents(t *testing.T) {
	c, f := fixtureCheck(t, "churn")
	if err := c.churn(); err != nil {
		t.Fatal(err)
	}
	cases := c.report["cases"].([]any)
	if len(cases) != 1 {
		t.Fatal(cases)
	}
	result := cases[0].(Object)
	if result["batches"] != 130 || len(result["finishedTasks"].([]Task)) != 260 || result["samples"] != 130 {
		t.Fatal(result)
	}
	if !reflect.DeepEqual(result["finishedPerHost"], map[string]int{"agent-a": 130, "agent-b": 130}) || !f.killed {
		t.Fatal(result)
	}
	creates := 0
	for _, method := range f.calls {
		if method == "createJob" {
			creates++
		}
	}
	if creates != 131 {
		t.Fatal("missing batch/service job", creates)
	}
}

func TestChurnRejectsFalseHistoryEvidenceAndCleansUp(t *testing.T) {
	for _, fault := range []string{"same-host", "reused-id", "service-id", "physical-pid"} {
		t.Run(fault, func(t *testing.T) {
			c, f := fixtureCheck(t, "churn")
			f.churnFault = fault
			if fault == "physical-pid" {
				runner := c.runner
				tops := 0
				c.runner = func(ctx context.Context, args ...string) (string, error) {
					result, err := runner(ctx, args...)
					if len(args) > 1 && args[1] == "top" {
						tops++
						if tops > 2 {
							result = strings.Replace(result, "100 /bin/sleep", "101 /bin/sleep", 1)
						}
					}
					return result, err
				}
			}
			if err := c.churn(); err == nil {
				t.Fatal("invalid churn evidence accepted")
			}
			if !f.killed || len(c.report["cases"].([]any)) != 0 {
				t.Fatal("failed churn was reported successful or not cleaned up")
			}
		})
	}
}
