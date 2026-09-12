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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const jobDocument = `{"version":"aurora-job-v1","job":{"role":"fixtures","environment":"test","name":"example"},"instances":2,"service":true,"resources":{"cpuMillis":600,"ramMb":32,"diskMb":32},"process":{"argv":["/bin/sleep","300"]}}`

func TestJobProfileAndWire(t *testing.T) {
	job, err := LoadJob("-", strings.NewReader(jobDocument))
	if err != nil {
		t.Fatal(err)
	}
	if job.Process.GraceMillis != 1000 || job.User != "fixtures" || !job.Update.RollbackOnFailure {
		t.Fatal("defaults", job)
	}
	// IDs below are the retained api.thrift contract, including the historical
	// empty-container sentinel required by GoTaskFactory.
	wire := job.Wire()
	if Get(wire, 8) != int32(2) || Get(Get(wire, 9), 3) != "example" {
		t.Fatal(wire)
	}
	task := Get(wire, 6)
	if Get(Get(task, 25), 1) != "go-process" || Get(Get(task, 34), 1) != 0 {
		t.Fatal(task)
	}
	var process map[string]any
	if err = json.Unmarshal([]byte(Get(Get(task, 25), 2).(string)), &process); err != nil {
		t.Fatal(err)
	}
	if process["version"] != "aurora-process-v1" || process["env"] == nil || process["graceMillis"] != float64(1000) {
		t.Fatal(process)
	}
	resources := Get(task, 32).([]any)
	if Get(resources[2], 1) != 0.6 || Get(resources[3], 2) != int64(32) || Get(resources[4], 3) != int64(32) {
		t.Fatal(resources)
	}
	if Get(Get(Get(task, 29), 1), 2).([]any)[1] != 0 {
		t.Fatal("container must have no volumes")
	}
	settings := Get(job.UpdateRequest(), 3)
	if Get(settings, 1) != int32(1) || Get(settings, 6) != 1 || Get(settings, 8) != 1 {
		t.Fatal(settings)
	}
}

func TestRejectExecutableAndAmbiguousConfiguration(t *testing.T) {
	invalid := []string{
		`Job(name='python')`, `null`, jobDocument + `{}`,
		strings.Replace(jobDocument, `"instances":2`, `"instances":2,"instances":3`, 1),
		strings.Replace(jobDocument, `"service":true`, `"service":true,"shell":"echo hi"`, 1),
		strings.Replace(jobDocument, `"cpuMillis":600`, `"cpuMillis":0.1`, 1),
		strings.Replace(jobDocument, `"cpuMillis":600`, `"cpuMillis":0`, 1),
		strings.Replace(jobDocument, `"name":"example"`, `"name":"../escape"`, 1),
		strings.Replace(jobDocument, `/bin/sleep`, `sleep`, 1),
		strings.Replace(jobDocument, `/bin/sleep`, `/bin/sl\u0000eep`, 1),
		strings.Replace(jobDocument, `"argv":`, `"graceMillis":60001,"argv":`, 1),
		strings.Replace(jobDocument, `"argv":`, `"env":{"bad-name":"x"},"argv":`, 1),
		strings.Replace(jobDocument, `"service":true`, `"service":true,"cronSchedule":"* * * * *"`, 1),
		strings.Replace(jobDocument, `"service":true`, `"service":true,"constraints":[{"name":"host","limit":1,"values":["x"]}]`, 1),
	}
	for i, data := range invalid {
		if _, err := LoadJob("-", strings.NewReader(data)); err == nil {
			t.Fatalf("accepted invalid document %d", i)
		}
	}
	var value any
	for _, data := range []string{strings.Repeat(" ", MaxBytes+1), strings.Repeat("[", 66) + "0" + strings.Repeat("]", 66)} {
		if Decode([]byte(data), &value) == nil {
			t.Fatal("accepted unbounded document")
		}
	}
}

func TestOriginalMutationArgumentContract(t *testing.T) {
	cases := []struct {
		args                   []string
		method                 string
		keyField, messageField int
	}{
		{[]string{"job", "kill", "fixtures/test/example"}, "killTasks", 4, 6},
		{[]string{"job", "restart", "fixtures/test/example", "0,2"}, "restartShards", 5, 0},
		{[]string{"cron", "start", "fixtures/test/example"}, "startCronJob", 4, 0},
		{[]string{"cron", "delete", "fixtures/test/example"}, "descheduleCronJob", 4, 0},
		{[]string{"update", "pause", "fixtures/test/example", "update-id"}, "pauseJobUpdate", 1, 3},
		{[]string{"update", "rollback", "fixtures/test/example", "update-id"}, "rollbackJobUpdate", 1, 2},
	}
	for _, test := range cases {
		method, args, _, err := command(test.args[0], test.args[1], test.args[2:], "audit", strings.NewReader(""))
		if err != nil || method != test.method {
			t.Fatalf("%v: %s %v", test.args, method, err)
		}
		key := Get(args, test.keyField)
		if test.args[0] == "update" {
			if Get(key, 2) != "update-id" {
				t.Fatal(key)
			}
			key = Get(key, 1)
		}
		if Get(key, 1) != "fixtures" || Get(key, 2) != "test" || Get(key, 3) != "example" {
			t.Fatal(key)
		}
		if test.messageField != 0 && Get(args, test.messageField) != "audit" {
			t.Fatal(args)
		}
	}
	for _, args := range [][]string{{"job", "restart", "fixtures/test/example"}, {"job", "kill", "fixtures/test/example", "0,0"}, {"job", "add", "fixtures/test/example", "0", "0"}, {"hosts", "drain"}} {
		if _, _, _, err := command(args[0], args[1], args[2:], "", nil); err == nil {
			t.Fatal("accepted", args)
		}
	}
}

func TestTransportReplyAndNoMutationRetries(t *testing.T) {
	for _, outcome := range []string{"ok", "sequence", "method", "exception", "rejected", "large", "duplicate", "redirect", "unavailable"} {
		t.Run(outcome, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "POST" || r.URL.Path != "/api" || r.Header.Get("Content-Type") != "application/x-thrift" {
					t.Error("invalid API request")
				}
				raw, _ := io.ReadAll(r.Body)
				var request []any
				if err := Decode(raw, &request); err != nil {
					t.Error(err)
				}
				if !bytes.Equal(bytes.TrimSpace(raw), raw) || bytes.Contains(raw, []byte(": ")) {
					t.Error("noncompact wire")
				}
				method, sequence, kind, code := request[1], request[3], 2, 1
				switch outcome {
				case "sequence":
					sequence = 999
				case "method":
					method = "differentMethod"
				case "exception":
					kind = 3
				case "rejected":
					code = 0
				case "large":
					fmt.Fprint(w, strings.Repeat("x", MaxBytes+1))
					return
				case "duplicate":
					fmt.Fprint(w, `[1,"createJob",2,1,{"0":{"rec":{"1":{"i32":1},"1":{"i32":1}}}}]`)
					return
				case "redirect":
					w.Header().Set("Location", "/other")
					w.WriteHeader(307)
					return
				case "unavailable":
					w.WriteHeader(503)
					return
				}
				json.NewEncoder(w).Encode([]any{1, method, kind, sequence, Struct{"0": Field("rec", Struct{"1": Field("i32", code)})}})
			}))
			defer server.Close()
			c, err := New(Connection{URL: server.URL, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer c.http.CloseIdleConnections()
			_, err = c.Call(context.Background(), "createJob", Struct{})
			if (err == nil) != (outcome == "ok") {
				t.Fatal("wrong outcome", outcome, err)
			}
			if calls.Load() != 1 {
				t.Fatal("mutation retried", calls.Load())
			}
		})
	}
}

func TestCLILocalValidationAndTaskSummary(t *testing.T) {
	var output, diagnostic bytes.Buffer
	if err := Run(context.Background(), []string{"job", "validate", "-"}, strings.NewReader(jobDocument), &output, &diagnostic); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"aurora-job-v1"`) {
		t.Fatal(output.String())
	}
	if err := Run(context.Background(), []string{"job", "create", "-"}, strings.NewReader(jobDocument), &output, &diagnostic); err == nil {
		t.Fatal("missing endpoint accepted")
	}
	// AssignedTask.instanceId is field6; field2 is the agent ID.
	summary, err := taskSummary(Struct{"3": Field("rec", Struct{"3": Field("rec", Struct{"1": Field("lst", Set("rec",
		Struct{"1": Field("rec", Struct{"1": Field("str", "task-a"), "2": Field("str", "agent-a"), "3": Field("str", "host-a"), "6": Field("i32", 7)}), "2": Field("i32", 2)}))})})})
	if err != nil || len(summary) != 1 || summary[0].(map[string]any)["instance"] != 7 {
		t.Fatal(summary)
	}
}

func TestConnectionRefusesAmbiguousOrInsecureTLSOptions(t *testing.T) {
	for _, config := range []Connection{{URL: "file:///tmp/socket", Timeout: time.Second}, {URL: "http://user:pass@localhost/api", Timeout: time.Second}, {URL: "http://localhost/wrong", Timeout: time.Second}, {URL: "http://localhost/api?q=x", Timeout: time.Second}, {URL: "http://localhost/api"}, {URL: "https://localhost/api", CertificateFile: "only-cert", Timeout: time.Second}} {
		if _, err := New(config); err == nil {
			t.Fatal("accepted", config.URL)
		}
	}
}

func TestExactConfigurationSchemaAndNullPolicy(t *testing.T) {
	cases := []struct{ old, replacement string }{
		{`"instances":2`, `"instances":2,"Instances":3`},
		{`"cpuMillis":600`, `"CpuMillis":600`},
		{`"name":"example"`, `"Name":"example"`},
		{`"argv":`, `"Argv":`},
		{`"instances":2`, `"instances":null`},
		{`"service":true`, `"service":null`},
		{`"argv":`, `"env":null,"argv":`},
		{`"argv":`, `"env":{"HOME":null},"argv":`},
		{`"argv":`, `"graceMillis":null,"argv":`},
		{`"service":true`, `"service":true,"update":null`},
		{`"service":true`, `"service":true,"constraints":null`},
		{`"service":true`, `"service":true,"constraints":[{"Name":"host","limit":1}]`},
		{`"service":true`, `"service":true,"constraints":[{"name":"host","limit":null}]`},
		{`"service":true`, `"service":true,"update":{"BatchSize":1}`},
	}
	for _, test := range cases {
		if _, err := LoadJob("-", strings.NewReader(strings.Replace(jobDocument, test.old, test.replacement, 1))); err == nil {
			t.Errorf("accepted %s", test.replacement)
		}
	}
	// Maps retain arbitrary case-sensitive keys; only struct field names are fixed.
	document := strings.Replace(jobDocument, `"service":true`,
		`"service":true,"metadata":{"Name":"one","name":"two"}`, 1)
	document = strings.Replace(document, `"argv":`, `"env":{"HOME":"/tmp","PATH":"/bin"},"argv":`, 1)
	job, err := LoadJob("-", strings.NewReader(document))
	if err != nil {
		t.Fatal(err)
	}
	if len(job.Metadata) != 2 || job.Process.Env["HOME"] != "/tmp" {
		t.Fatal(job)
	}
	// Local validated output remains a valid input, including empty collections.
	encoded, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = LoadJob("-", bytes.NewReader(encoded)); err != nil {
		t.Fatal("validated output failed reload", err)
	}
	var raw Struct
	if err = Decode([]byte(`{"1":{"str":null},"custom":{"Name":1,"name":2}}`), &raw); err != nil {
		t.Fatal("untyped RPC schema was restricted", err)
	}
}

func TestProcessLengthUsesJavaUTF16Units(t *testing.T) {
	for _, environment := range []bool{false, true} {
		for _, units := range []int{2048, 2049} {
			value, _ := json.Marshal(strings.Repeat("😀", units))
			document := strings.Replace(jobDocument, `"300"`, string(value), 1)
			if environment {
				document = strings.Replace(jobDocument, `"argv":`, `"env":{"VALUE":`+string(value)+`},"argv":`, 1)
			}
			_, err := LoadJob("-", strings.NewReader(document))
			if (err == nil) != (units == 2048) {
				t.Fatalf("environment=%v codepoints=%d: %v", environment, units, err)
			}
		}
	}
}

func TestTaskSummaryRejectsMalformedReplies(t *testing.T) {
	const task = `{"1":{"rec":{"1":{"str":"task-a"},"6":{"i32":0}}},"2":{"i32":0}}`
	const valid = `{"3":{"rec":{"3":{"rec":{"1":{"lst":["rec",1,` + task + `]}}}}}}`
	invalid := []string{
		`{}`, `{"3":{"rec":{}}}`,
		strings.Replace(valid, `"lst"`, `"set"`, 1),
		strings.Replace(valid, `["rec",1,`+task+`]`, `null`, 1),
		strings.Replace(valid, `["rec",1,`+task+`]`, `[]`, 1),
		strings.Replace(valid, `["rec",1`, `["str",1`, 1),
		strings.Replace(valid, `["rec",1`, `["rec",2`, 1),
		strings.Replace(valid, `["rec",1`, `["rec",1.0`, 1),
		strings.Replace(valid, task, `null`, 1),
		strings.Replace(valid, task, `{}`, 1),
		strings.Replace(valid, `"str":"task-a"`, `"i32":1`, 1),
		strings.Replace(valid, `"str":"task-a"`, `"str":1`, 1),
		strings.Replace(valid, `"str":"task-a"`, `"str":""`, 1),
		strings.Replace(valid, `"6":{"i32":0}`, `"6":{"i32":-1}`, 1),
		strings.Replace(valid, `"6":{"i32":0}`, `"6":{"i32":2147483648}`, 1),
		strings.Replace(valid, `"6":{"i32":0}`, `"3":{"i32":5},"6":{"i32":0}`, 1),
		strings.Replace(valid, `"2":{"i32":0}`, `"2":{"i32":99}`, 1),
	}
	for _, document := range invalid {
		var response Struct
		if err := Decode([]byte(document), &response); err != nil {
			t.Fatal("bad test fixture", err, document)
		}
		if _, err := taskSummary(response); err == nil {
			t.Error("accepted malformed task reply", document)
		}
	}
	for _, document := range []string{valid, strings.Replace(valid, `["rec",1,`+task+`]`, `["rec",0]`, 1)} {
		var response Struct
		if err := Decode([]byte(document), &response); err != nil {
			t.Fatal(err)
		}
		if _, err := taskSummary(response); err != nil {
			t.Fatal("rejected valid pending/empty reply", err)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[1,"getTasksStatus",2,1,{"0":{"rec":{"1":{"i32":1}}}}]`)
	}))
	defer server.Close()
	var output, diagnostic bytes.Buffer
	err := Run(context.Background(), []string{"--scheduler", server.URL, "job", "status", "fixtures/test/example"},
		strings.NewReader(""), &output, &diagnostic)
	if err == nil || output.Len() != 0 {
		t.Fatalf("malformed successful API response emitted success: %s %v", output.String(), err)
	}
}
