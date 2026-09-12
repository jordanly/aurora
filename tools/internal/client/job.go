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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

var keyPart = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
var environmentName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

type JobKey struct {
	Role        string `json:"role"`
	Environment string `json:"environment"`
	Name        string `json:"name"`
}

func ParseKey(value string) (JobKey, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 3 {
		return JobKey{}, errors.New("job key must be role/environment/name")
	}
	key := JobKey{parts[0], parts[1], parts[2]}
	return key, key.Validate()
}
func (key JobKey) Validate() error {
	if !keyPart.MatchString(key.Role) || !keyPart.MatchString(key.Environment) || !keyPart.MatchString(key.Name) {
		return errors.New("job key components must match [a-z][a-z0-9-]{0,63}")
	}
	return nil
}
func (key JobKey) Wire() Struct {
	return Struct{"1": Field("str", key.Role), "2": Field("str", key.Environment), "3": Field("str", key.Name)}
}

type Resources struct {
	CPUMillis int64 `json:"cpuMillis"`
	RAMMB     int64 `json:"ramMb"`
	DiskMB    int64 `json:"diskMb"`
}

func (resources Resources) Validate(allowZero bool) error {
	minimum := int64(1)
	if allowZero {
		minimum = 0
	}
	if resources.CPUMillis < minimum || resources.CPUMillis > math.MaxInt32 ||
		resources.RAMMB < minimum || resources.RAMMB > (1<<53-1)/1048576 ||
		resources.DiskMB < minimum || resources.DiskMB > (1<<53-1)/1048576 {
		return errors.New("resources outside process profile: whole cpuMillis, ramMb and diskMb required")
	}
	return nil
}
func (resources Resources) Wire() []any {
	return Set("rec", Struct{"1": Field("dbl", float64(resources.CPUMillis)/1000)},
		Struct{"2": Field("i64", resources.RAMMB)}, Struct{"3": Field("i64", resources.DiskMB)})
}

type Process struct {
	Argv        []string          `json:"argv"`
	Env         map[string]string `json:"env"`
	GraceMillis int32             `json:"graceMillis"`
}
type Constraint struct {
	Name    string   `json:"name"`
	Values  []string `json:"values,omitempty"`
	Negated bool     `json:"negated,omitempty"`
	Limit   *int32   `json:"limit,omitempty"`
}
type UpdateSettings struct {
	BatchSize              int32 `json:"batchSize"`
	MaxPerInstanceFailures int32 `json:"maxPerInstanceFailures"`
	MaxFailedInstances     int32 `json:"maxFailedInstances"`
	MinRunningMillis       int32 `json:"minRunningMillis"`
	RollbackOnFailure      bool  `json:"rollbackOnFailure"`
}
type Job struct {
	Version             string            `json:"version"`
	Job                 JobKey            `json:"job"`
	Instances           int32             `json:"instances"`
	Service             bool              `json:"service"`
	User                string            `json:"user"`
	Priority            int32             `json:"priority"`
	MaxTaskFailures     int32             `json:"maxTaskFailures"`
	Tier                string            `json:"tier"`
	Resources           Resources         `json:"resources"`
	Process             Process           `json:"process"`
	Constraints         []Constraint      `json:"constraints"`
	ContactEmail        string            `json:"contactEmail,omitempty"`
	Metadata            map[string]string `json:"metadata,omitempty"`
	CronSchedule        string            `json:"cronSchedule,omitempty"`
	CronCollisionPolicy string            `json:"cronCollisionPolicy,omitempty"`
	Update              UpdateSettings    `json:"update"`
}

func LoadJob(path string, input io.Reader) (Job, error) {
	job := Job{Instances: 1, MaxTaskFailures: 1, Tier: "preferred", CronCollisionPolicy: "KILL_EXISTING",
		Constraints: []Constraint{},
		Process:     Process{Env: map[string]string{}, GraceMillis: 1000},
		Update:      UpdateSettings{BatchSize: 1, MinRunningMillis: 1000, RollbackOnFailure: true}}
	if err := ReadJSON(path, input, &job); err != nil {
		return Job{}, err
	}
	if job.User == "" {
		job.User = job.Job.Role
	}
	if job.Process.Env == nil {
		job.Process.Env = map[string]string{}
	}
	return job, job.Validate()
}

func (job Job) Validate() error {
	if job.Version != "aurora-job-v1" {
		return errors.New("job version must be aurora-job-v1")
	}
	if err := job.Job.Validate(); err != nil {
		return err
	}
	if job.Instances <= 0 || job.MaxTaskFailures < 0 || job.Tier == "" {
		return errors.New("instances must be positive, maxTaskFailures nonnegative and tier nonempty")
	}
	if err := job.Resources.Validate(false); err != nil {
		return err
	}
	p := job.Process
	if len(p.Argv) == 0 || len(p.Argv) > 64 || !strings.HasPrefix(p.Argv[0], "/") {
		return errors.New("process argv requires 1–64 strings and an absolute executable path")
	}
	validString := func(value string) bool {
		return utf8.ValidString(value) && len(utf16.Encode([]rune(value))) <= 4096 && !strings.ContainsRune(value, 0)
	}
	for _, arg := range p.Argv {
		if !validString(arg) {
			return errors.New("invalid process argument")
		}
	}
	for name, value := range p.Env {
		if !environmentName.MatchString(name) || !validString(value) {
			return fmt.Errorf("invalid environment entry %q", name)
		}
	}
	if p.GraceMillis < 0 || p.GraceMillis > 60000 {
		return errors.New("graceMillis must be 0–60000")
	}
	seen := map[string]bool{}
	for _, constraint := range job.Constraints {
		if constraint.Name == "" || seen[constraint.Name] {
			return errors.New("constraint names must be unique and nonempty")
		}
		seen[constraint.Name] = true
		if constraint.Limit != nil {
			if *constraint.Limit <= 0 || len(constraint.Values) > 0 || constraint.Negated {
				return errors.New("limit constraints require a positive limit and no values/negation")
			}
		} else {
			if len(constraint.Values) == 0 {
				return errors.New("value constraints require values")
			}
			values := map[string]bool{}
			for _, value := range constraint.Values {
				if value == "" || values[value] {
					return errors.New("constraint values must be unique and nonempty")
				}
				values[value] = true
			}
		}
	}
	if job.CronCollisionPolicy != "KILL_EXISTING" && job.CronCollisionPolicy != "CANCEL_NEW" {
		return errors.New("cronCollisionPolicy must be KILL_EXISTING or CANCEL_NEW")
	}
	if job.CronSchedule != "" && job.Service {
		return errors.New("cron jobs cannot be services")
	}
	u := job.Update
	if u.BatchSize <= 0 || u.MaxPerInstanceFailures < 0 || u.MaxFailedInstances < 0 || u.MinRunningMillis < 0 {
		return errors.New("invalid rolling update settings")
	}
	return nil
}

func boolean(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (job Job) Task() Struct {
	process, _ := json.Marshal(struct {
		Version string `json:"version"`
		Process
	}{"aurora-process-v1", job.Process})
	constraints := []any{}
	for _, c := range job.Constraints {
		var spec Struct
		if c.Limit != nil {
			spec = Struct{"2": Field("rec", Struct{"1": Field("i32", *c.Limit)})}
		} else {
			values := []any{}
			for _, value := range c.Values {
				values = append(values, value)
			}
			spec = Struct{"1": Field("rec", Struct{"1": Field("tf", boolean(c.Negated)), "2": Field("set", Set("str", values...))})}
		}
		constraints = append(constraints, Struct{"1": Field("str", c.Name), "2": Field("rec", spec)})
	}
	task := Struct{"28": Field("rec", job.Job.Wire()), "17": Field("rec", Struct{"2": Field("str", job.User)}),
		"7": Field("tf", boolean(job.Service)), "11": Field("i32", job.Priority), "13": Field("i32", job.MaxTaskFailures),
		"30": Field("str", job.Tier), "32": Field("set", job.Resources.Wire()), "20": Field("set", Set("rec", constraints...)),
		"25": Field("rec", Struct{"1": Field("str", "go-process"), "2": Field("str", string(process))}),
		"34": Field("rec", Struct{"1": Field("tf", 0)}),
		// Retained wire default is an empty MesosContainer. It is only a plain
		// process sentinel; the distribution has no Mesos library or executor.
		"29": Field("rec", Struct{"1": Field("rec", Struct{"2": Field("lst", Set("rec"))})})}
	if job.ContactEmail != "" {
		task["23"] = Field("str", job.ContactEmail)
	}
	if len(job.Metadata) > 0 {
		names := make([]string, 0, len(job.Metadata))
		for name := range job.Metadata {
			names = append(names, name)
		}
		sort.Strings(names)
		values := []any{}
		for _, name := range names {
			values = append(values, Struct{"1": Field("str", name), "2": Field("str", job.Metadata[name])})
		}
		task["27"] = Field("set", Set("rec", values...))
	}
	return task
}

func (job Job) Wire() Struct {
	collision := 0
	if job.CronCollisionPolicy == "CANCEL_NEW" {
		collision = 1
	}
	result := Struct{"9": Field("rec", job.Job.Wire()), "7": Field("rec", Struct{"2": Field("str", job.User)}),
		"5": Field("i32", collision), "6": Field("rec", job.Task()), "8": Field("i32", job.Instances)}
	if job.CronSchedule != "" {
		result["4"] = Field("str", job.CronSchedule)
	}
	return result
}

func (job Job) UpdateRequest() Struct {
	u := job.Update
	settings := Struct{"1": Field("i32", u.BatchSize), "2": Field("i32", u.MaxPerInstanceFailures),
		"3": Field("i32", u.MaxFailedInstances), "5": Field("i32", u.MinRunningMillis),
		"6": Field("tf", boolean(u.RollbackOnFailure)), "7": Field("set", Set("rec")), "8": Field("tf", 1)}
	return Struct{"1": Field("rec", job.Task()), "2": Field("i32", job.Instances), "3": Field("rec", settings)}
}
