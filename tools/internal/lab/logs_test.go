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
	"testing"
)

func TestLogFixtureUsesPrintableProcessArguments(t *testing.T) {
	config := logsJob("logs-fixture")
	executor := get(get(config, 6), 25)
	var spec struct {
		Argv []string `json:"argv"`
	}
	if err := json.Unmarshal([]byte(text(get(executor, 2))), &spec); err != nil {
		t.Fatal(err)
	}
	if len(spec.Argv) != 3 || spec.Argv[0] != "/bin/sh" || spec.Argv[1] != "-c" {
		t.Fatal("invalid log fixture", spec)
	}
	for _, arg := range spec.Argv {
		for _, ch := range arg {
			if ch < 32 || ch > 126 {
				t.Fatalf("process protocol rejects nonprintable argv: %q", arg)
			}
		}
	}
}
