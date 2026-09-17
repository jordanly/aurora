// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lab

import "testing"

func TestHealthEvidenceRequiresFailureReasonAndRunningHistory(t *testing.T) {
	event := func(status int, message string) Object {
		return Object{"2": field("i32", status), "3": field("str", message)}
	}
	value := Object{"1": field("rec", Object{"1": field("str", "task-original")}),
		"2": field("i32", 4), "4": field("lst", []any{"rec", 1, event(4, "Go agent: failed: health-startup-timeout")})}
	if _, err := healthFailure([]any{value}, "health-startup-timeout", nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := healthFailure([]any{value}, "health-check-failed", nil, false); err == nil {
		t.Fatal("missing failure reason accepted")
	}
	if _, err := healthFailure([]any{value}, "health-startup-timeout", nil, true); err == nil {
		t.Fatal("missing RUNNING history accepted")
	}
	value["4"] = field("lst", []any{"rec", 2, event(2, "ready"), event(4, "Go agent: failed: health-check-failed")})
	if _, err := healthFailure([]any{value}, "health-check-failed", map[string]bool{"task-original": true}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := healthFailure([]any{value}, "health-check-failed", map[string]bool{"other": true}, true); err == nil {
		t.Fatal("replacement confused with original task")
	}
	if _, err := healthFailure([]any{value}, "health-check-failed", nil, false); err == nil {
		t.Fatal("startup failure became RUNNING")
	}
}
