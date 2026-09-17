//go:build linux

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package task

import "testing"

func TestGraphRunnerRefusesEphemeralIsolation(t *testing.T) {
	for _, c := range []struct {
		mounts string
		want   bool
	}{
		{"31 20 0:42 / /work rw,nosuid,nodev - tmpfs aurora-attempt rw,size=1024k", true},
		{"31 20 0:42 / /work rw - ext4 /dev/vdb rw", false},
		{"31 20 0:42 / /tmp rw - tmpfs tmpfs rw,size=1024k", false},
	} {
		if got := ephemeralIsolation(c.mounts); got != c.want {
			t.Fatalf("got %v for %q", got, c.mounts)
		}
	}
}
