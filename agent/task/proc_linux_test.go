//go:build linux

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package task

import (
	"fmt"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestProcExitDuringCleanupScan(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		gone bool
	}{
		{"before-open", &os.PathError{Op: "open", Path: "/proc/123/stat", Err: unix.ENOENT}, true},
		{"after-open", &os.PathError{Op: "read", Path: "/proc/123/stat", Err: unix.ESRCH}, true},
		{"wrapped-after-open", fmt.Errorf("identity: %w", &os.PathError{Op: "read", Path: "/proc/123/stat", Err: unix.ESRCH}), true},
		{"permission-unknown", &os.PathError{Op: "read", Path: "/proc/123/stat", Err: unix.EACCES}, false},
		{"io-unknown", &os.PathError{Op: "read", Path: "/proc/123/stat", Err: unix.EIO}, false},
		{"live", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := processDisappeared(tc.err); got != tc.gone {
				t.Fatalf("processDisappeared(%v)=%v, want %v", tc.err, got, tc.gone)
			}
		})
	}
}
