// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeveloperGoRejectsUnsafeOutputBeforeBootstrap(t *testing.T) {
	root := t.TempDir()
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "requires --output"},
		{[]string{"--output", filepath.Join(root, "tools/client")}, "outside its source tree"},
		{[]string{"unexpected"}, "unexpected"},
	} {
		if err := DeveloperGo(context.Background(), root, "build-client", tc.args); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%v: %v", tc.args, err)
		}
	}
	link := filepath.Join(root, "alias")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	if err := DeveloperGo(context.Background(), root, "build-client", []string{"--output", filepath.Join(link, "client")}); err == nil {
		t.Fatal("accepted output symlink ancestor")
	}
	if _, err := os.Stat(filepath.Join(root, ".cache")); !os.IsNotExist(err) {
		t.Fatal("invalid output started bootstrap")
	}
}
