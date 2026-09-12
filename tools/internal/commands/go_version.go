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
	"os/exec"
	"strings"
)

func goVersion(ctx context.Context, tool string, env []string) (string, error) {
	command := exec.CommandContext(ctx, tool, "version")
	command.Env = env
	data, err := command.Output()
	return strings.TrimSpace(string(data)), err
}
