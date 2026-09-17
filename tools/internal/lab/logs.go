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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

type logPage struct {
	TaskID     string `json:"taskId"`
	Stream     string `json:"stream"`
	Offset     int64  `json:"offset"`
	NextOffset int64  `json:"nextOffset"`
	HasMore    bool   `json:"hasMore"`
	Truncated  bool   `json:"truncated"`
	Complete   bool   `json:"complete"`
	Data       string `json:"data"`
}

func (c *check) logPage(taskID, stream string, offset int64) (logPage, error) {
	var page logPage
	path := strings.TrimSuffix(c.url, "/api") + "/tasklogs/" + url.PathEscape(taskID) + "/" + stream + fmt.Sprintf("?offset=%d&limit=65536", offset)
	request, err := http.NewRequestWithContext(c.ctx, http.MethodGet, path, nil)
	if err != nil {
		return page, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return page, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return page, err
	}
	if len(raw) > 1<<20 || response.StatusCode != 200 || response.Header.Get("Cache-Control") != "no-store" {
		return page, fmt.Errorf("invalid log proxy HTTP%d", response.StatusCode)
	}
	if err = json.Unmarshal(raw, &page); err != nil {
		return page, err
	}
	if page.TaskID != taskID || page.Stream != stream || page.Offset != offset || page.NextOffset < offset || page.NextOffset > offset+65536 || int64(len(page.Data)) != page.NextOffset-offset {
		return page, errors.New("log page identity/byte bounds mismatch")
	}
	return page, nil
}

// Qualifies retained output through the original scheduler and authenticated agent route.
func logsJob(name string) Object {
	config := job(name, false, 0, "")
	taskConfig := get(config, 6).(map[string]any)
	script := `printf '<stdout-marker>\n'; /usr/bin/head -c 1114112 /dev/zero | /usr/bin/tr '\000' x; printf '<stderr-marker>\n' >&2; /usr/bin/head -c 1114112 /dev/zero | /usr/bin/tr '\000' x >&2`
	process, _ := json.Marshal(Object{"version": "aurora-process-v1", "argv": []string{"/bin/sh", "-c", script}, "env": Object{}, "graceMillis": 1000})
	taskConfig["25"] = field("rec", Object{"1": field("str", "go-process"), "2": field("str", string(process))})
	return config
}

func (c *check) logs() error {
	name := c.prefix + "-logs"
	if _, err := c.rpc("createJob", Object{"1": field("rec", logsJob(name))}); err != nil {
		return err
	}
	finished, err := c.wait(name, func(xs []Task) bool { return len(xs) == 2 && all(xs, 3) }, 90*time.Second)
	if err != nil {
		return err
	}
	if hosts(finished) != 2 {
		return errors.New("log tasks must run on both agents")
	}
	for _, task := range finished {
		for _, stream := range []string{"stdout", "stderr"} {
			digest := sha256.New()
			offset := int64(0)
			pages := 0
			for {
				page, err := c.logPage(task.ID, stream, offset)
				if err != nil {
					return err
				}
				if !page.Complete || !page.Truncated || page.NextOffset <= offset {
					return errors.New("terminal/truncated log page expected")
				}
				if offset == 0 && !strings.HasPrefix(page.Data, "<"+stream+"-marker>\n") {
					return errors.New("stream marker missing or escaped as HTML")
				}
				digest.Write([]byte(page.Data))
				pages++
				offset = page.NextOffset
				if !page.HasMore {
					break
				}
				if pages >= 16 {
					return errors.New("agent log retention exceeded 1MiB")
				}
			}
			if offset != 1<<20 || pages != 16 {
				return fmt.Errorf("unexpected retained output: %d bytes in %d pages", offset, pages)
			}
			expected := "<" + stream + "-marker>\n"
			expected += strings.Repeat("x", (1<<20)-len(expected))
			sum := sha256.Sum256([]byte(expected))
			actual := hex.EncodeToString(digest.Sum(nil))
			if actual != hex.EncodeToString(sum[:]) {
				return errors.New("retained output hash mismatch")
			}
			end, err := c.logPage(task.ID, stream, offset)
			if err != nil || end.HasMore || end.Data != "" || end.NextOffset != offset {
				return errors.New("invalid end-of-log page")
			}
			c.add(Object{"name": "retained-output", "task": task, "stream": stream, "bytes": offset, "pages": pages, "sha256": actual, "truncated": true})
		}
	}
	for _, role := range roles {
		keeper := filepath.Join(c.root, role, "control/daemon.json")
		var before daemonState
		if err = readJSON(keeper, &before); err != nil {
			return err
		}
		if err = c.ownedAction(role, "restart"); err != nil {
			return err
		}
		after, err := waitDaemon(c.ctx, keeper, 90*time.Second, func(state daemonState) bool { return state.Generation > before.Generation && state.PID > 0 })
		if err != nil {
			return err
		}
		deadline := time.Now().Add(daemonRecoveryTimeout)
		for {
			allReadable := true
			for _, task := range finished {
				page, readErr := c.logPage(task.ID, "stdout", 0)
				if readErr != nil || !page.Complete || !strings.HasPrefix(page.Data, "<stdout-marker>\n") {
					allReadable = false
					break
				}
			}
			if allReadable {
				break
			}
			if time.Now().After(deadline) {
				return errors.New("retained output unavailable after daemon restart")
			}
			if err = c.pause(time.Second); err != nil {
				return err
			}
		}
		c.add(Object{"name": "retained-logs-after-restart", "node": role, "beforeGeneration": before.Generation, "afterGeneration": after.Generation})
	}
	_, err = c.churnHealth()
	return err
}
