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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"aurora.local/tools/internal/bootstrap"
	"aurora.local/tools/internal/commands"
)

const Label = "org.apache.aurora.inplace"
const BaseImage = "debian:bookworm-slim@sha256:6bd27d44e6c32a66bbd72d7cb2b76a8ae3497ec2e5274a81abd1b37f6013fa1f"

var roles = []string{"agent-a", "agent-b", "scheduler"}
var idPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var tokenPattern = regexp.MustCompile(`^[a-f0-9]{16}$`)

type Mount struct {
	Source, Destination string
	RW                  bool
}

func (m Mount) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{m.Source, m.Destination, m.RW})
}
func (m *Mount) UnmarshalJSON(data []byte) error {
	var fields []json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if len(fields) != 3 {
		return errors.New("invalid mount tuple")
	}
	if err := json.Unmarshal(fields[0], &m.Source); err != nil {
		return err
	}
	if err := json.Unmarshal(fields[1], &m.Destination); err != nil {
		return err
	}
	return json.Unmarshal(fields[2], &m.RW)
}

type NetworkRecord struct {
	Name string  `json:"name"`
	ID   *string `json:"id"`
}
type ContainerRecord struct {
	ID     *string  `json:"id"`
	Name   string   `json:"name"`
	Mounts []Mount  `json:"mounts"`
	Args   []string `json:"args"`
}
type RefreshRecord struct {
	OldSHA string `json:"oldSha256"`
	NewSHA string `json:"newSha256"`
	Staged string `json:"staged"`
}
type Data struct {
	Schema          int                         `json:"schema"`
	Token           string                      `json:"token"`
	UID             int                         `json:"uid"`
	GID             int                         `json:"gid"`
	Image           string                      `json:"image"`
	Prefix          string                      `json:"prefix"`
	Containers      map[string]*ContainerRecord `json:"containers"`
	Network         *NetworkRecord              `json:"network"`
	SourceHashes    map[string]string           `json:"sourceHashes"`
	Binaries        map[string]map[string]any   `json:"binaries"`
	JDK             map[string]any              `json:"jdk"`
	DistributionSHA string                      `json:"schedulerDistributionSha256"`
	ArtifactsSHA    string                      `json:"artifactsSha256,omitempty"`
	RefreshPending  *RefreshRecord              `json:"refreshPending,omitempty"`
	RefreshHistory  []*RefreshRecord            `json:"refreshHistory,omitempty"`
}
type DockerObject struct {
	ID       string `json:"Id"`
	Name     string
	Image    string
	Labels   map[string]string
	Internal bool
	Driver   string
	Config   struct {
		Labels     map[string]string
		User       string
		Entrypoint []string
		Cmd        []string
	}
	HostConfig struct {
		Privileged     bool
		ReadonlyRootfs bool
		CapDrop        []string
		NetworkMode    string
		PortBindings   map[string]json.RawMessage
		PidMode        string
		SecurityOpt    []string
	}
	Mounts []struct {
		Type, Source, Destination string
		RW                        bool
	}
	State           struct{ Running bool }
	NetworkSettings struct {
		Networks map[string]struct{ IPAddress string }
	}
}

// Runner is injected by fixture tests; real operations are explicit argument
// arrays, bounded in duration/output, and never use a shell for Docker control.
type Runner func(context.Context, ...string) (string, error)
type limitBuffer struct {
	data     []byte
	limit    int
	overflow bool
}

func (b *limitBuffer) Write(p []byte) (int, error) {
	n := len(p)
	room := b.limit - len(b.data)
	if n > room {
		b.overflow = true
		p = p[:room]
	}
	b.data = append(b.data, p...)
	return n, nil
}
func run(ctx context.Context, args ...string) (string, error) {
	if len(args) == 0 {
		return "", errors.New("empty command")
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, args[0], args[1:]...)
	var output = limitBuffer{limit: 16 << 20}
	var stderr = limitBuffer{limit: 2000}
	command.Stdout = &output
	command.Stderr = &stderr
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error { return syscall.Kill(-command.Process.Pid, syscall.SIGKILL) }
	if err := command.Run(); err != nil {
		return "", fmt.Errorf("command failed %s: %s: %w", args[0], stderr.data, err)
	}
	if output.overflow {
		return "", errors.New("command output limit")
	}
	return strings.TrimSpace(string(output.data)), nil
}
func Safe(path string) (string, error) {
	if !filepath.IsAbs(path) || strings.Contains(path, ",") {
		return "", errors.New("absolute safe path required")
	}
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		if part == ".." {
			return "", errors.New("parent path rejected")
		}
	}
	return bootstrap.CheckPath(path)
}
func require(value bool, message string) error {
	if !value {
		return errors.New(message)
	}
	return nil
}
func randomHex(size int) (string, error) {
	bytes := make([]byte, size)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
func readJSON(path string, value any) error {
	if _, err := Safe(path); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("lab JSON must be a regular file")
	}
	return bootstrap.ReadJSON(path, value)
}
func save(root string, data any) error { return saveJSON(filepath.Join(root, "lab.json"), data) }
func saveJSON(path string, data any) error {
	if _, err := Safe(path); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	f, err := os.CreateTemp(filepath.Dir(path), ".record-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(encoded); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func load(root string) (Data, error) {
	var data Data
	if _, err := Safe(root); err != nil {
		return data, err
	}
	info, err := os.Stat(root)
	if err != nil {
		return data, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Getuid() || info.Mode().Perm()&0077 != 0 || !info.IsDir() {
		return data, errors.New("private owned lab root required")
	}
	var raw map[string]json.RawMessage
	if err = readJSON(filepath.Join(root, "lab.json"), &raw); err != nil {
		return data, err
	}
	for _, key := range []string{"schema", "token", "uid", "gid", "image", "prefix", "containers", "network", "sourceHashes", "binaries", "jdk", "schedulerDistributionSha256"} {
		if _, ok := raw[key]; !ok {
			return data, errors.New("incomplete lab ownership record")
		}
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return data, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&data); err != nil {
		return data, err
	}
	for role, record := range data.Containers {
		if record == nil || record.Name == "" || len(record.Args) == 0 || record.Mounts == nil {
			return data, fmt.Errorf("incomplete container ownership record: %s", role)
		}
	}
	if data.Schema != 1 || data.UID != os.Getuid() || !tokenPattern.MatchString(data.Token) || data.Prefix != "aurora-inplace-"+data.Token || data.Containers == nil {
		return data, errors.New("invalid lab ownership record")
	}
	return data, nil
}
func lock(root string) (func(), error) {
	path, err := Safe(filepath.Join(root, ".lock"))
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		file.Close()
		return nil, errors.New("invalid lab lock")
	}
	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, err
	}
	return func() { syscall.Flock(int(file.Fd()), syscall.LOCK_UN); file.Close() }, nil
}
func Inspect(ctx context.Context, runner Runner, data *Data, role string, network bool) (DockerObject, error) {
	var item DockerObject
	var id *string
	var name string
	if network {
		if data.Network == nil {
			return item, errors.New("unknown network preserved")
		}
		id = data.Network.ID
		name = data.Network.Name
	} else {
		record := data.Containers[role]
		if record == nil {
			return item, errors.New("unknown container preserved")
		}
		id = record.ID
		name = record.Name
	}
	if id == nil || !idPattern.MatchString(*id) {
		return item, errors.New("unknown creation identity; resource preserved")
	}
	args := []string{"docker", "inspect", *id}
	if network {
		args = []string{"docker", "network", "inspect", *id}
	}
	raw, err := runner(ctx, args...)
	if err != nil {
		return item, err
	}
	var items []DockerObject
	if err = json.Unmarshal([]byte(raw), &items); err != nil {
		return item, err
	}
	if len(items) != 1 {
		return item, errors.New("ambiguous Docker inspection")
	}
	item = items[0]
	// A missing false-valued boundary must not be mistaken for a verified false.
	if !network {
		var records []map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &records); err != nil {
			return item, err
		}
		var config, host map[string]json.RawMessage
		if err := json.Unmarshal(records[0]["Config"], &config); err != nil {
			return item, err
		}
		if err := json.Unmarshal(records[0]["HostConfig"], &host); err != nil {
			return item, err
		}
		for _, key := range []string{"Privileged", "ReadonlyRootfs", "CapDrop", "NetworkMode", "SecurityOpt"} {
			if _, ok := host[key]; !ok {
				return item, errors.New("missing sandbox inspection field")
			}
		}
		for _, key := range []string{"Labels", "User", "Entrypoint", "Cmd"} {
			if _, ok := config[key]; !ok {
				return item, errors.New("missing container configuration field")
			}
		}
		var mounts []map[string]json.RawMessage
		if err := json.Unmarshal(records[0]["Mounts"], &mounts); err != nil {
			return item, err
		}
		for _, mount := range mounts {
			var kind string
			if err := json.Unmarshal(mount["Type"], &kind); err != nil {
				return item, err
			}
			if kind == "bind" {
				for _, key := range []string{"Source", "Destination", "RW"} {
					if _, ok := mount[key]; !ok {
						return item, errors.New("missing bind mount inspection field")
					}
				}
			}
		}
	}
	labels := item.Config.Labels
	if network {
		labels = item.Labels
	}
	if item.ID != *id || labels[Label] != data.Token || labels[Label+".role"] != role || strings.TrimLeft(item.Name, "/") != name {
		return item, errors.New("ownership/name mismatch; resource preserved")
	}
	if network {
		if !item.Internal || item.Driver != "bridge" {
			return item, errors.New("network boundary mismatch")
		}
		return item, nil
	}
	hc := item.HostConfig
	if hc.Privileged || !hc.ReadonlyRootfs || !reflect.DeepEqual(hc.CapDrop, []string{"ALL"}) || data.Network == nil || hc.NetworkMode != data.Network.Name || len(hc.PortBindings) != 0 || hc.PidMode != "" || !reflect.DeepEqual(hc.SecurityOpt, []string{"no-new-privileges:true"}) || item.Config.User != fmt.Sprintf("%d:%d", data.UID, data.GID) {
		return item, errors.New("sandbox mismatch; resource preserved")
	}
	actual := []Mount{}
	for _, mount := range item.Mounts {
		if mount.Type == "bind" {
			actual = append(actual, Mount{mount.Source, mount.Destination, mount.RW})
		}
	}
	expected := append([]Mount{}, data.Containers[role].Mounts...)
	order := func(m []Mount) {
		sort.Slice(m, func(i, j int) bool {
			a, _ := json.Marshal(m[i])
			b, _ := json.Marshal(m[j])
			return bytes.Compare(a, b) < 0
		})
	}
	order(actual)
	order(expected)
	if !reflect.DeepEqual(actual, expected) {
		return item, errors.New("mount ownership mismatch; resource preserved")
	}
	if !reflect.DeepEqual(item.Config.Entrypoint, []string{"/opt/bin/cluster-helper"}) || !reflect.DeepEqual(item.Config.Cmd, data.Containers[role].Args) || item.Image != data.Image {
		return item, errors.New("command/image mismatch; resource preserved")
	}
	return item, nil
}
func digestTree(root string) (string, error) {
	if _, err := Safe(root); err != nil {
		return "", err
	}
	return commands.TreeDigest(root)
}
func copyFile(source, target string) error {
	if _, err := Safe(source); err != nil {
		return err
	}
	if _, err := Safe(target); err != nil {
		return err
	}
	input, err := os.OpenFile(source, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("artifact must be regular")
	}
	output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return err
	}
	defer output.Close()
	if _, err = io.Copy(output, input); err != nil {
		return err
	}
	return output.Close()
}
func copyTree(source, target string) error {
	if _, err := Safe(source); err != nil {
		return err
	}
	if _, err := os.Lstat(target); !os.IsNotExist(err) {
		return errors.New("new staging destination required")
	}
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if _, err := Safe(path); err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			return os.Mkdir(destination, info.Mode().Perm())
		}
		return copyFile(path, destination)
	})
}
func validateDistribution(root string) error {
	if _, err := Safe(root); err != nil {
		return err
	}
	info, err := os.Stat(filepath.Join(root, "bin/aurora-scheduler"))
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("original scheduler distribution required")
	}
	paths, err := filepath.Glob(filepath.Join(root, "lib/*.jar"))
	if err != nil {
		return err
	}
	own := false
	for _, path := range paths {
		if _, err := Safe(path); err != nil {
			return err
		}
		archive, err := zip.OpenReader(path)
		if err != nil {
			return err
		}
		for _, file := range archive.File {
			if file.Name == "org/apache/aurora/scheduler/app/SchedulerMain.class" {
				own = true
			}
			if strings.HasSuffix(file.Name, "/NativeSchedulerMain.class") || strings.HasSuffix(file.Name, "/NativeEngine.class") {
				archive.Close()
				return errors.New("replacement scheduler class refused")
			}
		}
		archive.Close()
	}
	return require(own, "original SchedulerMain absent from distribution")
}
