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
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"aurora.local/tools/internal/bootstrap"
)

type Options struct {
	Action, Root, Node, Distribution, Java, Helper, Agent string
	DryRun                                                bool
}

func parseCluster(repo string, args []string) (Options, error) {
	options := Options{Node: "agent-a"}
	actions := map[string]bool{"up": true, "status": true, "restart-scheduler": true, "restart-agent": true, "crash-scheduler": true, "crash-agent": true, "refresh-scheduler": true, "down": true}
	rest := []string{}
	valueFlags := map[string]bool{"--root": true, "-root": true, "--node": true, "-node": true, "--distribution": true, "-distribution": true, "--java": true, "-java": true, "--helper": true, "-helper": true, "--agent": true, "-agent": true}
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if valueFlags[arg] {
			rest = append(rest, arg)
			if index+1 < len(args) {
				index++
				rest = append(rest, args[index])
			}
			continue
		}
		if actions[arg] {
			if options.Action != "" {
				return options, errors.New("one cluster action required")
			}
			options.Action = arg
		} else {
			rest = append(rest, arg)
		}
	}
	f := flag.NewFlagSet("inplace-cluster", flag.ContinueOnError)
	f.StringVar(&options.Root, "root", "", "absolute private lab root")
	f.StringVar(&options.Node, "node", "agent-a", "agent-a or agent-b")
	f.StringVar(&options.Distribution, "distribution", filepath.Join(repo, ".cache/inplace-build/build/scheduler/install/aurora-scheduler"), "original scheduler distribution")
	f.StringVar(&options.Java, "java", filepath.Join(repo, ".cache/java03-tools/java/jdk-25.0.4.1+1"), "materialized Java25 tree")
	f.StringVar(&options.Helper, "helper", filepath.Join(repo, ".pi-tools/agent-original-integration/cluster-helper"), "verified helper binary")
	f.StringVar(&options.Agent, "agent", filepath.Join(repo, ".pi-tools/agent-original-integration/aurora-agent"), "verified agent binary")
	f.BoolVar(&options.DryRun, "dry-run", false, "render requested action without accessing Docker or changing files")
	if err := f.Parse(rest); err != nil {
		return options, err
	}
	if f.NArg() != 0 || options.Action == "" {
		return options, errors.New("one supported cluster action required")
	}
	if options.Node != "agent-a" && options.Node != "agent-b" {
		return options, errors.New("invalid agent node")
	}
	var err error
	options.Root, err = Safe(options.Root)
	return options, err
}

type clusterError struct{ error }

func (clusterError) ExitCode() int   { return 2 }
func (e clusterError) Unwrap() error { return e.error }
func Cluster(ctx context.Context, repo string, args []string) error {
	if err := cluster(ctx, repo, args, run); err != nil {
		return clusterError{err}
	}
	return nil
}
func cluster(ctx context.Context, repo string, args []string, runner Runner) error {
	options, err := parseCluster(repo, args)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if options.DryRun {
		data, _ := json.MarshalIndent(map[string]any{"dryRun": true, "action": options.Action, "root": options.Root, "node": options.Node, "roles": roles, "ownershipLabel": Label, "baseImage": BaseImage}, "", "  ")
		fmt.Println(string(data))
		return nil
	}
	var data Data
	if options.Action == "up" {
		data, err = prepare(ctx, repo, options, runner)
	} else {
		data, err = load(options.Root)
	}
	if err != nil {
		return err
	}
	unlock, err := lock(options.Root)
	if err != nil {
		return err
	}
	defer unlock()
	if options.Action != "up" {
		data, err = load(options.Root)
		if err != nil {
			return err
		}
	}
	switch options.Action {
	case "up":
		err = up(ctx, options.Root, &data, runner)
	case "down":
		err = down(ctx, options.Root, &data, runner)
	case "refresh-scheduler":
		err = refresh(ctx, options.Root, &data, options.Distribution, runner)
	case "status":
	default:
		action, target, _ := strings.Cut(options.Action, "-")
		role := options.Node
		if target == "scheduler" {
			role = "scheduler"
		}
		if _, err = Inspect(ctx, runner, &data, role, false); err == nil {
			err = request(options.Root, role, action, "owned request\n")
		}
	}
	if err != nil {
		return err
	}
	result := map[string]any{"root": options.Root, "containers": map[string]any{}}
	for role := range data.Containers {
		item, err := Inspect(ctx, runner, &data, role, false)
		if err != nil {
			return err
		}
		network, ok := item.NetworkSettings.Networks[data.Network.Name]
		if !ok {
			return errors.New("missing owned network attachment")
		}
		result["containers"].(map[string]any)[role] = map[string]any{"id": item.ID, "running": item.State.Running, "address": network.IPAddress}
	}
	encoded, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(encoded))
	return nil
}
func prepare(ctx context.Context, repo string, options Options, runner Runner) (Data, error) {
	var data Data
	root := options.Root
	if _, err := os.Lstat(root); !os.IsNotExist(err) {
		return data, errors.New("new lab root required; use status/down for existing lab")
	}
	for _, path := range []string{options.Distribution, options.Java, options.Helper, options.Agent} {
		if _, err := Safe(path); err != nil {
			return data, err
		}
		if _, err := os.Stat(path); err != nil {
			return data, err
		}
	}
	if err := validateDistribution(options.Distribution); err != nil {
		return data, err
	}
	sourceHashes := map[string]string{}
	binaries := map[string]map[string]any{}
	for role, source := range map[string]string{"agent": filepath.Join(repo, "agent"), "helper": filepath.Join(repo, "build-support/lab/fixtures/cluster-helper")} {
		digest, err := digestTree(source)
		if err != nil {
			return data, err
		}
		sourceHashes[role] = digest
	}
	for role, path := range map[string]string{"agent": options.Agent, "helper": options.Helper} {
		var metadata map[string]any
		if err := readJSON(path+".provenance.json", &metadata); err != nil {
			return data, err
		}
		digest, err := bootstrap.Digest(path)
		if err != nil {
			return data, err
		}
		if metadata["sourceSha256"] != sourceHashes[role] || metadata["binarySha256"] != digest {
			return data, fmt.Errorf("%s source/build provenance mismatch; rebuild current source", role)
		}
		binaries[role] = metadata
	}
	javaSHA, err := digestTree(options.Java)
	if err != nil {
		return data, err
	}
	releaseSHA, err := bootstrap.Digest(filepath.Join(options.Java, "release"))
	if err != nil {
		return data, err
	}
	release, err := os.ReadFile(filepath.Join(options.Java, "release"))
	if err != nil {
		return data, err
	}
	distributionSHA, err := digestTree(options.Distribution)
	if err != nil {
		return data, err
	}
	raw, err := runner(ctx, "docker", "image", "inspect", BaseImage)
	if err != nil {
		return data, err
	}
	var images []struct {
		ID string `json:"Id"`
	}
	if err = json.Unmarshal([]byte(raw), &images); err != nil || len(images) != 1 || images[0].ID == "" {
		return data, errors.New("pinned image missing")
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		return data, err
	}
	if err = os.Chmod(root, 0700); err != nil {
		return data, err
	}
	token, err := randomHex(8)
	if err != nil {
		return data, err
	}
	data = Data{Schema: 1, Token: token, UID: os.Getuid(), GID: os.Getgid(), Image: images[0].ID, Prefix: "aurora-inplace-" + token, Containers: map[string]*ContainerRecord{}, SourceHashes: sourceHashes, Binaries: binaries, JDK: map[string]any{"treeSha256": javaSHA, "releaseSha256": releaseSHA, "release": string(release)}, DistributionSHA: distributionSHA}
	if err = save(root, &data); err != nil {
		return data, err
	}
	artifacts := filepath.Join(root, "artifacts")
	if err = os.Mkdir(artifacts, 0700); err != nil {
		return data, err
	}
	for source, target := range map[string]string{options.Distribution: filepath.Join(artifacts, "scheduler"), options.Java: filepath.Join(artifacts, "java")} {
		if err = copyTree(source, target); err != nil {
			return data, err
		}
	}
	if err = os.Mkdir(filepath.Join(artifacts, "bin"), 0700); err != nil {
		return data, err
	}
	for source, name := range map[string]string{options.Helper: "cluster-helper", options.Agent: "aurora-agent"} {
		if err = copyFile(source, filepath.Join(artifacts, "bin", name)); err != nil {
			return data, err
		}
	}
	for path, expected := range map[string]string{filepath.Join(artifacts, "scheduler"): distributionSHA, filepath.Join(artifacts, "java"): javaSHA} {
		actual, err := digestTree(path)
		if err != nil {
			return data, err
		}
		if actual != expected {
			return data, errors.New("artifact changed during staging")
		}
	}
	for role, name := range map[string]string{"agent": "aurora-agent", "helper": "cluster-helper"} {
		digest, err := bootstrap.Digest(filepath.Join(artifacts, "bin", name))
		if err != nil {
			return data, err
		}
		if digest != binaries[role]["binarySha256"] {
			return data, errors.New("binary changed during staging")
		}
	}
	data.ArtifactsSHA, err = digestTree(artifacts)
	if err != nil {
		return data, err
	}
	if err = save(root, &data); err != nil {
		return data, err
	}
	for _, role := range roles {
		for _, sub := range []string{"state", "work", "control", "tls", "config"} {
			if err = os.MkdirAll(filepath.Join(root, role, sub), 0700); err != nil {
				return data, err
			}
		}
	}
	certs := filepath.Join(root, "certificates")
	if _, err = runner(ctx, filepath.Join(artifacts, "bin/cluster-helper"), "certgen", "--out", certs); err != nil {
		return data, err
	}
	password, err := randomHex(24)
	if err != nil {
		return data, err
	}
	passwordFile := filepath.Join(root, "scheduler/tls/password")
	if err = os.WriteFile(passwordFile, []byte(password), 0600); err != nil {
		return data, err
	}
	if _, err = runner(ctx, "openssl", "pkcs12", "-export", "-name", "scheduler", "-inkey", filepath.Join(certs, "scheduler.key"), "-in", filepath.Join(certs, "scheduler.crt"), "-certfile", filepath.Join(certs, "ca.crt"), "-out", filepath.Join(root, "scheduler/tls/keystore.p12"), "-passout", "file:"+passwordFile); err != nil {
		return data, err
	}
	if _, err = runner(ctx, filepath.Join(artifacts, "java/bin/keytool"), "-importcert", "-noprompt", "-alias", "ca", "-file", filepath.Join(certs, "ca.crt"), "-keystore", filepath.Join(root, "scheduler/tls/truststore.p12"), "-storetype", "PKCS12", "-storepass:file", passwordFile); err != nil {
		return data, err
	}
	nodes := []map[string]any{}
	for _, role := range roles[:2] {
		for source, destination := range map[string]string{"ca.crt": "ca.crt", role + ".crt": "cert.crt", role + ".key": "key.pem"} {
			if err = copyFile(filepath.Join(certs, source), filepath.Join(root, role, "tls", destination)); err != nil {
				return data, err
			}
		}
		config := map[string]any{"cluster": "inplace", "incarnation": "recovery-" + token, "node": role, "journal": "journal-" + role, "boot": "boot-" + role, "runtime": "runtime-" + role, "session": "initial", "schedulerEpoch": "0", "peer": "scheduler", "cpuMillis": 1000, "memoryBytes": 536870912}
		if err = saveJSON(filepath.Join(root, role, "config/agent.json"), config); err != nil {
			return data, err
		}
		nodes = append(nodes, map[string]any{"name": role, "url": "https://" + role + ":8443", "journal": config["journal"], "boot": config["boot"], "runtime": config["runtime"], "cpuMillis": 1000, "memoryBytes": 536870912, "diskMb": 1024})
	}
	config := map[string]any{"cluster": "inplace", "incarnation": "recovery-" + token, "database": "/state/adapter.db", "keyStore": "/tls/keystore.p12", "keyStorePassword": password, "trustStore": "/tls/truststore.p12", "trustStorePassword": password, "nodes": nodes}
	if err = saveJSON(filepath.Join(root, "scheduler/config/scheduler.json"), config); err != nil {
		return data, err
	}
	return data, nil
}
func containerPlan(root string, data *Data, role string) (*ContainerRecord, []string) {
	mounts := []Mount{{filepath.Join(root, "artifacts/bin"), "/opt/bin", false}}
	for _, sub := range []string{"state", "work", "control", "tls", "config"} {
		mounts = append(mounts, Mount{filepath.Join(root, role, sub), "/" + sub, sub == "state" || sub == "work" || sub == "control"})
	}
	var command []string
	if role == "scheduler" {
		mounts = append(mounts, Mount{filepath.Join(root, "artifacts/scheduler"), "/opt/scheduler", false}, Mount{filepath.Join(root, "artifacts/java"), "/opt/java", false})
		command = []string{"/opt/scheduler/bin/aurora-scheduler", "-cluster_name=inplace", "-serverset_path=/aurora/inplace", "-zk_in_proc=true", "-zk_endpoints=localhost:2181", "-go_agent_config=/config/scheduler.json", "-http_port=8081", "-http_authentication_mechanism=NONE", "-backup_interval=30secs", "-max_saved_backups=3"}
	} else {
		command = []string{"/opt/bin/aurora-agent", "serve", "--config", "/config/agent.json", "--state", "/state/state.db", "--work-root", "/work", "--network", "agent-container", "--listen", ":8443", "--tls-cert", "/tls/cert.crt", "--tls-key", "/tls/key.pem", "--tls-ca", "/tls/ca.crt", "--supervise"}
	}
	arguments := append([]string{"keeper", "--"}, command...)
	record := &ContainerRecord{Name: data.Prefix + "-" + role, Mounts: mounts, Args: arguments}
	argv := []string{"docker", "create", "--cidfile", filepath.Join(root, role+".cid"), "--name", record.Name, "--pull", "never", "--init", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true", "--user", fmt.Sprintf("%d:%d", data.UID, data.GID), "--network", data.Network.Name, "--network-alias", role, "--pids-limit", "256", "--memory", "1g", "--cpus", "2", "--tmpfs", "/tmp:rw,nosuid,nodev,exec,size=64m,mode=1777", "--label", Label + "=" + data.Token, "--label", Label + ".role=" + role, "--log-opt", "max-size=2m", "--log-opt", "max-file=2"}
	if role == "scheduler" {
		argv = append(argv, "--env", "JAVA_HOME=/opt/java", "--env", "JAVA_OPTS=-Xmx512m --enable-native-access=ALL-UNNAMED --illegal-native-access=deny")
	}
	for _, mount := range mounts {
		value := "type=bind,source=" + mount.Source + ",target=" + mount.Destination
		if !mount.RW {
			value += ",readonly"
		}
		argv = append(argv, "--mount", value)
	}
	argv = append(argv, "--entrypoint", "/opt/bin/cluster-helper", data.Image)
	argv = append(argv, arguments...)
	return record, argv
}
func up(ctx context.Context, root string, data *Data, runner Runner) error {
	name := data.Prefix + "-network"
	data.Network = &NetworkRecord{Name: name}
	if err := save(root, data); err != nil {
		return err
	}
	id, err := runner(ctx, "docker", "network", "create", "--internal", "--driver", "bridge", "--label", Label+"="+data.Token, "--label", Label+".role=network", name)
	if err != nil {
		return err
	}
	data.Network.ID = &id
	if err = save(root, data); err != nil {
		return err
	}
	if _, err = Inspect(ctx, runner, data, "network", true); err != nil {
		return err
	}
	for _, role := range roles {
		record, argv := containerPlan(root, data, role)
		data.Containers[role] = record
		if err = save(root, data); err != nil {
			return err
		}
		_, createErr := runner(ctx, argv...)
		cid, err := Safe(filepath.Join(root, role+".cid"))
		if err != nil {
			return err
		}
		if raw, err := os.ReadFile(cid); err == nil {
			value := strings.TrimSpace(string(raw))
			if !idPattern.MatchString(value) {
				return errors.New("invalid creation ID")
			}
			record.ID = &value
			if err = save(root, data); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		if createErr != nil {
			return createErr
		}
		if _, err = Inspect(ctx, runner, data, role, false); err != nil {
			return err
		}
		if _, err = runner(ctx, "docker", "start", *record.ID); err != nil {
			return err
		}
	}
	return nil
}
func down(ctx context.Context, root string, data *Data, runner Runner) error {
	for i := len(roles) - 1; i >= 0; i-- {
		role := roles[i]
		if record := data.Containers[role]; record != nil {
			if _, err := Inspect(ctx, runner, data, role, false); err != nil {
				return err
			}
			if _, err := runner(ctx, "docker", "rm", "-f", *record.ID); err != nil {
				return err
			}
			delete(data.Containers, role)
			if err := save(root, data); err != nil {
				return err
			}
		}
	}
	if data.Network != nil {
		if _, err := Inspect(ctx, runner, data, "network", true); err != nil {
			return err
		}
		if _, err := runner(ctx, "docker", "network", "rm", *data.Network.ID); err != nil {
			return err
		}
		data.Network = nil
		return save(root, data)
	}
	return nil
}
func request(root, role, action, message string) error {
	path, err := Safe(filepath.Join(root, role, "control", action))
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(message)
	return err
}

type daemonState struct {
	Generation int64 `json:"generation"`
	PID        int   `json:"pid"`
	Paused     bool  `json:"paused"`
}

func pause(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func waitDaemon(ctx context.Context, path string, duration time.Duration, predicate func(daemonState) bool) (daemonState, error) {
	deadline := time.Now().Add(duration)
	for {
		var state daemonState
		if err := readJSON(path, &state); err != nil {
			return state, err
		}
		if predicate(state) {
			return state, nil
		}
		if !time.Now().Before(deadline) {
			return state, errors.New("keeper transition not observed; evidence preserved")
		}
		if err := pause(ctx, 100*time.Millisecond); err != nil {
			return state, err
		}
	}
}
func within(path, parent string) bool {
	return path == parent || strings.HasPrefix(path, parent+string(os.PathSeparator))
}
func refresh(ctx context.Context, root string, data *Data, distribution string, runner Runner) error {
	if _, err := Inspect(ctx, runner, data, "scheduler", false); err != nil {
		return err
	}
	if err := validateDistribution(distribution); err != nil {
		return err
	}
	if within(distribution, root) || within(root, distribution) {
		return errors.New("refresh input must be outside lab")
	}
	digest, err := digestTree(filepath.Join(root, "artifacts"))
	if err != nil {
		return err
	}
	if digest != data.ArtifactsSHA {
		return errors.New("staged artifact drift; refresh refused")
	}
	expected, err := digestTree(distribution)
	if err != nil {
		return err
	}
	token, err := randomHex(8)
	if err != nil {
		return err
	}
	staged := filepath.Join(root, "refresh-"+token)
	if err = copyTree(distribution, staged); err != nil {
		return err
	}
	actual, err := digestTree(staged)
	if err != nil {
		return err
	}
	if actual != expected {
		return errors.New("distribution changed while copying")
	}
	control := filepath.Join(root, "scheduler/control")
	for _, action := range []string{"pause", "resume", "restart", "crash"} {
		if _, err = os.Lstat(filepath.Join(control, action)); !os.IsNotExist(err) {
			return errors.New("pending keeper action; refresh refused")
		}
	}
	var before daemonState
	if err = readJSON(filepath.Join(control, "daemon.json"), &before); err != nil {
		return err
	}
	data.RefreshPending = &RefreshRecord{data.DistributionSHA, expected, staged}
	if err = save(root, data); err != nil {
		return err
	}
	if err = request(root, "scheduler", "pause", "scheduler artifact refresh\n"); err != nil {
		return err
	}
	if _, err = waitDaemon(ctx, filepath.Join(control, "daemon.json"), 15*time.Second, func(state daemonState) bool {
		_, e := os.Stat(filepath.Join(control, "pause"))
		return state.Paused && state.PID == 0 && os.IsNotExist(e)
	}); err != nil {
		return err
	}
	if _, err = Inspect(ctx, runner, data, "scheduler", false); err != nil {
		return err
	}
	original := filepath.Join(root, "artifacts/scheduler")
	actual, err = digestTree(original)
	if err != nil {
		return err
	}
	if actual != data.DistributionSHA {
		return errors.New("scheduler distribution drift")
	}
	children, err := os.ReadDir(original)
	if err != nil {
		return err
	}
	for _, child := range children {
		if err = os.RemoveAll(filepath.Join(original, child.Name())); err != nil {
			return err
		}
	}
	children, err = os.ReadDir(staged)
	if err != nil {
		return err
	}
	for _, child := range children {
		if err = os.Rename(filepath.Join(staged, child.Name()), filepath.Join(original, child.Name())); err != nil {
			return err
		}
	}
	actual, err = digestTree(original)
	if err != nil {
		return err
	}
	if actual != expected {
		return errors.New("replacement verification failed; scheduler remains paused")
	}
	if err = os.Remove(staged); err != nil {
		return err
	}
	data.RefreshHistory = append(data.RefreshHistory, data.RefreshPending)
	data.RefreshPending = nil
	data.DistributionSHA = expected
	data.ArtifactsSHA, err = digestTree(filepath.Join(root, "artifacts"))
	if err != nil {
		return err
	}
	if err = save(root, data); err != nil {
		return err
	}
	if err = request(root, "scheduler", "resume", "scheduler artifact refresh complete\n"); err != nil {
		return err
	}
	_, err = waitDaemon(ctx, filepath.Join(control, "daemon.json"), 15*time.Second, func(state daemonState) bool {
		return !state.Paused && state.PID > 0 && state.Generation > before.Generation
	})
	return err
}
