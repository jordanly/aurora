//go:build linux

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package agent

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// IsolationOptions selects the enforced Linux process profile. The operator
// supplies a private agent mount namespace, an empty delegated cgroup subtree,
// and a trusted root filesystem. Network and PID namespaces remain shared.
type IsolationOptions struct {
	CgroupRoot string
	RootFS     string
	UIDBase    uint32
	UIDCount   uint32
	PidsMax    uint64
	WorkBytes  uint64
}

// IsolationRef is durable before any cgroup or mount is created. Task identities
// remain reserved until artifact retirement, including after terminal cleanup.
type IsolationRef struct {
	UID         uint32 `json:"uid"`
	Fingerprint string `json:"fingerprint"`
	CgroupRoot  string `json:"cgroupRoot"`
}
type isolationLaunch struct {
	RootFS, Work string
	UID          uint32
}

func (o IsolationOptions) fingerprint() string {
	b, _ := json.Marshal(o)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func rootOwnedDir(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return errors.New("isolation path must be absolute canonical non-root directory")
	}
	for p := path; p != "/"; p = filepath.Dir(p) {
		f, e := os.Lstat(p)
		if e != nil {
			return e
		}
		s, ok := f.Sys().(*syscall.Stat_t)
		if !f.IsDir() || f.Mode()&os.ModeSymlink != 0 || !ok || s.Uid != 0 || f.Mode().Perm()&0022 != 0 {
			return fmt.Errorf("isolation directory must be root-owned and not writable by other users: %s", p)
		}
	}
	return nil
}
func (o *IsolationOptions) validate() error {
	if o.UIDBase == 0 || o.UIDCount < MaxRetainedTickets || o.UIDCount > 65536 || uint64(o.UIDBase)+uint64(o.UIDCount) > uint64(^uint32(0)) || o.PidsMax < 16 || o.PidsMax > 65536 || o.WorkBytes < 1<<20 || o.WorkBytes > 1<<40 {
		return errors.New("invalid isolation UID range, pid limit or work size")
	}
	if os.Geteuid() != 0 {
		return errors.New("enforced isolation requires root in a dedicated agent namespace")
	}
	for _, p := range []string{o.RootFS, o.CgroupRoot} {
		if e := rootOwnedDir(p); e != nil {
			return e
		}
	}
	var fs unix.Statfs_t
	if e := unix.Statfs(o.CgroupRoot, &fs); e != nil {
		return e
	}
	if fs.Type != unix.CGROUP2_SUPER_MAGIC {
		return errors.New("isolation requires cgroup v2")
	}
	controllers, e := os.ReadFile(filepath.Join(o.CgroupRoot, "cgroup.subtree_control"))
	if e != nil {
		return e
	}
	have := map[string]bool{}
	for _, v := range strings.Fields(string(controllers)) {
		have[v] = true
	}
	for _, v := range []string{"cpu", "memory", "pids"} {
		if !have[v] {
			return fmt.Errorf("enforced isolation controller %s unavailable or not delegated", v)
		}
	}
	procs, e := os.ReadFile(filepath.Join(o.CgroupRoot, "cgroup.procs"))
	if e != nil {
		return e
	}
	if len(strings.TrimSpace(string(procs))) != 0 {
		return errors.New("isolation delegated subtree must contain no agent processes")
	}
	var h unix.CapUserHeader
	var caps [2]unix.CapUserData
	h.Version = unix.LINUX_CAPABILITY_VERSION_3
	if e = unix.Capget(&h, &caps[0]); e != nil {
		return e
	}
	for _, cap := range []uint{unix.CAP_DAC_OVERRIDE, unix.CAP_KILL, unix.CAP_SETUID, unix.CAP_SETGID, unix.CAP_SETPCAP, unix.CAP_SYS_CHROOT, unix.CAP_SYS_PTRACE, unix.CAP_SYS_ADMIN} {
		if caps[cap/32].Effective&(1<<(cap%32)) == 0 {
			return fmt.Errorf("enforced isolation requires capability %d", cap)
		}
	}
	for _, name := range []string{"work", "proc", "dev"} {
		if e = rootOwnedDir(filepath.Join(o.RootFS, name)); e != nil {
			return e
		}
	}
	return nil
}
func (r *Runtime) isolationIdentity(key string) (*IsolationRef, error) {
	if r.opts.Isolation == nil {
		return nil, nil
	}
	if r.opts.isolationRef != nil {
		if e := r.checkIsolation(r.opts.isolationRef); e != nil {
			return nil, e
		}
		return r.opts.isolationRef, nil
	}
	// Include cold terminal records: their identity remains reserved until GC.
	st, e := r.store.Inspect()
	if e != nil {
		return nil, e
	}
	reserved := make(map[uint32]bool)
	reserve := func(a Attempt) {
		if a.Execution != nil && a.Execution.Isolation != nil {
			reserved[a.Execution.Isolation.UID] = true
		}
	}
	for k, a := range st.Attempts {
		if k != key {
			reserve(a)
		}
	}
	if st.Retention != nil {
		for _, item := range st.Retention.Garbage {
			reserve(item.Attempt)
		}
	}
	sum := sha256.Sum256([]byte(key))
	start := binary.BigEndian.Uint32(sum[:4]) % r.opts.Isolation.UIDCount
	for offset := uint32(0); offset < r.opts.Isolation.UIDCount; offset++ {
		uid := r.opts.Isolation.UIDBase + (start+offset)%r.opts.Isolation.UIDCount
		if !reserved[uid] {
			return &IsolationRef{UID: uid, Fingerprint: r.opts.Isolation.fingerprint(), CgroupRoot: r.opts.Isolation.CgroupRoot}, nil
		}
	}
	return nil, errors.New("isolated task UID pool exhausted; retained attempts or artifact GC still own every identity")

}
func validIsolationKey(key string) bool {
	b, e := hex.DecodeString(key)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == key
}
func (r *Runtime) checkIsolation(ref *IsolationRef) error {
	if ref == nil {
		if r.opts.Isolation != nil {
			return errors.New("trusted-process attempt cannot be recovered in enforced profile")
		}
		return nil
	}
	if r.opts.Isolation == nil || ref.Fingerprint != r.opts.Isolation.fingerprint() || ref.CgroupRoot != r.opts.Isolation.CgroupRoot || ref.UID < r.opts.Isolation.UIDBase || uint64(ref.UID) >= uint64(r.opts.Isolation.UIDBase)+uint64(r.opts.Isolation.UIDCount) {
		return errors.New("persisted isolation profile changed; recovery refused")
	}
	return nil
}
func (r *Runtime) verifyIsolationObservation(previous, next *Execution) error {
	if next == nil {
		return errors.New("missing supervisor execution")
	}
	if e := r.checkIsolation(next.Isolation); e != nil {
		return e
	}
	if previous != nil && ((previous.Isolation == nil) != (next.Isolation == nil) || previous.Isolation != nil && *previous.Isolation != *next.Isolation) {
		return errors.New("supervisor isolation identity changed")
	}
	return nil
}

func cgroupPath(ref *IsolationRef, key string) (string, error) {
	if ref == nil || !validIsolationKey(key) {
		return "", errors.New("invalid isolated attempt identity")
	}
	if e := rootOwnedDir(ref.CgroupRoot); e != nil {
		return "", e
	}
	var fs unix.Statfs_t
	if e := unix.Statfs(ref.CgroupRoot, &fs); e != nil {
		return "", e
	}
	if fs.Type != unix.CGROUP2_SUPER_MAGIC {
		return "", errors.New("persisted isolation cgroup root is not cgroup v2")
	}
	return filepath.Join(ref.CgroupRoot, key), nil
}
func controlWrite(path, name, value string) error {
	f, e := os.OpenFile(filepath.Join(path, name), os.O_WRONLY|unix.O_NOFOLLOW, 0)
	if e != nil {
		return e
	}
	_, w := f.WriteString(value)
	return errors.Join(w, f.Close())
}
func (r *Runtime) prepareIsolation(key string, ref *IsolationRef, resources map[string]any) (*os.File, *os.File, error) {
	path, e := cgroupPath(ref, key)
	if e != nil {
		return nil, nil, e
	}
	if e = os.Mkdir(path, 0700); e != nil {
		return nil, nil, e
	}
	cpu, mem := resources["cpuMillis"].(uint64), resources["memoryBytes"].(uint64)
	if cpu == 0 || cpu > uint64(^uint64(0)>>1)/1000 || mem == 0 {
		return nil, nil, errors.New("invalid isolation resource limits")
	}
	for _, limit := range [][2]string{{"cpu.max", fmt.Sprintf("%d 1000000", cpu*1000)}, {"memory.max", strconv.FormatUint(mem, 10)}, {"memory.swap.max", "0"}, {"memory.oom.group", "1"}, {"pids.max", strconv.FormatUint(r.opts.Isolation.PidsMax, 10)}} {
		if e = controlWrite(path, limit[0], limit[1]); e != nil {
			return nil, nil, e
		}
	}
	// The mount belongs to the agent namespace, so daemon restart preserves task
	// scratch files. Confirmed terminal cleanup unmounts it; GC retries after crashes.
	work := filepath.Join(r.opts.Root, key, "work")
	if e = os.Mkdir(work, 0700); e != nil {
		return nil, nil, e
	}
	if e = unix.Mount("aurora-"+key, work, "tmpfs", unix.MS_NOSUID|unix.MS_NODEV, fmt.Sprintf("size=%d,mode=0700,uid=%d,gid=%d", r.opts.Isolation.WorkBytes, ref.UID, ref.UID)); e != nil {
		return nil, nil, e
	}
	cg, e := os.Open(path)
	if e != nil {
		return nil, nil, e
	}
	config, e := os.CreateTemp(filepath.Join(r.opts.Root, key), "isolation-")
	if e != nil {
		cg.Close()
		return nil, nil, e
	}
	os.Remove(config.Name())
	if e = json.NewEncoder(config).Encode(isolationLaunch{r.opts.Isolation.RootFS, work, ref.UID}); e == nil {
		_, e = config.Seek(0, 0)
	}
	if e != nil {
		cg.Close()
		config.Close()
		return nil, nil, e
	}
	return cg, config, nil
}
func isolationEmpty(ref *IsolationRef, key string) (bool, error) {
	path, e := cgroupPath(ref, key)
	if e != nil {
		return false, e
	}
	b, e := os.ReadFile(filepath.Join(path, "cgroup.events"))
	if os.IsNotExist(e) {
		_, err := os.Lstat(path)
		return os.IsNotExist(err), errIfPresent(err)
	}
	if e != nil {
		return false, e
	}
	for _, line := range strings.Split(string(b), "\n") {
		if line == "populated 0" {
			return true, nil
		}
		if line == "populated 1" {
			return false, nil
		}
	}
	return false, errors.New("missing cgroup populated state")
}
func errIfPresent(e error) error {
	if os.IsNotExist(e) {
		return nil
	}
	return e
}
func isolationKill(ref *IsolationRef, key string) (bool, error) {
	path, e := cgroupPath(ref, key)
	if e != nil {
		return false, e
	}
	if e = controlWrite(path, "cgroup.kill", "1"); e != nil && !os.IsNotExist(e) {
		return false, e
	}
	return isolationEmpty(ref, key)
}
func isolationFinish(ref *IsolationRef, key string) (bool, error) {
	empty, e := isolationKill(ref, key)
	if e != nil || !empty {
		return false, e
	}
	path, e := cgroupPath(ref, key)
	if e != nil {
		return false, e
	}
	e = os.Remove(path)
	if os.IsNotExist(e) {
		e = nil
	}
	return e == nil, e
}
func (r *Runtime) finishIsolation(key string, x *Execution) (bool, error) {
	if x.Isolation == nil {
		return groupEmpty(ProcessIdentity{x.PID, x.Start})
	}
	if e := r.checkIsolation(x.Isolation); e != nil {
		return false, e
	}
	until := time.Now().Add(time.Second)
	for {
		done, e := isolationFinish(x.Isolation, key)
		if done {
			e = cleanupIsolationArtifacts(r.opts.Root, key, Attempt{Execution: x})
			return e == nil, e
		}
		if e != nil || time.Now().After(until) {
			return false, e
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// cleanupIsolationArtifacts follows confirmed cgroup emptiness, either during
// terminal cleanup or as a retry from durable retention GC. Never use a lazy
// detach: uncertain mount cleanup must retain the reservation.
func cleanupIsolationArtifacts(root, key string, a Attempt) error {
	if a.Execution == nil || a.Execution.Isolation == nil {
		return nil
	}
	empty, e := isolationEmpty(a.Execution.Isolation, key)
	if e != nil {
		return e
	}
	if !empty {
		return errors.New("isolated cgroup still populated")
	}
	if !validIsolationKey(key) {
		return errors.New("invalid isolation artifact key")
	}
	parent := filepath.Join(root, key)
	if _, e := os.Lstat(parent); os.IsNotExist(e) {
		return nil
	}
	if e := rootOwnedDir(parent); e != nil {
		return e
	}
	work := filepath.Join(parent, "work")
	if f, e := os.Lstat(work); e == nil && (!f.IsDir() || f.Mode()&os.ModeSymlink != 0) {
		return errors.New("isolation work directory replaced")
	} else if e != nil && !os.IsNotExist(e) {
		return e
	}
	data, e := os.ReadFile("/proc/self/mountinfo")
	if e != nil {
		return e
	}
	mounted := false
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.Split(line, " - ")
		if len(parts) != 2 {
			continue
		}
		left, right := strings.Fields(parts[0]), strings.Fields(parts[1])
		if len(left) < 5 || len(right) < 2 {
			continue
		}
		mountPath := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(left[4])
		if strings.HasPrefix(mountPath, work+"/") {
			return errors.New("unexpected nested isolation work mount")
		}
		if mountPath == work {
			if right[0] != "tmpfs" || right[1] != "aurora-"+key {
				return errors.New("unowned isolation work mount")
			}
			mounted = true
		}
	}
	if !mounted {
		return nil
	}
	if e = unix.Unmount(work, 0); e != nil {
		return e
	}
	return nil
}

func enterIsolation(config *os.File) error {
	var p isolationLaunch
	d := json.NewDecoder(config)
	d.DisallowUnknownFields()
	if e := d.Decode(&p); e != nil {
		return e
	}
	config.Close()
	if p.UID == 0 {
		return errors.New("root task identity forbidden")
	}
	if e := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); e != nil {
		return e
	}
	if e := unix.Mount(p.RootFS, p.RootFS, "", unix.MS_BIND|unix.MS_REC, ""); e != nil {
		return e
	}
	if e := unix.MountSetattr(unix.AT_FDCWD, p.RootFS, unix.AT_RECURSIVE, &unix.MountAttr{Attr_set: unix.MOUNT_ATTR_RDONLY | unix.MOUNT_ATTR_NOSUID | unix.MOUNT_ATTR_NODEV}); e != nil {
		return e
	}
	if e := unix.Mount(p.Work, filepath.Join(p.RootFS, "work"), "", unix.MS_BIND, ""); e != nil {
		return e
	}
	if e := unix.Mount("proc", filepath.Join(p.RootFS, "proc"), "proc", unix.MS_RDONLY|unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC, ""); e != nil {
		return e
	}
	dev := filepath.Join(p.RootFS, "dev")
	if e := unix.Mount("tmpfs", dev, "tmpfs", unix.MS_NOSUID, "size=65536,mode=0755"); e != nil {
		return e
	}
	for _, name := range []string{"null", "zero", "random", "urandom"} {
		dst := filepath.Join(dev, name)
		f, e := os.OpenFile(dst, os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return e
		}
		f.Close()
		if e = unix.Mount("/dev/"+name, dst, "", unix.MS_BIND, ""); e != nil {
			return e
		}
	}
	if e := unix.MountSetattr(unix.AT_FDCWD, dev, unix.AT_RECURSIVE, &unix.MountAttr{Attr_set: unix.MOUNT_ATTR_RDONLY | unix.MOUNT_ATTR_NOSUID}); e != nil {
		return e
	}
	if e := unix.Chroot(p.RootFS); e != nil {
		return e
	}
	if e := os.Chdir("/work"); e != nil {
		return e
	}
	if e := unix.Prctl(unix.PR_CAP_AMBIENT, unix.PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0); e != nil {
		return e
	}
	for cap := uintptr(0); cap < 64; cap++ {
		if e := unix.Prctl(unix.PR_CAPBSET_DROP, cap, 0, 0, 0); e != nil && e != unix.EINVAL {
			return e
		}
	}
	if e := syscall.Setgroups([]int{}); e != nil {
		return e
	}
	if e := syscall.Setgid(int(p.UID)); e != nil {
		return e
	}
	if e := syscall.Setuid(int(p.UID)); e != nil {
		return e
	}
	h := unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	caps := [2]unix.CapUserData{}
	if e := unix.Capset(&h, &caps[0]); e != nil {
		return e
	}
	// setuid resets parent-death delivery. Re-arm before the readiness gate.
	if e := unix.Prctl(unix.PR_SET_PDEATHSIG, uintptr(unix.SIGKILL), 0, 0, 0); e != nil {
		return e
	}
	return unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)
}
