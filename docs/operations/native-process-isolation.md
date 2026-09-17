# Enforced native process isolation

The native agent has an opt-in Linux process profile. Without the isolation
flags it retains the existing trusted-process behavior and rejects `hard-memory`.
With the profile enabled, every launched process receives its requested CPU and
memory limits; the agent admits the `hard-memory` capability only after startup
has checked its enforcement prerequisites. A missing controller or privilege
causes failure, never fallback to trusted execution.

Configure all of these operator-owned resources:

* `--isolation-cgroup-root`: an empty, exclusively delegated cgroup v2 subtree
  with `cpu`, `memory`, and `pids` already enabled in `cgroup.subtree_control`.
  The agent and supervisors remain outside the individual task cgroups.
* `--isolation-rootfs`: a trusted root filesystem with real `/work`, `/proc`,
  and `/dev` directories and the required executable files/libraries. The
  launcher mounts the tree recursively read-only. Do not include credentials,
  control sockets, other tasks' files, or host filesystem bind mounts. Rootfs
  population and executable selection are operator responsibilities; the agent
  does not fetch images.
* `--isolation-uid-base` and `--isolation-uid-count`: an exclusive task UID/GID
  pool (defaults 200000 and 65536), unused by host services or other agents.
  The count must be 1024–65536, covering the full retained-ticket bound.
  Allocation probes the bounded pool (at most 65536 IDs) and skips identities
  held by retained attempts or pending garbage collection. Exhaustion fails
  closed. IDs remain reserved through artifact retirement.
* `--isolation-pids-max`: task process/thread bound, default 128.
* `--isolation-work-bytes`: the size bound for the task's writable `/work`
  tmpfs, default 64 MiB. Work is ephemeral: it survives daemon loss while the
  supervisor owns the attempt, and disappears after confirmed terminal cleanup.

Run the root agent in a dedicated private mount namespace (for example a
purpose-built VM or container started with private mount propagation). All
runtime/rootfs/cgroup paths and their ancestors must be root-owned and not
writable by other users. Required capabilities are `DAC_OVERRIDE`, `KILL`,
`SETUID`, `SETGID`, `SETPCAP`, `SYS_CHROOT`, `SYS_PTRACE`, and `SYS_ADMIN`.
`SYS_PTRACE` lets the trusted supervisor prove ownership of task readiness
listeners after dropping the task UID. Do not expose a Docker socket to the
agent or its tasks. Budget the whole agent subtree as well as individual tasks.

Each task is created atomically inside its cgroup using `CLONE_INTO_CGROUP`,
before the launch gate can open. CPU uses `cpu.max`; memory uses `memory.max`
with swap disabled and group OOM killing; process counts use `pids.max`.
The helper creates private mount, IPC, and UTS namespaces, enters the rootfs,
sets the distinct task UID/GID, clears supplementary groups and all capabilities,
and sets no-new-privileges. Inherited authority descriptors close before the
workload exec. The task sees a read-only proc mount and only the standard
null/zero/random/urandom devices. Authoritative execution/supervisor journals,
TLS material, immutable specifications, and bounded stdout/stderr logs remain
outside the task filesystem.

This is the simple argv process profile. The standalone `execute-task` graph
runner is explicitly unsupported here because its persistent child journal
requires a separate volume contract. Network and PID namespaces remain shared
with the agent; this profile does not promise network isolation or conceal all
host process metadata. Distinct UIDs deny task-to-agent signaling and access to
protected proc descriptors. The existing TCP readiness and exact process-identity
checks continue to apply.

Recovery refuses changed isolation options or a downgrade to trusted execution.
Cleanup kills the entire task cgroup, including descendants that changed
sessions, proves it empty, removes the cgroup, and unmounts `/work` without a
lazy detach. Unconfirmed cleanup retains the reservation and is retried.
Terminal log viewing uses the separate retained outer logs. Durable artifact GC
also retries mount cleanup before deleting directories or releasing the UID.
Observed boot changes retain the existing fencing requirement; tmpfs contents
are not persistent across a reboot.

See the [isolated kernel testing guide](isolation-testing.md) for the disposable
VM procedure and evidence checks. For kernel qualification, compile the agent
test binary and run
`TestIsolationKernelEnforcement` in the dedicated privileged lab with
`AURORA_ISOLATION_TEST_ROOT` set to a root-owned path and
`AURORA_ISOLATION_TEST_CGROUP` set to the delegated subtree. The test checks CPU
throttling, memory OOM killing, writable-work exhaustion, read-only root and UID
boundaries, supervisor recovery, retained logs, and cleanup. Its normal host
run is skipped unless explicitly configured. See the kernel's
[cgroup v2 documentation](https://docs.kernel.org/admin-guide/cgroup-v2.html)
for controller and delegation semantics.
