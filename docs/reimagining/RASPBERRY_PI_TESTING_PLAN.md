# Raspberry Pi testing plan

Research: 2026-09-09 America/New_York / 2026-09-10 UTC. Execution host verified
before research: `raspberrypi`. Isolated remote checkout:
`/home/jordanly/.codex/worktrees/cb00/aurora`, initially clean and detached at the
requested `e3350f63d446cca17e8f1763ce30cc94346e64cf` (`codex/aurora-context` base).
[AURORA_CONTEXT.md](../../AURORA_CONTEXT.md) was read first. No applicable
`AGENTS.md` was found in the checkout or inspected parent directories.

**Status: source-grounded planning handoff with completed host preflight.** New
research stopped when the final account-usage check failed to return within the
bounded wait. The findings and runbook below were prepared before that stop.
No successful Aurora build, scheduler startup or real-agent execution is claimed.

## Recommendation: separate three experiments

Use this Pi as a small functional laboratory: two logical agents first, three
after smoke tests. Prefer implementing a process-based harness for the future
Go agents over reconstructing the entire native Mesos dependency stack. Use the
existing simulator to study scheduler behavior if that is the immediate priority.

| Track | Available today? | Evidence/next gate |
| --- | --- | --- |
| Host inspection, Python 3 probes, disposable namespaces | Yes; checks executed below | Limited OS primitives only |
| Legacy fake-cluster simulator | No | Gradle wrapper exits immediately because Java is missing; more prerequisites unresolved |
| Native Mesos + Aurora + multiple Mesos agents/Thermos | No | Native binaries/bindings absent; Docker inaccessible; kernel compatibility unverified |
| Mesos-free Java scheduler + Go agents | No | No tracked Go files/module or agent implementation at baseline; integration and persistence changes required |

Three processes on one Pi are not three physical failure domains. They share
power, kernel, memory, disk and clock. Process crashes and controlled transport
failures can be tested; machine-loss availability cannot be established here.

## Actual host findings and checks

| Property | Observed on the Pi |
| --- | --- |
| Hardware | Raspberry Pi 5 Model B Rev 1.0; four Cortex-A76 CPUs, maximum reported 2.4 GHz |
| OS/kernel | Debian GNU/Linux 13.6 trixie; `6.18.39+rpt-rpi-2712`, aarch64 |
| Page size | 16384 bytes (`getconf PAGESIZE`) |
| RAM/swap | 8062 MiB RAM, 6015 MiB available at sample; 2047 MiB swap, zero used |
| Storage | Checkout on `/dev/mmcblk0p2`, 111 GiB filesystem, 95 GiB available; `/tmp` is a 4 GiB tmpfs with 3.2 GiB available |
| Temperature | One reading: 58.4 °C; no sustained-load test |
| Python | `python` and `python3` execute Python 3.13.5; no Python 2/2.7 on PATH |
| Native tools | GCC 14.2.0 executes; g++, make, containerd, runc, unshare, systemd-run found |
| Missing on PATH | java, javac, go, gradle, thrift, node, npm, podman, Mesos binaries, ZooKeeper launcher, cmake, protoc |
| Conventional install paths | `/usr/lib/jvm` and `/usr/local/go` absent; no exhaustive disk search |
| Docker | Client executes: 29.8.0, build 88096ef; daemon socket access denied |
| Cgroups | Unified hierarchy, root controllers `cpuset cpu io pids`; memory absent from `/proc/cgroups`; boot command line contains `cgroup_disable=memory` |

Executed checks and outcomes:

1. `pwd`, `uname -a`, `hostname`, `git rev-parse HEAD` verified execution host and
   baseline. Initial working tree was clean.
2. `./gradlew --version` exited 1: `JAVA_HOME is not set and no 'java' command
   could be found in your PATH`. No dependency resolution or compilation occurred.
   Python 2 rejection is source-backed, not a second executed build failure.
3. `ss -ltn` failed in the tool sandbox (netlink permission), then succeeded in
   an approved read-only retry outside it. Existing listeners included 22, 1455,
   8080, 8123 and 21064. None were modified.
4. `docker info` failed with socket permission denied both inside and outside
   the sandbox. Server version, architecture, image availability and execution
   remain unverified. This does not prove the daemon is stopped. No sudo, socket
   permissions, group membership or daemon changes were attempted.
5. `unshare --user --map-root-user --net true` exited 0. This proves creation of
   a disposable namespace only, not functioning namespace networking or cgroups.
6. A Python `ExitStack` probe simultaneously bound IPv4 loopback sockets on
   2181, 5050–5053, 8081, 8083, 1338–1340, 18081, 19090, 19101–19103,
   20100, 20200 and 20300. Socket creation failed in the sandbox; the approved
   outside-sandbox retry passed. All sockets closed immediately. No server was
   started. This verifies these particular ports at one instant, not entire
   workload ranges or future availability.

All probe processes exited; no temporary background processes remain from this
task. No system packages, global toolchains, boot settings or services changed.
No image pulls, builds, load tests or destructive fault injection occurred.

## Source-backed behavior and ARM64 dependencies

### Legacy simulator

[build.gradle](../../build.gradle) points `run` at test output and
[LocalSchedulerMain](../../src/test/java/org/apache/aurora/scheduler/app/local/LocalSchedulerMain.java).
It substitutes fake driver/storage, fake executor paths and no-op snapshots,
and selects BASIC authentication with an example realm.
[ClusterSimulatorModule](../../src/test/java/org/apache/aurora/scheduler/app/local/simulator/ClusterSimulatorModule.java)
offers six fake workers in three racks; two are dedicated to `database`. Those
advertised resources do not consume equivalent Pi RAM.
[FakeMaster](../../src/test/java/org/apache/aurora/scheduler/app/local/FakeMaster.java)
synthesizes RUNNING without executing workloads, has unsupported operations and
reports FINISHED on its kill path. A passing simulator validates neither real
execution nor durability/failover.

Containment has two boundaries. The scheduler HTTP option is `-ip=127.0.0.1` in
[JettyServerModule](../../src/main/java/org/apache/aurora/scheduler/http/JettyServerModule.java);
otherwise HTTP binds all interfaces. The local main appends HTTP port 8081.
Despite its `localhost:2181` argument, the in-process ZooKeeper provider replaces
the endpoint with the server's actual ephemeral port.
[ZooKeeperTestServer](../../commons/src/main/java/org/apache/aurora/common/zookeeper/testing/ZooKeeperTestServer.java)
binds using `new InetSocketAddress(port)`, a wildcard address; see also
[ServiceDiscoveryModule](../../src/main/java/org/apache/aurora/scheduler/discovery/ServiceDiscoveryModule.java).
**Setting only HTTP loopback does not fully contain this simulator.** Run the
whole experiment in an isolated network namespace, or implement an explicit
embedded-ZooKeeper loopback-bind change before host-network use.

### Native Mesos

Topology: ZooKeeper + Aurora scheduler + Mesos master + two/three Mesos agents,
with executor/Thermos runner processes per task and optional observers. The
scheduler's replicated durable log also uses Mesos native code.
[Packer setup](../../build-support/packer/build.sh) fetches an explicitly
`py2.7-linux-x86_64.egg`; that cannot serve as an ARM64 executor artifact.
[make-mesos-native-egg](../../build-support/python/make-mesos-native-egg) is a
historical source-build specification, not a verified Pi recipe. Do not execute
Vagrant provisioning, which mutates host services and Mesos metadata.

Mesos retired in August 2025. Treat reconstruction as capped historical
compatibility work. [Apache Mesos Attic](https://attic.apache.org/projects/mesos.html)
The published native build prerequisites target older distributions; they do
not establish Mesos 1.6.1 compatibility with ARM64, GCC 14, 16 KiB pages or this
cgroup hierarchy. [Mesos building guide](https://mesos.apache.org/documentation/latest/building/)

### Mesos-free Go agents

No tracked Go source/module was found. A Go worker cannot connect to unchanged
Aurora merely by supplying a new executable. Implement scheduler registration,
capacity/placement integration, assignment/status transport, reconciliation and
an execution backend. Remove/abstract the native replicated-log dependency as
well as the driver. An initial in-memory local scheduler mode is useful, but
exclude it from durable scheduler-restart claims. Select and validate a durable
backend before advancing to that gate.

Agents need stable ID plus per-start incarnation, exclusive state-directory
locking, unique attempt IDs, durable launch/status journal, idempotent assignment
handling, status replay, process-tree supervision, named-port reservation, health
checks, bounded logs and kill escalation. Scheduler acknowledgements must follow
durable writes. Preserve process retries versus scheduler replacements and the
existing storage's lack of automatic local rollback.

### Prerequisite matrix

| Dependency | Simulator | Native Mesos | Future Go track |
| --- | --- | --- | --- |
| Java | Validated ARM64 JDK 8 environment | Same plus matching JNI/native library | Required while retaining existing Java scheduler/build |
| Gradle | Wrapper 4.10.2 | Same | Same until explicit build modernization |
| Python | CPython 2.7 for Thrift entity generation, even for Java build | Also native executor/client dependencies and development headers | Avoidable for new Go executor, still inherited by existing Java codegen |
| Thrift | Compiler/bindings 0.10.0 | Matching Java/Python schemas and stored checkpoints | Explicit protocol/schema generation; preserve storage compatibility deliberately |
| UI | Node 12.14.1, historical npm dependency graph | Same | Same unless separately replaced |
| Mesos | Java types; fake runtime avoids native driver/log | 1.6.1 master/agents, ARM64 libmesos and native Python egg | None only after both execution and log dependencies removed |
| C/C++ | May be needed by native prerequisite builds | make, C++, APR, SVN, curl, SASL, zlib, Python headers; autotools for Git builds | Depends on chosen storage/cgo bindings; native tooling for race builds |
| Go | Not required | Not required | Pinned Linux ARM64 toolchain, module sums |
| Isolation | Network namespace for embedded wildcard listener | First assess trusted process mode; cgroup/container mode separately | Trusted processes first, enforced cgroup/namespace isolation later |

Repository pins: `build.gradle`, `buildSrc/gradle.properties`, wrapper properties,
`pants.ini`, [3rdparty/python/BUILD](../../3rdparty/python/BUILD). The Java entity
generator explicitly rejects Python 3.

The official Node 12.14.1 manifest lists ARM64 Linux archives: artifact absence
is not an established blocker, but plugin architecture selection and execution
on this Pi remain untested. [Node manifest](https://nodejs.org/download/release/v12.14.1/SHASUMS256.txt)
Official Go downloads provide Linux ARM64 archives/checksums. Pin an exact version
at implementation time and unpack into a fresh checkout-local tools directory;
do not alter `/usr/local/go` or profiles. [Go downloads](https://go.dev/dl/),
[Go installation](https://go.dev/doc/install)

Record each artifact's URL, checksum, architecture, native library requirements
and actual version/minimal execution result. A filename or cross-build alone
does not prove execution on this 16 KiB-page kernel. Historical dependency
availability was not exhaustively probed.

## Resource budget, identity and isolation

The following are initial planning budgets, not measured Aurora consumption.
Run one track at a time, reserve roughly 3 GiB for existing services/headroom,
and stop if `MemAvailable` drops below 2 GiB or sustained swapping begins.

| Consumer | Proposed starting budget |
| --- | --- |
| Java scheduler | 768–1024 MiB maximum heap; allow roughly 1.5 GiB total process memory |
| ZooKeeper | 128–256 MiB heap plus overhead; embedded mode shares scheduler JVM, so do not double count |
| Mesos master | Plan 256 MiB RSS, then measure |
| Three native agents/runners | Plan 256 MiB each before workload memory; reduce to two if exceeded |
| Three Go agents | Target 128 MiB RSS each before workloads; not an established requirement |
| Workloads | Three small tasks, each requesting 0.1 CPU/64 MiB; initial per-worker capacity 0.5 CPU/256 MiB |
| Builds | Separate phase, 1–2 workers and roughly 3 GiB combined memory target; Mesos compile starts at `make -j1` |
| Disk | 5 GiB runtime/log ceiling; 10 GiB Java/Go cache allowance; native reconstruction separately budgeted at 20 GiB |

Placement must include executor overhead; measure before assuming a task fits a
256 MiB offer. Logical resource advertising is not physical enforcement. Do not
let every agent advertise the full Pi's RAM/CPU.

Memory cgroups are disabled here. Neither Docker `--memory` nor systemd
`MemoryMax` establishes enforcement without kernel support. OOM tests require a
separately approved host configuration change, controller availability/delegation
verification and a tiny isolated enforcement probe. Do not modify boot settings
for this planning task. [Docker resource constraints](https://docs.docker.com/engine/containers/resource_constraints/),
[Linux cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html)

Use trusted workloads under the current user initially. Different working
directories and mode 0700 prevent accidental mixing, not hostile same-UID access.
UID/mount/PID/cgroup isolation requires separate implementation. Never give tasks
the Docker socket. Keep state on the checkout filesystem: `/tmp` consumes RAM.

```text
.pi-lab/<run-id>/
  manifest.json, pids/, results/, logs/, tmp/
  scheduler/{state,backups,config}/
  zk/{data,log}/
  master/{work,log}/
  agent-1/{state,work,logs,tmp}/
  agent-2/{state,work,logs,tmp}/
  agent-3/{state,work,logs,tmp}/
```

Build caches belong under `.pi-tools/`; do not commit runtime state/caches. Lock
each state directory. Restart retains stable identity and state; a new worker
gets a fresh directory/ID. Never erase state as an ordinary recovery action.

| Service | Native/simulator | Future process harness |
| --- | --- | --- |
| Scheduler HTTP | 127.0.0.1:8081 | 127.0.0.1:18081 |
| Native libprocess | 127.0.0.1:8083 | Absent after Mesos removal |
| Master/worker transport | Mesos master 127.0.0.1:5050 | Scheduler 127.0.0.1:19090 |
| Coordination | Standalone ZK 127.0.0.1:2181; simulator ephemeral inside namespace | Omit only in explicit single-leader development mode |
| Agent APIs | 127.0.0.1:5051/5052/5053 | 127.0.0.1:19101/19102/19103 |
| Optional observers | 127.0.0.1:1338/1339/1340 | New logs/status API; no Thermos observer assumed |
| Workload ports | a1 20100–20199; a2 20200–20299; a3 20300–20399 | Same disjoint ranges |

Distinct logical hostnames do not permit sharing real ports in one network
namespace. Use explicit worker/rack labels for simulated placement and verify
legacy reported hostnames are distinct. These labels do not configure DNS.
All services and fixture workloads bind loopback; run simulator probes inside
its isolated namespace. UI forwarding, if needed later, must itself be loopback
only. Recheck all ports at launch; leave existing host listeners alone.

## Phased commands and validation gates

Commands below are future procedures except the preflight commands already
reported. Go paths/flags and harness tests are proposed contracts, not implemented
commands. Native templates require checking the actual pinned binaries' help.

### 0. Repeat preflight

```sh
cd /home/jordanly/.codex/worktrees/cb00/aurora
git rev-parse HEAD
git status --short
uname -a
cat /etc/os-release
getconf PAGESIZE
free -m
df -h . /tmp
cat /sys/fs/cgroup/cgroup.controllers
ss -ltn
./gradlew --version
```

Record each exit code separately. The last command currently fails without Java.
Gate: baseline, available resources and port allocation known; not build-ready.

### 1. Bounded legacy toolchain preparation

Supply verified ARM64 artifacts locally; inspect scripts and relocate caches
without changing global tools. Cap the first attempt at one generation target,
stop on the first dependency failure, and record failing URL/version/task.

```sh
# Only after these local prerequisites exist and execute correctly.
export JAVA_HOME="$PWD/.pi-tools/jdk8"
export PATH="$JAVA_HOME/bin:$PWD/.pi-tools/python2/bin:$PWD/.pi-tools/thrift/bin:$PATH"
export GRADLE_USER_HOME="$PWD/.pi-tools/gradle-cache"
export PANTS_HOME="$PWD/.pi-tools/pants-bootstrap"
./gradlew --no-daemon --max-workers=2 :api:generateThriftEntitiesJava
# Run only after generation passes:
./gradlew --no-daemon --max-workers=2 testClasses
```

Gate: exact versions recorded, generated API/entities and test helpers compile,
UI resources available. Neither command has passed here. Setting these variables
does not prove all legacy tools relocate all caches. Avoid full `build`, Vagrant,
Jenkins and e2e scripts for discovery. Focused Java tests may trigger suite-level
JaCoCo thresholds; distinguish coverage-policy failure from test failure.

### 2. Simulator in a disposable network namespace

Stage dependencies before disabling network access. Create a run-local Gradle
init file (proposed, not Gradle-validated here):

```groovy
gradle.projectsEvaluated {
  rootProject.tasks.getByName('run') {
    args '-ip=127.0.0.1'
    jvmArgs '-Xmx1024m', '-Djava.io.tmpdir=' + System.getenv('PI_LAB_TMP')
  }
}
```

```sh
export PI_LAB_ROOT="$PWD/.pi-lab/simulator-001"
mkdir -p "$PI_LAB_ROOT/tmp"
chmod 700 "$PI_LAB_ROOT"
export PI_LAB_TMP="$PI_LAB_ROOT/tmp"
# Save the init file above as $PI_LAB_ROOT/simulator.init.gradle.
# Requires ip; verify command -v ip before entering the namespace.
unshare --user --map-root-user --net bash
# Inside that new namespace shell:
ip link set lo up
./gradlew --offline --no-daemon --max-workers=2 \
  -I "$PI_LAB_ROOT/simulator.init.gradle" run
# Ctrl-C; verify all task-owned Java/Gradle children exit; exit the shell.
```

For automated API checks, implement a supervisor starting Gradle and probes
inside the same namespace, with signal traps and process-group cleanup. Namespace
creation alone was tested; the full launcher is unverified. Do not background
the example without cleanup supervision.

Gate: active scheduler, six fake workers, tiny trusted job reaches synthetic
RUNNING, impossible constraint stays PENDING with reason, three-instance update
converges, kill removes intended instances. Use explicit bundled realm/client
settings. Restart loses simulator state by design; it is not a persistence test.

### 3. Preferred Go-agent vertical slice: implementation required

Implement agent, scheduler backend/transport, fixture protocol and supervisor.
Publish actual flags and replace these proposed commands with tested ones:

```sh
# Only after a Go module and these packages exist.
export GOTOOLCHAIN=local
export GOCACHE="$PWD/.pi-tools/go-cache"
export GOMODCACHE="$PWD/.pi-tools/go-mod-cache"
GOMAXPROCS=2 go test -p 1 ./...
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
  -o .pi-tools/bin/aurora-agent ./cmd/aurora-agent
GOMAXPROCS=2 go test -p 1 -race ./internal/agent/...
```

The static build assumes no cgo dependency; revise if the chosen backend needs
one. Run focused race tests separately with native tooling and memory headroom.
They detect only exercised races. [Go race detector](https://go.dev/doc/articles/race_detector)

Proposed per-agent CLI contract:

```text
aurora-agent --id=pi-a1 --state-dir=<run>/agent-1/state
  --work-dir=<run>/agent-1/work --listen=127.0.0.1:19101
  --scheduler=127.0.0.1:19090 --ports=20100-20199
  --capacity-cpu=0.5 --capacity-memory-mib=256
```

a2/a3 use their own IDs/directories/endpoints. Java needs an implemented local
agent backend and explicit storage selection; no such scheduler flag exists at
this baseline. First gate: two registered agents run real processes, produce
logs/status, and terminate cleanly. Add third agent and three-instance service.
Each fixture writes agent ID, attempt ID, PID/start identity and assigned ports
to its own sandbox and binds its assigned loopback port. Require actual marker
files, distinct ports and correct exit status, not only scheduler RUNNING.

### 4. Optional native Mesos compatibility

Proceed only after actual ARM64 artifacts execute. Prefer a separately pinned
compatible ARM64 build environment over an open-ended Pi compile. The historical
stack's modern-kernel compatibility remains a gate, not an assumption.

```sh
# Foreground children managed by the planned supervisor, not system services.
# Validate these flags against Mesos 1.6.1 --help before running.
mesos-master --ip=127.0.0.1 --port=5050 \
  --work_dir="$PI_LAB_ROOT/master/work"
mesos-agent --master=127.0.0.1:5050 --ip=127.0.0.1 --port=5051 \
  --hostname=pi-a1 --work_dir="$PI_LAB_ROOT/agent-1/work" \
  --resources='cpus:0.5;mem:256;disk:1024;ports:[20100-20199]' \
  --containerizers=mesos --isolation=posix/cpu,posix/mem
```

Duplicate with a2/a3 hostnames, ports and work directories. POSIX mode is an
unverified low-privilege compatibility proposal, not hard resource isolation.
Stop if unsupported; do not silently switch to privileged containers. Trusted
jobs and executor configuration must agree on the current Unix user instead of
copying historical root/vagrant assumptions.

Standalone ZooKeeper needs run-local data/log paths and loopback client binding.
Adapt only required settings from the historical
[scheduler unit](../../examples/vagrant/systemd/aurora-scheduler.service):
`-ip=127.0.0.1`, `-http_port=8081`, loopback `LIBPROCESS_IP`, port 8083,
local native library/executor paths, `-mesos_master_address=127.0.0.1:5050`,
`-zk_endpoints=127.0.0.1:2181`, per-run ZooKeeper paths, native-log directory
and backups. Quorum size 1 is a single-scheduler laboratory setting only.
Initialize the native log once for a positively identified new empty cluster,
never as a restart/repair action. Do not copy debug listeners or global mounts.

Optional observer command, source-backed by
[thermos_observer.py](../../src/main/python/apache/aurora/tools/thermos_observer.py):

```sh
thermos_observer.pex --ip=127.0.0.1 --port=1338 \
  --mesos-root="$PI_LAB_ROOT/agent-1/work"
```

Use 1339/1340 and matching work roots for other agents. If Mesos disk collection
is enabled, point `--agent_api_url` at the matching agent. Thermos checkpoints
live inside executor sandboxes; never share a generic checkpoint directory among
agents. Gate: all distinct workers register, each runs a real Thermos task, ports
and observer logs agree, then restart tests pass. A Mesos sample-framework run
alone would not prove Aurora executor compatibility.

## Recovery and fault-test acceptance matrix

All application tests below are planned, not executed. Use small deterministic
fixtures, explicit deadlines, run IDs and event logs. Run smoke once, each focused
failure scenario three times, then at most a ten-minute mixed test with three
tiny active tasks. Stop on invariant failure instead of increasing load.

| Scenario | Pass condition and method | Track |
| --- | --- | --- |
| Placement/pending | Three logical workers used; impossible request has reason without busy-loop | All, with fake semantics acknowledged |
| Process retry | Fixture fails twice then succeeds; process retries distinct from task generations and capped | Real tracks |
| Kill escalation | TERM-ignoring child gets bounded KILL; descendants reaped, port released | Real tracks |
| Agent crash/restart | Kill only agent; retain state; reconcile surviving children or terminate them before replacement; no duplicate live attempt | Native validate actual behavior; Go implementation gate |
| Scheduler crash | Crash around durable assignment/status write; restart same state; committed intent survives and unacknowledged status replays | Native/future durable backend; not simulator |
| Duplicate launch/status | Retry lost response, duplicate/out-of-order events; no extra launch or state regression; reject conflicting payload | Future protocol, legacy comparison tests |
| Transport interruption | Test proxy/fault hook drops one stream while workload remains alive; reconnect, lease and incarnation rules hold | Future Go; native only in contained network setup |
| Kill during interruption | Kill intent survives disconnect and reconnect; no resurrection | Native partition semantics/future Go |
| Update recovery | One-at-a-time version change; scheduler crash mid-update; resume once, bad version rolls back | Durable real tracks; simulator tests live logic only |
| Drain/rejoin | No new placement on draining a1; replacements fit remaining budget; eligibility returns on rejoin | All with track limits |
| Identity/state collision | Second agent on same state/ID fails clearly without stealing live workloads | Future Go; native semantics independently checked |
| Storage error/corruption | Inject failed write or corrupt copied journal; no success acknowledgement of lost data; explicit recovery/failure | Future backend/native copy-only experiment |
| Disk/log ceiling | Fault hook or small bounded test filesystem hits ceiling; logs cap/rotate predictably | Real tracks; never fill host disk or tmpfs |
| Memory/OOM isolation | Deferred until memory cgroup support/delegation/enforcement proved; tiny task failure leaves scheduler healthy | Later enforced-isolation phase |
| CPU and port isolation | Verified CPU quota bounds tiny load; cross-agent port collision rejected, release follows actual child exit | Later real isolation mode |

An unreachable agent can still execute work. Explicitly choose lease termination,
fencing or tolerated overlap; a scheduler timeout cannot prove the old process
stopped. Do not claim exactly-once external effects. Do not use host-wide firewall
rules, power loss or remote-session disconnection as laboratory fault injection.

Legacy specifications to select later: `TaskStateMachineTest`,
`StateManagerImplTest`, `PartitionManagerTest`, `MesosCallbackHandlerTest`,
`StorageTransactionTest`, `JobUpdaterIT.testRecoverFromStorage`, Thermos
`test_runner_integration.py` and `test_staged_kill.py`. None ran in this task.

## Supervisor, cleanup and evidence requirements

Implement before unattended multi-agent tests:

1. Exclusive run directory and manifest: commit, tool versions/checksums, exact
   argv/config, IDs, limits and ports. No credentials in committed artifacts.
2. Track process groups and PID plus start identity, not only PID files. Separate
   agent locks/journals/work/tmp paths; capture bounded stdout/stderr.
3. Deadline-based readiness and fixture probes; collect initial state/process/port
   inventory before fault injection. Never assume sleep duration means ready.
4. On exit/failure, stop submissions and workloads, TERM then bounded KILL only
   task-owned process groups, reap descendants, stop workers/scheduler/coordination
   and close sockets. No broad `pkill java` or `killall mesos-agent`.
5. Verify recorded processes and descendants gone and ports free. Retain small
   logs/journals for diagnosis; remove only known disposable artifacts afterward.
   Existing host services/containers are never cleanup targets.

Restart tests retain state; fresh-cluster tests use a new root. A crashed
supervisor requires manifest-based cleanup with verified process identity to
avoid PID-reuse mistakes. Record setup/build/runtime exit codes, time to register,
start/kill/recover, peak RSS, minimum MemAvailable, log/state size and violated
invariants. Pi samples are not production performance guarantees.

## Remaining unknowns and budget handoff

Dependency resolution, Java/Go compilation, native ARM64 Mesos, container runtime,
complete namespace launcher, actual service startup and all application recovery
tests remain unverified. The Mesos isolator page, raw Mesos 1.6.1 flags source and
historical Gradle application-plugin page failed browser retrieval; no verified
compatibility claim relies on them. Successful external sources are linked next
to claims above; local Aurora source is authoritative for this baseline.

Account usage was checked before research and between major phases. Initial and
subsequent successful main-weekly samples were 4% used. The conservative task
threshold was 11% used (initial +7 points), also bounded by the shared 50% early
stop and other limits. The final check stalled and was terminated after a bounded
wait; new research stopped and this handoff was saved as instructed. Final usage
is unknown; do not interpret the last successful sample as a current total.
No subagents, resets, credits, push or merge were used. Only this report is an
intended checkout change. Future work must recheck usage before resuming research.
