<!-- Licensed under the Apache License, Version 2.0. http://www.apache.org/licenses/LICENSE-2.0 -->
# Original scheduler integration lab

`inplace-cluster` stages the original `aurora-scheduler` installed distribution
and two Go agents in a new private Docker lab. It does not launch the abandoned
replacement Java scheduler or operate existing labs. Build the original
scheduler with the Go-agent adapter before starting this lane.

Default inputs (override with the corresponding flags):

- `--distribution`: `.cache/inplace-build/build/scheduler/install/aurora-scheduler`
- `--java`: `.cache/java03-tools/java/jdk-25.0.4.1+1`
- `--agent`: `.pi-tools/agent-original-integration/aurora-agent`
- `--helper`: `.pi-tools/agent-original-integration/cluster-helper`

Build the static agent and helper binaries, including the provenance records
required by `inplace-cluster`, with the pinned Go toolchain:

```
GOMODCACHE="$PWD/.pi-tools/go-mod" GOCACHE="$PWD/.pi-tools/go-cache" \
  build-support/lab/build-agents --offline
```

Add `--check` to run uncached Go tests and `go vet` for both modules before
building the binaries.

Omit `--offline` on a clean checkout to obtain the checksum-pinned Go archive;
the helper keeps its archive and temporary tool workspace under
`.cache/inplace-go`.

The helper supplies only lab lifetime management and certificate generation.
The helper is built from `fixtures/cluster-helper` using pinned Go1.27.1.
Both Go binaries require adjacent `.provenance.json` build records matching
the current source tree and binary SHA256. All inputs are copied into the new
lab root and checked again after copying. Source hashes, binary metadata, the
JDK release file and tree SHA256, and the distribution tree SHA256 are recorded
in `lab.json`. The distribution must contain the original `SchedulerMain`
and must not contain `NativeSchedulerMain` or `NativeEngine`. OpenSSL and the staged JDK keytool generate fresh private
PKCS12 credentials. Passwords remain in private configuration files, never
status output. The exact pinned Debian base must already be available in Docker.

From repository root, using a Docker-authorized process with the same UID:

```
build-support/lab/inplace-cluster up --root "$PWD/.pi-lab/original-integration-new"
build-support/lab/inplace-cluster status --root "$PWD/.pi-lab/original-integration-new"
build-support/lab/inplace-cluster crash-agent --node agent-a --root "$PWD/.pi-lab/original-integration-new"
build-support/lab/inplace-cluster restart-scheduler --root "$PWD/.pi-lab/original-integration-new"
build-support/lab/inplace-cluster down --root "$PWD/.pi-lab/original-integration-new"
```

Other actions are `restart-agent` and `crash-scheduler`. Restart sends graceful
TERM through the keeper; crash sends SIGKILL, then the keeper starts the daemon
again in the same container namespace. Agents use `--supervise`; graceful agent
shutdown drains work while daemon-only crash can preserve supervised work.
Container recreation with retained agent state is deliberately refused by the
agent's observed runtime identity checks.

The bridge is internal and no host ports are published. `status` reports private
container addresses: the original scheduler HTTP service is port8081, and agent
mTLS is port8443. Status proves Docker liveness, not scheduler readiness. The
original unauthenticated scheduler API is suitable only for this isolated lab;
agent transport requires the scheduler's verified client certificate.

Every container is nonroot, read-only, drops all capabilities and enables
no-new-privileges. Only its own state/work/control mounts are writable. Actions
require recorded IDs, exact labels, names, image, commands and mounts; they never
adopt a container by name or enumerate old labs for cleanup. A failed partial
creation preserves its manifest/cidfile for investigation. Unknown identities,
externally removed resources or changed ownership fail closed; they may require
manual review rather than automatic cleanup. `down` retains local evidence and
configuration. Never point this tool at an existing lab root.

Executed qualification and exact artifact hashes are recorded in
[the integration evidence](../../docs/reimagining/INPLACE05_07_IMPLEMENTATION_STATUS.md).
A successful Docker status command alone does not establish API readiness.

## API acceptance runner

Once the original scheduler and adapter are ready:

```
build-support/lab/inplace-check --root "$PWD/.pi-lab/original-integration-new" --phase smoke
build-support/lab/inplace-check --root "$PWD/.pi-lab/original-integration-new" --phase recovery --rounds 3
build-support/lab/inplace-check --root "$PWD/.pi-lab/original-integration-new" --phase soak
build-support/lab/inplace-check --root "$PWD/.pi-lab/original-integration-new" --phase policy
```

The runner uses original `/api` Thrift JSON fields from `api.thrift`, with no
Python Thrift installation required. It creates uniquely named fixture jobs,
sets the isolated `fixtures` quota, and writes a timestamped JSON report even
when a check fails. It never interprets a failed RPC as success. Run one check
at a time in a new lab without unrelated fixture jobs.

Smoke checks two batch completions, two 600mCPU services assigned to different
agents, physical sleep processes, a rolling update, and termination. Quota,
cron schedule/deschedule and drain/end-maintenance are API acceptance checks;
the drain call operates on empty hosts and does not claim active-task migration.
Recovery performs three rounds of scheduler restart, scheduler crash and agent
daemon crash. It checks keeper generations, stable original task IDs/hosts and
physical workload PIDs. Soak keeps two services running for at least ten minutes
while twenty two-instance batch jobs finish. Policy checks an automatic rollback
from a failing update, exact executor restoration, active-host draining and
replacement, and manual execution of a scheduled cron job.

Run these phases sequentially: maintenance and failure injection affect the
whole owned lab. Failures retain evidence and may leave fixture state requiring
review. The API runner executes on the Pi host; the scheduler and agents are
containerized.

`refresh-scheduler --root ABS [--distribution ABS]` updates only the original
scheduler distribution. It checks the current staged tree, validates and stages
the replacement, requests keeper `pause` (SIGKILL), and waits for paused/PID0
confirmation before changing any mounted executable. It preserves the mounted
directory inode, verifies the replacement, records old/new hashes and refresh
history, then requests `resume` and waits for a newer daemon generation. Agent
containers and namespaces remain untouched. This is a crash/recovery integration
action, not a graceful production upgrade. A failure preserves evidence and may
leave the scheduler paused; inspect `refreshPending` before taking further action.

## Supported task profile

Submit original Aurora `TaskConfig` objects with `ExecutorConfig.name` set to
`go-process` and `ExecutorConfig.data` containing JSON such as:

```json
{"version":"aurora-process-v1","argv":["/bin/sleep","300"],"env":{},"graceMillis":1000}
```

Set `partitionPolicy.reschedule` to false. CPU reservations use whole millicores;
RAM and disk remain original resource fields. The profile rejects ports, GPU,
revocable CPU, images, volumes and fetcher URIs before assignment. Job-key parts
match `[a-z][a-z0-9-]{0,63}`. This is a trusted process cohort with reservations;
full Thermos behavior and production isolation are separate compatibility work.

Agents retain bounded history (128 attempts, 1,024 command results). Reaching the
inventory limit requires an operator lifecycle/retention decision; this lab does
not claim indefinite production operation or automatic history compaction.
