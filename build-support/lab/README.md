<!-- Licensed under the Apache License, Version 2.0. http://www.apache.org/licenses/LICENSE-2.0 -->
# Original scheduler integration lab

`inplace-cluster` stages the original `aurora-scheduler` installed distribution
and two Go agents in a new private Docker lab. The maintained scheduler uses
Java 25 and SQLite, and reconciles Go agent state over mTLS watch streams.
Subsequent actions require the lab's recorded Docker ownership and identities.
The shell commands below delegate to the Go tools; no Python is required.
This process MVP supports restart recovery, while production multi-scheduler HA
is deferred. Historical Mesos/Thermos documentation is not its deployment guide.

Build the installed distribution from the repository root first (see the
[Java build guide](../java/README.md) for prerequisites):

```sh
compiler="$(build-support/bootstrap-go thrift)"
./gradlew -PthriftCompiler="$compiler" installDist
build-support/lab/build-agents --check
build-support/bootstrap-go stage-java --output "$PWD/.pi-tools/lab-jdk"
```

Inputs can be selected explicitly with the following flags:

- `--distribution`: `.cache/inplace-build/build/scheduler/install/aurora-scheduler`
- `--java`: the absolute JDK directory produced by `stage-java` above, using
  `build-support/java/toolchains.json`. Pass `--java` explicitly; the historical
  default and the Gradle launcher's temporary SDK are not clean-checkout inputs.
  Staging requires a fresh output directory and writes an adjacent provenance
  receipt. Add `--offline` when the verified Java archive is cached or seeded.
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

The helper supplies lab lifetime management, certificate generation and bounded
health-check fixture workloads.
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
lab_java="$PWD/.pi-tools/lab-jdk"
build-support/lab/inplace-cluster up --java "$lab_java" --root "$PWD/.pi-lab/original-integration-new"
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
configuration. The `up` action requires a fresh lab root; subsequent actions use
that same root and its recorded manifest.

The [earlier integration evidence](../../docs/reimagining/INPLACE05_07_IMPLEMENTATION_STATUS.md)
records only its named source and artifact versions. Re-run acceptance for a new
checkout and retain its reports before claiming qualification. Docker status
alone does not establish API readiness.

## API acceptance runner

Once the original scheduler and adapter are ready:

```
build-support/lab/inplace-check --root "$PWD/.pi-lab/original-integration-new" --phase smoke
build-support/lab/inplace-check --root "$PWD/.pi-lab/original-integration-new" --phase recovery --rounds 3
build-support/lab/inplace-check --root "$PWD/.pi-lab/original-integration-new" --phase soak
build-support/lab/inplace-check --root "$PWD/.pi-lab/original-integration-new" --phase churn
build-support/lab/inplace-check --root "$PWD/.pi-lab/original-integration-new" --phase policy
```

The runner uses original `/api` Thrift JSON fields from `api.thrift` through the
runner's Go API client, with no Python Thrift installation required. It creates uniquely named fixture jobs,
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

Churn retains two 100mCPU services, one per agent, while 130 two-instance batch
jobs run sequentially. Each batch uses a three-second process, 600mCPU per
instance, and a one-instance-per-host constraint. The receipt requires exactly
260 distinct FINISHED task IDs, exactly 130 completions on each enrolled agent,
and unchanged service task IDs, hosts and physical PIDs after every batch.
It also checks scheduler health and unchanged running daemon generations/PIDs
before and after churn. Each job has a 90-second deadline; the phase has a
30-minute deadline and usually takes several minutes. Failed in-flight batches
and the service receive cleanup requests; finished history remains available.

This phase crosses the former 128-lifetime-attempt reconciliation boundary on
both agents without journal resets or daemon restarts. It qualifies continued
execution and observation delivery under retained history; it does not qualify
bounded disk growth, physical history compaction, or 128 concurrent workloads.

New labs configure scheduler backups every 30 seconds and retain the latest
three snapshots (`-backup_interval=30secs -max_saved_backups=3`). Churn records
backup filenames/counts before and after its workload; these receipts support
separate backup publication/retention checks, not a restore qualification.
Existing labs keep the scheduler arguments recorded in their ownership manifest.

Health qualifies optional TCP readiness in the original process executor: two
200mCPU services sharing a health socket must use different agents, a delayed
listener causes a startup failure and automatic rollback to the exact original
executor config, and a listener that closes while its process stays alive causes
FAILED with a health diagnostic. Run it with `inplace-check --phase health`.

Logs qualifies the scheduler's Go-backed log route on both agents: two completed
batch tasks emit more than 1 MiB to each stream, all retained pages and truncation
are verified against expected output hashes, and completed logs remain readable
after each agent and the scheduler restart. Run it with `inplace-check --phase logs`.
See [health configuration](../../docs/operations/go-process-health.md) and
[task logs](../../docs/operations/task-logs.md) for the supported contracts.

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

## Submit a JSON job

Use the scheduler's private container address reported by `status` from the lab
host. Replace `SCHEDULER_PRIVATE_IP` with that address:

```sh
export AURORA_SCHEDULER=http://SCHEDULER_PRIVATE_IP:8081
./aurora job validate examples/jobs/process-service.json
./aurora job check examples/jobs/process-service.json
./aurora job create examples/jobs/process-service.json
./aurora job status fixtures/test/process-service
./aurora job kill fixtures/test/process-service
```

Run manual jobs separately from acceptance phases. The [Go client guide](../../docs/reference/go-client.md)
provides batch and cron examples, TLS options and the full JSON schema. After an
uncertain mutation reply, inspect scheduler state before resubmitting.

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
memory is reserved rather than cgroup-enforced, and disk is scheduler accounting.
The `user` field is metadata, not an operating-system user switch. Thermos process
graphs, health checks and service announcement are unsupported, as are production
isolation and multi-scheduler HA.

Agent reconciliation inventories contain at most 128 outstanding reservations.
At capacity, new Runs receive retryable backpressure; replayed commands and Stops
remain available. Completed attempts and command results stay in the durable
journal for replay safety, while observation pages carry their required history.
Disk history still grows; automatic physical compaction and indefinite production
operation remain unqualified.
