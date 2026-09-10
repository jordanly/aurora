# Slice 1 continuation: durable core and native containers

Implemented on `codex/standalone-foundations`, based on upstream
`11ebaeeb071cb182c388a40755e84f60dda32260`. Both remote master refs were refreshed
again on 2026-09-10; there were no new upstream commits. This extends the
[first foundation increment](FIRST_SLICE_STATUS.md).

The result is executable admission and persistence code with real ARM64 Docker
qualification. Workload execution and scheduler/agent communication remain the
next integration work. The three smoke containers run one Java store check and
two independent Go admission checks; they are not yet an Aurora cluster.

| Component | Implemented boundary | Evidence |
| --- | --- | --- |
| Java protocol | Full draft 2020-12 schema validation plus lexical/semantic checks, immutable validated results, canonical hashes, capability predicates and Job-to-Run resolution. Separate Java 8 classpath. | 7 tests; all 13 valid, 27 invalid and 12 parser-negative shared fixtures; exact canonical/hash parity with Go. |
| Go admission | Strict validator; bbolt transactions commit command result, immutable attempt reservation/tombstone, observation and cursor together. Exclusive journal ownership, corruption/version checks, authority refresh and bounded ACK pruning. | 14 storage tests plus 3 protocol tests; concurrent admission, restart/replay, corruption and real subprocess crash-before/after-commit checks; `go vet` passes. |
| Native SQL core | WAL/FULL, owner lock, scoped transactions and rollback-only nesting; durable jobs/membership/attempts/allocations/commands; observation dedupe and contiguous cursor. | 11 focused SQL tests plus 21 existing scheduler state-machine tests; real JNI execution on this Pi's 16 KiB kernel. |
| Docker qualification | Digest-pinned ARM64 base, Java/JDBC and Go artifacts, separate owned state roots, nonroot containers and scoped teardown; retained state checked after recreation. | 21 lab tests pass. Independent actual Pi run passes both rounds of all three checks, with fresh container IDs, retained results and completed cleanup. |

The orchestrator independently reviewed and reran Java protocol, Go storage,
cross-language conformance, SQL/scheduler and container checks. Luna prepared the
Docker tooling; Astra medium implemented the durable stores and reviewed the
code, including final smoke-lifecycle hardening. Fixes from review included
attempt identity scope, missing-state refusal, rollback poisoning of caught
errors, JSON encoding parity, per-container failure detection and cleanup scope.

## Why these boundaries matter

An agent's `accepted` response now follows a successful local commit. A lost
response followed by replay returns the original result without reserving a
second attempt or advancing its cursor. A changed command body under the same
ID rejects. Stop-before-Run permanently blocks that attempt. Stopped and uncertain
Run reservations remain allocated because no cleanup reducer exists yet.

The SQL core keeps desired membership separate from execution history. Completing
a batch preserves both its job and terminal attempt; cancellation clears desired
membership while retaining history. A failed inner mutation marks the entire
transaction rollback-only, including when an outer callback catches the error.
Independent readers cannot observe its uncommitted changes. Observation receipts
above a gap do not advance the contiguous cursor; future reduction must occur in
the same transaction, and transport ACKs must wait for the outer commit.

Known state cannot silently become a fresh empty journal after just its database
file is deleted or truncated. SQL uses its owner marker; Go uses a separate
enrollment marker alongside the database. Losing the entire state and marker
still requires explicit recovery with a new journal identity. Process crash tests
do not establish physical power-loss durability or recovery from ambiguous I/O
errors at commit.

The smoke lane intentionally reuses trusted fixture identities across one-shot
check containers. It proves retained admission data and native-library execution.
It does not exercise real daemon sessions, workload adoption, inventory snapshots,
runtime-incarnation changes or execution fencing on container loss.

## Reproduce

```sh
# Existing Java 8 recovery helper also resolves the pinned JDBC artifact.
build-support/java/gradle-local focusedTest \
  --tests '*NativeSqlStoreTest' --tests '*TaskStateMachineTest'
build-support/java/gradle-local -p protocol/java test installDist

export GOCACHE="$PWD/.pi-tools/go-cache"
export GOMODCACHE="$PWD/.pi-tools/go-mod"
export GOTOOLCHAIN=local
export GOMAXPROCS=2
.pi-tools/go1.27.1/go/bin/go -C agent test ./...
.pi-tools/go1.27.1/go/bin/go -C agent vet ./...
.pi-tools/go1.27.1/go/bin/go -C agent build \
  -o "$PWD/.pi-tools/agent-dist/aurora-agent" ./cmd/aurora-agent

export JAVA_HOME="$PWD/.pi-tools/jdk8u462-b08"
python3 protocol/native-v1alpha1/conformance/check.py \
  --validator '.pi-tools/protocol-java-dist/install/aurora-native-protocol/bin/aurora-native-protocol' \
  --validator '.pi-tools/agent-dist/aurora-agent validate --document'
python3 -m unittest discover -s build-support/lab/tests -v

# Acquire the pinned base once if absent from Docker's local image store.
sg docker -c 'docker pull --platform linux/arm64 debian:bookworm-slim@sha256:6bd27d44e6c32a66bbd72d7cb2b76a8ae3497ec2e5274a81abd1b37f6013fa1f'
# A new run root is required. sg activates this session's Docker membership.
sg docker -c 'build-support/lab/native-smoke --run-root "$PWD/.pi-lab/native-review"'
```

The [lab README](../../build-support/lab/README.md) records pinned image inputs,
evidence and cleanup behavior. The [agent README](../../agent/README.md),
[Java validator README](../../protocol/java/README.md) and
[SQL README](../../src/main/java/org/apache/aurora/scheduler/storage/sql/README.md)
document APIs, dependencies and failure contracts. Downloads, images' staging
directories and runtime state remain outside source control.

Local evidence includes `.pi-tools/agent-parent-tests.jsonl`,
`.pi-tools/native-sql-parent-tests.log`, `.pi-tools/protocol-java-final.log`,
JUnit XML under `dist/test-results/focusedTest/`, and the independent Docker run at
`.pi-lab/native-smoke-parent-review/evidence/result.json` (schema 2, `ok=true`,
`recreated=true`, `cleanup=complete`). All six container executions exit zero;
the Java marker changes from new to preexisting and each agent retains cursor 1,
one reservation and one observation. Go's race detector cannot run on this host's VMA layout;
ordinary concurrent tests and vet pass. Race qualification remains a separate
check on a compatible host. The full legacy suite/frontend were not rerun.

## Next integration work

1. Add the agent process runtime, launch gate, readiness, bounded logs and verified
   cleanup. Persist launch intent before effects; crash recovery must preserve
   uncertain reservations until cleanup/fencing. Introduce real runtime identities.
2. Add authenticated transport and enrollment, then scheduler native module
   assembly. Integrate Java protocol validation with the SQL core before enabling
   submit/read/stop. Reuse retained scheduler policy/state transitions.
3. Connect committed command dispatch, observations, gap-safe ACKs and complete
   inventory snapshots. One scheduler controller owns replacements from durable
   desired membership; it must not resurrect completed batches or cancelled work.
4. Build actual scheduler/agent/proxy images and run the planned batch/service,
   scheduler restart, partition/stop/reconnect, agent crash and isolated restore
   scenarios. Then remove Mesos runtime dependencies and perform full Java
   modernization against that standalone baseline.

The SQL component is intentionally not a fake implementation of every legacy
`Storage` interface. It has no production scheduler wiring, network service,
placement controller, backup/restore API or durable operation facade yet. The Go
store retains an aggregate snapshot and permanent dedupe/tombstones; growth and
write cost are not production-bounded. No hard memory enforcement is advertised.
