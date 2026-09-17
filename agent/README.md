# Aurora agent

This standalone Go module executes tasks for Aurora's original `SchedulerMain`.
The integration lab runs two agents and the Java 25 scheduler in separate containers.

Build the two lab binaries reproducibly from the repository root:

```sh
build-support/lab/build-agents --offline
```

The helper uses the checksum-pinned Go 1.27.1 Linux ARM64 archive, seeded from
`.pi-tools/downloads` in offline mode, and writes binaries plus provenance
records under `.pi-tools/agent-original-integration`. A clean checkout may
omit `--offline` to download the same pinned archive into `.cache/inplace-go`.
The provenance records bind each binary to the source tree digest and build
recipe; `inplace-cluster` verifies them before staging a lab.

For focused development with the already seeded toolchain:

```sh
GOCACHE="$PWD/.pi-tools/go-cache" GOMODCACHE="$PWD/.pi-tools/go-mod" \
  GOTOOLCHAIN=local GOMAXPROCS=2 .pi-tools/go1.27.1/go/bin/go -C agent test ./...
```

The agent's local and authenticated HTTPS interfaces are documented in the
source package and exercised by its Go tests. The lab remains the only supported
integration boundary for scheduler communication. The enforced Linux workload
profile and its operator prerequisites are documented in
[Native process isolation](../docs/operations/native-process-isolation.md).

HTTPS reconciliation publishes at most 128 simultaneous reservations. New Run
admissions at that bound return retryable HTTP 503 without recording a command
result. Stop remains admissible, including when the bound is reached; a stopped
Run keeps its reservation until durable cleanup completes.

`/v1/state` and `/v1/watch` expose reserved attempts only. Terminal outcomes and
Stop tombstones are reported through the paginated observation journal. Command
results accompany their observation page (at most 128 results), rather than the
entire journal history. Watch emits a replacement snapshot whenever reservations
are removed, so clients must replace their attempt map on `snapshot` and merge it
on `delta`. A client must consume and commit all observation pages before treating
the reservation inventory as reconciled. The existing scheduler follows these
rules. Local inspection continues to expose full retained history.

Durable command results, attempt bodies, Stop tombstones and sequence counters
remain replay evidence until a coordinated scheduler retirement barrier. ACK alone
never deletes that evidence. Indexed, checksummed bbolt history keeps the hot
snapshot small; startup and full inspection validate retained cold history.

The upgraded scheduler automatically enables ordered-ticket retention on a fresh,
quiescent journal. For an existing journal it first durably fences new launches
and withholds offers, while existing Stop delivery and cleanup continue. Drain
legacy services once: activation requires no scheduler active/pending commands,
no agent reservations, no unacknowledged observations and no live/unacknowledged
supervisors. The activation transaction discards legacy replay rows and permanently
rejects legacy deliveries. Do not delete journal files to bypass this barrier.
Older scheduler/agent binaries fail closed on the new SQLite schema, snapshot
metadata or ticket-bearing protocol identities.

After activation, each Run/Stop pair carries one immutable, durably allocated
per-node ticket. The scheduler keeps the most recent 64 eligible completed
attempts by default; set JVM `-Daurora.go.retained-completed=N` (0–896) to change
this. Retirement requires committed terminal cleanup and no pending command,
then freezes further scheduler intent for that ticket. The agent independently
checks reservations, observation receipt and supervisor exit before retirement.
Compact disjoint retired-ticket intervals reject replay forever without retaining
individual old commands. A long-lived ticket does not pin later completed tickets.
Helper event journals durably prune acknowledged prefixes using a persistent event
base. Above 256 queued helper events or 8192 node observations, new readiness-only
changes coalesce into one durable latest-readiness slot per attempt. Already
queued events stay immutable; ACK/reconnection publishes the latest readiness with
a fresh sequence. Terminal, health-failure, Stop and cleanup facts are never
coalesced, and new ticket admission backpressures at the observation threshold.
Health probing and failure deadlines continue while readiness publication waits.
The finite existing attempt window retains headroom for its remaining control and
terminal transitions, so the thresholds are not hard byte quotas.
Retirement replies may confirm only the currently eligible subset; other tickets
remain fenced and retry. Lost replies and restart replay the same barrier safely.

At most 1024 unretired tickets, two command identities per ticket and 1024 retired
intervals are admitted per agent. New admissions backpressure at capacity; existing
Stop delivery and cleanup remain available. The scheduler caps enrollment records
at 128 and pins each node's incarnation/journal scope. Ordinary daemon/scheduler
restarts reuse that scope. Changing it in-place is rejected.

Retirement atomically replaces cold command/attempt/sequence rows with the compact
replay fence and a durable artifact deletion queue. Only then are completed log,
work and supervisor directories deleted; symlink roots are rejected and traversal
is confined to the runtime root. Isolation cleanup must also succeed. The canonical
runtime root is pinned on the first upgraded runtime open; later root changes fail
before execution or GC. For the initial legacy upgrade, supply the original work
root. Failed or interrupted deletion keeps its metadata and blocks new ticket
admission until retry, including after restart. SQLite outbox and per-attempt receipt watermarks
are deleted only after the agent confirms the barrier. Global receipt payloads
are replaced by one durable cursor per enrolled journal; replay below that cursor
fails closed. Acknowledged supervisor child journals are removed with their attempt.

The configured completed count is a retention target, not a disk quota: unfinished
cleanup, undelivered commands and failed artifact removal retain their bounded
window. With the 1024-ticket maximum, workload log payload alone is bounded by
`2048 * --log-bytes` per agent (2 GiB at the default, 32 GiB at the maximum), plus
bounded command/attempt/observation metadata and filesystem/database overhead.
Preexisting legacy files are an inherited high-water mark until migration drains.
bbolt reuses freed pages but does not shrink its file automatically; compact the
stopped journal to recover that historical high-water disk allocation. Operator
backups and arbitrary files outside the managed runtime directories are separate
from this retention policy.

Offline physical compaction reclaims unused bbolt pages without deleting history:

```sh
aurora-agent compact --config agent.json --state state.db --output compacted.db
```

Stop the daemon first. Compaction requires an exclusive source lock and refuses
an existing output database or `.owner` marker. It validates all retained history,
copies every bucket and counter, syncs the new files, and reports source and output
byte sizes. Small databases may not shrink. Neither the source bytes nor stored
authority are changed, and the copy is never activated automatically. To use the
copy, explicitly start the daemon with `--state compacted.db`, the same enrollment
config, and the same runtime work root; keep `compacted.db.owner` alongside it.
Never run the source and copy concurrently: they carry the same journal identity.

Publication persists the new owner marker before publishing the database. A crash
between those steps can leave a marker-only output, which deliberately cannot be
opened or silently re-enrolled. The source remains usable; choose another unused
output path to retry. A crash can also leave a private `.aurora-compact-*` staging
directory. Compaction does not perform the coordinated retention barrier or delete workload
logs; normal retention removes eligible per-attempt supervisor journals.
