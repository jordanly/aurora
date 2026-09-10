# Durable admission and local process runtime (AGENT-01 / AGENT-02)

This standalone Go module validates native-v1alpha1, persists admission, and runs
trusted preinstalled Linux processes through an optional local runtime. Accepted
Run commands reserve resources before execution. The runtime releases those
reservations only after verified cleanup; uncertain executions remain reserved.
Stop-before-Run permanently tombstones the attempt.

The local runtime has no scheduler connection or authenticated network API.
Workloads run as the agent user with explicit argv/environment, in a private
per-attempt working directory. This is a trusted process lane, with no hard
memory enforcement or security boundary against hostile same-user workloads.
Surviving supervisors, arbitrary detached descendants, workload adoption after
container loss, retries beyond maxRuns=1, and production backup/restore remain
outside this increment.

## Local interface

From repository root:

```sh
GOCACHE="$PWD/.pi-tools/go-cache" GOMODCACHE="$PWD/.pi-tools/go-mod" GOTOOLCHAIN=local GOMAXPROCS=2 .pi-tools/go1.27.1/go/bin/go -C agent test ./...
GOCACHE="$PWD/.pi-tools/go-cache" GOMODCACHE="$PWD/.pi-tools/go-mod" GOTOOLCHAIN=local GOMAXPROCS=2 .pi-tools/go1.27.1/go/bin/go -C agent build -o /tmp/aurora-agent ./cmd/aurora-agent
/tmp/aurora-agent version
/tmp/aurora-agent validate --document protocol/native-v1alpha1/fixtures/valid/delivery.json
/tmp/aurora-agent admit --config agent/examples/agent-a.json --state /tmp/aurora-agent.db --command protocol/native-v1alpha1/fixtures/valid/delivery.json
/tmp/aurora-agent inspect --config agent/examples/agent-a.json --state /tmp/aurora-agent.db
```

`admit` takes a Delivery, never a bare Run/Stop. Config is strict, all 11 fields
in the example are required, and unknown/duplicate fields reject. Paths are local.
The operator-owned config is the local CLI trust boundary: it supplies the enrolled
identity and authenticated caller assertion. Do not expose this CLI as a remote
endpoint or construct `Caller` from received JSON. The reusable `Store.Admit` and
`Store.Ack` independently compare trusted caller context with current enrolled
peer/session/epoch; matching envelope fields alone cannot authenticate anything.
Trusted config refreshes session authority atomically at Open, including a new
session within the same epoch. Epoch rollback rejects; the local operator is
responsible for installing the latest authenticated session config. Accepted command results
survive refresh because envelope authority is excluded from immutable body hashing.

All CLI output is JSON. `admit` returns command, bodySha256, outcome, cursor, and
Stop deadline. Replays return that exact durable result. Rejections exit nonzero.
`inspect` returns cursor/ack decimal strings, commands, redacted attempt summaries
(identity, reservation, stop deadline and execution outcome/PID/readiness/log counters),
and observations; it omits argv/environment.
`validate` deliberately outputs the full canonical input, so use fixture inputs.
The library inspect API exposes full local records to its trusted caller.
Inputs are bounded to 1 MiB and depth 64. No secrets are required by this profile.

## Local process control

Build the binary above, then keep its stdin/stdout attached:

```sh
/tmp/aurora-agent serve-local --config agent/examples/agent-a.json \
  --state /tmp/aurora-agent.db --work-root /tmp/aurora-agent-work \
  --network agent-container --log-bytes 1048576
```

Each JSON line is either a complete Delivery, `{"action":"inspect"}`, or
`{"action":"shutdown"}`. Replies contain `ok` and `result` or a generic `error`.
Delivery acceptance confirms the admission transaction; inspect observations to
learn execution results. This interface trusts its local operator and must not
be wrapped as an unauthenticated remote endpoint. It has no remote ACK interface.
EOF, SIGINT, SIGTERM, and explicit shutdown persist stop intent and drain the
runtime. Shutdown success follows runtime and journal closure. A stalled response
reader is bounded to five seconds and causes cleanup; a failed or uncertain
cleanup returns an error. Requests are limited to 1 MiB. Inputs should arrive
sequentially and each reply should be consumed before sending another request.

`NewRuntime`/`Tick` consume each attempt's launch intent once. An internal helper
blocks on an inherited descriptor until the agent has durably recorded its
PID/start identity and release intent. Admission of Stop and final gate release
share a lock. The helper uses a new session, explicit environment, no-new-privileges
and parent-death signaling; set-ID/file-capability executables reject. The spawning
OS thread remains alive until Wait completes. The private helper entry point is
an implementation detail, not a workload API.

TCP readiness requires the assigned listening socket to belong to the recorded
workload PID, plus a successful connection. Probes honor cancellation and cap each
connection attempt at 250 ms to keep the control loop responsive. It is transport readiness: the fixture's
HTTP `/ready` delay is application evidence and does not change this protocol's
TCP readiness semantics. Exact assigned ports have no fallback; externally occupied
ports fail execution. Accounting release follows cleanup, not signal submission.
Stop uses its durable deadline, first TERM and then KILL if needed. Signaling uses
pidfds and verified identities, with no raw-PID fallback. Startup after daemon loss
never relaunches consumed intent: it cleans verified processes and reports Lost,
retaining uncertainty if identity or group absence cannot be established.

Each stdout/stderr file retains its first configured 1 KiB..16 MiB; excess bytes
are drained and counted. Log truncation is visible in inspect. This bounds each
stream, not aggregate journal/work-directory growth. There is no rotation, remote
log API, per-task cgroup enforcement or physical power-loss guarantee yet.

Linux behavior references: [parent-death signal](https://man7.org/linux/man-pages/man2/PR_SET_PDEATHSIG.2const.html),
[pidfds](https://man7.org/linux/man-pages/man2/pidfd_open.2.html), and
[Go process and pipe waiting](https://pkg.go.dev/os/exec#Cmd).

## Storage and invariants

[bbolt v1.5.0](https://github.com/etcd-io/bbolt/releases/tag/v1.5.0) was verified
against the official latest-release page on 2026-09-10 and pinned with go.sum.
Its [transaction and sync model](https://github.com/etcd-io/bbolt#transactions)
provides the embedded single-writer basis. The file is opened mode 0600 with an
exclusive writer lock (final-component symlinks reject atomically with O_NOFOLLOW) and a short acquisition timeout. Default commit fsync stays
enabled. Each database has a `.owner` enrollment sidecar containing its format version and
cluster/recovery/node/journal scope. New enrollment publishes a fsynced sidecar
atomically under the database lock, then syncs the parent directory before Open
succeeds. Deleting only the database refuses startup without recreating it; an
existing database with a missing, malformed, mismatched, or symlink marker also
refuses. Interrupted initial enrollment fails closed if only one file remains.
Move/preserve both files together. If the entire database **and** marker are lost,
local state cannot distinguish that from a new path: the operator must enroll a
new journal identity before resuming. The trusted parent directory must not be
changed concurrently by untrusted users. Use a persistent local
filesystem; this is not evidence of power-loss behavior of this Pi or an SD card.

One bbolt transaction commits the immutable command hash/result, attempt reservation
or tombstone, sequence, durable node cursor and observation. Conflicting command ID
reuse rejects. Attempts are keyed by cluster/recovery/job/instance/attempt and reject identity
mutation. Exact TCP/IPv4 sockets include network domain. Admission sums all existing
Run reservations, including stopped/uncertain attempts until their execution cleanup is complete,
under the writer transaction.
No capabilities are advertised, so hard-memory requests reject at admission.
Stop replay never changes its original deadline; later distinct Stops can only
shorten the tombstone deadline. Tombstones and dedupe records are never ACK-pruned.

ACK accepts only the enrolled scope and authenticated current caller, cannot exceed
the durable cursor or move backward, and deletes at most 1..1024 observations per
transaction. Repeating an ACK continues bounded pruning. It trusts the scheduler's
assertion of contiguous committed receipt; proving the remote commit is scheduler
work. Pruning does not reset node cursors or attempt sequences.

Snapshots carry an explicit format version and integrity checksum and are validated on every read.
Admission-only journals start at format 1. First `NewRuntime` atomically upgrades to
format 2 and records observed host boot ID, PID and network namespace identities,
and namespace-init start time. The older admission-only binary refuses format 2;
do not downgrade it after enabling execution. Enrollment labels alone cannot
substitute for these actual kernel identities. A changed scope refuses runtime
startup before any PID is signaled, even when the same state volume survives.
Container-loss recovery needs explicit fencing/re-enrollment integration later. Existing
empty files, wrong scope, malformed state, checksum mismatch and corrupt bbolt
headers refuse admission. Checksums detect accidental corruption, not malicious
local rewriting. The compact prototype stores one aggregate snapshot per
transaction, so writes/read validation grow with retained history. Dedupe/tombstones
are permanent and storage growth is not production bounded. No backup/restore,
compaction, schema migration, multi-host store, or network authentication is claimed.

## Local-store spike evidence

`go test ./...` exercises the complete shared valid/invalid/parser corpus and
canonical hashes, schema-copy drift, capability and template resolution parity;
reopen/replay and conflicting IDs; stop-before-run and cancellation replay;
old-session rejection with epoch refresh through max uint64; exclusive ownership;
concurrent CPU/memory admission; exact socket conflicts and separate networks;
wrong enrollment; scoped/future ACK rejection; checksum corruption refusal;
injected transaction rollback; subprocess abrupt exits immediately before commit
and after successful commit. Reopen checks command, attempt, cursor and outbox are
all absent before commit or all present after it, with idempotent retry afterward.
These are application crash tests, not host power-loss or filesystem fault tests.

`go test -race ./...` could not execute on this ARM64 host: ThreadSanitizer reports
`unsupported VMA range` (found 47, supported 48). Ordinary concurrent admission tests
pass; rerun the race detector on a compatible host before production integration.

The authoritative schema remains `../protocol/native-v1alpha1/schema.json`.
`protocol/schema.json` is an embedded build copy; tests require byte equality.
After an intentional schema change copy it and rerun the full shared corpus.
[santhosh-tekuri/jsonschema v6.0.3](https://github.com/santhosh-tekuri/jsonschema/releases/tag/v6.0.3)
provides maintained draft 2020-12 validation (official latest release verified
2026-09-10). The embedded schema compiles once; parse/compile errors fail every
validation closed. External schema loading is disabled. A bounded regexp adapter
translates only the exact trailing `(?![\s\S])` to Go absolute-end `\z`; other
unsupported constructs fail compilation. The authoritative schema is unchanged.
Validate returns mutable maps;
callers must not mutate a value concurrently with hashing/resolution checks.
