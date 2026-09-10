# Durable admission, trusted process runtime, and lab HTTPS transport

This standalone Go module validates native-v1alpha1, persists admission, and runs
trusted preinstalled Linux processes through an optional local runtime. Accepted
Run commands reserve resources before execution. The runtime releases those
reservations only after verified cleanup; uncertain executions remain reserved.
Stop-before-Run permanently tombstones the attempt.

The runtime supports operator-owned local stdin and an optional mutually
authenticated HTTPS transport for the bounded single-scheduler lab.
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

## Authenticated HTTPS transport (v0lab)

```sh
/tmp/aurora-agent serve --config agent/examples/agent-a.json \
  --state /var/lib/aurora/state.db --work-root /var/lib/aurora/work \
  --network agent-container --listen :8443 \
  --tls-cert /run/aurora-tls/agent.crt --tls-key /run/aurora-tls/agent.key \
  --tls-ca /run/aurora-tls/ca.crt
```

Every connection requires TLS 1.3 and a client certificate verified by the supplied
CA. Every request additionally requires the leaf certificate to contain the exact
DNS SAN from config `peer` (normally `scheduler`). Common names and wildcard SANs
do not authorize. There is no plaintext or unauthenticated HTTP fallback. The
scheduler must verify the agent server certificate against its enrolled node DNS
name and CA. Private keys are operator-mounted inputs and are never returned.

| Endpoint | Request | Success response |
| --- | --- | --- |
| `GET /v1/state?afterCursor=0&limit=128` | Optional canonical uint64 cursor and limit 1..128 | `{config,state,nextCursor,hasMore}` |
| `POST /v1/session` | Exactly `{schedulerEpoch: string, session: string}` | Current 11-field Config |
| `POST /v1/deliver` | Complete native Delivery | Immutable admission Result |
| `POST /v1/ack` | Complete native ObservationAck, plus `X-Aurora-Epoch` and `X-Aurora-Session` headers | `{ok:true}` |

POST requests require `Content-Type: application/json`. Bodies and responses are
limited to 1 MiB; unknown fields, duplicate keys, unsupported methods and malformed
queries reject. Errors are generic JSON and never contain assignment argv/env.
An accepted Result has HTTP 200; a durable rejected command Result has HTTP 409.
Other invalid deliveries, stale authority, invalid ACKs and cursor retention gaps
also have HTTP 409 with `{error: string}`. Authentication failures use HTTP 403
(or fail the TLS handshake); malformed session/query input uses HTTP 400.
The server caps concurrent handlers at eight, headers at 8 KiB, header reads at
three seconds, complete request reads and response writes at five seconds, and
idle connections at thirty seconds. Runtime ticks independently every 50 ms.
Connection loss does not stop existing workloads. SIGINT/SIGTERM stops accepting
HTTP requests and drains the runtime using its existing bounded shutdown policy.

Session refresh accepts a larger epoch or the identical current epoch/session
pair. A different session within the same epoch rejects on this network endpoint.
The authenticated certificate supplies the peer; caller authority is read from
trusted current store state and independently checked against Delivery authority
or ACK headers. A stale connection cannot reuse its old session to deliver or ACK.
This is a single trusted scheduler profile, not a distributed leader election or
certificate-based fencing protocol.

`serve` uses `OpenServer`: after restart it restores epoch/session from the durable
journal, while requiring every static enrollment/capacity field to match its config.
The initial config file does not need rewriting after `/v1/session`. Other local
CLI commands retain strict explicit-config `Open` semantics. Immutable command
hashes/results survive session refresh and reconnect. Actual kernel boot/PID/network
scope binding still refuses container recreation over an old journal; daemon-only
restart in the same keeper/container namespace follows conservative Lost recovery.

The state response always contains the entire redacted attempt/command inventory,
plus at most `limit` observations with cursor strictly above `afterCursor`.
Attempt `sequence`, state `cursor`/`ack`, and `nextCursor` are decimal strings.
State `cursor` is the durable high-water mark; `nextCursor` is the last observation
returned (or the requested cursor for an empty page). `hasMore` indicates another
observation page. A requested cursor below the durable ACK or beyond the high-water
mark returns HTTP 409; never infer missing events or execution absence from that
failure. Each page is an atomic current snapshot, not a frozen multi-page inventory.
The scheduler must commit contiguous observations before ACK, and compare attempt
sequences when combining current inventory with older observation pages.

Full inventory is capped at 128 attempts, 1024 command results, and 1 MiB encoded
response. Exceeding a limit returns HTTP 503 with no successful partial inventory;
placement must stop and existing reservations remain. Permanent dedupe/tombstones
are not collected to bypass this limit. This intentionally bounded lab profile
needs a later inventory/retention protocol for longer-lived operation. ACK pruning
deletes at most 128 observations per call; repeated identical ACKs finish pruning.

Tests exercise actual TLS handshakes with missing/untrusted/wrong-SAN certificates,
strict bodies, stale Delivery/ACK authority, epoch/session replay through uint64 max,
pagination and retention gaps, inventory bounds and redaction. A real HTTPS runtime
test launches a workload, completes it, restarts with unchanged initial config,
and proves replay does not execute it twice.

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
compaction, general schema migration, or multi-host store is claimed.

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
