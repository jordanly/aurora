# Durable admission foundation (AGENT-01)

This standalone Go module validates the bounded native-v1alpha1 profile and
persists admission. It does **not** launch a process, bind sockets, enforce resources,
serve a network API, authenticate remote peers, perform reconciliation, or prove
cleanup. An accepted Run reserves resources and produces an `unknown` observation.
A Stop permanently tombstones its attempt and preserves any existing reservation.
Only a future cleanup/fencing reducer can release resources; terminal outcome alone
will not do so. Scope/config changes fail closed, including runtime/boot changes,
pending a future recovery protocol.

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
(identity, reserved, stopped, deadline), and observations; it omits argv/environment.
`validate` deliberately outputs the full canonical input, so use fixture inputs.
The library inspect API exposes full local records to its trusted caller.
Inputs are bounded to 1 MiB and depth 64. No secrets are required by this profile.

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
Run reservations, including stopped/uncertain attempts, under the writer transaction.
No capabilities are advertised, so hard-memory requests reject at admission.
Stop replay never changes its original deadline; later distinct Stops can only
shorten the tombstone deadline. Tombstones and dedupe records are never ACK-pruned.

ACK accepts only the enrolled scope and authenticated current caller, cannot exceed
the durable cursor or move backward, and deletes at most 1..1024 observations per
transaction. Repeating an ACK continues bounded pruning. It trusts the scheduler's
assertion of contiguous committed receipt; proving the remote commit is scheduler
work. Pruning does not reset node cursors or attempt sequences.

Snapshots carry an explicit format version and integrity checksum and are validated on every read. Existing
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
