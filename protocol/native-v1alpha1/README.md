# Bounded native-v1alpha1 contract

This directory defines the original scheduler integration wire contract and
its canonical JSON fixtures. It is independent of the legacy Thrift build and
does not introduce a replacement scheduler or a `build-support/native` build
tree. Consumers must apply the schema and semantic checks before accepting a
message; fixture acceptance is not evidence that a workload ran.

The contract uses strict objects, opaque case-sensitive identifiers, decimal
unsigned counters, explicit argv and environment, and canonical UTF-8 JSON
without a trailing newline for hashes. Duplicate or unknown fields, malformed
counters, unresolved port references, and mismatched body hashes are rejected.
Run and Stop bodies are immutable deduplication inputs, while authority and
session fields are checked separately by the authenticated transport.

The executable fixtures under `fixtures/` are the reviewable wire and hash
vectors. The Go agent and the original scheduler adapter consume this contract
through their own package boundaries; this directory contains no scheduler
implementation.

To run the fixture checker with the pinned Go toolchain:

```sh
GOCACHE="$PWD/.pi-tools/go-cache" GOTOOLCHAIN=local GOMAXPROCS=2 \
  .pi-tools/go1.27.1/go/bin/go -C agent test ./...
```

The isolated lab stages only the original scheduler distribution and verifies
the agent/helper provenance records before creating containers.

## Agent event stream

The scheduler opens `GET /v1/watch?afterCursor=N&limit=128` on the agent's existing
HTTPS listener. The same verified client certificate and exact scheduler DNS SAN
checks apply as for command delivery. Exactly one `X-Aurora-Epoch` and
`X-Aurora-Session` header must match the durable current authority. A watch does
not refresh authority; establish it with `/v1/session` first. Query counters are
canonical unsigned decimal strings; the page limit is 1–128 (default 128), and
`afterCursor` defaults to zero. Cursors outside the retained journal range receive
HTTP 409 before streaming. The existing `/v1/state`, `/v1/deliver`, and `/v1/ack`
endpoints remain available.

HTTP 200 uses `application/x-ndjson`, one JSON object per newline, with at most
1 MiB including the newline per frame. The first frame and each periodic full
reconciliation frame have this shape:

```json
{"kind":"snapshot","config":{},"state":{"cursor":"1","ack":"0","attempts":{},"commands":{},"observations":[]},"nextCursor":"1","hasMore":false}
```

`config` and the public state entries use the same schema as `/v1/state`.
Snapshots contain complete attempt and command maps within the supported profile
(128 attempts, 1024 commands). A `delta` has the same fields, but its maps contain
only changed entries. Consumers upsert these maps; omitted entries are unchanged.
There is no inventory deletion or command/tombstone garbage collection in this
profile. The first frame after **every reconnect** is a complete snapshot, even
if the reconnect cursor already equals the journal's current cursor.

Observation lists are bounded pages strictly after the preceding frame's
`nextCursor`. `hasMore` causes immediate additional delta pages. `state.cursor`
can be ahead of the page's `nextCursor`; it is not an acknowledgement watermark.
Consumers must durably commit observation effects before acknowledging only the
contiguous `nextCursor` through `/v1/ack`. On disconnect they reconnect from their
durable committed cursor, never a merely received cursor. Neither snapshots nor
stream writes prune observations. Lost acknowledgements and disconnected readers
therefore replay safely under the existing command/observation deduplication
rules. Full inventory and its observation page come from one committed store
snapshot; later pages may include subsequent durable changes.

A lightweight frame is sent about every 30 seconds:

```json
{"kind":"heartbeat","config":{},"nextCursor":"1"}
```

Its cursor is the last **sent** observation cursor, not the current journal
cursor. It carries no inventory and does not cause inventory reconciliation or
advance durable receipt. Full reconciliation occurs every five minutes with a
fresh uniform jitter of ±30 seconds. Admission, lifecycle, cleanup, and other
public attempt changes wake the stream immediately after their durable store
commit, including changes that create no observation. ACK pruning and unchanged
writes do not wake it. Registration precedes the store read so a commit racing
snapshot acquisition cannot be missed.

Backpressure is bounded: each watcher caches only the profile-limited public
inventory and observes one coalescing notification channel; pending observations
stay in the durable journal, not an in-memory delivery queue. There are two watch
slots separate from eight short-request slots. Each frame has a five-second
write/flush deadline, cleared during the idle heartbeat interval. Oversized
frames, retention gaps, authority changes, store closure, request cancellation,
and write failures terminate the stream; clients reconnect with backoff. Daemon
shutdown cancels active watch request contexts before draining HTTP handlers.
This is a single active scheduler transport; HA handoff and overlapping writers
remain out of scope.

The upgraded retention profile adds optional identity `ticket`, a positive uint64
encoded as a canonical decimal string. The scheduler durably allocates one ticket
per Run/Stop attempt, and both immutable bodies carry it. After the authenticated
quiescent `/v1/retention` activation barrier, the agent requires this field and
rejects tickets covered by durable retired intervals. Observation ACK alone does
not retire an identity. Old strict schemas reject ticket-bearing identities;
legacy identities remain readable for the one-time drain barrier. See
[agent retention operations](../../agent/README.md) for bounds and migration.
