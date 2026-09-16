# Aurora agent

This directory is the original integration agent used by the isolated lab. It
is a standalone Go module and does not depend on the replacement Java scheduler
or a `build-support/native` tree.

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
integration boundary for scheduler communication.

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

Durable command results, attempt tombstones, and sequence counters remain
permanent for replay protection across ACK and restart. This change bounds the
wire inventory, not disk usage or the cost of reading/writing the journal; safe
physical history compaction remains future work. Legacy journals with arbitrary
terminal history reconcile without migration. A legacy journal with more than
128 actual reservations still returns 503 for state/watch rather than hiding
reservations. Stop delivery and execution cleanup remain available; inventory
recovers after cleanup reduces reservations to the bound. Do not delete its
journal or tombstones to recover capacity.
