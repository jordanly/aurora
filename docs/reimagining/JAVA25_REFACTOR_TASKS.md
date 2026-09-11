# Java 25 refactoring tasks after JAVA-04

These repository tasks implement the [source audit](JAVA25_IDIOM_AUDIT.md).
[JAVA-07 is complete](JAVA07_STATUS.md), with all three Java profiles qualified.
JAVA-08 resource and lifecycle hardening is the recommended next slice and
comes before any concurrency experiment. Public SQL records and concurrency
remain conditional on their design decisions. These are repository tasks, not
externally created issues.

Effort estimates describe scope: **S** is one focused PR; **M** spans a few
components or two separately reviewable changes; **L** requires a design,
prototype and failure/measurement matrix. Qualification time is additional.
Dependencies below order semantic changes, not formatting-only commits.

| Task | Status | Depends on | Size | Outcome |
| --- | --- | --- | --- | --- |
| JAVA-05 | [Complete and qualified](JAVA05_STATUS.md) | JAVA-04 | S | Precise JDK helpers and bounded input with existing output/error contracts. |
| JAVA-06 | [Complete and qualified](JAVA06_STATUS.md) | JAVA-05 | M | Named private socket and committed-poll result values; clearer controller code. |
| JAVA-07 | [Complete and qualified](JAVA07_STATUS.md) | JAVA-06 | S | Grouped validation branches and readable fixtures without changing lexical acceptance. |
| JAVA-08 | Queued hardening; recommended next | JAVA-06 | M | Explicit store-resource and daemon lifecycle failure handling. |
| JAVA-09 | Conditional design | JAVA-06; supported adapter decision | M | Decide whether public SQL snapshot records justify their compatibility cost. |
| JAVA-10 | Conditional study | JAVA-08; measured need | L | Decide whether bounded I/O concurrency or JDK HTTP transport improves the real workload. |

**JAVA-05 — JDK helpers and bounded input**

Scope: `Json.sha/read`, `ProtocolValidator.hash/COUNTERS` and `ProtocolTool.main`
(audit F1–F3). Begin with a formatting-only commit for the touched compressed
methods. Replace digest-format loops with `HexFormat`; use fixed collection
factories only for private nonnull constants. Replace manual bounded readers
with `readNBytes(limit + 1)` and explicit rejection, retaining current stream
ownership, error types, messages, CLI exit codes and output bytes. Use `Path.of`
or local type inference only where a touched expression becomes clearer.

Acceptance: canonical/hash goldens and authority-refresh vectors remain exact;
CLI exact-limit and limit-plus-one cases verify inclusive bounds, no partial
stdout and exit behavior. HTTP oversized/stalled responses retain bounds and
timeouts. No mutable working set, canonical encoder, SQL source or Java 8 helper
is converted incidentally. Existing relevant tests include
`validCorpusMatchesCanonicalBytesAndHashes`,
`authorityRefreshPreservesImmutableBodyHash`, `lexicalAndResourceLimitsReject`,
and `transportBoundsResponsesAndReadWait`.

**JAVA-06 — Private records and explicit committed results**

Scope: `ProtocolValidator.semantics` socket tuples and `NativeEngine.poll`
(audit F4), in separate commits. Add private `AssignedSocket` and `PollResult`
records. Name existing validation/reduction phases and expand compressed code
without changing its operation order. Keep the four socket identity strings,
single write transaction, post-commit publication, cursor check and ACK order.

Acceptance: existing duplicate-name and canonical fixtures are unchanged; add
the missing distinct-name/same-assigned-socket Run rejection and valid variations
of supported socket identity components to the Java/Go corpus. Failed
observation/inventory reduction cannot publish a new unknown-reservation flag
or send an ACK; a successful ACK sees committed state. Existing cursor-gap,
conflicting-sequence, unknown-reservation, cancellation/replay and exact-port
tests pass. The new records do not become serialized API objects, public SQL
rows or mutable-tree wrappers. Coordinate with SUPERVISE-01 if both touch
inventory reconciliation.

**JAVA-07 — Validation control flow and focused fixture readability**

Scope: the enum-token chain in `ProtocolValidator.validate` (audit F5).
Introduce a grouped switch while preserving the null loop sentinel, depth
accounting, original integer-token checks, float rejection and default passage
to schema validation. Improve fixture construction only where it makes intent
clearer; retain deliberate lexical bytes and existing fixture ownership.

Acceptance: valid, invalid and parser-invalid cases still have the same
canonical output or rejection. Unknown/duplicate fields, trailing tokens,
negative zero, exponent forms, non-ASCII/BOM input and depth/size limits retain
their treatment. No global stream conversion, sealed protocol rewrite, enum
serialization or DDL text-block conversion is included.

**JAVA-08 — Resource ownership and daemon lifecycle**

Scope: two independent hardening changes identified in audit F6. First, make
store initialization and lock/channel cleanup attempt all owned closes while
preserving primary exceptions. Retain Java 8 source compatibility for this
shared SQL path unless JAVA-09 has explicitly changed that support contract.
Second, give the standalone server/executors a lifecycle owner that handles
partial startup, interruption, shutdown-hook policy, bounded waits and store-last
closure. Define cancellation behavior before choosing executor close APIs.

Acceptance: targeted failure-injection tests show channel closure is attempted
after lock-release failure, secondary exceptions are suppressed, failed
construction releases ownership, and repeated close follows a documented
contract. Daemon startup/shutdown tests exercise an occupied listen port,
failure after resource creation, interruption and an active blocked request or
controller. Existing owner/schema rejection, rollback-only, process-crash,
reopen and physical recovery tests pass. Record these as hardening findings;
the audit did not reproduce a production resource leak.

**JAVA-09 — Decide SQL record and compatibility-adapter contracts**

Scope: `AttemptRecord`, `CommandRecord`, their Java callers and embedded/legacy
test adapters (audit F7). Start with a short design decision and complete
consumer search. The same `--release 8` helper currently runs against old and
new libraries using public fields. The old smoke path also compiles current SQL
source as Java 8. Select an explicit supported-path strategy before changing
either contract; frozen historical bundles remain unchanged.

Acceptance of the decision: document keep-as-class versus record tradeoffs,
public construction, source/binary API, nullable row fields, equality/hash,
payload-safe `toString`, and old/new helper dispatch. A justified decision to
retain the classes completes the decision task. If records are selected, create
separate implementation commits and require old/new snapshot, write/replay,
rollback and launcher evidence before declaring them complete. Preserve explicit
JSON projections, `JobKey` persisted spelling and exact SQLite DDL. Do not
silently retarget the historical Java 8 fixtures.

**JAVA-10 — Measure a bounded concurrency/HTTP design**

Scope: a design and optional prototype (audit F8), not an executor substitution.
Establish the bottleneck using the JAVA-03 workload and representative overload
or slow-agent cases. Compare bounded network concurrency followed by serialized
reduction with the existing single-owner controller. Evaluate virtual threads
only with explicit admission, cancellation, deadlines and shutdown ownership.
Evaluate `HttpClient` separately, preserving direct proxy policy, TLS identity,
redirect refusal, canonical requests, bounded bodies and stalled-body deadlines.

Acceptance: demonstrate deterministic placement and per-agent command/ACK order,
no transaction sharing, no premature resource release, bounded overload, safe
stop/delivery races and cleanup under blocked I/O. Run equivalent instrumented
Pi trials with the same resource limits; document throughput, latency tails,
memory and thread/FD changes. A no-change decision is valid if the complexity
has no measured benefit. Scoped-value transaction replacement and preview
structured concurrency are outside the prototype. Keep Java 25 as default.

**Validation shared by implementation tasks**

Use existing focused tests during development and add tests for actual behavior
gaps. At each coherent shipped slice, produce a fresh bundle and record the
source/image identity, Java tests, canonical Java/Go corpus, generated launchers,
reproducibility and appropriate two-agent recovery gate. Preserve builds/tests
and launcher execution for `java25`, `java26-runtime` and `java26`; Java 25 is
the required default physical gate. I/O, lifecycle or concurrency changes also
rerun the full physical gate on both Java 26 profiles. Persisted-model changes
require old/new state compatibility. Compare performance when behavior or
performance-sensitive paths change; do not invent a production SLO.

Use the [native build guide](../../build-support/native/README.md) and existing
[JAVA-03 evidence](java03-evidence.json) as the reproducible baseline. Preserve
the original MVP and use new owned lab roots. Historical test counts are a
baseline, not a fixed quota; document meaningful additions.

Legacy findings stay with their established future consumer: queue/update
semantics with POLICY-01, cron cardinality with CRON-01, and old HTTP/utilities
with a retained consumer or retirement decision. They do not justify broad
Guava removal or Java 25 syntax in the Java 8 root build.
