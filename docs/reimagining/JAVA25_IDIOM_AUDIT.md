# JAVA-04: Java 25 idiomatic-code audit

There are worthwhile, bounded improvements in the standalone Java code. Start
with JDK utility APIs and clearer internal value types, then simplify selected
validation branches. Preserve the existing persistence and protocol boundaries.
Concurrency and public model changes need their own designs and evidence.

This review examines source commit `37132613b86da3ac107b7973bcd2965a35c6423d`
on `codex/standalone-foundations`, following the [JAVA-03 qualification](JAVA03_STATUS.md).
Fork and upstream master were rechecked at
`11ebaeeb071cb182c388a40755e84f60dda32260`. The [inventory](java04-inventory.json)
records source hashes; the [implementation tasks](JAVA25_REFACTOR_TASKS.md)
translate these findings into small changes. No application source, dependency,
schema, fixture or running container was changed by this audit.

**Coverage and current build boundaries**

All 647 tracked `.java` files, totaling 101,941 physical lines including comments
and blanks, are inventoried. Manual review covered the eight active main-source
files and their four test files, with representative inspection of legacy
policy, cron, HTTP, utility and test code. This is not an exhaustive manual
review of all legacy lines. Source inventory and successful build coverage are
different claims.

| Source area | Main files | Test files | Current disposition |
| --- | ---: | ---: | --- |
| `scheduler/native/src` | 4 | 2 | Standalone scheduler, configuration, JSON and HTTPS. |
| `protocol/java/src` | 2 | 1 | Standalone strict validator and packaged CLI. |
| `src/{main,test}/java/.../storage/sql` | 2 | 1 | Store plus qualification helper; also consumed by the older Java 8 smoke path. |
| Remaining `src/{main,test}/java` | 327 | 203 | Retained legacy scheduler and declared root-build tests. |
| `commons/src` | 62 | 24 | Legacy utility/dependency graph. |
| `src/jmh/java` | 18 | — | Legacy benchmark sources; outside current standalone qualification. |
| `protocol/native-v1alpha1/conformance/Canonical.java` | 1 | — | Deliberate Java 8 conformance adapter. |

The standalone Gradle build compiles 12 Java files: seven production source
files, one qualification helper and four test files. `NativeStoreTool` is
compiled but explicitly excluded from the production scheduler JAR at
[build.gradle:111](../../scheduler/native/build.gradle#L111).
The qualified baseline has 41 Java tests, 48 packaging tests and 72 lab tests;
these are historical JAVA-03 results, not tests rerun for this documentation audit.

The root [build.gradle:28](../../build.gradle#L28) still declares Java 8 source
and target levels. Its legacy source sets, commons dependencies, Thrift
generation and JMH tasks are declared consumers, without a fresh successful
root-build claim here. Future POLICY-01/CRON-01 consumers remain planned.

The inventory also accounts for Java outside `.java` files: the embedded
`JvmCompatibility` helper, smoke/build wiring and the Thrift wrapper generator
and plugins. Generated output under ignored build directories is excluded;
the tracked generators are recorded instead. The 130-line embedded helper is
hashed as its exact Python string value, including its surrounding newlines.

**Verified Java 25 feature choices**

Idiomatic Java 25 includes useful features finalized in earlier releases.
The following availability checks use official Java 25 documentation. Keep
preview disabled and retain both qualified Java 26 profiles.

| Feature | Availability | Audit decision |
| --- | --- | --- |
| `var`; switch expressions; text blocks | Final since 10, 14 and 15 respectively. | Selective readability use; preserve literal contents. |
| Records and `instanceof` patterns | Final since 16. | Prefer private value records; check shared Java 8 source consumers first. |
| Sealed types | Final since 17. | No compelling small hierarchy conversion identified. |
| Record patterns and pattern `switch` | Final since 21. | Available; use only where actual branching becomes clearer. |
| Unnamed variables/patterns | Final since 22. | Optional for truly unused values; no broad rewrite. |
| Module imports, compact source/instance main, flexible constructors | Final in 25. | No useful production conversion identified; preserve explicit entrypoints and imports. |

These language statuses are documented in Oracle's
[release-by-release language changes](https://docs.oracle.com/en/java/javase/25/language/java-language-changes-release.html).

| API | Availability and decision |
| --- | --- |
| [`HexFormat`](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/HexFormat.html) | Since 17; use its lowercase digest formatting. |
| [`InputStream.readNBytes(int)`](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/io/InputStream.html#readNBytes(int)) | Since 11; use an explicit maximum plus one-byte oversize check. |
| [`List`](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/List.html) / [`Set`](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/Set.html) factories | `of` since 9, `copyOf` since 10; select only where null, mutation, duplicate and ordering behavior fit. |
| [Virtual threads](https://docs.oracle.com/en/java/javase/25/core/virtual-threads.html) | Final since 21; future bounded-concurrency study, with an explicit admission policy. |
| [`ScopedValue`](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/ScopedValue.html) | Final in 25; retain current transaction `ThreadLocal` for now. |
| [`HttpClient`](https://docs.oracle.com/en/java/javase/25/docs/api/java.net.http/java/net/http/HttpClient.html) | Since 11; optional transport study with TLS, proxy, body-bound and deadline parity. |

Structured concurrency and stable values remain on the
[Java 25 preview API list](https://docs.oracle.com/en/java/javase/25/docs/api/preview-list.html).
Primitive patterns remain a language preview; string templates were withdrawn,
as recorded in the language history above. None belongs in the default build.

**F1 — Make the active code easier to review**

[NativeSchedulerMain.main:33](../../scheduler/native/src/main/java/org/apache/aurora/nativescheduler/NativeSchedulerMain.java#L33)
and [NativeEngine.place:211](../../scheduler/native/src/main/java/org/apache/aurora/nativescheduler/NativeEngine.java#L211)
pack declarations, branches and side effects onto single lines. Expand touched
methods into conventional formatting and name existing phases. Keep ordinary
loops where their order, early return or mutation describes the algorithm.

Representative change: `String prior=...; if(prior!=null) { ... }` becomes a
separate declaration and normally formatted branch. Use `var` for a clear local
constructor result when helpful, retaining explicit types at API boundaries.
Explicit imports should show the small standalone dependency graph. No new
formatter dependency or repository-wide reformat is necessary.

Benefit: reviewers can see validation, durable writes and network effects in
order. Risk: extraction can accidentally move transaction boundaries, UUID/time
sampling or exception handling. Keep formatting separate from semantic edits;
keep placement in configuration-node order, existing query order, and the
single-owner controller. Effort: **S**, folded into JAVA-05/06. Existing engine
replay, capacity/port and cancellation tests remain the behavior authority.

**F2 — Replace repeated utility machinery with precise JDK APIs**

[Json.sha:60](../../scheduler/native/src/main/java/org/apache/aurora/nativescheduler/Json.java#L60)
and [ProtocolValidator.hash:267](../../protocol/java/src/main/java/org/apache/aurora/nativeprotocol/ProtocolValidator.java#L267)
format each SHA-256 byte through `String.format`. The proposed shape is:

```java
// Before: loop over digest, append String.format("%02x", value & 0xff).
return HexFormat.of().formatHex(digest);
```

This expresses encoding directly and preserves two lowercase digits per byte.
Keep the input charset, canonicalization and exception behavior of each helper.
There is no measured speedup claim.

The validator's private [COUNTERS:45](../../protocol/java/src/main/java/org/apache/aurora/nativeprotocol/ProtocolValidator.java#L45)
can change from `new HashSet<>(Arrays.asList(...))` to `Set.of(...)`: its members
are fixed, distinct and nonnull. Mutable `capabilities`, `names`, `sockets` and
the removal-based set in `Json.fields` must remain mutable. Existing owned
unmodifiable SQL lists already protect their backing lists; replacing all of
them with `copyOf` offers little benefit. A collection copy does not make mutable
elements immutable, and nullable SQL row components must remain representable.

Effort: **S**, JAVA-05. Verify exact canonical/hash goldens and
`authorityRefreshPreservesImmutableBodyHash`; existing byte comparisons are
stronger evidence than a test that merely restates the new expression.

**F3 — Keep bounded reads, with less handwritten buffering**

[ProtocolTool.main:25](../../protocol/java/src/main/java/org/apache/aurora/nativeprotocol/ProtocolTool.java#L25)
and [Json.read:36](../../scheduler/native/src/main/java/org/apache/aurora/nativescheduler/Json.java#L36)
both accumulate chunks and enforce a 1 MiB limit. A representative replacement
for the latter is:

```java
byte[] bytes = input.readNBytes(LIMIT + 1);
if (bytes.length > LIMIT) {
  throw new IOException("Body too large");
}
return bytes;
```

Keep the CLI's `IllegalArgumentException` category, redacted stderr, exit 2,
canonical stdout and final newline. Keep stream ownership with its current
caller and retain transport timeouts. This reads at most limit + 1 rather than
potentially a whole additional chunk; oversize consumption becomes tighter.
`readAllBytes()` would lose the bound. Do not truncate oversized input and then
validate the prefix.

Effort: **S**, JAVA-05. Existing validator limits do not fully exercise CLI
buffering. Add meaningful exact-limit and limit-plus-one packaged CLI cases,
plus a partial-read stream check where needed. Retain HTTP oversize and stalled
response tests, including the error type and message they assert.

**F4 — Introduce private records at two clear internal boundaries**

At [ProtocolValidator.semantics:146](../../protocol/java/src/main/java/org/apache/aurora/nativeprotocol/ProtocolValidator.java#L146),
`Set<List<String>>` represents socket identity using network, protocol, family
and port-number text. Replace that positional tuple with:

```java
private record AssignedSocket(
    String network, String protocol, String family, String number) {}
```

Keep the same four validated strings and duplicate detection. Converting the
number to an integer at the same time would expand the change's semantics.

At [NativeEngine.poll:162](../../scheduler/native/src/main/java/org/apache/aurora/nativescheduler/NativeEngine.java#L162),
a captured `boolean[]` passes the unknown-reservation result out of a transaction
whose return value is its committed cursor. Use:

```java
private record PollResult(String committedCursor, boolean unknownReservations) {}
// Within the existing write callback, return both computed values:
return new PollResult(tx.committedCursor(node.scope()), unknown);
```

The callback uses a normal local boolean. Publish the flag only after
`store.write` returns successfully, then compare the committed cursor and send
the ACK in the same order as today. Keep observation reduction and inventory
validation in one transaction. The existing array is not a demonstrated bug;
the record makes the result and ownership clearer. These sketches show proposed
shapes, not complete replacement methods.

Effort: **M**, JAVA-06, split into two independently reviewable commits. The
existing `duplicate-port.json` rejects repeated Job port names; it does not
exercise two distinct Run port names assigned the same socket. Add that
missing Java/Go conformance case and valid variations of supported socket
identity components. Retain cursor-gap/conflicting-sequence and unknown
reservation placement tests. Add a failed-reduction case proving that no new
flag is published and no ACK is sent, plus successful ACK visibility of the
committed state. Records here have private scope and immutable components.

**F5 — Use grouped switches for finite validation branches**

[ProtocolValidator.validate:78](../../protocol/java/src/main/java/org/apache/aurora/nativeprotocol/ProtocolValidator.java#L78)
has a token-type `if` chain. A grouped enum switch can display the cases clearly:

```java
case START_ARRAY, START_OBJECT -> require(++depth <= 64, "document exceeds nesting limit");
case END_ARRAY, END_OBJECT -> depth--;
case FIELD_NAME, VALUE_STRING -> ascii(parser.getText());
// Preserve integer-token validation, float rejection and the permissive default.
```

Keep the parser's null end sentinel outside the switch and retain original-token
checks before tree parsing. Boolean and null values currently reach schema
validation; a blanket rejecting default would change behavior. Avoid rebuilding
Jackson trees as a sealed hierarchy just to use pattern matching. HTTP route
extraction should likewise retain authorization before routing and exact status
and request checks.

Effort: **S**, JAVA-07. Use the valid, invalid and parser-invalid corpus and
`lexicalAndResourceLimitsReject`. Text blocks may help readable test inputs, but
preserve fixture bytes where lexical form is what a test checks. The existing
terminal-state equality chain is already clear and null-tolerant; replacing it
with a switch adds a null-handling obligation without a strong benefit.

**F6 — Separate resource-lifecycle hardening from syntax cleanup**

[NativeSqlStore.close:609](../../src/main/java/org/apache/aurora/scheduler/storage/sql/NativeSqlStore.java#L609)
releases the lock before closing its channel; a release exception skips channel
close after `closed` has already been set. Constructor cleanup and `connect`
also contain sequential closes that can mask the initiating failure. This is a
source-visible failure-path weakness, not a reproduced production failure.
The proposed shape is an ownership helper that always attempts both cleanup
steps and suppresses secondary exceptions onto the primary one. Transfer
ownership only after successful initialization; do not close a long-lived lock
at the end of a constructor's successful try-with-resources block.

[NativeSchedulerMain.main:51](../../scheduler/native/src/main/java/org/apache/aurora/nativescheduler/NativeSchedulerMain.java#L51)
creates a server and two executors, then depends on a shutdown hook for cleanup.
Partial startup failure and main-thread interruption need an explicit lifecycle.
Introduce a runtime owner with bounded shutdown, partial-start cleanup, hook
policy and store-last closure. Java's
[`ExecutorService.close`](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/ExecutorService.html#close())
waits for termination, so replacing `shutdownNow` with automatic close changes
shutdown behavior.

Effort: **M**, JAVA-08, separate store and daemon changes. Add failure-injection
tests for release/close failure, preserved primary exceptions, partial startup,
interruption and shutdown during active I/O. Retain normal ownership, crash and
reopen tests. The transaction rollback path already suppresses rollback failures
and uses try-with-resources; preserve that working structure.

**F7 — Defer public SQL records until compatibility is resolved**

[AttemptRecord and CommandRecord:530](../../src/main/java/org/apache/aurora/scheduler/storage/sql/NativeSqlStore.java#L530)
look like record candidates because their components are strings/primitives.
However, `public record CommandRecord(...)` changes private construction to
public canonical construction, public fields to accessors, and identity-based
equality to structural equality. Generated `toString` would include command or
run bodies. `AttemptRecord.node` and `runBody` can be null after a left join.
Explicit JSON projection must remain independent of Java representation.

There is an actual compatibility consumer:
[native-jvm-compat:48](../../build-support/native/native-jvm-compat#L48) reads
`CommandRecord` fields. It compiles one helper with `--release 8` and executes
that same class against historical and new bundles. Accessors added only to
today's classes cannot fix historical bundles. Separately,
[native-smoke:157](../../build-support/lab/native-smoke#L157) still compiles the
current SQL sources with Java 8 source/target settings. Even a new `instanceof`
pattern in `JobKey.equals` would break that source path.

Effort: **M**, conditional JAVA-09. First decide the supported smoke and helper
contracts and a versioned-helper or adapter strategy. Public-row record adoption
then needs explicit API, construction, equality and redacted-diagnostic decisions
plus old/new state tests. Until that benefit is established, keep the classes.
`JobKey.toString()` is the persisted slash-separated key and its hash is based
on that exact string; its current representation is purposeful. `Journal` and
the enclosing-instance-dependent `NativeConfig.Node.scope()` also should not
be converted mechanically.

**F8 — Keep concurrency changes conditional and measured**

The HTTP pool has two threads and a 16-entry queue. `submit`, `stop`, `tick` and
`state` synchronize on the engine; `tick` performs network requests while holding
that monitor. A virtual-thread-per-task executor removes the current admission
bound while leaving serialization in place. Concurrent polling also changes
node ordering, mutable accounting, reduction and ACK behavior. The store still
serializes writes and rejects cross-thread transaction access.

A future shape would fetch bounded network results concurrently and apply them
through a serialized owner, with explicit admission, deadlines and cancellation.
That is a design change. Oracle's [virtual-thread guide](https://docs.oracle.com/en/java/javase/25/core/virtual-threads.html)
describes their scaling use; it does not establish a benefit for this two-agent
workload. Keep the measured JAVA-03 baseline as the comparison point.

The store's [ThreadLocal transaction context:223](../../src/main/java/org/apache/aurora/scheduler/storage/sql/NativeSqlStore.java#L223)
also handles nested read restrictions, rollback-only poisoning, lifetime and
thread ownership. `ScopedValue` provides an immutable binding, not an immutable
JDBC transaction. Its one current binding/removal site offers little reason for
a replacement; never propagate this mutable transaction into child work.

Effort: **L**, conditional JAVA-10 after lifecycle work. A JDK `HttpClient`
experiment belongs here as a separately measured option: prove direct routing,
mTLS/hostname checks, redirect refusal, canonical bodies, size limits and stalled
body deadlines before replacing `HttpsURLConnection`. Preserve bounded buffers
and connection cleanup. Add overload, stop/delivery races, blocked-I/O shutdown,
active cross-thread transaction rejection and deterministic placement tests.

**Preserve these contracts during all implementation**

- [ProtocolValidator.Message:282](../../protocol/java/src/main/java/org/apache/aurora/nativeprotocol/ProtocolValidator.java#L282)
  has private validated construction and defensive copies of its mutable JSON
  tree and byte array. A plain record would expose those referents and allow
  unvalidated construction. Retain it; the [Record API](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/Record.html)
  does not make component objects deeply immutable.
- [NativeSqlStore.verifySchema:205](../../src/main/java/org/apache/aurora/scheduler/storage/sql/NativeSqlStore.java#L205)
  compares exact SQLite catalog SQL with a recreated schema. Pretty multiline
  DDL can reject old databases even when SQL meaning is unchanged. Preserve
  DDL text exactly or treat normalization as a separate schema compatibility
  change. SELECT-only queries do not have that catalog-text constraint.
- Keep duplicate/unknown-field rejection, lexical checks, canonical key/array
  ordering, uint64 counters and explicit JSON projection. Streams, automatic
  DTO serialization, broad dependency removal and stronger enum models are
  not mechanical equivalents of these behaviors.
- Keep original launchers, Java 8 compatibility adapters, crash-before/after
  halt points and historical fixtures within their declared contracts.

**Representative legacy findings and disposition**

| Source and current shape | Possible improvement | Required preservation / disposition |
| --- | --- | --- |
| [QuitHandler.quit:60](../../commons/src/main/java/org/apache/aurora/common/net/http/handlers/QuitHandler.java#L60): raw thread per POST. | Lifecycle-owned shutdown submission. | Define duplicate admission and asynchronous response semantics first. Legacy-only hardening; no proven production incident. |
| [TaskGroup.peek:51](../../src/main/java/org/apache/aurora/scheduler/scheduling/TaskGroup.java#L51): FIFO prefix collected as an immutable set. | Explicitly document and characterize prefix selection, duplicates and result iteration before changing collection APIs. | Preserve selection-before-dedup and synchronized snapshot behavior. The custom collector declares `UNORDERED`; that alone does not prove callers ignore observed iteration order. Defer to a retained scheduling consumer. |
| [ActiveLimitedStrategy.getNextGroup:45](../../src/main/java/org/apache/aurora/scheduler/updater/strategy/ActiveLimitedStrategy.java#L45): Guava sorted copy, limit, then set. | A `Comparator` and JDK stream can reduce adapter APIs. | Sorting determines which instances survive the limit even though output is a set. Preserve membership, active limits and rollback arithmetic; only port with POLICY-01 fixtures. |
| [CronSchedulerImpl.getSchedule:48](../../src/main/java/org/apache/aurora/scheduler/cron/quartz/CronSchedulerImpl.java#L48): FluentIterable plus `getOnlyElement`. | A JDK collection/stream adapter with explicit cardinality validation. | `findFirst` would change zero/multiple-trigger behavior. Preserve this method's distinct contract from debug inspection; defer to CRON-01. |
| [LeaderHealth.RESPONSE_CODES:48](../../src/main/java/org/apache/aurora/scheduler/http/LeaderHealth.java#L48) and [JobUpdateControllerImpl.STATE_MAP:812](../../src/main/java/org/apache/aurora/scheduler/updater/JobUpdateControllerImpl.java#L812): generic `Pair` values/keys. | Named `HealthResponse` / `UpdateState` records in retained consumers. | Preserve status/body/help ordering and transition-map membership. Generic `Pair` permits nulls, subclassing and existing equality/hash behavior; do not globally replace it. |

Each legacy candidate is **S–M** once its consumer and Java target are selected.
Existing legacy unit tests are the intended characterization sources; this
review does not claim they execute in the standalone gate. Guava and the old
DI/HTTP/Quartz graph are absent from the shipped standalone classpath, so these
changes would not reduce that artifact's dependencies. Keep legacy work linked
to the corresponding policy/cron/retirement task instead of starting a broad
Java 25 rewrite of the Java 8 root build.

**Review evidence and completion**

Luna independently inventoried sources and inspected representative legacy code;
Astra medium reviewed transaction, protocol, model and concurrency proposals.
The parent checked every inventory path/hash/line count, supplemental hashes,
the actual embedded helper bytes, build boundaries and cited findings. Review
corrections included the excluded qualification class, embedded-helper newline
hash, legacy collection/cardinality semantics and old/new field-access contract.

Validation for this audit consists of source/provenance, feature documentation,
reference and consistency checks. Implementation examples are illustrative and
were not compiled as patches. No build, cluster fault gate or performance trial
was rerun for unchanged application code. JAVA-04 completion means the audit and
ordered tasks were delivered; JAVA-05 through JAVA-10 were unimplemented at
that audit checkpoint. Their subsequent implementation and conditional design
decisions are now complete; see the [current task status](JAVA25_REFACTOR_TASKS.md)
and [JAVA-08 qualification](JAVA08_STATUS.md).
