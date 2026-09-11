# Java 25 source audit

Audited on 2026-09-11 against source commit
[`6cf7f0ea07c6355af62ceafb029fe086f55b8c65`](https://github.com/jordanly/aurora/tree/6cf7f0ea07c6355af62ceafb029fe086f55b8c65).
This is an implementation plan and file-by-file review of the **existing Aurora
application**. It changes no Java code. The target remains Java **25+**, with no
Java 8 compatibility requirement and no replacement scheduler.

The best first changes make resource ownership explicit, preserve interruption,
and replace obsolete reflection APIs. Follow those with smaller language and
collection improvements. Public records, time/configuration types, HTTP/security
frameworks, and concurrency models need separate contract reviews.

## Coverage and how to read the results

Every tracked Java file has an assessment, including tests, helpers and JMH
benchmarks. Source links point to the audited commit so evidence does not drift
as implementation proceeds.

| Source set | Files | Lines | File assessments |
| --- | ---: | ---: | --- |
| Original application | 327 | 45,582 | [Application/API/security (151)](java25-audit/application.md), [core/state/storage/updater (176)](java25-audit/core.md) |
| Commons production | 62 | 5,978 | [Commons and benchmarks](java25-audit/commons-and-benchmarks.md) |
| Application tests and helpers | 205 | 42,399 | [Tests](java25-audit/tests.md) |
| Commons tests and helpers | 24 | 2,789 | [Commons and benchmarks](java25-audit/commons-and-benchmarks.md) |
| JMH benchmarks and helpers | 18 | 2,631 | [Commons and benchmarks](java25-audit/commons-and-benchmarks.md) |
| **Total** | **636** | **99,379** | **389 production, 229 test/helper, 18 benchmark files** |

The consolidated results are **72 files with local changes to consider**, **110
with changes deferred to a contract or framework migration**, and **454 retained
without a separate change prioritized in this pass**. There are 188 findings;
several files share one coordinated migration, so these are not 185 separate PRs.

- **Change / local:** a bounded change can be reviewed within the current source
  structure. This is not a claim that the change is mechanical or test-free.
- **Defer / after-boundary:** revisit when the relevant execution, storage, DTO,
  configuration or lifecycle contract is stable. This avoids refactoring code
  twice during Mesos extraction.
- **Defer / framework:** qualify related libraries, wiring and consumers together.
- **Retain:** no separate modernization is prioritized. Interfaces, annotations,
  existing streams/lambdas, mutable algorithms and test fixtures often already
  express their purpose clearly. This is not a correctness certification.

Priorities express implementation value and risk: P1 covers resource ownership,
obsolete integration APIs and important qualification gaps; P2 covers behavior,
test reliability and API improvements; P3 covers readability and small idioms.
They are not vulnerability severity ratings.

The [machine-readable ledger](java25-audit/files.jsonl) contains every source
hash, line anchor, recommendation, caution and proposed validation. The
[manifest](java25-audit/manifest.json) records counts and provenance. Validation
listed on a finding is **required future work**, not a claim that a test was run.

## Concrete findings worth acting on

| Area | Existing code | Improvement and contract to preserve |
| --- | --- | --- |
| Compression resource ownership | `ThriftBinaryCodec.deflateNonNull` | Give the explicitly supplied `Deflater` a try-with-resources lifetime. The output stream only owns a deflater it creates itself. Finish compression before reading the byte array; preserve compressed fixtures and error translation. |
| File and recovery resources | `CommandLineDriverSettingsModule`, `storage.backup.Recovery`, `DurableStorage`, `storage.durability.Recovery` | Close owned credential/backup streams and recovered edit streams on success and failure. Keep replay sequential, preserve transaction locking and observe close failures correctly. |
| Backup names and time | `StorageBackup` | Replace shared `SimpleDateFormat` with immutable `DateTimeFormatter`; retain the existing name pattern, English locale, minute granularity and default zone captured at construction. |
| Cancellation and service shutdown | `AuroraCronJob`, `TaskGroups`, versioned driver services, `BatchWorker`, `TaskReconciler` | Distinguish execution failure from interruption, restore the flag when translating interruption, and give scheduled work an owner and a stop policy. Do not silently move work across transaction boundaries. |
| Constructor reflection | `MoreModules`, `GsonMessageBodyHandler`, `ThriftStatsExporterInterceptor`, test `Generator` and `SchedulerIT` | Replace `Class.newInstance()` with explicit constructor lookup, or direct construction for a statically known type. Preserve accessibility, constructor selection and exception behavior. Cache constructor metadata only if useful; counters must remain fresh per invocation. |
| Kerberos integration | `Kerberos5Realm`, `Kerberos5ShiroRealmModule` | Dispose each authentication context and migrate `Subject.doAs` to `Subject.callAs` with deliberate exception translation. Service credentials and JAAS login have a different lifetime from an individual request. |
| Local JDK APIs | `TransactionRecorder`, `MemJobUpdateStore`, `RescheduleCalculator`, `FieldGetters`, `MemSchedulerStore` | Use guarded `getFirst`/`getLast`, a `reversed()` view used only for reading, `Optional.flatMap`, and direct `AtomicReference` construction where they preserve existing behavior. |
| Stable text conventions | `VolumeConverter`, `TaskStatCalculator`, discovery `Encoding` | Use `Locale.ROOT` for protocol-like names and `StandardCharsets.UTF_8` for already-UTF-8 discovery data. Keep the separate ZooKeeper credential-byte compatibility question explicit. |
| Numeric validity | Commons `Ratio` | Its `== Double.NaN` comparisons never match. Decide whether invalid samples should return zero or propagate NaN, then use `Double.isNaN` if the zero fallback is intended. This needs explicit metric-behavior tests. |
| Equality and internal data | `Query`, `TaskGroupKey`, `TierInfo`, updater values; private `SchedulingMatch` and `PulseState` | Prefer pattern-bound `instanceof` now. Consider private records individually; retain custom equality, hashes, null checks and mutation/identity semantics where required. |
| Test reliability | `CronIT`, `WebhookTest`, storage reader tests and other async tests | Bound test-thread waits, observe worker failures and use event completion instead of arbitrary sleeps. Do not release an inner barrier in a way that hides serialized reads. Investigate the existing ignored multiple-reader test. |

`Deflater` implements `AutoCloseable` in Java 25, making its explicit lifetime a
particularly useful target. This is about prompt resource release, not evidence
of a measured production leak.
[JDK 25 Deflater API](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/zip/Deflater.html),
[DeflaterOutputStream ownership](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/zip/DeflaterOutputStream.html).
`Subject.callAs` wraps action exceptions in `CompletionException`; a textual
replacement of `doAs` would change the current failure contract.
[JDK 25 Subject API](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/javax/security/auth/Subject.html).
These behaviors were also checked against the pinned JDK source archive.

## Ordered implementation batches

These batches refine [INPLACE-09](IMPLEMENTATION_BACKLOG.md) and identify small
improvements that can accompany INPLACE-01/02 before broader modernization.
They do not replace INPLACE-03 through INPLACE-08 or change the dependency order
for Mesos removal and Go-agent integration.

| Batch | Scope | Gate and evidence |
| --- | --- | --- |
| J25-A — Reliable baseline | Keep closing original build/packaging/quality gaps; repair bounded waits in tests touched by subsequent changes. | Preserve discovered/executed/skipped counts. Timeout failures and worker exceptions must reach the test runner. Keep the existing ignored storage test visible. |
| J25-B — Resource ownership | Small PRs for compression, credentials/backup input streams and edit-stream consumption; a separate backup formatter change. | Existing codec, recovery and backup tests plus close-on-failure and filename/zone cases. No replay, ordering or transaction redesign. |
| J25-C — Explicit failure and shutdown | Split interruption catches; remove deprecated constructor reflection. Review `BatchWorker` and reconciliation executor shutdown separately. | Throwing/inaccessible constructors; failed versus interrupted futures; stop during queued work, retries and recovery. Do not hide behavioral changes in syntax cleanup. |
| J25-D — Authentication lifetime | Per-request GSS disposal, `Subject.callAs` exception translation, then explicit service credential/login cleanup. | Existing security suites plus context isolation, error/disposal paths and a disposable-KDC handshake. Framework upgrade remains a separate batch. |
| J25-E — Local language and collections | Pattern bindings, small switch expressions, Optional composition, precise collection APIs, explicit locale/charset, selected private records. | Existing affected suites; focused coverage only for exposed gaps in equality, nulls, ordering, aliasing or exceptions. Defer files still being reshaped by execution/storage extraction. |
| J25-F — Typed boundaries and generators | Configuration snapshots; `Duration`/`Instant`/clock boundaries; shared value types; generated wrapper simplification. | Stable backend/storage contracts, explicit wire-unit conversions, generator goldens and API/JSON/storage compatibility. Broad rollout follows INPLACE-08. |
| J25-G — Framework and test stack | HTTP/Servlet/JAX-RS/DI/Shiro/JSON qualification and coordinated Jupiter test infrastructure. | Original API/UI/auth routes, rules/teardown/mock verification, packaging and full test discovery remain covered. Pick and pin supported versions during implementation. |
| J25-H — Measured concurrency | Evaluate virtual threads for bounded blocking I/O once framework and task ownership permit them. | Admission limits, cancellation, request identity/auth propagation, shutdown, latency and Pi memory measurements. Preserve serialized scheduler/state work. |

**Recommended next code PR:** start J25-B with explicit lifetimes in
`ThriftBinaryCodec`, credential-file reading and backup-file reading. Exercise
normal and failed decoding/compression and preserve historical wire fixtures.
Follow with edit-stream ownership and the backup formatter as separate small
changes. This yields a useful Java 25 improvement without a large DTO or
framework migration.

Use Luna for bounded edits with explicit contracts and test cases. Use Astra
medium for authentication, lifecycle, transactions and compatibility questions.
The orchestrator reviews diffs and test evidence before accepting each change.

## What “idiomatic Java 25” means here

Use the stable language and library features available on the target. Many useful
features predate release 25: records and `instanceof` patterns, switch expressions
and patterns, sequenced collections, and virtual threads. Java 25 also finalizes
flexible constructor bodies, compact source files/instance main methods and module
imports. Those last two offer little benefit to Aurora's established packaged
application entry point, so they are not recommended merely for novelty.
[Java 25 language changes](https://docs.oracle.com/en/java/javase/25/language/java-language-changes-summary.html).

Apply the following rules during implementation:

- **Records are a design choice.** Private immutable tuples are good candidates.
  `Pair`, `Amount`, discovery JSON objects, CLI options, template-facing beans,
  mutable counters and generated Thrift wrappers need their own compatibility
  analysis. Default record accessors, finality, equality/hash and string output
  can differ from current contracts. Updater `SideEffect` deliberately excludes
  its failure field from equality; generated record equality would change it.
- **Choose collection semantics explicitly.** `Stream.toList()`, JDK immutable
  factories and Guava collections are not interchangeable with mutable lists,
  null-bearing inputs or insertion-ordered sets/maps. Retain useful Guava
  abstractions such as multisets, ranges and cache ownership. The custom list
  collector can delegate to Guava's existing collector; review set/map collector
  characteristics separately because Aurora explicitly declares `UNORDERED`.
- **Keep units at boundaries.** Use `Duration` for durations and `Instant` for
  timestamps where useful, with an injected clock for tests. Convert explicitly
  to current epoch-millisecond wire/storage fields. `Amount` also represents
  data and numeric conversion rules; it is not universally replaceable by
  `Duration`. Scheduling delays and elapsed-time measurement need their own
  clock decisions.
- **Preserve task ownership.** A virtual thread is not a replacement for a
  scheduled executor, a transaction queue, a lock or backpressure. Evaluate
  blocking-I/O workloads separately from serialized scheduling and state changes.
  [Virtual threads design](https://openjdk.org/jeps/444).
- **Avoid preview dependencies in the baseline.** `ScopedValue` is final in 25,
  but integrating it with Shiro/request context still needs explicit propagation
  design. `StructuredTaskScope` and primitive patterns remain preview in 25;
  they are not prerequisites for this plan.
  [ScopedValue API](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/ScopedValue.html),
  [StructuredTaskScope API](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/StructuredTaskScope.html),
  [Java 25 language changes](https://docs.oracle.com/en/java/javase/25/language/java-language-changes-summary.html).
- **Modern tests do not require a per-file framework switch.** Scope exception
  assertions to the operation under test. Coordinate Jupiter with shared rules,
  EasyMock lifecycle and test discovery. An explicit assertion API is also
  available in JUnit 4.13 if an interim change is useful; Java 25 itself does not
  require Jupiter. [JUnit 4.13 assertion API](https://junit.org/junit4/javadoc/4.13/org/junit/Assert.html).
  Benchmark syntax changes should not silently alter setup,
  workload, warmup, forks or measured work.

## Generated Java and build-owned work

The cached build also contains **228 generated Java files**: **139 Thrift output
files** and **89 immutable wrapper/service-metadata files**. The separate
[generated-output inventory](java25-audit/generated.jsonl) records all filenames
and hashes. This is generator-family coverage, not 228 claimed manual source
reviews; these files are not part of the 636 tracked sources.

Change their owners:

- Thrift schemas in `api/src/main/thrift`, and
  `buildSrc/src/main/groovy/org/apache/aurora/build/ThriftPlugin.groovy`.
- `src/main/python/apache/aurora/tools/java/thrift_wrapper_codegen.py` and
  `buildSrc/src/main/groovy/org/apache/aurora/build/ThriftEntitiesPlugin.groovy`.

Preserve field/enum/union IDs, optional/default-field behavior, immutable/mutable
builder ownership, comparison and RPC metadata. Qualify a newer Thrift toolchain
through deterministic generation and wire/storage fixtures before using its
output. Do not edit cached generated `.java` files or turn every wrapper into a
record. Generated annotation and Python 3 fixes already in the baseline remain
separate from any new generator modernization.

## Evidence and limits

The review split production core to Astra medium and bounded commons/test work
to Luna; the orchestrator reviewed application/API/security sources, checked
recommendations and corrected false positives and overly generic suggestions.
The review combines source inspection, call-site/test context and a complete
path/hash ledger. It is an initial modernization review, not a full correctness,
security, dependency or performance audit of every method.

Automated integrity checks matched all 636 entries to `git ls-files '*.java'`,
verified source hashes and every finding's source-line evidence, and checked
named existing test references. Generated hashes describe the cached baseline
outputs and may require regeneration to verify in a fresh checkout.

No Java, build or dependency changes were made and no Gradle tests were rerun for
this documentation audit. The audited implementation previously passed **1,294
Java tests with one existing skip**, plus **13 generator/Python tests**; that
does not qualify proposed changes. The UI, full distribution/quality checks,
fresh-checkout reproducibility and remaining native integration gaps are still
tracked in the [build status](INPLACE01_BUILD_STATUS.md).

Treat a changed source hash as a prompt to revisit that file's assessment. Keep
this baseline immutable; record implemented findings and new evidence alongside
the corresponding slice status rather than rewriting historical findings as if
they were already true of the source.
