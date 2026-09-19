# Foundation, lifecycle, metrics and benchmark review — 2026-09-18

Baseline: `91b9bd74746d27102a96fa47d4e06e0dd25097ca`. All **173 assigned files / 21,243 lines** were read, including commons, scheduler foundations, tests, benchmarks and build helpers. The ledger identifies each source hash and a file-specific decision. Some reads elided block comments/import boilerplate; contracts and callers were revisited for substantive findings. P1 means correctness/security first, P2 a worthwhile behavioral or structural improvement, P3 optional cleanup. Local means a contained change; coordinated means multiple contracts or components must change together. Neither is an instruction to change every line of a file.

## FND-001 — P1: Give diagnostic exports and object logging a safe data boundary

`commons/src/main/java/org/apache/aurora/common/stats/JvmStats.java:207` exports JVM input arguments, followed by every system property at line 215 and every environment value at line 222. `StatsModule.java:72` connects the global registry to `/vars` and `/vars.json`; these handlers serialize supplied values without filtering. Application review confirmed their Jetty bindings and that the API-specific Shiro filter does not establish authorization for these diagnostic routes. Separately, `src/main/java/org/apache/aurora/scheduler/events/Webhook.java:76` logs `WebhookInfo`, whose `toString` at `WebhookInfo.java:164` includes header values and the complete URI. Authorization headers and URI credentials can therefore enter logs. This is source-confirmed behavior, not a claim that a particular secret was present on the running Pi; no live secrets were scraped.

Use explicit safe diagnostic fields/allowlists and a shared redaction policy with APP-003. Log header names and safe destination metadata, not header values or credential-bearing URI components. Do not rely on authentication alone to make arbitrary environment export appropriate. Preserve ordinary metric names/types/order. Validate with synthetic secret sentinels through actual export/logging boundaries; update the current WebhookTest toString expectations deliberately. Avoid automatic record toString for credential-bearing objects.

## FND-002 — P1: Make batch futures represent durable outcomes

`src/main/java/org/apache/aurora/scheduler/BatchWorker.java:283` opens one write for a batch, but line 288 completes each future immediately and line 292 schedules retries before the remaining items and commit. If a later item or commit fails, SQLite rolls back the transaction while an earlier future stays successful; inline completion callbacks can already have run. `AuroraCronJob.java:241` waits on such a result. `BatchWorkerTest.java:233` explicitly preserves earlier success on same-batch failure, so this is a **coordinated contract correction**, not a semantics-preserving rewrite. The Astra policy reviewer independently confirmed the path.

Stage values and retry requests during the write. Complete/schedule only after successful outer commit; fail affected results on rollback or uncertain commit. Account for nested writes and post-commit publication failures rather than treating any method return as sufficient. Keep the recently added admission/stop/failure cleanup and active-transaction shutdown behavior. Introduce a named retry state or sealed result (`Completed<T>`/`Retry`) only after deciding the outcome contract; two independent Optionals and a boolean/value pair permit meaningless combinations today. Do not incidentally change the tested poll-to-tail order. Validate with real SQLite: successful first mutation followed by a failing second mutation, commit failure, callbacks reading committed data, retry enrollment, stop races and nested writes. Preserve TaskStatusHandlerImpl's already-correct acknowledgement-after-write boundary.

## FND-003 — P2: Return detached, coherent metrics snapshots

`commons/src/main/java/org/apache/aurora/common/stats/TimeSeriesRepositoryImpl.java:195` returns an unmodifiable **live** timestamps iterable; line 216 does the same for samples. Mutation occurs under the repository monitor at line 172, but callers iterate after leaving that monitor. `TimeSeriesDataSource.java:73` reads timestamps and series separately. Sampling/eviction can therefore change values during a response, misalign columns or invalidate iterators. Read-only wrappers do not provide snapshots.

Add one snapshot operation that copies the requested timestamps and columns under the existing lock, then serialize outside it. A private immutable snapshot record fits this correlated data. Preserve zero backfill, bounded eviction, requested column order, and strict greater-than time filtering. Also fix retained-count arithmetic at repository line 90: converting both periods to whole seconds makes a legal positive subsecond sampling period zero before division. Use checked common-unit/Duration arithmetic and bounded sample-count validation. Validate mutation after snapshot acquisition, concurrent eviction/HTTP reads, delayed registration, 500 ms periods and overflow/retention boundaries. Do not introduce a second asynchronous metrics cache.

## FND-004 — P2: Retire generic quantity arithmetic incrementally, beginning with value correctness

`commons/src/main/java/org/apache/aurora/common/quantity/Amount.java:80` hashes its raw `(value, unit)` Pair, while equality at lines 85–121 normalizes units. The local Java 25 probe confirms `1L DAY` equals `24L HOURS`, yet their hashes differ and a HashSet retains both. Conversion at lines 143–209 also routes integral types through double; `asChecked` at line 67 identifies overflow by equality with the maximum value, including an exactly representable maximum.

Characterize cross-unit equality/hash, numeric-type distinctions, exact large integers, negative values, truncation and overflow before repairing this abstraction. Use `Duration` for internal time spans and an exact domain-specific byte quantity where needed; retain CLI unit spelling and conversion adapters. Do not replace Amount wholesale with a generic record or casually hash a rounded double: that can preserve neither equality nor exact arithmetic. Keep Clock's monotonic ticker/wait seams—`java.time.Clock` alone cannot replace them. Validate hash collection behavior, existing mixed-unit tests and boundary conversions, then migrate consumers package by package. Configuration TimeAmount/DataAmount compatibility belongs to the same coordinated work.

## FND-005 — P2: Make Credentials an actual immutable value

`commons/src/main/java/org/apache/aurora/common/zookeeper/Credentials.java:80` compares auth bytes by content via EqualsBuilder, but line 88 uses array identity inside Objects.hashCode. The probe confirms equal instances can have different hashes and occupy two HashSet entries. Constructor line 52 and accessor line 70 also expose the backing mutable bytes.

Use defensive byte copies, `Arrays.equals` and `Arrays.hashCode`, with an explicitly non-secret textual representation if one is needed. A default record is insufficient for arrays. Preserve scheme validation and digest encoding compatibility; make UTF-8 explicit only after verifying the configured ZooKeeper peer contract. Validate equal independently allocated byte arrays, HashSet lookup and mutation of input/output arrays. This is still used by discovery configuration even though HA is deferred.

## FND-006 — P2: Give asynchronous resources and shutdown one explicit owner

`Lifecycle.java:62` consumes interruption and calls shutdown while holding its waiter monitor; line 80 executes cleanup before notifying waiters. `ShutdownRegistry.java:75` runs arbitrary callbacks while holding its own monitor. `SchedulerLifecycle.java:204`, `:270` combine registry callbacks, reentrant state transitions, leader withdrawal, driver waits and storage stop; line 293 has an unbounded driver wait. This is a difficult lifetime boundary, not a case for swapping synchronized with virtual threads.

Extract named lifecycle actions and explicit bounded shutdown phases, with a state/completion signal that distinguishes requested shutdown from finished cleanup. Snapshot callbacks under lock and run them outside only after preserving idempotence, LIFO order, reentrancy and waiter semantics. Ensure final notification/interrupt policy is deliberate and preserve primary/suppressed failures. Keep fail-stop and leader advertisement ordering. Own executors created by `SchedulerModule.java:91`, `AsyncModule.java:67` and `PubsubEventModule.java:86`; their current shutdown paths rely on VM exit. Move the webhook client's acquisition at `WebhookModule.java:92` into an owner that handles partial startup as well as successful stop. Do not shut down shared executors from individual consumers.

A contained companion fix: `AsyncUtil.java:157` calls Future.get in afterExecute without handling CancellationException, so normal cancellation can escape the hook and terminate a worker. Handle cancellation explicitly, retain failure logging and interrupt restoration, and decide remove-on-cancel policy with timer users. Validate partial startup, concurrent/reentrant shutdown, callback failure, cancelled timers and zero callbacks after stop, using real cancellable handles. FakeScheduledExecutor currently returns null futures and cannot establish cancellation behavior. This does not add HA or alter the entrypoint.

## FND-007 — P2: Keep the state-machine abstraction but make its concurrency boundary honest

`commons/src/main/java/org/apache/aurora/common/util/StateMachine.java:144` captures currentState before taking the write lock at line 146. Concurrent transitions can therefore emit an incorrect from-state. `Builder.build` at line 415 also shares the builder's mutable transition multimap with the built machine. Freeze transition rules and capture the actual old state inside the transition critical section.

Callbacks currently execute outside the lock and can recursively transition (SchedulerLifecycle does this). Preserve/reconsider their ordering explicitly; simply moving arbitrary callbacks inside a lock is not a safe fix. Keep the transition graph and generic checked-state semantics. Java predicates and a compact immutable transition value can simplify implementation later, but existing Transition equality intentionally ignores `allowed`; a default record would change that. Validate concurrent transitions with controlled ordering, builder mutation after build, forbidden/self transitions and nested callback shutdown. This is a real abstraction worth repairing rather than replacing with scattered switches.

## FND-008 — P2: Make diagnostics tolerate thread churn and describe Java 25 accurately

`commons/src/main/java/org/apache/aurora/common/net/http/handlers/ContentionPrinter.java:58` dereferences each ThreadInfo from a separate ID snapshot; entries can be null after termination. Stack lookup at line 63 can also miss threads created between snapshots, and line 87 iterates the potentially null stack. Use one suitable MXBean dump/snapshot where possible, tolerate absent owners, and check contention-monitoring support before enabling it. Modernize Thread.getId calls to threadId in owned code.

The bundled JDK 25 source explicitly says Thread.getAllStackTraces and ThreadMXBean omit virtual threads. `ThreadStackPrinter` and JvmStats thread counters should document that scope; add a separately owned JDK/JFR diagnostic route only if needed, rather than presenting platform-thread numbers as all scheduler activity. Use pattern variables and the current OS MXBean memory API for the small deprecated/cast cleanup in JvmStats. Validate synthetic terminated/missing-owner snapshots, unsupported contention monitoring and output compatibility. No runtime thread-leak diagnosis was performed.

## FND-009 — P2: Replace a cache-shaped iterator flag with ordinary state

`commons/src/main/java/org/apache/aurora/common/collections/Iterables2.java:45` uses a LoadingCache merely to default per-iterator exhaustion flags. `next` at line 82 never rejects complete exhaustion. The compiled-source probe consumed the only row, observed hasNext=false, then got an extra `[0]` row.

Use explicit iterator state or a small exhausted set, enforce NoSuchElementException after exhaustion and characterize remove-before-next/double-remove. Preserve lazy zip-longest behavior, default filling, and removal from only participating underlying iterators. These semantics justify a small custom iterator; a stream-only replacement would lose remove and potentially laziness. Extend existing Iterables2Test cases and exercise the time-series caller.

## FND-010 — P3: Close the build-properties resource where it is opened

`commons/src/main/java/org/apache/aurora/common/util/BuildInfo.java:69` opens a classloader stream and never closes it. Use try-with-resources spanning load, retain missing-resource fallback and useful causes, and return/store a detached map where the test constructor currently accepts a caller-owned map. Validate success/failure closure and existing property values. This is a small ownership cleanup; no resource loading framework is warranted.

## FND-011 — P2: Compute metrics from one snapshot with plain accumulators

`src/main/java/org/apache/aurora/scheduler/stats/TaskStatCalculator.java:61` computes totals and line 65 computes each of four per-role categories through separate active-task reads; quotas are read twice too. SQLite currently decodes full task records for these predicates (BACK-002). `ResourceCounter.java:132` constructs a LoadingCache used only within one aggregation. `SlotSizeCounter.java:118` materializes a set just to find a minimum, then regroups the same offers for each slot size. `TaskVars.java:214`/`:216` fetches host attributes twice per state event.

Read one detached task/quota snapshot, fold it into totals and per-role maps, and use a local HashMap/computeIfAbsent for aggregation. Read host attributes once per event. Group machine resources once and compute minima directly, preserving missing-vector semantics and all zero-valued slot groups. Explicitly reset gauges for roles/resource types that disappear; the current update-only-present path can retain old values. Keep loops where mutation is clearer than collectors. Validate all dedicated/nonproduction/quota/resource cases, role disappearance and number of storage reads; measure allocations before claiming a speedup. This is a much better simplification target than blanket stream conversion.

## FND-012 — P2: Give metric registration and cardinality an explicit lifetime

`commons/src/main/java/org/apache/aurora/common/stats/Stats.java:50` owns three global registries; numeric sampling order is separately retained. Duplicate numeric export reuses the old stat, yet exportLong at line 230 returns a new unregistered counter—a behavior explicitly asserted in StatsTest. TaskVars' per-job/rack/role caches at lines 80–81 and the time-series name cache never retire names; bounded samples per name do not bound the number of names. CachedCounters likewise relies on one-time registration.

Keep StatsProvider as the capability seam, introduce an application-owned registry with registration handles/collision policy, and define budgets/retirement or aggregation for dynamic names. Preserve derived-stat registration order, startup replay, tracked versus untracked behavior and public metric naming during staged migration. Do not just add cache eviction: it would leave exported gauges or samples registered and could silently reset counters. Validate repeated lifecycle construction, collisions, sampled dependency order and job-name churn. This is coordinated ownership work, not a prerequisite for every local cleanup.

## FND-013 — P3: Use Java values and standard APIs where they remove real boilerplate

Good narrow record candidates are TierInfo, TierManager.TierConfig, TaskGroupKey, Rate's timestamp/value Pair, BenchmarkSettings and the detached metrics snapshot. Retain compatibility getters, validation, collection copies, JSON names, custom toString and any deliberately tested hash behavior. Selected PubsubEvent payloads can follow after their publication/serialization contracts are separated; TaskStateChange's historical `oldState` object shape and task representation are protected by exact fixtures. Do not expose secrets through generated toString.

Use arrow cases in Jobs.updateStats, immutable public views for Tasks/Jobs state sets, list getLast for actual lists, method references and pattern variables where they clarify a local expression. Reuse Gson for VarsJsonHandler and TaskStateChange instead of constructing it per response/event. GuavaUtils' custom collectors can delegate to pinned Guava collectors, but audit encounter order and UNORDERED characteristics before changing them. Keep Guava ranges, multimaps, timed caches, services and ordered immutable collections when they encode required behavior.

Query.Builder is a persistent copy-on-write wrapper around mutable Thrift, despite its builder name. Keep its immutable chaining behavior and document empty/unset semantics; consider an internal immutable query specification only alongside measured query/index work (BACK-002), not a superficial record conversion. Generated Thrift and intercepted Guice services are not record candidates. Validate serialization, equality/hash/ordering, unknown JSON properties, state-set mutation rejection and collector duplicate-key behavior. No preview API is required; no Java 8 compatibility constraint remains.

## FND-014 — P2: Repair benchmark workloads before using them to justify refactors

`src/jmh/java/org/apache/aurora/benchmark/SnapshotBenchmarks.java:78` calls SnapshotterImpl.asStream and discards the lazy stream: the snapshot operation mapping is never traversed. Consume operations through a JMH Blackhole or perform the actual restoration that the name promises. Returning the current clock does not consume lazy work. StateManager insertion, StatusUpdate and fill-cluster benchmarks also retain growing state across invocations; define bounded/reset fixture lifetimes and await asynchronous startup/termination. Current task/update/API store benchmarks select MemStorage, so their results do not establish SQLite performance.

Fix fixture shape: Hosts.java:59 increments rack after host zero, making the first rack too small; Offers.java:99 generates ports-minus-one entries. Scope task ID generation to benchmark concurrency. FakeOfferManager.getAll returns null; empty or deliberate unsupported behavior is clearer. Keep legacy task fixtures for compatibility cases and add clearly named Go-profile fixtures for production-path measurements. Validate cardinalities and actual consumed work before short JMH smoke runs; run comparative measurements with the same dataset/backend and no production containers involved. No benchmarks were run during this review.

## FND-015 — P2: Make tests assert failures and own their resources

`MorePreconditionsTest.java:67` catches expected exceptions without failing when none occurs. `PercentileTest.java:194` receives empty varargs at every call, so its flush-value loop never runs, and line 199 checks the test field rather than the passed percentile. Its reverse test supplies ascending input. Use assertThrows/current assertions and correct the fixtures; these need no JUnit migration. FakeClockTest actually tests FakeTicker and uses a real sleep. ZooKeeperUtilsTest starts a server for pure path normalization. Remove unnecessary runtime dependencies and name coverage accurately.

Close HTTP clients in WebhookTest teardown, stop executors/services in AsyncUtilTest/AsyncModuleTest, avoid shared mutable WebhookInfo builders, and use a controlled local failure instead of external DNS. Preserve teardown exceptions with causes/suppression in TearDownTestCase. BatchWorkerUtil can use completedFuture instead of mocking get; FakeScheduledExecutor needs real cancellation state for lifecycle tests. Validate tests can fail when the targeted behavior is broken and leave no resources running. Avoid rewriting thousands of broad assertions just to change test-framework style.

## FND-016 — P3: Replace positional reflection keys with a named signature value

`src/main/java/org/apache/aurora/GuiceUtils.java:63` keys interface matching by Pair<String, Class<?>[]>. It works because Pair has deep array equality/hash; a naive record with an array would break it. A private `MethodSignature(String name, List<Class<?>> parameterTypes)` makes identity and immutability explicit and removes dependence on incidental generic Pair semantics. Keep exact inherited/declaration-only and overload matching, exception-trap whitelist behavior and non-void rejection. Audit whether the exception-trap utility still has production callers before expanding its infrastructure; this pass found only its dedicated tests calling bindExceptionTrap. Validate array-parameter and overloaded/inherited methods. Do not remove the meaningful authorization/interceptor infrastructure described by application review.

## FND-017 — P2: Centralize bracket-aware network address handling

`commons/src/main/java/org/apache/aurora/common/net/InetSocketAddressHelper.java:40` splits at the first colon and its formatter uses getHostName without bracketing IPv6. It is used by the CLI converter and Curator connection-string builder. Use bracket-aware host/port parsing and formatting (an existing library helper is sufficient), retaining wildcard port/host conventions, unresolved DNS behavior and port validation. Use getHostString where avoiding reverse DNS is intended. Add IPv6, unresolved names, wildcard and malformed-bracket cases alongside existing IPv4 tests. Coordinate URI formatting with APP-008; do not claim this alone establishes end-to-end IPv6 cluster support.

## Evidence and limits

A small probe compiled current MorePreconditions, Pair, Iterables2, Unit, Time, Amount and Credentials sources against the installed dependency jars with the pinned JDK 25. It produced:

```text
Amount equal=true sameHash=false setSize=2
Credentials equal=true sameHash=false setSize=2
Zip exhausted=true extraNext=[0]
```

The source and reproduction instructions are in `foundation-probe.md`. No full build, regression suite, benchmark or cluster operation was run. Thread API scope was checked against the bundled JDK 25 src.zip. Future-validation paragraphs describe work to perform when implementing, not passing tests from this review. Every foundation ledger hash was verified against the worktree. The account's remaining usage percentage was unavailable; the review used one bounded source pass and focused verification rather than asserting a measured 20% cutoff.
