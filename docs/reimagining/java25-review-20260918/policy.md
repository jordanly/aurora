# Scheduling policy, state, resources, cron and updater review

Baseline: `91b9bd74746d27102a96fa47d4e06e0dd25097ca`. Reviewed all **171/171 assigned Java files (108 production, 63 test; 34,270 manifest lines)**. Every current file SHA-256 matches the assignment manifest. The companion `policy.jsonl` records each file's disposition and rationale: 119 retain, 23 local, 29 coordinated, zero not reviewed. This was a bounded source review, including test bodies and selected callers; no implementation, build, test execution, benchmark, or deployment was performed. Prior audit findings were not presumed current. Account usage percentage is unavailable to this reviewer.

Priorities below describe implementation value, not security severity. Confirmed defects mean an inconsistent code path is demonstrable from source; runtime frequency and performance impact have not been measured. Suggested future validation is not a claim that those tests were run.

The highest-value changes are sequential placement accounting (POL-001), a total offer ordering (POL-002), exact reservation durations (POL-003), and the zero-jitter boundary (POL-004). The remaining recommendations reduce repeated work or clarify existing contracts without introducing another scheduler.

## POL-001 — P1: Match and assign each task against the updated placement aggregate

Evidence: `src/main/java/org/apache/aurora/scheduler/scheduling/TaskAssignerImpl.java:222` collects every match before `maybeAssign` launches any at line 268; the job's attribute aggregate changes only at line 126. `SchedulingFilterImpl` uses that aggregate for limit constraints. Consequently, two tasks in one scheduling group can match two offers sharing a rack against the same pre-batch count, then both launch even when the remaining rack allowance is one. This is a confirmed accounting defect in the batched path, not a proposal to change limit semantics.

Simplify to an explicit task loop that chooses an eligible offer, assigns/launches, then searches for the next task with the changed aggregate. Keep the used-offer guard applicable to affinity and ordinary matching, the task-aware placement veto, and the existing reservation policy. This removes the detached `SchedulingMatch` collection and makes the point where placement state changes visible. Do not move launch/prepare failures into a broader catch: the current tests deliberately distinguish preparation failure, `LaunchException`, and unchecked dispatch failure. Preserve the current transaction, stopping the group on `LaunchException`, and LOST handling.

Future validation: an integrated assigner/filter test with two offers in the same rack and a limit of one; different-rack success; reserved offers; rejection without releasing an unusable reservation; and existing launch failure tests. `TaskAssignerImplTest` currently mocks offer filtering, while `SchedulingFilterImplTest` validates limits separately, so neither closes this batch-level gap.

## POL-002 — P1: Make offer priority ordering a total order

Evidence: `src/main/java/org/apache/aurora/scheduler/offers/OfferOrderBuilder.java:93` returns only configured comparisons. `OfferSetImpl.java:36` stores offers in a `ConcurrentSkipListSet`, and `HostOffers.java:100` also inserts into ID/agent indexes. With `-offer_order=CPU`, two different offers with equal base and CPU values compare equal. The sorted set can drop one while the indexes retain both. Removing a comparator-equal offer can also affect the other entry. Default RANDOM masks this; it does not make the supported CPU-only configuration safe.

Append a stable offer-ID comparison after all requested ordering criteria. Preserve maintenance/unavailability precedence and RANDOM when requested. An iterative comparator fold with an exhaustive enum switch is easier to audit than recursive list slicing and a default that means RANDOM. Retain `OfferSet` as the configured extension boundary and its weakly consistent traversal; replacing it with a plain sorted snapshot would change concurrency behavior.

Future validation: equal CPU/memory values on distinct offers, no configured secondary ordering, removal of either offer, attribute-update reindexing, and index/set size agreement. Existing resource-order tests primarily use distinct values. Do not regress the already implemented host-attribute static-ban invalidation.

## POL-003 — P2: Preserve configured duration precision in reservation caches

Evidence: `src/main/java/org/apache/aurora/scheduler/preemptor/BiCache.java:74` converts an arbitrary configured amount to whole minutes before constructing `Duration`. A 30-second reservation becomes zero-duration; 90 seconds becomes one minute. Preemption and update-affinity reuse this cache. This is a confirmed conversion defect, not a reason to change either public option's unit syntax.

Construct `Duration` from the supported finer unit, with deliberate overflow handling consistent with the configuration type. `OfferSettings` already demonstrates a millisecond-based conversion. Retain the injected clock/ticker, synchronized forward/inverse bookkeeping, expiration statistics, and the shared `BiCache` abstraction: bidirectional expiration consistency is real behavior and should not be duplicated by its clients.

Future validation: fake-clock tests at 30 seconds, 90 seconds and their exact expiry boundaries, plus existing replacement/removal and inverse lookup tests. Keep zero-duration behavior if configuration currently permits it; do not silently round positive durations up to one minute.

## POL-004 — P2: Give zero jitter its literal meaning and avoid narrowing a long window

Evidence: `src/main/java/org/apache/aurora/scheduler/offers/RandomJitterReturnDelay.java:38` accepts a nonnegative long window, but line 48 calls `random.nextInt((int) maxJitterWindowMs)`. The real `Random.SystemRandom` delegates to `java.util.Random.nextInt`, which rejects zero. `RandomJitterReturnDelayTest.java:68` explicitly accepts zero but uses a mock that returns a value for that invalid bound. Large long windows also narrow before sampling.

Return the minimum hold time directly for a zero window. For positive windows, use a long-bound random operation through the existing randomness seam, or explicitly constrain the accepted range with a justified compatibility decision. Keep the exclusive upper-bound distribution and existing millisecond units. This tiny supplier is a useful test seam; it does not need a new timing framework.

Future validation: zero hold/window with a real random implementation, positive range invariants, a window larger than `Integer.MAX_VALUE`, and sum overflow policy. Replace the permissive mock boundary assertion with observable delay assertions.

## POL-005 — P2: Iterate over already-complete update batches instead of recursing

Evidence: `src/main/java/org/apache/aurora/scheduler/updater/OneWayJobUpdater.java:170` computes idle/working sets, evaluates a group, and recursively calls itself at line 189 when no action remains. A long run of immediately healthy instances with a small batch can consume one stack frame per batch and repeatedly rebuild and merge maps. `OneWayJobUpdaterTest.testEvaluateCompletedInstance` covers the short version of this path.

Use an explicit loop with one result accumulator, preserving strategy selection and the current conditions for advancing. First remove recursive map copying; only then consider incremental idle/working sets if profiling warrants the added state. Do not accidentally add an earlier failure cutoff or change the order of per-instance evaluation while doing this structural cleanup.

Retain `UpdateStrategy`, `InstanceStateProvider`, and `StateEvaluator`. Batch, queue and variable-batch strategies express materially different admission and rollback behavior; collapsing them into flags would make this harder to reason about. Future validation: thousands of immediately complete one-instance batches, mixed complete/working/failed results, failure allowance, and all existing strategy and updater integration timelines.

## POL-006 — P2: Read and index update history once per evaluation

Evidence: `src/main/java/org/apache/aurora/scheduler/updater/JobUpdateControllerImpl.java:687` fetches update details for instructions; line 725 fetches the whole update again for every side effect, filters all instance events, then creates another action set. Work grows with both emitted instances and accumulated history.

Capture a transaction-local details snapshot and build a by-instance event/action index once at the point where the loop currently starts. Use that index for empty-history and duplicate-action decisions. A small private derived-history value, potentially a record with defensively copied collections, can describe this snapshot; a new history service or persistent cache is unnecessary. Audit any nested writes before moving the snapshot earlier, and update the local index if subsequent logic can revisit an instance.

Preserve no-op suppression, pause/resume duplicate suppression, auto-pause filtering, rollback directions, timestamps, counters and transactional event ordering. SLA messages can share an action while remaining distinct audit events; do not deduplicate the entire history by action. Future validation: query-count assertions for many side effects, long history, repeated pause/resume, SLA checking/passed events, and the existing `JobUpdaterIT` timelines.

## POL-007 — P2: Sum resources directly into an EnumMap

Evidence: `src/main/java/org/apache/aurora/scheduler/resources/ResourceManager.java:168` groups every resource into temporary lists, then streams each list to reduce its values. The operation needs only one sum per `ResourceType`.

Use one sequential pass into an `EnumMap<ResourceType, Double>` with merge, then construct the immutable `ResourceBag`. Preserve encounter order within each resource type so floating-point accumulation is unchanged. The test utility already demonstrates the general direct-accumulation shape. The same principle can simplify single-resource quantity lookup without making every caller use streams.

Retain `ResourceBag` and the converter/mapper split. Sparse membership, explicit zero entries, absent-value behavior, range/port conversion and division by zero are observable and tested; a blanket record or dense-vector rewrite is unjustified. Do not alter the separate aggregate-input path's duplicate-key rejection. Future validation: mixed and repeated resource types, empty input, large values, ports and accelerator types, explicit zero versus absence, and existing converter/mapper tests. Allocation improvement is a source-based expectation, not a benchmark result.

## POL-008 — P2: Precompute update-covered instance ranges for quota accounting

Evidence: `src/main/java/org/apache/aurora/scheduler/quota/QuotaManager.java:388` creates both initial and desired `RangeSet`s inside the task predicate. Every running task belonging to the same update rebuilds equivalent ranges; consumption categories repeat that work.

Derive a per-job covered-instance lookup once for the consumption snapshot and reuse it while filtering each category. Keep initial and desired sets separate, or union them with an overlap-tolerant builder: desired and initial ranges commonly overlap. This is local derived data, not a long-lived cache with invalidation rules.

Retain the quota model's distinct production/dedicated categories and max-of-initial-versus-desired resource arithmetic. Cron templates and running cron instances also intentionally use maximum consumption rather than summing both. A refactor that counts update-covered running tasks again would change admission behavior. Future validation: partially overlapping ranges, missing desired state, tasks outside update ranges, mixed categories, cron/live maxima and the existing quota tests; add a construction-count test only if it can assert the public calculation rather than implementation spelling.

## POL-009 — P2: Share SLA groups within a single metric refresh

Evidence: `src/main/java/org/apache/aurora/scheduler/sla/MetricCalculator.java:207` iterates each algorithm/group pair and calls `createNamedGroups(tasks)` at line 209. Multiple uptime/median algorithms reuse the same job grouping, and resource groupings are also recalculated.

Memoize named groups by `GroupType` in an `EnumMap` scoped to one `runAlgorithms` call. Preserve separate production/nonproduction inputs and the shared time range. A resource grouping can subsequently classify a task once rather than recomputing its bag for every bucket, but this is optional after the simpler reuse change.

Retain `SlaAlgorithm` and `SlaGroup`: independent grouping and aggregation are useful composition, not gratuitous abstraction. Do not turn this into a cross-refresh cache. Preserve metric names, percentile interpretation, PARTITIONED uptime behavior and configured categories. Future validation: equal metric values and names across all enabled algorithm/group pairs, overlapping categories, empty groups and partition transitions; use a focused grouping-count assertion or benchmark if demonstrating reduced work is important.

## POL-010 — P2: Evaluate SLA and maintenance deadlines using the existing Clock seam

Evidence: `src/main/java/org/apache/aurora/scheduler/sla/SlaManager.java:224` uses `System.currentTimeMillis()` per task at line 230; `maintenance/MaintenanceController.java:316` creates maintenance timestamps from system time and line 472 computes remaining time the same way. These paths bypass the fake-clock conventions used by surrounding scheduling and updater logic.

Inject the existing `Clock`, and capture one now value for a single SLA decision. Pass it through the duration predicate; use that same dependency for maintenance deadline arithmetic. No new clock abstraction or wholesale `Instant` conversion is needed at Thrift millisecond boundaries. This is a determinism and boundary-simplification recommendation, not a claim that current timestamps are generally wrong.

Preserve strict duration comparison, maintenance end semantics and aggressive SLA policy adjustments. Keep coordinator HTTP work outside storage transactions and revalidation/action inside the existing write callback. Future validation: exact duration equality versus one millisecond past, a decision with several tasks at a boundary, overdue maintenance, and clock advancement without sleeping. Avoid changing executor concurrency or introducing virtual threads as part of this cleanup.

## POL-011 — P2: Represent task-state effects by the actions they actually contain

Evidence: `src/main/java/org/apache/aurora/scheduler/state/SideEffect.java:27` stores an optional next state with no accessor or operational use; `TaskStateMachine.java:522` always supplies `Optional.empty()`. `TransitionResult.java:32` carries wrappers, and `StateManagerImpl.java:274` sorts and unwraps them.

After checking repository-wide callers, replace this private domain's wrappers with an immutable set of `Action` values, using an `EnumSet` while accumulating. Retain the interpreter's explicit action order; enum declaration order should not silently become execution order. An exhaustive modern switch is appropriate for this internal action enum, provided missing-task and illegal-transition behavior remains explicit.

Retain the separation between `TaskStateMachine` decisions and `StateManagerImpl` effects. Do not rewrite the full transition graph, relax compare-and-set preconditions, move event publication, or unify this class with `updater.SideEffect`: the updater type carries meaningful status/action/failure data and has a different equality contract. Future validation: existing all-state transition matrix, missing-task KILL behavior, failure accounting and state-manager event order. Update expected-effect helpers rather than replacing behavioral tests with record equality tests.

## POL-012 — P3: Use the replay callback's store parameter for all delayed-cron access

Evidence: `src/main/java/org/apache/aurora/scheduler/cron/quartz/AuroraCronJob.java:207` submits delayed replay with parameter `store`, but line 211 reads tasks through the outer `storeProvider`; insertion then correctly uses `store`. `BatchWorker` executes replay under later storage writes. Current SQLite storage uses a shared provider, so this review does **not** claim a demonstrated stale-transaction failure on that implementation.

Use `store` for the read as well as the write and avoid retaining a provider in delayed work. This makes the callback's transaction boundary self-contained and valid for a transaction-scoped provider implementation. Keep Quartz collision handling and the batch-worker replay abstraction; a separate cron scheduling engine would add no value here.

Future validation: a delayed KILL_EXISTING execution whose initial and replay callbacks receive distinct providers, with the old provider rejecting access, plus existing cron collision tests. Preserve the current storage validation behavior tested by `CronSqliteValidationTest`, including usable outer transactions for pre-mutation validation errors and rollback/fail-stop behavior after mutation failures.

## POL-013 — P3: Bind the affinity cache type actually consumed

Evidence: `src/main/java/org/apache/aurora/scheduler/updater/UpdaterModule.java:111` binds singleton `BiCache<IInstanceKey, TaskGroupKey>`, while `UpdateAgentReserver.java:77` requests `BiCache<IInstanceKey, String>`. The explicitly scoped generic binding is not the requested cache. The singleton reserver can still hold one implicitly constructed cache, so this is a wiring mismatch, not proof that affinity currently fails.

Bind the instance-to-agent-ID cache with the exact `String` value type and remove the unrelated task-group import. Keep the private module, per-feature cache settings and `NullAgentReserver` for disabled affinity. Future validation: construct the enabled and disabled module paths, verify the actual cache uses affinity expiration/statistics settings, and retain reservation/release tests. Do not use raw generic keys to hide the mismatch.

## Abstractions and behavior worth retaining

- The original `SchedulerMain` composition, transactional storage callbacks, task state machine, update state machine and failure boundaries remain the architecture. None of these recommendations calls for a replacement scheduler.
- Attribute aggregation is lazy for a reason; static and dynamic vetoes have different cacheability. Host-offer indexes and synchronization carry real consistency requirements. Recent host-attribute ban invalidation and task-aware offer veto behavior should remain.
- Preemption separates victim selection from execution-time validation; role/tier/priority limits, revocable exclusions, reservation expiration and deterministic comparison deserve their existing tests. Cluster snapshots and partition timestamp guards protect against stale callbacks.
- Cron validation, Quartz lifecycle adapters, resource converters/mappers and public Thrift/configuration boundaries are meaningful seams. Existing range-set operations are often clearer than replacing them with hand-written interval logic.
- Task-group backoff, throttling and rate limits should remain. Existing interruption handling distinguishes `InterruptedException` from an execution failure; executor replacement is outside this pass.
- Pruners encode distinct count/time/retention rules; exception classes and small result types do not need modernization for its own sake. Updater strategies are real policies, not redundant classes.

Modern Java is most useful here for `Duration`, enum-specialized collections, exhaustive internal switches, simple loops and occasional private derived-value records. Preview features, broad sealed hierarchies, blanket record/stream conversion, and mechanical Guava removal have no demonstrated benefit in this slice. Tests were read to judge semantics; all validation above remains future work.
