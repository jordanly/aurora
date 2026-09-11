# Aurora codebase simplification audit

Research date: 2026-09-09 (America/New_York). Baseline: `e3350f63d446cca17e8f1763ce30cc94346e64cf`, the requested `codex/aurora-context` snapshot. Audit branch: `codex/codebase-simplification-audit`.

Execution was verified before research: hostname `raspberrypi`, architecture `aarch64`, hardware `Raspberry Pi 5 Model B Rev 1.0`. Isolated remote checkout: `/home/jordanly/.codex/worktrees/6f85/aurora`. `AURORA_CONTEXT.md` was read first. No applicable `AGENTS.md` was found in the checkout or its ancestor directories. This report changes no application code.

## Assessment

The best immediate simplifications are to make storage guarantees unambiguous, remove duplicated test representations of the API, fix ownership of UI polling, and separate focused scheduler testing from the UI/toolchain build. The scheduler already has useful boundaries and unusually valuable lifecycle/recovery tests. Replacing those state machines or flattening their effects would discard evidence rather than reduce the difficult part of the system.

The strongest correctness findings here are a confirmed UI test-schema mismatch and missing polling cleanup. Storage publication before persistence is also directly visible, but this audit does **not** establish a production data-loss incident or prove every failure interleaving unsafe. Compatibility helpers include demonstrably inert code, alongside live validation and replay behavior that must remain until compatibility policy changes.

Likely Mesos removal, Java modernization, and a Go agent are planning assumptions supplied with this assignment, not existing implementations. Mesos replacement must address both execution and scheduler persistence. A Go agent should inherit documented process/readiness/recovery semantics, not automatically inherit every Python class boundary.

## Scope and evidence conventions

This was a selective source and test audit: scheduler storage and state effects, task status handling, update RPC orchestration, Mesos interfaces and simulator, executor status aggregation and Thermos recovery, UI polling/schema fixtures, and build wiring. It was not an exhaustive security, algorithmic-performance, or deployment audit. No dependencies were installed, and no Gradle, Pants, Jest, Mesos, or workload execution was attempted. Existing tests cited below were inspected, not run.

Source references use repository-relative paths and one-based lines at the baseline above. To keep repeated references readable:

| Prefix | Repository directory |
| --- | --- |
| `J/` | `src/main/java/org/apache/aurora/scheduler/` |
| `JT/` | `src/test/java/org/apache/aurora/scheduler/` |
| `P/` | `src/main/python/apache/` |
| `PT/` | `src/test/python/apache/` |

“High” confidence means directly observed in source, often corroborated by tests. Effort estimates are planning judgments for a contributor familiar with the area: S = roughly 1–3 engineer-days, M = 4–10 days, L = multiple weeks. These exclude reconstructing unavailable toolchains and operational rollout. Priority reflects the next useful work, not a vulnerability rating.

## Prioritized backlog

| Order | Item | Impact | Confidence | Effort | Timing / dependency |
| --- | --- | --- | --- | --- | --- |
| 1 | F1: Correct the storage contract and failure guidance | High correctness/operability value | High | S for docs; M for failure characterization | Useful today; precedes F7 |
| 2 | F2: Make UI schema fixtures derive from the actual API | High test credibility | High | S–M | Useful today; generation depends on F4 environment |
| 3 | F3: Give update polling one owner and explicit empty/error states | Bounded correctness and request-volume improvement | High for missing cleanup; runtime frequency unmeasured | S | Useful today |
| 4 | F4: Establish reproducible, focused build/test targets | High; enables all behavioral work | High | M–L | Useful today; Java modernization prerequisite |
| 5 | F5: Remove inert backfill scaffolding; separate live compatibility work | Moderate reduction in misleading abstractions | High | S cleanup; M compatibility inventory | Useful today; deletion of live shims needs policy |
| 6 | F6: Extract update admission logic from the RPC facade | Moderate maintainability/API reuse value | High evidence; medium payoff estimate | M | After focused test baseline |
| 7 | F7: Characterize then centralize durable-write side-effect ordering | High correctness/testability value | High for ordering; medium for chosen remedy | M characterization; L behavior change | Useful today; coordinate with persistence design |
| 8 | F8: Turn the simulator into a bounded failure-contract harness | High migration confidence | High | M | Start after F4; extend for replacement adapter |
| 9 | F9: Replace Mesos through explicit execution and persistence contracts | High architectural simplification, high migration risk | High coupling evidence; medium design confidence | L, separate program | Architecture-dependent |

### F1. Replace the obsolete transactional storage story

**Observed.** `docs/operations/storage.md:31–32` describes H2/MyBatis, and `:67–90` describes database-backed volatile storage and transactional isolation. `J/storage/durability/DurableStorage.java:56–64` also describes aborting local storage on failure. Actual `J/storage/mem/MemStorage.java:93–96` simply invokes the write callback. `J/storage/Storage.java:196–203` explicitly permits uncommitted reads. `JT/storage/mem/StorageTransactionTest.java:112–140` asserts retained mutations after exceptions; `:165–182` does the same for nested failures. Production wiring installs `MemStorageModule` at `J/app/SchedulerMain.java:191`.

**Impact.** A maintainer reading the operations guide can reasonably assume rollback or a consistent multi-query read that does not exist. This is a concrete documentation defect at a correctness boundary, not merely old terminology.

**Recommendation and tradeoff.** Rewrite the storage guide and durability Javadoc around serialized writers, immediately visible memory mutations, outer-write persistence, non-isolated reads, replay and failover. Distinguish native log failures that initiate shutdown (`J/log/mesos/MesosLog.java:332–354`) from arbitrary callback exceptions; do not document universal failover without evidence. Keep `Storage.read` for now: although its TODO suggests direct store injection, `J/storage/CallOrderEnforcingStorage.java:122–132` still gates access on readiness. Removing the wrapper without preserving that gate would change behavior.

**Validation criterion.** Every guarantee in the revised guide should map to an implementation or behavioral test. Add a focused combined volatile/durable failure test when the harness runs: mutate, throw before persistence, observe memory, restart from persisted state, and record the distinction. Cover nested failure and persistence failure separately. Do not turn existing retained-write assertions into rollback assertions as a cleanup.

### F2. Remove the handwritten browser-test copy of Thrift state

**Observed.** `ui/test-setup.js:7–29` acknowledges duplicated Thrift globals and manually defines `ScheduleStatus` and `ACTIVE_STATES`. It omits `PARTITIONED=18`, present in `api/src/main/thrift/org/apache/aurora/gen/api.thrift:456` and included in `ACTIVE_STATES` at `:468`. `ui/src/main/js/utils/Task.js:6` consumes that global directly. Production creates a generated client at `ui/src/main/js/client/scheduler-client.js:17–20`; tests substitute constructors at `ui/test-setup.js:31–34`.

**Impact.** Test behavior for active-state classification differs from the production schema. This is a verified test-fixture defect; it does not establish that the production UI misclassifies partitioned tasks.

**Recommendation and tradeoff.** Load the generated bindings in a controlled test runtime, or generate a small enum/constants test artifact from the same Thrift input if the legacy global-emitting generator prevents direct import. Keep that artifact generated and checked for drift. Preserve API injection for ordinary component tests. A small adapter can contain legacy globals without requiring a React rewrite or replacing Thrift transport.

**Validation criterion.** Compare all enum members and state sets used by the UI against generated output, including numeric values. Add a partitioned-task classification case and one round-trip/browser smoke test using real generated bindings. A test fixture regenerated by hand does not meet the drift-prevention criterion.

### F3. Make update-page polling lifecycle explicit

**Observed.** `ui/src/main/js/pages/Update.js:20–24` schedules a new timeout whenever `componentWillUpdate` sees a loaded, in-progress update. The class retains no timer handle and has no unmount cleanup. Each callback invokes `fetchUpdateDetails`; `:43–45` directly dereferences the successful response and the first detail entry. `ui/src/main/js/pages/__tests__/Update-test.js:60–85` checks polling and terminal status, but does not exercise unmount, repeated lifecycle calls, route changes, empty results or error responses.

**Impact.** A scheduled timeout survives leaving the page and can issue another request. Repeated qualifying lifecycle calls can queue multiple timeouts. The missing ownership is directly visible; the frequency of duplicate polling in a real browser was not measured. Empty/error results have no explicit rendering path in this component.

**Recommendation and tradeoff.** Retain one timeout; schedule after completion of the current fetch; clear it on unmount and when the route identity changes. Ignore stale responses using a request generation or equivalent local guard. Render “not found” and request failure explicitly. Keep the existing class component and callback API unless a later UI modernization independently warrants changing them.

**Validation criterion.** With fake timers, repeated state updates leave at most one scheduled poll; unmount followed by timer advancement causes no API call; a terminal response schedules none; a late response for update A cannot replace update B; empty/error results render deliberately. Retain the 60-second interval contract.

### F4. Decouple focused checks from packaging and pin their inputs

**Observed.** `build.gradle:181–183` makes scheduler resource processing depend on UI webpack. UI installation uses `npm install` at `:151–162`, with ranged dependencies in `ui/package.json:6–49` and no tracked UI npm/yarn/pnpm lockfile. `build.gradle:216–234` requires Python 2.7 for Java entity generation. `:575–593` chains test completion to coverage reporting and suite-level coverage verification. Java source/target is 8 (`:28`, `:42–44`), while `buildSrc/gradle.properties:3` pins Gradle 4.10.2 and `buildSrc/build.gradle:15–16` enforces it.

**Impact.** A small scheduler test inherits frontend and legacy code-generation prerequisites. A filtered suite can also be judged against full-suite coverage thresholds. Unlocked UI resolution makes reproducing a failure harder. These are observed dependencies; this review did not prove that dependency resolution currently fails.

**Recommendation and tradeoff.** Create explicit API generation, scheduler unit, scheduler integration, UI check and distribution targets. Keep the full distribution path building the UI. Give scheduler unit tests a resource set that does not imply a browser bundle, while retaining real resources for HTTP integration tests. Put whole-suite coverage on the full verification target; keep coverage reports available for focused runs. Produce and commit a reviewed UI lockfile using the selected reproducible toolchain, including plugin dependencies.

For Java modernization, first capture generated outputs and dependency resolution, then migrate build tooling/plugins, then runtime/dependencies, and finally source-level idioms. Port the Python entity generator or replace it behind output-equivalence checks as a separate step. Do not simultaneously replace generated immutable wrappers with records: those wrappers are part of the current model and serialization boundary. As one concrete compatibility constraint, Gradle documents Java 21 toolchain support from 8.4 and support for running Gradle on Java 21 from 8.5; changing only Aurora's Java target does not modernize its pinned build runtime. [Gradle compatibility matrix](https://docs.gradle.org/current/userguide/compatibility.html).

**Validation criterion.** A clean checkout can generate API artifacts and run a selected scheduler test without npm installation; full packaging still includes a working UI. Two clean UI installs resolve the same dependency graph. A focused test does not fail solely for whole-suite coverage, while the full check still enforces existing thresholds. Compare generated entity/API signatures and serialization fixtures before/after generator changes. Record actual aarch64/native-library limitations rather than treating this Pi as proof of production compatibility.

### F5. Separate inert scaffolding from live compatibility responsibilities

**Observed.** `J/storage/durability/ThriftBackfill.java:45–53` promises resource backfilling but returns its input unchanged. `:61–75`, `:100–102`, and `:111–119` retain traversal/wrapping paths around that identity operation. Its test `JT/storage/durability/ThriftBackfillTest.java:40–56` verifies an already-populated configuration. In the same class, `:85–97` actively validates quota resources and `:124–138` converts old update settings. `J/thrift/SchedulerThriftInterface.java:799–801` invokes the latter on requests despite a “remove after 0.22.0” TODO. `J/storage/durability/Loader.java:84–87` intentionally consumes old lock operations without applying them.

**Impact.** A migration-named class now mixes identity conversion, validation and actual compatibility translation. Reading it overstates what task backfilling accomplishes. Expired TODO dates are not evidence that old clients, snapshots or logs have disappeared.

**Recommendation and tradeoff.** Remove identity-only task backfill calls and simplify immutable wrapping after checking callers and error behavior. Name quota validation for what it does. Keep the live update translator as a clearly owned compatibility function shared by request ingestion and replay. Inventory supported client versions, readable snapshot/log versions, and downgrade expectations before retiring any live shim. Preserve wire field IDs and the ability to recognize historical operations until that policy explicitly ends.

**Validation criterion.** Current-format tasks and jobs produce equal immutable results before/after cleanup. Invalid quotas still fail identically. Golden old/new update settings and legacy log-operation fixtures replay to equivalent state. Compatibility deletion requires a documented minimum readable version and an upgrade/export path; deleting a no-op switch case can change handling of a recognized historical operation.

### F6. Extract one domain operation from the large RPC facade

**Observed.** `J/thrift/SchedulerThriftInterface.java:788–932` combines legacy settings normalization, pure request checks, configuration population, job diff calculation, scope/quota checks, audit identity, update construction/controller invocation and Thrift response mapping. This is a concrete concentration of responsibilities, not a recommendation based only on its 1,113-line size. Read-only behavior already delegates to `ReadOnlySchedulerImpl` (`:412–437`); there is no benefit in extracting that same boundary again.

**Recommendation and tradeoff.** Start with a pure update-settings validator/normalizer and a tested update-admission operation. Keep response-code translation at the transport facade, authorization on the annotated interface (`J/thrift/aop/AnnotatedAuroraAdmin.java:76–79`), and all state-dependent admission checks in the existing serialized write boundary. Pass audit identity explicitly. Avoid a generic command bus, a new service per RPC, or moving quota checks outside the writer merely to make tests simpler.

**Impact.** Smaller pure validation tests, clearer ownership of schema translation, and a reusable admission boundary if a future API is introduced. The tradeoff is another object/interface; justify it with removal of orchestration from the facade and reduced fixture setup, not forwarding methods alone.

**Validation criterion.** Preserve existing invalid-group/failure-limit cases (`JT/thrift/SchedulerThriftInterfaceTest.java:1477–1543`), no-op/update-in-progress responses, audit fields, scoped updates and authorization behavior. Compare response codes/messages and stored instructions on identical inputs. Do not claim an arithmetic or null-handling vulnerability from this inspection alone.

### F7. Define a single contract for effects around durable writes

**Observed.** `J/storage/durability/DurableStorage.java:190–214` performs callback work before persisting the accumulated operations, and nested writes reuse that operation recorder. `J/state/StateManagerImpl.java:352–353` invokes driver kills during transition effects; `:373–379` posts events with an explicit warning that enclosing persistence may not have completed. By contrast, `J/TaskStatusHandlerImpl.java:153–175` acknowledges status updates after `storage.write` returns successfully. `JT/storage/durability/DurableStorageTest.java:488–514` verifies shared nested persistence operations; `JT/TaskStatusHandlerImplTest.java:116–142` exercises a failed state change without configuring an acknowledgement.

**Impact.** Callers must reason separately about stored mutations, external commands, events and outermost completion. This complicates testing and later persistence replacement. The observed mixed ordering is not proof that a blanket “defer everything” change is safe.

**Recommendation and tradeoff.** First write a contract table for mutation, event, kill, acknowledgement and recovery initialization. Add a fault-injectable persistence boundary and record traces. Only then consider an outer-write completion facility for effects that require persistence. Preserve intentional action ordering (`J/state/StateManagerImpl.java:235–250`) and stale-event revalidation. Decide how effects are ordered relative to the next writer, how callback failure is handled, and how pending effects are rebuilt after restart. An in-memory post-write callback is not a durable outbox and cannot guarantee delivery across a crash after persistence.

**Validation criterion.** Cover nested success, inner/outer exceptions, failed persistence, duplicate status delivery, crash after persistence but before effect, and startup event reconstruction. Verify no status acknowledgement on failed writes and no new task identity collisions. Compare ordered state/effect traces and `JobUpdaterIT` recovery behavior before changing delivery timing. If durable delivery is required, separately design intent persistence and idempotent replay; do not hide that work inside a cleanup PR.

### F8. Improve the simulator as a contract harness, not a production proof

**Observed.** `JT/app/local/FakeMaster.java:224–232` synthesizes RUNNING one second after launch; `:248–258` reports FINISHED for a kill; `:161–164` leaves abort unsupported. It is useful for UI/placement experiments but cannot establish real process supervision or faithful driver-failure behavior. Existing smaller tests already model difficult ordering: `JT/mesos/MesosCallbackHandlerTest.java:332–356` covers rescind arriving before queued offer addition.

**Recommendation and tradeoff.** Give the simulator deterministic clock/event control and named scenarios: offer rescind, delayed/duplicate status, kill acknowledgement, disconnect/reconnect and unsupported capability. Document its simplified existing semantics; add a more faithful mode only where contract tests need it. Reuse existing fake executors/clocks and focused race tests instead of creating a second broad fake cluster framework. Keep persistence recovery tests separate from a volatile simulator run.

**Validation criterion.** Scenarios produce stable task/event traces without wall-clock sleeps and run against the retained adapter and eventual replacement. A delayed launch after rescind must not resurrect capacity. Launch/run/kill/reconcile tests must distinguish simulator behavior from native integration results. Restore active updates with `JT/updater/JobUpdaterIT.java:1196` and coordinated-update cases at `:473`, `:563`; retain their substantive assertions.

### F9. Remove Mesos at two boundaries; specify the Go agent contract first

**Observed.** `J/mesos/Driver.java:20–24` imports Mesos types into its interface; `:42`, `:72`, `:89` expose offer operations, status acknowledgements and reconciliation in those types. `J/app/SchedulerMain.java:105–121` selects among three driver kinds, defaulting to the historical scheduler driver. Independently, production installs `MesosLogStreamModule` at `:255`, and `J/log/mesos/MesosLog.java:332–354` couples log mutation failure to scheduler shutdown. Deleting a driver implementation does not remove Mesos from durability.

On the worker, `J/mesos/MesosTaskFactory.java:139–143`, `:219` serializes the assigned task into launch data. `P/aurora/executor/common/status_checker.py:106–142` defines terminal-state precedence, STARTING versus RUNNING aggregation, and latched terminal state. `P/aurora/executor/status_manager.py:52–65` reports RUNNING once and polls until a terminal result. `P/thermos/core/runner.py:109–121` restores coordinator identity with PID and fork time and suppresses cleanup side effects during recovery; `:560–604` manages checkpoint control and replay.

**Recommendation and tradeoff.** Define execution commands/events with explicit task-attempt identity, agent identity, resource/port allocation, acknowledgement, reconciliation and retry semantics. Adapt current Mesos behavior at that boundary while preserving task/update policy. Separately choose durability guarantees and a snapshot/log migration approach before replacing the Mesos log. Avoid first building a permanent three-driver compatibility platform if the deployment objective is removal of all three.

For a Go agent, build a small conformance corpus from current status aggregation, process retry/daemon/ephemeral behavior, shutdown and checkpoint scenarios. Preserve the distinction between scheduler replacement and process retry. Decide explicitly whether Go must recover live Python checkpoints or starts only new attempts while Python drains; the latter reduces compatibility work but prolongs coexistence. Define readiness, discovery registration and resource enforcement independently. Language choice alone does not define those contracts.

Mesos retirement makes long-term dependency investment a material planning consideration: Apache records retirement in August 2025 and completion of its Attic move in October 2025. That is current external status, not evidence that any specific local Mesos path is dead. [Apache Mesos Attic record](https://attic.apache.org/projects/mesos.html).

**Validation criterion.** Before cutover, demonstrate duplicate-command tolerance, recovery/reconciliation after scheduler or agent restart, preserved deliberate kill intent, resource/port accounting, and storage export/import equivalence. For Go, use the behavior encoded by `PT/aurora/executor/common/test_status_checker.py:96–145` and `PT/aurora/executor/test_status_manager.py:43–65` as initial conformance cases; add process/checkpoint integration before claiming replacement compatibility. Never validate mixed implementations by allowing both agents to supervise the same attempt.

## Staged implementation and stopping points

1. **Clarify and repair bounded defects.** Complete F1 documentation, F2 schema drift prevention, F3 polling ownership, and F5 identity-only cleanup. Each should be a small review with its own source-backed acceptance criteria. Behavioral merges wait for a runnable relevant test harness.
2. **Establish a repeatable verification baseline.** Complete F4 targets and record successful focused tests, generated-artifact equivalence and package/UI smoke results. This is the gate for Java toolchain changes, F6 extraction and F8 harness work. Preserve full verification rather than weakening it to obtain a green subset.
3. **Reduce control-flow ambiguity.** Extract F6 while preserving writer scope and authorization. Characterize F7 failures and decide effects/recovery semantics; implement only the selected contract. Extend F8 with the specific traces needed by that work.
4. **Make architectural decisions with evidence.** Select supported client/storage/checkpoint compatibility windows, execution protocol, persistence recovery model and Java runtime/build pair. Implement F9 adapters and a Go conformance prototype behind those decisions. Cut over one boundary at a time with old-state export and rollback/drain procedures; retire implementations only after their consumers are accounted for.

Useful stopping point: stages 1–2 leave a substantially easier repository to change even if Mesos removal or the Go agent is postponed. Stages 3–4 should not delay the bounded fixes.

## Complexity worth preserving and work not justified yet

- Keep task state transitions and their ordered effects, job/instance identity versus execution-attempt identity, and update recovery tests. Their complexity expresses real lifecycle behavior.
- Keep the distinction between volatile state, durable log and worker checkpoints. Collapsing them into a generic storage abstraction would obscure different failure guarantees.
- Keep executor health/status aggregation separate from Thermos process planning. Representative Python tests demonstrate intentional precedence and one-time RUNNING notification; no broad Python cleanup defect was established here.
- Do not bulk-replace Guava, Guice or vendored commons on stylistic grounds. Remove dependencies when a chosen boundary or modernized build makes their remaining users concrete.
- Do not delete deprecated Thrift fields or legacy replay cases based on version TODOs. No inventory of deployed clients or retained backups was available.
- No claim is made that writer contention, offer scanning, updater size or polling is a measured production bottleneck. Add measurements only for a specific planned change, such as writer wait/persist duration and side-effect queue delay in F7; avoid a broad new observability framework.

## Checks performed and limitations

Verified execution host/hardware, clean initial checkout and exact requested baseline; created the isolated audit branch. Read `AURORA_CONTEXT.md` and checked repository/ancestor instructions. Inspected the source and representative tests cited above with line-numbered reads.

Dependency-free Python assertions confirmed four static findings: Thrift includes active `PARTITIONED=18` while UI test globals omit it; the update component schedules a timeout without a cleanup hook or `clearTimeout`; task backfill is an identity body; and no npm/yarn/pnpm lockfile is tracked under `ui/`. These checks confirm source properties, not application behavior. Node was not found on this task's PATH; no runtime UI reproduction was attempted. No successful build, application test suite, failover exercise, native Mesos launch or performance result is claimed.

Current external claims were checked against Apache's Mesos Attic page and Gradle's compatibility matrix. An attempted OpenJDK JEP fetch was denied by the remote site and supplies no claim in this report. Dependency availability and production deployments were not surveyed.

Usage was checked before research and between major phases. The first observed main weekly usage was 4%, giving this task an additional conservative stop point of 11% total used, below the shared 50% early-stop threshold. Research-phase checks remained at 4%; no subagent, reset, credit purchase, push or merge was used. Shared account sampling is not a per-task consumption measurement.

The report was checked for source-reference existence/line bounds and whitespace before handoff. Application files remain unchanged. Absolute remote deliverable path: `/home/jordanly/.codex/worktrees/6f85/aurora/docs/reimagining/CODEBASE_SIMPLIFICATION_AUDIT.md`.
