# Modernize Aurora in place

Journal compaction, application health and task logs are implemented and qualified
in the [follow-up status](HARDENING02_IMPLEMENTATION_STATUS.md).
Earlier qualification results below remain tied to their recorded revisions.

Active plan, updated 2026-09-12. This supersedes the parallel scheduler strategy and the
JAVA-11 proposal to retire the original scheduler, Python code and UI.
The later decision to retire Python and Mesos applies to their runtime, build,
client and deployment paths; the original scheduler and UI remain. JSON job
documents and a Go client replace executable `.aurora` files for the supported
process profile. General Thermos feature parity is explicitly outside that
profile. Production HA comes after push transport, Python retirement and Mesos
cleanup. See the [current checkpoint](INPLACE08_IMPLEMENTATION_STATUS.md) for
implementation evidence and the precise remaining boundaries. Earlier slice
descriptions below retain their original acceptance context.
INPLACE-00 is complete. The original Java 25 build, behavior gates, execution
boundary extraction and seven-store SQLite backend are implemented on
`codex/in-place-java25`. See the [current implementation and qualification
status](INPLACE02_04_IMPLEMENTATION_STATUS.md) for INPLACE-01 through INPLACE-04,
executed evidence and remaining environment/HA boundaries. The next integration
checkpoint is documented in [INPLACE-05 through INPLACE-07 status](INPLACE05_07_IMPLEMENTATION_STATUS.md):
the original scheduler can now execute a bounded Go process cohort with SQLite.
Full executor compatibility and production HA remain separate gates. Earlier
build and behavior receipts remain historical records.

## The architectural decision

Modernize the existing Aurora application. Keep
`org.apache.aurora.scheduler.app.SchedulerMain` as its entry point and retain the
existing task state machine, scheduling policy, updates, cron, quotas,
maintenance, reconciliation, public Thrift API and UI. Replace their Mesos,
execution and persistence dependencies through explicit internal interfaces.
The Java target is **25+**, with no Java 8 source, bytecode or runtime obligation.
The [file-by-file Java 25 audit](JAVA25_FILE_AUDIT.md) supplies concrete candidates,
contract cautions and validation requirements for all 636 tracked Java files.
Its local implementation and remaining decisions are recorded in the
[Java 25 implementation status](JAVA25_IMPLEMENTATION_STATUS.md).

The `scheduler/native` implementation is an abandoned product direction.
`NativeEngine`, `NativePolicy`, their replacement job model and `/v1/jobs` API
will not become Aurora's scheduler. Working infrastructure from that experiment
can be adapted after review; its tests establish behavior of the experiment,
not compatibility with original Aurora.

Every implementation slice must name the existing code it changes, the behavior
it preserves and the original tests that exercise it. Missing build coverage is
a defect to repair, not evidence that a source tree can be removed. Existing
features stay in scope unless a separate product decision explicitly changes them.

## Starting point and branch recovery

- Preserve the published `codex/standalone-foundations` experiment at
  `dc8d7908d9d957389b0780aa1cf70aa822f8457c` as reference history. Do not reset or
  force-push it. Uncommitted source-retirement drafts were set aside; the original
  source trees remain present.
- Start the implementation branch from upstream master, verified on 2026-09-11
  as `11ebaeeb071cb182c388a40755e84f60dda32260`. Fork master and local master
  matched it. Refresh these refs again when creating the implementation branch.
  A fresh branch restores the original build contract without carrying the
  replacement application's source selection into the new design.
- Selectively port useful foundation fixes from
  `79509a77e388134f1281c6b877730de93998808a`: Python 3 generator repairs,
  deterministic Thrift generation, focused test wiring and build inventory.
  Reuse modern tool bootstrap mechanics separately. Do not wholesale cherry-pick
  the experimental scheduler or restore Java 8 as the supported build.
- These planning changes do not change application code or deploy a cluster.
  Keep the existing Pi MVP and unrelated Home Assistant installation intact;
  the corrected lab gets separate resources and an explicit cleanup scope.

## Keep the behavior owners; replace their dependencies

| Area | Retained authority | In-place change |
| --- | --- | --- |
| Assignment and policy | `TaskSchedulerImpl`, `TaskAssignerImpl`, `TaskGroups`, scheduling filters, preemption and reservations | Translate backend resource availability into neutral internal types; preserve placement, vetoes, affinity and reservation ownership. |
| Task lifecycle | `StateManagerImpl`, `TaskStateMachine`, existing task identities and events | Replace direct external effects with durable command intents; route observations through the same state transitions. |
| Higher-level behavior | Quota manager, job updater, Quartz cron, SLA and maintenance controllers, reconciler | Adapt collaborators and recovery wiring; keep the existing algorithms and public behavior. Cron and rolling updates are existing features, not later replacements to invent. |
| Persistence | `Storage`, its store interfaces and immutable entities | Supply a complete transactional implementation, migration and recovery underneath the existing callers. |
| Public contracts | `SchedulerThriftInterface`, API schemas, clients and UI | Preserve RPC behavior, field/enum/union identifiers and compatibility at the public boundary. Version the internal worker protocol separately. |
| Execution | Existing resolved task configuration and executor behavior | Integrate the Go agent behind an execution adapter, with explicit compatibility tests and capability checks. |

Reuse candidates are the Go journal and supervisor, authentication and transport
mechanisms, verified tool downloads, reproducible packaging, fault injection,
and database transaction/backup techniques. Each needs integration tests against
the retained application. The experimental `NativeSqlStore` is not an
implementation of Aurora's complete `Storage` contract.

## Ordered implementation slices

Slices are acceptance boundaries, not a requirement for one large PR each.
Split substantial storage and executor work into small reviewable changes.

### INPLACE-00 — Restore an honest source and test inventory

Create the implementation branch and inventory original source sets, API/entity
generation, commons, resources, frontend, distributions, recovery tools and quality
tasks. Record all discovered tests and their executed, skipped, failed or
environment-blocked status. Preserve the current API/schema and representative
configuration, state and executor fixtures as comparison inputs.

**Gate:** every original component has a build owner and verification route;
experimental code and experimental test receipts are clearly identified.
No source retirement is part of this slice.

### INPLACE-01 — Build the original application on Java 25

Reconstruct the root Gradle graph around the original scheduler, generated API,
commons, existing tests, frontend and `aurora-scheduler` distribution. Port the
necessary generator fixes and upgrade build plugins, dependencies and test
infrastructure only as needed to compile and test on Java 25. Keep focused Java
tests independent of frontend packaging while restoring both to the full build.

Java 25 can run Gradle beginning with 9.1.0; select and pin a compatible version
and validate the actual Aurora plugins and tasks. Version compatibility alone
does not validate the application build.
[Gradle compatibility documentation](https://docs.gradle.org/current/userguide/compatibility.html).
Use compilation, runtime tests and dependency analysis together; `jdeps` does
not expose all reflective access.
[JDK 25 migration guide](https://docs.oracle.com/en/java/javase/25/migrate/preparing-migration.html).

**Gate:** a clean checkout compiles the original source sets on Java 25 and runs
the generator and state-machine baselines. Generated output differences are
explained, not silently accepted. The build inventory tracks packaging, UI,
integration and quality tasks through completion. Original Mesos/JNI execution
may remain a named environment gap until its replacement; excluded or skipped
tests never count as passing the full application gate.

This is the minimum early build modernization. Broad Java idiom, HTTP and DI
refactoring follows Mesos removal so effort goes into code that will remain.

### INPLACE-02 — Establish original behavior as the acceptance contract

Run the existing scheduling, filter, state, quota, updater, cron, maintenance,
SLA, reconciliation, storage, RPC and security suites. Add characterization
tests only for important behavior that lacks coverage. Capture representative
API requests/responses, persisted records and resolved task configurations.
Check UI assets, installed launchers and recovery-tool packaging.

**Gate:** reproduce the original behavior through the restored build and publish
the complete test ledger. Include `JobUpdaterIT`, `SchedulerIT`, `HttpSecurityIT`,
`ApiIT`, `ThriftIT`, `CronIT`, `SnapshotterImplIT` and storage compatibility tests.
Retain the historical JaCoCo thresholds of 0.87 instruction / 0.79 branch coverage
unless a separately reviewed change justifies another baseline; verify coverage
actually includes the original production classes. Make the historical optional
quality tasks explicit in CI. New backend work may address a named JNI blocker,
but that does not close the blocked integration gate by itself.

### INPLACE-03 — Extract Mesos dependencies inside the existing scheduler

Introduce neutral internal contracts for agent identity, resource availability,
assignment, launch/kill commands and observations. Refactor `HostOffer`,
`OfferManager`, `Driver`, `MesosTaskFactory`, resource conversion and status
handling around those contracts. Keep a temporary Mesos adapter so the extraction
can be tested separately from a new execution backend.

`Driver` currently exposes Mesos protobufs, and `StateManager.assignTask` accepts
a Mesos agent identifier. Replacing a driver implementation alone cannot remove
that coupling. Preserve existing public wire fields with boundary translation
while removing protobuf dependencies from policy and state code.

**Gate:** the same existing policy and fake-cluster scenarios produce equivalent
assignments, vetoes, reservation behavior, resource accounting and transitions.
There is one policy/state implementation, used by both backend adapters.

### INPLACE-04 — Implement complete transactional storage

Before implementation, record the required availability, permitted data loss and
failover/recovery objectives, then select the production storage ownership and
fencing approach. This prevents the lab's local-file assumptions from dictating
the production design. HA qualification remains a separate, explicit gate.

Implement all **seven** existing stores through `Storage.StoreProvider` and
`MutableStoreProvider`: scheduler metadata, cron jobs, tasks, quotas, host
attributes, job updates and host maintenance. Preserve their queries and
cross-store invariants, including task configuration/events, update history and
recovery metadata. Initially retain immutable entities and versioned serialized
payloads where that reduces migration risk.

Add schema versions, command outbox records and observation receipts. Define
nested write semantics, rollback, read isolation, uniqueness and ownership
fencing. A nested failure must not leak partial state or permit an accidental
outer commit. Split implementation by related stores, then prove cross-store
transactions with the actual controllers.

The existing `DurableStorage`/`WriteRecorder` mutate memory before persistence.
Storing their log in SQL alone would not fix rollback or read isolation. Improved
rollback is an intentional correctness change; update the old transaction tests
explicitly rather than claiming that defective behavior remains unchanged.

**Gate:** failed writes leave no visible partial changes; committed changes and
their command intents recover together. Exercise database failure, restart,
nested transactions, concurrent readers, backup/restore and all store contracts.
SQLite is a reasonable first Pi backend with one scheduler owner. A local file
does not replace Mesos replicated-log availability; the production storage and
leader-fencing design must preserve the required HA contract before cutover.

### INPLACE-05 — Commit commands and observations before external effects

Keep `StateManagerImpl` authoritative. Persist launch/kill intent in the same
transaction as assignment or state changes; dispatch only after the outermost
durable commit. Preserve the state machine's deliberate action ordering,
failure counters and replacement rules, including effects of rejected transitions.
Buffer state-change and deletion events within the transaction; publish them
only after its outermost successful commit and discard them on rollback.
Preserve event order and make consumers tolerate redelivery or reconstruct from
committed state.

`TaskAssignerImpl` currently treats launch as synchronous. Model definite launch
rejection, expired availability and uncertain delivery separately. Retries use
the same command and execution identity; uncertain delivery must not itself
create another execution. Apply existing partition policies through explicit
reconciliation and fencing rules. Resource/port reservations must remain valid
between assignment commit and delayed dispatch, or be rejected safely.
Before dispatch, validate authorization against current desired state and
leadership. Define cancellation and supersession ordering so a delayed launch
cannot resurrect a stopped attempt. Agents reject stale ownership epochs.

Deduplicate observations in the transaction that calls `StateManager.changeState`;
acknowledge them after commit. Extend the existing status-handler sequencing.
Reconstruct task groups, updater work, cron templates, maintenance and
reconciliation on restart. After-commit callbacks alone leave a crash window:
correctness-critical consumers need reconstructable state or a durable cursor.
Define startup ordering and readiness gates for storage recovery, controller
reconstruction and backend reconciliation. Failed recovery of a critical
controller keeps the scheduler unready; logging an updater-resume failure is
insufficient. This is an explicit correction to existing recovery behavior.

**Gate:** no command escapes an aborted transaction; committed commands survive
lost callbacks and restarts; replayed observations do not repeat failure counting
or replacements. Test every boundary from assignment through agent admission,
execution, observation commit and acknowledgement.
Include kill-before-launch, rollback-before-dispatch and leader loss during replay.

### INPLACE-06 — Integrate Go execution under Aurora's existing contracts

Adapt the reusable Go journal/supervisor and protocol to the neutral backend.
Preserve Aurora task IDs, job keys, instance IDs, assigned ports, attempt lineage
and terminal-state semantics. Convert existing resolved `AssignedTask`/`TaskConfig`
data through a versioned worker envelope. Keep `.aurora` loading and its trusted
Python/Pystachio evaluation boundary available until an equivalent client/config
path is qualified; the Go agent need not interpret Python.

Make the conversion auditable field by field: `taskId`, `slaveId`, `slaveHost`,
`instanceId`, `assignedPorts`, role/environment/name, failure limits, CPU/RAM/disk/
GPU/ports, `ExecutorConfig.data`, container/volume settings, partition policy and
health/discovery configuration. Preserve required public wire names while
translating their semantics to the neutral backend; do not silently drop fields.

Define capability negotiation, resource advertisements/reservations, authenticated
agent identity, leader fencing, durable admission and reconciliation. Implement
compatibility in small groups: basic processes; dependencies/retries; termination
and finalization; health and discovery; sandbox/log/resource behavior.

**Gate:** real Go execution reports through the original status/state pipeline.
Existing API requests exercise original assignment, replacement, update and
maintenance code. Initially use a clearly declared process-only lab cohort.
Unsupported features reject before assignment; a limited cohort does not retire
the full feature from Aurora. Do not import the prototype's 4096-byte manifests,
64-entry histories or altered retry limits as product-wide constraints.
Use the original executor and configuration test groups listed below as reference
contracts, not solely new tests written against the Go implementation.

### INPLACE-07 — Qualify the original scheduler with two agents on the Pi

Package the existing `SchedulerMain` application on Java 25 with the new storage
and Go backend. Run one scheduler and two Go agents in separate Docker containers,
each with its own durable volume. Add a test-runner container using the existing
Thrift API and client/UI contracts. Retain a ZooKeeper service if startup or
discovery requires it until its separate dependency decision is qualified.

**Gate:** the supported cohort passes the Docker acceptance matrix below,
including original quotas, constraints, update/rollback, cron, maintenance and
reconciliation through `SchedulerMain` and `SchedulerThriftInterface`. Repeat
the physical recovery matrix for three clean rounds and
run a ten-minute mixed workload at the integration milestone. Identify the
source commit, images, JDK, configuration and durable volumes in every receipt.
This is the corrected cluster MVP, not completion of all executor or HA parity.

### INPLACE-08 — Close compatibility and migration gaps, then remove Mesos

Complete the feature ledger, including executor behaviors outside the first lab
cohort. Build and test a historical snapshot/log importer for all seven stores,
retaining original readers, schemas and golden fixtures until migration works.
Verify object counts, semantic contents, references, task lineage and restored
controller behavior; fail explicitly on unsupported versions or records.
Retain `DataCompatibilityTest`, the Java durability goldens, and original
snapshot/log/backup recovery suites as named migration evidence; these are
distinct from the Thermos process-checkpoint fixtures.

Rehearse a stopped, fenced migration: back up the source, import into separate
state, validate, start the new owner, and verify jobs, cron and updates. Establish
the point beyond which new writes prevent simply restarting the old scheduler.
Rollback then requires a qualified reverse path or a planned drain/restore, with
external executions reconciled. Go does not inherit live Thermos checkpoints;
migrate attempts by controlled drain/relaunch unless adoption is separately built.

**Gate:** required execution features, resource isolation, storage availability,
leader failover/fencing, migration and recovery pass their acceptance tests.
Remove both the Mesos driver/JNI and Mesos replicated-log dependencies from the
active scheduler distribution. `SchedulerMain` currently installs both
`LibMesosLoadingModule` and `MesosLogStreamModule`; removing only the first is
insufficient. Retire obsolete adapters and Python execution paths only after
their responsibilities have verified replacements. Public clients and config
loading are separate responsibilities. ZooKeeper/HA removal is not implied.

### INPLACE-09 — Make the retained code idiomatic Java 25+

Re-audit the original application after dependency removal. Modernize its
remaining dependency graph, then use records for suitable internal value types,
pattern matching and exhaustive switches where they clarify state handling,
modern collection/path/time APIs, and simpler resource management and tests.
Replace redundant utilities and obsolete abstractions in small coherent groups.

Preserve generated/wire compatibility and deliberate equality, nullability and
diagnostic behavior. Assess DI, HTTP and servlet changes independently. Introduce
virtual threads or other concurrency changes only where measurements and
lifecycle/cancellation tests justify them; the scheduling ownership model is not
changed merely because newer language features exist. No preview feature is
required for the Java 25 baseline.

**Gate:** all maintained Java source sets and tools target Java 25+, the original
behavior suite and packaging pass, and each refactor has a demonstrated clarity,
dependency or maintenance benefit. Report production/test/generated code and
dependency additions/removals separately; line-count reduction alone is not a
reason to remove a feature.

### INPLACE-10 — Modernize the UI and clients against the preserved API

Keep the current UI functional throughout preceding slices. Update its build,
dependencies and components incrementally, then improve job/task, update,
maintenance, agent and log views using the same application contracts. Preserve
links, authentication/authorization and operational workflows. Modernize the
Python client/configuration toolchain with compatibility fixtures; any API or
framework replacement needs its own justified migration proposal.

**Gate:** representative submit, inspect, update, rollback, cron, drain and log
workflows work through the supported clients and UI. No duplicate scheduler API
or separate policy implementation is introduced to support the UI.

## Testing ground and compatibility ledger

The lab topology is: test runner → existing Aurora API → original scheduler and
transactional store → Go agent A / Go agent B. The agent protocol is internal;
the test runner must not qualify policy through the prototype's `/v1/jobs` API.
Use a separate Java 25 build container, optional fault proxies that preserve
end-to-end authentication, and isolated runtime containers on an owned network.

- **Isolation:** distinct Compose project, loopback-bound host ports, bounded
  CPU/memory/disk usage, private scheduler and per-agent volumes, and explicit
  ownership labels. The scheduler and agents do not need the host Docker socket
  for the initial process cohort. Run expensive Pi builds and recovery workloads
  sequentially, observing available resources and the user's usage stop threshold.
  Cleanup only resources created by this lab; preserve the old MVP and Home Assistant.
- **Per-change checks:** original unit/contract tests appropriate to the changed
  code, complete source-set inventory, deterministic API generation when touched,
  Java 25 compilation and relevant packaging/static analysis. A documentation
  change or local refactor does not need three complete physical profiles.
- **Integration checks:** original API/security, updater, cron and storage suites;
  two-agent placement and vetoes; quotas and preemption; service replacement;
  update success and automatic rollback; drain; health/readiness; ports/logs;
  scheduler and agent restarts; duplicate/reordered/lost messages; launch and kill
  replay; partition/fencing; disk-full and failed commits; backup/restore; cleanup.
  Existing native fault-injection mechanics are reusable after their assertions
  and API driver are adapted to the original scheduler.
- **Execution parity:** compare original fixtures for retry accounting, daemon
  and ephemeral dependencies, staged kill/finalizers, exit/signal precedence,
  HTTP/shell health checks, grace/snooze, discovery, environment/UID/GID,
  sandbox/log rotation, resources, fetches, volumes and container workloads.
  Process-level and task-level `max_failures=0` have nuanced effects; preserve
  the original tests rather than flattening them into a universal retry rule.
- **Evidence limits:** label source-derived expectations separately from executed
  old/new comparisons. Original Thermos fixtures remain under
  `src/test/resources/org/apache/thermos/root/`. If historical Python/Mesos
  execution or Go race detection cannot run on this Pi, use a suitable isolated
  CI runner and record that gap until it runs. Mocked JNI tests and a successful
  Gradle dry run are not deployed-runtime evidence.

The feature ledger records, for each original capability: its source owner,
public/data contract, replacement seam, reference fixtures, old/new execution
results, migration route and remaining gap. The single-scheduler SQLite lab is
an MVP profile; it does not by itself qualify production HA or all workloads.

Concrete reference suites include:

| Contract | Original evidence to preserve and extend |
| --- | --- |
| Assignment and lifecycle | [TaskAssignerImplTest](../../src/test/java/org/apache/aurora/scheduler/scheduling/TaskAssignerImplTest.java), [TaskStateMachineTest](../../src/test/java/org/apache/aurora/scheduler/state/TaskStateMachineTest.java), [TaskReconcilerTest](../../src/test/java/org/apache/aurora/scheduler/reconciliation/TaskReconcilerTest.java). |
| Config and worker envelope | [MesosTaskFactoryImplTest](../../src/test/java/org/apache/aurora/scheduler/mesos/MesosTaskFactoryImplTest.java), [config conversion](../../src/test/python/apache/aurora/config/test_thrift.py), [task info](../../src/test/python/apache/aurora/executor/common/test_task_info.py). |
| Dependencies and retries | [Task planner](../../src/test/python/apache/thermos/common/test_task_planner.py), [failure limits](../../src/test/python/apache/thermos/core/test_failure_limit.py), [ephemerals](../../src/test/python/apache/thermos/core/test_ephemerals.py). |
| Process termination and recovery | [Finalization](../../src/test/python/apache/thermos/core/test_finalization.py), [staged kill](../../src/test/python/apache/thermos/core/test_staged_kill.py), [runner integration](../../src/test/python/apache/thermos/core/test_runner_integration.py), [Thermos task runner](../../src/test/python/apache/aurora/executor/test_thermos_task_runner.py). |
| Health and status | [Status checker](../../src/test/python/apache/aurora/executor/common/test_status_checker.py), [health checker](../../src/test/python/apache/aurora/executor/common/test_health_checker.py), [status manager](../../src/test/python/apache/aurora/executor/test_status_manager.py). |
| Storage and migration | [Storage transactions](../../src/test/java/org/apache/aurora/scheduler/storage/mem/StorageTransactionTest.java), [data compatibility](../../src/test/java/org/apache/aurora/scheduler/storage/durability/DataCompatibilityTest.java), [snapshot integration](../../src/test/java/org/apache/aurora/scheduler/storage/log/SnapshotterImplIT.java), [durability goldens](../../src/test/resources/org/apache/aurora/scheduler/storage/durability/goldens/). |

## Review discipline and the first implementation PR

Use Luna for bounded mechanical build/generator changes, fixtures and documentation.
Use Astra at medium reasoning for backend contracts, transactional storage,
recovery and compatibility design. The orchestrating reviewer checks every diff,
verifies the cited original behavior and runs the relevant tests before accepting
delegated work. Delegation changes who writes a patch, not the architecture.

The first implementation PR combines the source inventory and the smallest
Java 25 build restoration: original entry point and source sets, required
generator fixes, pinned tools, focused original tests, and an honest list of
remaining full-build blockers. It adds no new scheduler engine, job API or Go
runtime integration. Follow-up build fixes close that list before claiming a
fully qualified original application. The next two slices establish original
behavior coverage and extract the backend contracts.

Current historical evidence is limited: the foundation compiled generated API,
commons, scheduler and scheduler tests; ran 21 `TaskStateMachineTest` tests and
eight generator regressions; and reproduced 240 generated Java/resource outputs.
Those checks used the old toolchain. They are useful comparison evidence, not
Java 25 or full-suite acceptance. The later 73-test Java result belongs to the
parallel application. No new implementation tests were run to author this plan.

## Relationship to the six research tasks

| Research input | Where it fits now |
| --- | --- |
| [Mesos removal](MESOS_REMOVAL_PLAN.md) | INPLACE-03 through 08: replace backend and persistence within the original application. |
| [Go agent](GO_AGENT_DESIGN.md) | INPLACE-06 through 08: preserve existing executor/configuration semantics behind an adapter. |
| [Java modernization](JAVA_MODERNIZATION_PLAN.md) | Minimum Java 25 build in INPLACE-01; broad retained-code modernization in INPLACE-09. Earlier Java 8 constraints are superseded. |
| [Simplification audit](CODEBASE_SIMPLIFICATION_AUDIT.md) | Remove proven obsolete dependencies after replacement; simplify retained code in INPLACE-09. |
| [UI reimagining](UI_REIMAGINING.md) | Keep the existing UI working, then modernize it incrementally in INPLACE-10. |
| [Pi testing](RASPBERRY_PI_TESTING_PLAN.md) | Build and contract checks from the start; original-scheduler Docker acceptance in INPLACE-07 and migration/HA qualification afterward. |

These documents remain research inputs with their original provenance. This
plan and the [ordered backlog](IMPLEMENTATION_BACKLOG.md) control implementation
where earlier research or prototype status documents conflict with this direction.
