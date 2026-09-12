# Original application: behavior, execution boundaries and transactional storage

Current implementation on `codex/in-place-java25`, 2026-09-12. This updates the
[earlier checkpoint](INPLACE02_BEHAVIOR_STATUS.md); its historical receipts remain
unchanged. The original `SchedulerMain`, policy, state machine, controllers,
public Thrift API and UI remain the application. All Aurora Java artifacts target
Java 25; third-party libraries may contain older bytecode.

## Acceptance status

| Slice | Implemented and locally qualified | Remaining qualification boundary |
| --- | --- | --- |
| INPLACE-01/02 | Original Java source sets and integration suites, coverage, generated API/entities/browser bindings, UI, installed scheduler/recovery launchers, main/test/JMH quality gates and ARM64 CI workflow | Real Mesos/JNI cluster startup, real Kerberos credentials and Python client/executor runtime are separate environment gates. Local results do not claim those scenarios or remote CI passed. |
| INPLACE-03 | Neutral offers, resources, execution control, preparation, observations, maintenance and reconciliation inside the original scheduler, with a temporary Mesos adapter | Go execution adapter and actual Mesos removal follow in INPLACE-06/08. |
| INPLACE-04 | All seven existing stores in one transactional SQLite backend, schema migration, durable command/receipt records, failure recovery and backup | Production integration awaits INPLACE-05. Production HA, host/media failure and power-loss qualification remain open. |

The Java behavior and local storage contracts are concrete acceptance boundaries.
Broader execution compatibility remains visible; completing the local backend
must not be interpreted as completing production cutover.

## Build and behavior evidence

The [machine-readable receipt](inplace02-04-evidence.json) records test counts,
coverage counters, analyzer results, tool and source hashes, full suite ledger,
installed artifacts and clean-checkout validation. Reproduction commands are in
[the build guide](../../build-support/java/README.md).

The clean-checkout run passed **1,556 Java tests** (1,432 scheduler and 124
commons), with **zero failures, errors or skips**. It also passed 37 Python build
helper tests, all 33 UI suites / 144 tests, lint, packaging and both installed
launchers. All nine main/test/JMH analyzer tasks passed, together with 18 paired
Checkstyle fixture cases plus three record cases, 106 PMD cases and 76 SpotBugs
filter cases. API/entity generation reproduced all 228 Java files byte-for-byte.
Clean-checkout coverage is **90.44% instructions / 83.25% branches**; exact
counters are retained in the receipt. That report contains 803 application
classes. Earlier incremental output still held seven obsolete class files from
renames/refactors, so its counts are not used as the final qualification baseline.

The restored build runs the original scheduling, filter, state, quota, updater,
cron, maintenance, SLA, reconciliation, storage, RPC and security tests. This
includes `JobUpdaterIT`, `SchedulerIT`, `HttpSecurityIT`, `ApiIT`, `ThriftIT`,
`CronIT` and `SnapshotterImplIT`. Coverage includes all root production class
directories, without exclusions; historical thresholds remain 87% instructions
and 79% branches. Commons tests are required and reported separately.

The previously ignored secondary-index concurrency test now runs against memory
and SQLite. It retains 10,000 initial tasks, 100 concurrent job reads and 100
writes. Its shared 120-second deadline bounds correctness checking under Pi
instrumentation and analyzer contention; it is not a throughput objective.
Worker cleanup is bounded and checked. The cron dogpile test pauses only its
calendar trigger while explicitly starting the same two jobs, preventing a
wall-clock minute boundary from injecting an unrelated third execution. Separate
calendar-trigger coverage remains active.

The UI uses checksum-pinned Node, locked `npm ci`, lint, the original Jest suites
and production webpack output. Installed-distribution verification checks both
launchers' argument/help paths, every classpath JAR, required generated API and
UI assets, and Java 25 bytecode for scheduler, commons and API/entity artifacts.
It starts no cluster services. Thrift 0.10.0 is built from its verified release
source, with compiler/source/recipe hashes in a reusable receipt; no preinstalled
Pi compiler is required. The clean-checkout check also found a legacy
`commons-args` project with no tracked directory; a README now preserves that
empty project identity without claiming source or test coverage.

Checkstyle 14.1.0, PMD 7.27.0 and SpotBugs 4.10.4 analyze main, test and benchmark
sources. Apache headers and analyzer migration fixtures are explicit gate
prerequisites. Excluding, disabling or skipping a prerequisite cannot qualify the
aggregate; legitimate Gradle up-to-date results remain usable. CI separates
behavior/distribution and quality jobs and retains reports on failure. An
excluded benchmark analyzer was rejected despite its existing passing report.
The final complete gate also passed offline with cached dependencies.

Analyzer upgrades preserve the reviewed historical policy rather than silently
adopting every newly introduced detector. Narrow mappings and exclusions are
explained and checked with positive and negative fixtures:
[Checkstyle](../../config/checkstyle/CHECKSTYLE_14_MIGRATION.md),
[PMD](../../config/pmd/PMD_7_MIGRATION.md),
[SpotBugs](../../config/spotbugs/SPOTBUGS_4_MIGRATION.md).
Actual findings also produced source fixes: defensive byte-array copies in log
entries, explicit path validation, simpler control flow and assertions, local
fixture variables, and immutable fields where lifecycle permits. Expanded rule
adoption and remaining framework/dependency modernization stay in INPLACE-09.

## One policy implementation, explicit execution boundaries

`execution` contains neutral records/interfaces for agent identity, available
resources, offer transport, task preparation, launch/kill/abort control, task
observations, maintenance requests and reconciliation targets. Original callers
in scheduling, offers, preemption, maintenance, reconciliation and state use
these contracts. `StateModule` remains the composition point that binds the
current Mesos adapter. Public `slaveId`/`slaveHost` fields are unchanged.

Mesos conversions and protobuf resource logic live in the Mesos adapter package.
The adapter retains the original offer and prepared TaskInfo for exact launch
translation, including reserved/revocable resources and port ranges. Preparation
still precedes resource consumption. Lazy status translation still occurs inside
the storage callback; acknowledgments follow successful batch completion and a
failed batch acknowledges neither update. Inverse offers are accepted before
maintenance drain, and reconciliation retains its batching, timing and counters.

Original policy tests plus adapter characterization tests cover assignments,
vetoes, reservations, resource accounting, identifier edge cases, resource types,
launch/decline behavior, maintenance ordering and status/acknowledgment sequencing.
The benchmark sources also compile against these same contracts. There is no
second policy engine or replacement public job API.

## SQLite implementation and deliberate correctness changes

`SqliteStorage` implements the existing `Storage` interface and all seven mutable
stores: scheduler metadata, cron jobs, tasks, quotas, host attributes, job updates
and host maintenance. It retains immutable entities and versioned Thrift payloads.
Shared pure job-update query/filter logic is also used by the memory store to
avoid two competing definitions of query semantics. Every existing abstract
store contract runs against SQLite, along with the original state-manager and
40 rolling-updater integration cases.

The [preimplementation contract](INPLACE04_STORAGE_CONTRACT.md) defines the local
ownership and durability scope. One canonical local database is protected by
both an OS ownership lock and a JVM registry. Connections close before ownership
is released. Local ownership epochs are checked with guarded writes; this is
not distributed leader fencing. Dangling symlinks, hard-link aliases, unsupported
schemas, unversioned nonempty databases and root-directory paths are rejected.
URI-encoded database names prevent JDBC option characters from opening an alias.

Each outer write uses one transaction across all stores, command intents,
observation receipts and the operation outcome. Nested writes join it. An escaping
nested failure poisons the outer transaction even if subsequently caught.
Independent reads establish one committed snapshot before invoking the caller;
nested reads see their own write, and read-to-write promotion is rejected.
These rollback/isolation guarantees intentionally correct memory-store behavior;
existing memory tests are not relabeled as proof of transactional rollback.

Stable operation IDs distinguish committed work from unattempted work. A lost
commit acknowledgment blocks subsequent writes until fresh-connection outcome
reconciliation; callbacks are never automatically replayed. Failed connection
cleanup retains ownership for retry. SQLite may automatically roll back after an
I/O/storage error, so store failures also poison the transaction before another
statement can run in autocommit mode. A real `SQLITE_FULL` test uses a database
page limit, verifies rollback even when the callback catches the error, and
then verifies an independent write succeeds.

The selected ARM64 JDBC/native dependency is SQLite JDBC 3.53.4.0. Startup verifies
WAL, FULL synchronization and foreign-key settings. Process-crash tests halt a
child JVM immediately before and after commit and reopen the database, checking
state, intents, receipts and operation outcomes together. Additional tests cover
uniqueness conflicts, checked/unchecked rollback, concurrent snapshots, schema
migration (including a failed upgrade that preserves the prior schema, owner
and operation outcomes before retry), ambiguous commits and cleanup faults.

Backups use `VACUUM INTO` a private temporary file, integrity verification,
explicit flushing and atomic publication without overwriting the destination.
Restoring to a separate location verifies all seven stores plus command/receipt
state. This qualifies process crash with intact local storage, not power loss,
media loss, distributed promotion or production RPO/RTO.

## Next implementation boundary

INPLACE-05 connects the existing state transitions to the durable outbox and
observation receipts. Launch/kill dispatch and state events must follow the
outermost commit, survive commit-to-publication crashes, preserve ordering,
respect cancellation and leadership, and reconstruct controller state before
readiness. The new SQLite backend is deliberately not selected by the production
scheduler until those invariants hold. Its records are foundations for delivery,
not an already operating command dispatcher.

After that, integrate the Go execution adapter and qualify the original Java
scheduler with two agents in isolated Pi containers. Historical-state import,
full executor compatibility and production HA remain required before Mesos
retirement. Query indexing and bounded retention of operation/outbox/receipt
history need workload qualification before production scale; the initial task
store scans serialized records for complex queries.
