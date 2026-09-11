# In-place modernization backlog

Active as of 2026-09-11. This replaces the parallel application's backlog and
cancels JAVA-11 source retirement. The [full plan](IN_PLACE_MODERNIZATION_PLAN.md)
defines the architecture, detailed acceptance criteria and Docker testing ground.
These are planned work items, not claims of created GitHub issues or completed
implementation. INPLACE-00 is **complete**. INPLACE-01 has a passing original Java
baseline; its full build gates remain open. INPLACE-02 has initial Java evidence;
its broader compatibility qualification and subsequent slices remain open.
See the [current build status](INPLACE01_BUILD_STATUS.md) for executed evidence.
The [Java 25 file audit](JAVA25_FILE_AUDIT.md) assesses all 636 tracked Java files
and orders concrete modernization batches. Small resource, failure-handling and
test-reliability fixes can accompany the early baseline work; broad framework
and application refactoring remains sequenced through INPLACE-09.

| Order / ID | Deliverable | Depends on | Required evidence |
| --- | --- | --- | --- |
| INPLACE-00 | New implementation branch from refreshed upstream; original source, test, API, build and distribution inventory | Current upstream/fork verification | Every original component accounted for; historical prototype evidence separated from application qualification. |
| INPLACE-01 | Original scheduler, API/entities, commons and tests built on Java 25; restored full build graph | 00 | Clean compilation; deterministic generation; original state-machine/generator baselines; explicit remaining packaging, UI and integration blockers. |
| INPLACE-02 | Original behavior, API, storage and execution compatibility ledger | 01 | Original policy/state/updater/cron/storage/API/security suites; complete discovered/executed/skipped/blocked test ledger; UI/distribution and quality checks. |
| INPLACE-03 | Neutral execution/resource/observation contracts within existing scheduler, with temporary Mesos adapter | 01; relevant 02 behavioral baselines | Equivalent assignment, vetoes, reservations, resource accounting and transitions through original policy code. |
| INPLACE-04 | Complete transactional implementation of all seven Aurora stores; schema, outbox and receipt records | 02; 03 command/identity contracts | Availability/fencing design recorded first; atomic cross-store writes, nested rollback, read isolation, crash recovery and backup tests. |
| INPLACE-05 | Durable dispatch, committed state events, observation deduplication and controller recovery | 03, 04 | No effects before outer commit; correct replay/cancellation/leadership ordering; original state semantics; readiness gated by successful recovery. |
| INPLACE-06 | Go backend and resolved-config adapter integrated with original task/status/reconciliation paths | 03, 05 | Original identities and API preserved; capability validation; real execution; compatibility fixtures for supported cohort. |
| INPLACE-07 | Original Java 25 scheduler plus two Go agents in isolated Pi containers | 06; applicable 02 gates closed | Existing API drives original policy; three recovery rounds and ten-minute workload; source/image/config provenance and safe cleanup. |
| INPLACE-08 | Remaining execution parity, historical-state migration, HA/fencing qualification and Mesos retirement | 04–07 | Full feature ledger; all-store import/restore and cutover rehearsal; required isolation/availability; driver and replicated-log dependencies removed. |
| INPLACE-09 | Idiomatic Java 25+ throughout the retained application and tools | 08 | Original suites and packaging pass; reviewed language/dependency/DI/HTTP improvements; explicit semantic and concurrency changes. |
| INPLACE-10 | Incremental UI and client modernization through preserved application contracts | 09; API/UI compatibility retained throughout | Submit, inspect, update/rollback, cron, drain and logs work; auth and existing links/workflows preserved. |

## First implementation batch

The first PR combines INPLACE-00 and the smallest INPLACE-01 build restoration:
original entry point/source sets, generator fixes, pinned Java 25 tools, focused
original tests and a precise list of remaining full-build blockers. Follow-up
build fixes close that list. Then establish the INPLACE-02 behavioral baseline
and perform the INPLACE-03 adapter extraction. No replacement scheduler engine
or new job API belongs in this batch.

Split storage implementation by related stores and then cross-store invariants.
Split Go compatibility by configuration/identity, process execution, retries and
dependencies, finalization, health/discovery, and resource/sandbox/log behavior.
Contract and fixture preparation may proceed independently once the relevant
baseline is known; runtime integration waits for its durability gates.

The first Pi cohort can be limited to supported process workloads. Full Aurora
feature and HA preservation remain required work, not silently dropped scope.
A blocked test stays visible until executed or its obsolete dependency is
replaced and the equivalent behavior has a passing test.

Use Luna for bounded mechanical work and Astra medium for complex boundaries
and recovery. The orchestrator reviews all delegated changes and evidence.

## Superseded backlog

The [previous backlog at the published experimental commit](https://github.com/jordanly/aurora/blob/dc8d7908d9d957389b0780aa1cf70aa822f8457c/docs/reimagining/IMPLEMENTATION_BACKLOG.md)
and existing status/evidence files remain historical records. In particular,
native cluster and JAVA-01 through JAVA-11 results do not qualify the original
scheduler. Reuse their mechanisms only through the active in-place plan.
