# Aurora modernization roadmap

Journal compaction, application health and task logs are implemented and qualified
in the [follow-up status](HARDENING02_IMPLEMENTATION_STATUS.md).
Earlier qualification results below remain tied to their recorded revisions.

The active roadmap is the [in-place modernization plan](IN_PLACE_MODERNIZATION_PLAN.md),
with its [ordered implementation backlog](IMPLEMENTATION_BACKLOG.md).

On 2026-09-11 the user abandoned the parallel scheduler approach. Modernization
will retain the original Aurora application, policy/state owners, API and UI,
then replace their backend dependencies. The proposed retirement of the original
source is cancelled. Java 25+ remains the target for the entire maintained Java
project, with no Java 8 compatibility obligation.

The order is:

1. Restore the original source/build inventory and establish a Java 25 build.
2. Qualify existing behavior and extract internal Mesos adapter boundaries.
3. Implement complete transactional storage and committed command/event delivery.
4. Integrate Go execution and qualify the original scheduler with two Docker agents.
5. Complete feature/migration/HA compatibility and remove Mesos dependencies.
6. Refactor the retained code into idiomatic Java 25+, then modernize UI and clients.

Minimum build modernization comes first so the original code can be tested;
broader Java refactoring follows Mesos removal. The full plan defines acceptance
criteria, testing tiers, compatibility obligations and the first implementation PR.
INPLACE-00 is complete and the initial INPLACE-01 Java baseline passes on
`codex/in-place-java25`; the
[current build status](INPLACE01_BUILD_STATUS.md) distinguishes executed evidence
from remaining gates.

## Historical experiment

The [previous roadmap at the published experimental commit](https://github.com/jordanly/aurora/blob/dc8d7908d9d957389b0780aa1cf70aa822f8457c/docs/reimagining/UNIFIED_AURORA_ROADMAP.md)
remains available through Git history. Existing status reports and evidence
receipts describe the parallel application and are retained as historical results.
Their completion labels do not establish original Aurora compatibility or
completion of the new backlog. The six original research documents remain
unchanged; the active plan explains how their useful findings fit the corrected
architecture.
