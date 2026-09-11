# JAVA-04: review Java 25 idioms across Aurora

Status: complete. The [audit](JAVA25_IDIOM_AUDIT.md),
[647-file inventory](java04-inventory.json) and
[six ordered follow-up tasks](JAVA25_REFACTOR_TASKS.md) are delivered and
independently reviewed. Application code is unchanged; JAVA-05 is the
recommended first implementation slice. The review uses the qualified
[JAVA-03 baseline](JAVA03_STATUS.md).

Identify where modern Java can make Aurora easier to understand, maintain and
verify. Produce a source-backed audit and ordered implementation tasks. The
review should recommend changes for a concrete benefit, with examples and
compatibility checks for each proposal.

In this repository, “native” labels the standalone, Mesos-free implementation.
Its Java scheduler, storage and protocol still run on a JVM; its agents are Go.
Use “standalone Java” in the review where that is clearer.

**Scope and order**

1. Inventory Java source and its build/test coverage across the repository.
   Identify actively shipped code, retained legacy code, generated sources and
   test/build helpers. Record exclusions and their reasons.
2. Inspect the active standalone scheduler in `scheduler/native/src`, protocol
   code in `protocol/java/src`, SQL storage in
   `src/main/java/org/apache/aurora/scheduler/storage/sql`, and their tests first.
3. Review remaining handwritten Java for reusable findings. Establish whether
   each legacy component has a future consumer before proposing implementation
   work. Preserve generated-code ownership boundaries.

**Questions to investigate**

| Area | Review questions |
| --- | --- |
| Domain models | Could records, sealed hierarchies or stronger types replace boilerplate, loosely typed maps or invalid state combinations? Check validation, mutable members, equality and serialization implications. |
| Branching and decomposition | Could pattern matching, switch expressions or smaller methods clarify state handling? Preserve exhaustiveness, null handling, ordering and error behavior. |
| Collections and APIs | Could standard JDK APIs, immutable collections, clearer iteration or selective streams simplify code? Check ownership, null acceptance, ordering, duplicates and allocation costs. |
| Resources and errors | Are resource lifetimes, try-with-resources, exception propagation, interruption and shutdown explicit and correct? Preserve transaction and cleanup guarantees. |
| Concurrency | Assess virtual threads and other applicable Java 25 facilities where blocking I/O or context propagation creates a real problem. Preserve bounded admission, backpressure, cancellation, deadlines, transaction ownership and thread-safety; require a separate design and measurements for behavioral changes. |
| Everyday readability | Assess text blocks, local type inference, helper methods and test fixtures where they improve clarity. Retain explicit types and straightforward loops where they communicate intent better. |
| Dependencies | Identify retained utility wrappers or dependencies that standard JDK facilities could replace with fewer moving parts and equivalent behavior. |

For every proposed language feature or API, verify availability and final versus
preview status against official Java 25 documentation. Keep the default target
at Java 25 with preview features disabled. Preserve the qualified Java 26 lanes.

**Deliverables**

- A coverage inventory and findings report tied to a source commit, with file
  and symbol references, representative before/after examples, expected benefit,
  semantic risks, test plan and estimated effort for each finding.
- An ordered set of small implementation tasks: straightforward local cleanup
  first, then model/API changes, then any concurrency redesign. Identify
  dependencies and findings that should remain unchanged or be deferred.
- An independent review of high-impact proposals. Delegate bounded inventory
  work to Luna and complex semantic review to Astra medium; the orchestrator
  checks their findings against the code.

**Acceptance**

The review is complete when its inventory accounts for the Java source areas,
recommendations cite actual code, feature availability is verified, and the
resulting tasks each have a concrete benefit and relevant validation. Creating
this task does not mark the audit or any refactoring complete.

Implementation tasks must preserve canonical JSON, Java/Go interoperability,
persisted SQL state, replay/idempotency, authorization, scheduling order and
resource cleanup unless a separately documented behavior change is intended.
Use existing focused tests for local refactors; add behavior tests where a
meaningful gap exists. Changes to shipped code must finish with the appropriate
packaged Java profile and two-agent recovery gates described in the
[build guide](../../build-support/native/README.md). Compare runtime/resource
measurements when changing concurrency or performance-sensitive paths.

This review can run alongside SUPERVISE-01 planning. Coordinate implementation
where proposed refactors touch shared protocol or scheduling contracts.
