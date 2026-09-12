# Java 25 audit implementation

This increment applies the local findings from the [source audit](JAVA25_FILE_AUDIT.md)
to the original Aurora application on `codex/in-place-java25`. Java 25+ remains
the target. The audit's source hashes and assessments remain historical; the
[implementation ledger](java25-audit/implementation.jsonl) records a separate
disposition for each of its 188 findings.

## Changes

- Close owned compression, credential, snapshot, recovery and JSON reader
  resources. Compression preserves the historical byte fixture. Recovery keeps
  sequential replay and transaction ownership. JSON read failures remain primary
  when closing the reader also fails.
- Replace obsolete constructor reflection with explicit constructor lookup and
  invocation, preserving accessibility, constructor preference, checked failure
  propagation and fresh per-call counters. Construct the known scheduler entry
  point directly in its integration test.
- Replace shared `SimpleDateFormat` with immutable `DateTimeFormatter`, retaining
  the filename pattern, English locale, minute precision and construction-time
  default zone. Tests cover a year boundary and a daylight-saving overlap.
- Restore interruption when translating `InterruptedException`; keep ordinary
  execution failures distinct. Give `BatchWorker` ownership of its retry executor
  and accepted futures: pre-start submissions remain supported; stopping workers
  reject new submissions; active batches finish; unfinished work is cancelled on
  normal termination or completed exceptionally on service failure.
- Dispose request-owned Kerberos contexts on success and failure, retaining the
  authentication failure when disposal also fails. A disposal failure after an
  otherwise successful request fails authentication. Use `Subject.callAs` while
  preserving subject scope and the previous action exception contract.
- Introduce four internal records; retain required null checks, accessor aliases
  and the machine-resource hash. Simplify equality with pattern bindings,
  optional composition, immutable list collection, guarded `getLast`, character
  sets and locale-independent metric/protocol normalization. Remove the unused
  task-ID clock dependency while retaining the identifier format and UUID suffix.
- Fix `Ratio`'s ineffective NaN comparisons: a NaN input now produces the intended
  zero fallback. Finite/infinite arithmetic and zero-denominator behavior remain
  separately covered. Migrate blank checking only after a comparison across all
  1,114,112 Unicode code points and the empty string.
- Bound async test completion, observe worker failures and release test-owned
  barriers/resources. Storage reader tests keep the first reader blocked until
  the independent reader completes, so serialization cannot produce a false pass.

These are in-place language/API improvements and targeted lifecycle fixes. They
do not replace the state machine, scheduling policy, public RPC model or storage
implementation. Successful batch futures still complete inside the existing
storage write; transaction/outbox semantics remain part of the storage slices.
Completion callbacks still execute inline and must not wait for other pending
results from the same worker.

## Decisions and remaining work

Of 75 local findings, **72 are implemented**, **one is partially implemented**,
and **two are retained pending further review**. Implementation does not mean
every production integration or proposed experiment is qualified.

- `MoreModules` uses explicit reflection, but retains its public `Class<?>`
  signature and runtime wrong-type behavior. Narrowing that API remains a
  separate contract change.
- Keep `BatchWorker`'s collection and existing poll/requeue ordering until a
  realistic allocation benchmark supports changing them.
- Keep the public non-final `Thresholds` class until typed configuration review
  addresses extension, equality and display contracts.
- The existing ignored secondary-index concurrency test now collects reader and
  writer futures with a shared deadline and stronger assertions. It remains
  **skipped**; the separate investigation/requalification finding is still open.

The other **113 findings** remain sequenced behind framework or internal-contract
work in the [in-place plan](IN_PLACE_MODERNIZATION_PLAN.md). This includes public
DTO records, Thrift generation, Java time/configuration types, coordinated test
framework migration, executor redesign and library upgrades. Shared Kerberos
credential disposal and JAAS logout require service-lifetime ownership work.

## Validation and provenance

On 2026-09-11, the pinned Java 25.0.4.1 / Gradle 9.7.1 build completed with
**1,359 passing Java tests, zero failures/errors and one existing skip**:
1,235 application tests and 124 commons tests passed. This is 65 additional
passing tests over the audited baseline. The final application suite was rerun;
Gradle reused the commons result from this batch because its inputs were unchanged.

Compared with the audit baseline, production Java grew by **32 lines net**, with
no production files added or deleted. Tests/helpers grew by **1,534 lines net**,
including five new test files. The inventory is now 641 Java files; the 18 JMH
files are unchanged. Most growth is failure-path and lifecycle regression coverage.

Upstream master and fork master were refreshed and both resolved to
`11ebaeeb071cb182c388a40755e84f60dda32260`, already an ancestor of this branch.

The [evidence receipt](java25-audit/implementation-evidence.json) records the
executed Java suites, per-suite XML hashes, source counts, tool versions and
remaining qualification gaps. The [source inventory](java25-audit/implementation-sources.jsonl)
records the current tracked Java sources separately from the original audit baseline.

Run from the repository root with the pinned Thrift compiler installed:

```sh
./gradlew --offline \
  -PthriftCompiler="$PWD/.pi-tools/thrift-0.10.0/compiler/cpp/thrift" \
  focusedTest :commons:test --continue
```

These suites need local test sockets for HTTP and ZooKeeper. Earlier runs caught
new-test fixture/setup errors and an invalid 30-second timeout for a once-per-minute
cron schedule; the final results, rather than those failed runs, qualify this batch.
Luna handled bounded edits, Astra handled reflection and lifecycle/authentication
work, and the orchestrator reviewed their changes and regression tests. Independent
review also corrected async cleanup and exception-suppression edge cases.

The baseline's 13 Python/generator test passes are historical; those unchanged
inputs were not rerun here. Fresh-checkout reproducibility, the UI/distribution,
full quality/coverage/license tasks, native Mesos integration and a real KDC are
still open in [INPLACE-01 build status](INPLACE01_BUILD_STATUS.md). There is no new
performance, race-stress or mutation-test claim.
