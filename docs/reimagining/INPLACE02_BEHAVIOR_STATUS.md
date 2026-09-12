# Original behavior qualification and next roadmap increments

This continues INPLACE-02, INPLACE-03 and INPLACE-04 on `codex/in-place-java25`
after the [local Java 25 modernization](JAVA25_IMPLEMENTATION_STATUS.md).
It is an incremental checkpoint; none of these three complete roadmap slices is
declared finished.

## INPLACE-02: measured original Java behavior

`verifyOriginalBehavior` now runs the complete original scheduler test suite with
JaCoCo and requires the commons tests. It uses the restored Java resources and
all root production class directories, independently of UI packaging. No class
exclusions or lower thresholds were introduced. The required thresholds remain
**87% instruction coverage and 79% branch coverage**.

The final run on 2026-09-11 passed **1,361 Java tests**, with zero failures/errors
and one existing skip. Coverage was **90.20% instructions / 83.56% branches**
across 761 reported application classes. The scheduler suite was rerun; Gradle
reused the passing commons result because its inputs were unchanged.

`behaviorTest` rejects include/exclude filters. The aggregate command rejects
excluded, disabled and skipped prerequisites, and requires the execution data
and XML report. Gradle's valid up-to-date/cache results remain usable. Negative
checks exercise both filtered tests and an excluded test task, so stale coverage
cannot pass those invocations. Ignored JUnit tests remain explicit ledger gaps.

Run from the repository root:

```sh
./gradlew --offline \
  -PthriftCompiler="$PWD/.pi-tools/thrift-0.10.0/compiler/cpp/thrift" \
  verifyOriginalBehavior --continue
```

The [evidence receipt](inplace02-evidence.json) binds final inputs, suite counts,
coverage counters and report/log hashes. Early failed runs exposed a JaCoCo
TaskProvider/file-notation mismatch and a deprecated execution-time project
lookup; both were corrected before the accepted run. The report uses the test
task's explicit JaCoCo destination file.

Remaining INPLACE-02 qualification includes UI/installed launchers/distribution,
full optional quality/CI checks, the ignored secondary-index test, broader
execution/configuration fixtures and native integration. A passing Java coverage
gate does not close those requirements or the remaining INPLACE-01 build gates.

## INPLACE-03: first neutral identity boundary

`StateManager.assignTask` now accepts a plain agent ID string. The Mesos-facing
`TaskAssignerImpl` extracts that value from the offer before calling the existing
state manager. Resource assignment, persisted host/agent mutation and the ASSIGNED
transition retain their order. Public Thrift `slaveId` and `slaveHost` fields are
unchanged.

The existing assignment, resource and state tests exercise the changed callers.
New characterizations verify verbatim empty, whitespace and Unicode identifiers,
and null rejection before resource assignment or stored-task mutation.

This removes protobuf identity imports from `StateManager` and its implementation.
The Mesos `Driver` package dependency, offers, resources, launch/kill/reconciliation
contracts and adapter wiring still need extraction. Both backends must continue
through the same existing policy and state implementation.

## INPLACE-04: storage contract preparation

The [storage contract](INPLACE04_STORAGE_CONTRACT.md) records the first Pi backend's
ownership and process-crash durability scope, all seven store contracts, nested
rollback-only behavior, read isolation, uncertain commits, atomic command/receipt
requirements and backup/restore gates. It also records production availability
and recovery objectives that remain unspecified. **No transactional storage
backend is implemented by this increment.** Production HA is still a separate
gate before cutover.

Luna handled the bounded identity-signature edits; Astra reviewed the boundary,
coverage wiring and storage contract. The orchestrator reviewed changes, added
identity characterization tests, and ran the qualification and negative checks.
Work stopped at the user's 50%-remaining usage limit after this tested checkpoint;
the open work above remains required before the three slices can be closed.
