# Java 25 review fix validation

This record qualifies the in-place scheduler changes described in the
[implementation ledger](../JAVA25_FIXES_2026_09_18.md). The review baseline is
`91b9bd74746d27102a96fa47d4e06e0dd25097ca`; the implementation and CLI regression
commits end at `e66c81c04474bda8bf3ec0ab2bb5828493192c2d`.

## Java and packaging

- The complete behavior gate passed: **1,456 scheduler tests and 141 commons
  tests**, with no failures, errors, or skips.
- Application coverage is **91.73% instructions / 80.98% branches**, above the
  unchanged 87% / 79% thresholds.
- The complete original quality gate passed. The subsequent CLI help correction
  passed the affected main/test analyzers, license checks, and six focused CLI
  tests. Final Checkstyle, PMD, and SpotBugs reports contain zero unsuppressed
  findings for main, test, and benchmark source sets.
- Analyzer migration checks passed 18 shared Checkstyle contracts plus three
  record contracts, 106 PMD cases, and 76 SpotBugs filter cases.
- Executable benchmark fixtures passed five scheduling/reset cycles and workload
  cardinality checks. These are correctness checks, not timing measurements.
- The installed distribution passed original scheduler/recovery launcher checks,
  classpath and Java 25 artifact checks, and bundled UI verification.

The final change after the full behavior run replaces an artificial CLI test
fixture with Aurora's real ZooKeeper credential options. All six CLI tests and
their analyzers passed again. Production code did not change. The full behavior
task and its coverage gate succeeded inside a build whose separate PMD test task
flagged that artificial fixture; the final targeted build resolved that failure
and exited successfully. The command receipts preserve this distinction.

The source digest selects existing tracked/nonignored Java files, the root build
and settings files, `.auroraversion`, and Checkstyle/PMD/SpotBugs configuration.
For each of 702 files in sorted repository-relative path order, hash its contents
with SHA-256; hash the concatenation of `hash`, two spaces, path, and newline.
The final input digest is:

```
ba61734223f9335ce27cc521c4055a42d6e7d7f266941d274beef6050ef8590a
```

The staged distribution tree digest, using the lab tool's tree-digest format, is:

```
e30fe9933055db35b3659408f32c3ed8ce3182b78f29705f1e36f9be12238bce
```

## Isolated cluster

Two fresh labs use the original scheduler and two Go agents in separate
containers. The existing LAN lab was not modified. The lab tool verified and
copied the distribution, JDK, agent, and helper inputs before creating each
private network and its containers.

The first lab, `.pi-lab/java25-review-fixes`, passed smoke, health, retained-log,
three-round recovery, and policy acceptance. Churn failed at zero-based batch 5,
exposing a Go agent exit-classification bug. That lab has been shut down; its
failed and successful receipts remain available locally. The second lab,
`.pi-lab/java25-review-fixes-v2`, uses the same Java distribution and the corrected
agent. All six acceptance phases passed on that second lab:

| Phase | Result |
| --- | --- |
| Smoke | Six cases: batch completion, two-agent service placement, rolling update, termination, quota/cron APIs, and empty-host maintenance. |
| Health | Three cases: readiness on two agents, listener loss, and startup rollback. |
| Logs | Seven cases covering retained output, pagination/truncation, and daemon restarts. |
| Recovery | Nine checks across three rounds of scheduler restart, scheduler crash, and agent crash; original task IDs, hosts, and physical workload PIDs preserved. |
| Policy | Automatic rollback with exact executor restoration, active-host drain/replacement, and manual cron execution. |
| Churn | 130 two-instance batches: 260 distinct FINISHED task IDs, exactly 130 per agent, two unchanged service processes, and unchanged daemon identities throughout the phase. |

Churn took 799.61 seconds. Three published backup files were observed before and
after the phase; this is publication/retention evidence, not a restore check.
Both temporary labs were shut down after qualification, preserving local report
files. The [machine-readable receipt](validation.json) records source and artifact
hashes, commands, final phase summaries, and earlier failed attempts separately.

The first smoke request preceded scheduler API startup and received connection
refused. A retry after the scheduler reached ACTIVE passed; no acceptance
assertions or deadlines were relaxed. Both attempt receipts are retained locally.

The first policy attempt exposed a timing assumption in the acceptance harness:
its one-second stable-RUNNING window admitted deliberately failing processes
before delayed exit observations arrived. The two accepted attempts' recorded
RUNNING-to-FAILED spans were 2,977 ms and 1,794 ms; both were accepted after more than one
second, consistent with the existing updater contract. The policy fixture now
requires 30 seconds of stable running, a stricter admission condition, with its
120-second rollback timeout and all outcome/configuration assertions unchanged.
The retry passed against the same scheduler and agent artifacts. Harness commit
`9c817e700` passed the pinned Go tool module's uncached tests and `go vet`.

### Agent regression found during qualification

The failed churn task recorded exit code zero, failed outcome, complete cleanup,
and empty output. The runtime treated every error from `exec.Cmd.Wait` as a task
failure, including `exec.ErrWaitDelay`. Go documents that this error can follow a
successful process exit when output pipes do not close before the drain deadline
([Go execution contract](https://pkg.go.dev/os/exec#ErrWaitDelay)).

Commit `edb49e08e` preserves the successful outcome when that specific error and
the process status confirm success. It durably records potentially incomplete
output, exposes it through the existing log `truncated` field, and leaves measured
dropped-byte counters unchanged. The pipe-drain deadline, health/stop overrides,
and cleanup requirements remain unchanged. The known-exit regression now asserts
the outcome and incomplete-output marker before and after supervisor loss; the
log regression checks both streams and byte counters. Uncached agent/helper
tests and `go vet` passed with pinned Go 1.27.1 before rebuilding the lab binaries.

An existing finalizer test's 60 ms setup budget expired before admission on this
busy Pi. Its fixture now allows one second; the first finalizer still exceeds the
shared deadline, and the same assertions require cleanup and prohibit admission
of the second finalizer. No production finalizer deadline changed.

## Reproduction and limits

Use the pinned tools and separate commands from the [Java build guide](../../../build-support/java/README.md):
`verifyOriginalBehavior`, `-Pq verifyOriginalQuality`, and
`verifyInstalledDistribution`, supplying the verified Thrift compiler through
`-PthriftCompiler`. Use a fresh root and the sequential acceptance phases in the
[lab guide](../../../build-support/lab/README.md).

Kerberos cleanup uses controlled login/GSS collaborators in tests; no external
KDC or end-to-end IPv6 qualification is claimed. This does not qualify production
HA, establish comparative performance, or remove the compatibility boundaries
listed in the implementation ledger.
