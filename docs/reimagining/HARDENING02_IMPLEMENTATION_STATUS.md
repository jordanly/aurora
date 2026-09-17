# Journal, health and task-log hardening

This slice implements the next three items after
[continuous-operation hardening](HARDENING01_IMPLEMENTATION_STATUS.md), inside the
original SchedulerMain application on `codex/in-place-java25`.

## Journal compaction

The Go agent atomically migrates older monolithic journals into a small hot
snapshot plus indexed, checksummed history buckets. Its main runtime, admission,
ACK and watch paths no longer scan/rewrite all completed attempts. The hot
snapshot retains reservations, pending Stop processing, unacknowledged supervisor
work and unpruned observations/results. Indexed lookups preserve old command
replay, immutable-body conflict detection, Stop-before-Run tombstones and per-
attempt sequencing. Full startup validation still scans retained history.

`aurora-agent compact --config agent.json --state source.db --output compacted.db`
reclaims unused pages into a new, durable offline copy. It exclusively locks an
existing source, validates complete contents, preserves authority, refuses an
existing output, and never activates the copy. Publication persists the ownership
marker before the data so interrupted publication fails closed. Source byte
identity, unsynced freelists, actual page reclamation, replay and publication
crashes have regression coverage. See the [agent guide](../../agent/README.md).

This is not bounded lifetime retention: cold command records, full attempt bodies,
sequence counters, child supervisor events and workload log directories remain
retained. Safely retiring replay metadata requires a separate recovery/fencing
contract; ACK or elapsed time alone cannot authorize deletion.

## Application health

The existing process executor accepts optional TCP health policy. A process must
own an IPv4 listener reachable on loopback and pass a probe before RUNNING. Startup deadlines
apply independently of probe intervals; early exit before readiness, timeout or
consecutive failures terminate the process group and report FAILED after cleanup.
Failure reasons reach original task events and the existing update rollback policy.
Unconfigured processes retain immediate readiness.

Fixed health sockets participate in placement alongside CPU/memory reservations,
including pending delivery, update affinity and agent cleanup. Conflicts select
another agent or remain pending. Monitoring survives agent daemon restart in the
per-attempt supervisor when `--supervise` is enabled, as in the Docker lab.
This provides TCP connectivity checks, not HTTP or
application-level response validation. See [health configuration](../operations/go-process-health.md).

## Task log access

The original UI now opens Go-backed stdout/stderr pages through the scheduler.
The scheduler routes by durable task/Run identity to its enrolled agent using
mutual TLS and current session authority. Reads are bounded, reject caller paths,
links and special files, and release storage transactions before network I/O.
Pagination, refresh, literal rendering, truncation and unavailable output are
covered. Completed logs remain readable after daemon/scheduler restart. The new
UI route is distinct from existing job/task routes. See [task logs](../operations/task-logs.md).

## SQLite connection lifecycle

Fresh-cluster smoke and health checks exposed repeated read failures while new
connections opened during the previous last connection's WAL cleanup. This is a
[documented SQLite concurrency case](https://sqlite.org/wal.html#sometimes_queries_return_sqlite_busy_in_wal_mode).
Holding one external idle read-only connection let the previously failing health
scenario pass, supporting the diagnosis. The diagnostic lab and receipts remain
separate from final qualification.

The storage owner now retains its initialized connection in autocommit until
shutdown. Transactions still get independent connections and snapshots; the
one-second busy timeout and fail-stop write policy are unchanged. Regressions
verify that the idle connection permits a complete WAL checkpoint, transaction
connections close, ownership remains held after failed shutdown, retries release
it safely, and failed initialization still cleans up correctly.

## Supervisor startup

The first log qualification exposed another startup failure: both agents' new
supervisors spent more than five seconds initializing durable journals, exceeding
the gated-process handshake deadline before any workload launched. Aurora
rescheduled those attempts, so the strict log acceptance check correctly failed.

The handshake now establishes the validated supervisor's exact identity before
journal initialization. A live supervisor without a ready socket remains attached
with its reservation retained, including after daemon recovery. Wrapped Unix
socket ENOENT errors take that same unavailable path. The readiness timeout is
unchanged. Dead supervisors with missing or corrupt journals still fail closed;
this change does not infer successful cleanup from an absent journal.

## Qualification

All six acceptance phases passed on the fresh two-agent ARM64 Docker lab
`.pi-lab/hardening02-final`, with implementation commit `18e3021da0fd1f5a5e99016f587bfddafd9bd088`.
The cluster uses the original SchedulerMain, its Java 25 distribution and the
checksum-recorded Go agents; no external SQLite connection helper was present.

| Check | Result |
| --- | --- |
| Java behavior | 1,375 scheduler + 124 commons tests; zero failures, errors or skips |
| Coverage | 91.57% instructions / 81.09% branches; thresholds unchanged at 87% / 79% |
| Java quality, packaging and installed launchers | Passed |
| UI | Lint, production build, 150 tests in 35 suites passed |
| Go | Uncached agent/helper tests, vet and verified binary builds passed |
| Smoke | Batch completion, two-agent services, rolling update, kill, quota/cron and drain APIs |
| Application health | Two-agent placement, startup-failure rollback with exact executor restoration, listener-loss failure |
| Logs | Four 1 MiB streams, 64 pages total, exact SHA256s, truncation/end-page checks; retained after both agents and scheduler restarted |
| Recovery | Three rounds: graceful scheduler restart, scheduler crash and alternating agent crash; workload identities and PIDs unchanged |
| Scheduling policy | Failed-update rollback, maintenance replacement and cron execution |
| Sustained churn | 130 batches / 260 completed tasks; two service identities/PIDs and all daemon generations stable; automatic backup publication retained |

All three jobs in [GitHub CI](https://github.com/jordanly/aurora/actions/runs/35233550889) passed on the implementation revision.
Earlier remote jobs that stopped at pinned Thrift bootstrap are retained as
failed diagnostics; they are not represented as application test passes.
The Java/UI/tools sources are unchanged from the final local full-gate revision
`42ad3c6dc28a032cb771a609b6223e32d2fd1559`; the subsequent supervisor change has
its own final Go tests, rebuilt artifacts and fresh-cluster qualification.

The offline compaction drill paused each agent, validated a complete copy, checked
that the original database SHA256 was unchanged, and resumed the original daemon.
Copies were not activated:

| Agent | Source bytes | Compacted bytes | Original unchanged |
| --- | ---: | ---: | --- |
| agent-a | 1,048,576 | 524,288 | Yes |
| agent-b | 1,048,576 | 524,288 | Yes |

The lab remains running at `http://172.24.0.4:8081` with
`fixtures/test/health-mvp-demo`: two healthy services on different agents, readable
stdout/stderr, scheduler health OK and the UI log route available. The bridge is
private to the Pi. The demo reserves its health port; use a fresh lab for another
complete acceptance run.

The implementation changes 65 files, adding 4,284 lines and deleting
128 (+4,156 net), excluding qualification documents under
`docs/reimagining`. 23 changed files contain tests.
Astra implemented/reviewed complex journal, health and durability changes; Luna
handled/reviewed UI and operational documentation. Parent review corrected issues
and qualified the combined result. Regression coverage includes health-deadline
ordering, monotonic stop escalation, UTF-8 pagination, UI route collisions,
SQLite lifetime ownership, journal lock contention and delayed supervisor startup.

Evidence: [qualification receipt](hardening02-evidence.json),
[source SHA256s](hardening02-inputs.json), [Java test ledger](hardening02-tests.jsonl).
The receipt identifies the earlier failed diagnostic labs and test failures
alongside final passing checks, without reusing diagnostic workarounds as final
qualification.

## Remaining work

Bounded long-term retention, workload isolation, broader application/runtime
features and larger-scale qualification remain. HA remains last. This is still a
trusted-process deployment with a single SQLite-owning scheduler.
