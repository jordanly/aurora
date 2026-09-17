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
own the IPv4 loopback listener and pass a probe before RUNNING. Startup deadlines
apply independently of probe intervals; early exit before readiness, timeout or
consecutive failures terminate the process group and report FAILED after cleanup.
Failure reasons reach original task events and the existing update rollback policy.
Unconfigured processes retain immediate readiness.

Fixed health sockets participate in placement alongside CPU/memory reservations,
including pending delivery, update affinity and agent cleanup. Conflicts select
another agent or remain pending. Monitoring survives agent daemon restart in the
per-attempt supervisor. This provides TCP connectivity checks, not HTTP or
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

## Qualification

Qualification is in progress for implementation commit
`42ad3c6dc28a032cb771a609b6223e32d2fd1559`, published on
[`codex/in-place-java25`](https://github.com/jordanly/aurora/tree/codex/in-place-java25).
Final receipts will record exact source, artifacts and fresh two-agent Docker results.

Local behavior checks pass 1,375 scheduler tests and 124 commons tests, with
zero failures, errors or skips. Instruction coverage is 91.57% and branch coverage
is 81.09%; the original 87% / 79% thresholds remain unchanged. Final static
analysis, packaging and fresh-cluster qualification are in progress.

The implementation changes 65 files, adding 4,100 lines and deleting 116 before
this qualification documentation; 23 changed files contain tests. Review found
and fixed startup-deadline ordering, monotonic health-stop escalation, UTF-8 page
boundaries, UI route collisions, test injector bindings and transient supervisor
journal lock contention. The finalizer regression now checks shared-budget
behavior instead of timing unrelated task setup against an invalid total bound.
The watch fixture applies its short response deadline after TLS negotiation,
so the streaming regression no longer imposes a 20 ms handshake limit.

## Remaining work

Bounded long-term retention, workload isolation, broader application/runtime
features and larger-scale qualification remain. HA remains last. This is still a
trusted-process deployment with a single SQLite-owning scheduler.
