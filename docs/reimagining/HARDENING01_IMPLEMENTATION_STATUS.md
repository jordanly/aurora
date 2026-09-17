# Continuous-operation hardening

The next three hardening items—journal compaction, application health and task
logs—are tracked in the [follow-up status](HARDENING02_IMPLEMENTATION_STATUS.md).
Earlier qualification results below remain tied to their recorded revisions.

This slice addresses the validation, agent history, outbox fairness and automatic
backup defects identified in the
[comparative review](COMPARATIVE_REVIEW_2026_09_12.md). It changes the existing
`SchedulerMain`, SQLite adapter and Go agent. The original scheduling policies
remain in place; production multi-scheduler HA remains deferred.

## Expected request failures

Invalid quota values and insufficient quotas return `INVALID_REQUEST` without
triggering the SQLite write-failure shutdown policy. Resource-shape validation
runs before entering a write transaction. Consumption checks remain inside the
transaction, with their known pre-mutation rejections returned from the callback.

Cron and update controllers similarly return known validation errors from their
innermost write callbacks before throwing them to the enclosing API handler.
Missing cron templates, duplicate internal cron creation, missing updates,
invalid update transitions and active-update conflicts cannot mark an enclosing
transaction rollback-only merely because the API rejects the request.

This is not a general exception exemption. Failed mutations and callbacks still
roll back and require restart under the existing fail-stop policy. Regressions
use real SQLite stores and verify both subsequent successful writes and a
post-mutation Quartz failure that must still fail closed.

## Reconciliation and dispatch

Agent transport inventories now describe outstanding reservations rather than
all lifetime attempts. Completed attempts and command results remain in the
durable journal for exact replay, conflicting-command detection and Stop
tombstones. Terminal facts continue through ordered observation pages and are
acknowledged only after the scheduler commits them.

When cleanup removes a reservation, the agent sends a replacement snapshot so
the scheduler's merged inventory cannot accumulate retired keys. Reconciliation
continues beyond the former 128-attempt and 1,024-command lifetime boundaries.
The 128-reservation bound still applies: new Runs receive retryable HTTP 503 at
capacity, while command replay and Stops remain available. Journals with more
than 128 actual reservations still fail closed until cleanup restores the bound.

SQLite selects pending commands for each agent before applying the query limit.
An unavailable agent's backlog therefore cannot hide another agent's commands.
Stops take priority over Runs waiting for that same agent's capacity. Receipt
acknowledgement uses exact command identity instead of a bounded queue window.
Additive indexes support both queries on existing version-3 databases without
changing their schema version or backup format. Network exchanges remain outside
SQLite's writer transaction.

Durable history is preserved, not physically compacted. Disk use and full-journal
processing costs still grow with lifetime work; indefinite operation and large
cluster scalability remain unqualified.

## Automatic backups

The scheduler-active service now uses `-backup_interval`, `-backup_dir` and
`-max_saved_backups`. Defaults are one hour, a directory beside the database,
and 48 backups. Scheduled and manual backups share one synchronized service and
include all stores, outbox entries and observation receipts.

Retention runs after successful publication and always preserves the new
snapshot, including after a backward clock adjustment. Older eligible regular
files are pruned by modification time and filename. Symlinks, temporary files,
unrecognized names and the live database are excluded. Creation and retention
failures have separate counters; last-success timestamp and age gauges support
monitoring. A failed scheduled attempt does not stop future attempts.

See the [backup and recovery guide](../operations/backup-restore.md) for the
offline restore contract. Restoring a complete backup does not authorize a stale
database to adopt arbitrary later agent work.

## Qualification

Qualified on 2026-09-15 (America/New_York; the receipts use UTC).

Implementation commit: `144f2b641fe0e27d36c3f603f01ac8a86b3df5bc` on
`codex/in-place-java25`. Local, fork and upstream master remain at
`11ebaeeb071cb182c388a40755e84f60dda32260`; that revision is an ancestor of this
implementation. All three [GitHub CI jobs](https://github.com/jordanly/aurora/actions/runs/35047052624)
passed for the implementation commit.

| Check | Result |
| --- | --- |
| Java behavior | 1,358 scheduler and 124 commons tests; zero failures, errors or skips |
| Coverage | 91.53% instructions and 81.32% branches; existing 87% / 79% thresholds retained |
| Java quality | Checkstyle, PMD, SpotBugs, licenses and analyzer compatibility fixtures passed |
| UI and packaging | 144 tests in 33 suites, UI build and installed scheduler/recovery launchers passed |
| Go | Full agent, cluster-helper and tools tests and vet passed |
| Fresh cluster smoke | Batch completion, placement on both agents, rolling update and termination passed |
| Recovery | Nine checks across three rounds of scheduler restart/crash and agent daemon crash; service task identities and workload PIDs retained |
| Policy | Automatic rollback, active drain/replacement and manual cron completion passed |
| Live request validation | 24 RPCs covering rejected requests and subsequent successful mutations; health remained OK and scheduler process identity was unchanged |
| Sustained churn | 260 distinct FINISHED task IDs, exactly 130 per agent, over 752.72 seconds; all 130 service identity/PID samples passed and daemon identities stayed unchanged |
| Backup and restore | Three snapshots retained, 29 successful scheduled backups since the last restart and zero backup/retention errors; offline restore passed integrity checks and preserved the complete source bytes |

The new lab is `.pi-lab/hardening01`, with a scheduler at `172.20.0.4:8081`
and agents at `172.20.0.2` and `172.20.0.3`, reachable from the Pi host. It uses
the verified original scheduler distribution and fresh Go binaries, copied into
its own artifact tree. Existing labs and workloads were preserved.

The [evidence receipt](hardening01-evidence.json),
[1,272 input hashes](hardening01-inputs.json) and
[Java suite ledger](hardening01-tests.jsonl) bind these results to this source and
the staged artifacts. Final checks found no source drift. The maintained
`inplace-check --phase churn` runner checks service identities, hosts and physical
PIDs after each batch, plus scheduler health and daemon identities before and
after the phase. New labs schedule backups every 30 seconds with retention of
three; the original lab ownership safeguards apply.

The selected automatic backup restored through the packaged `recovery-tool`
contained 277 tasks, 289 outbox commands and 3,348 observation receipts. Source
and destination SHA256 matched, all table counts matched, and both passed SQLite
integrity checks. This was an offline publication/content drill: the restored
copy was never started as a scheduler or connected to the live agents.

An initial full-suite run exposed shared global counter registration between
the new backup test and legacy backup tests. The final service uses injected
per-instance counters; its regression and all subsequent local/CI gates passed.
The failed diagnostic is identified under `diagnosedFailures` in the evidence receipt.

## Remaining work

Physical history compaction, application readiness/health semantics, Go-backed
UI log access, workload isolation and larger-scale qualification remain separate
work. Production HA follows those foundations. The supported deployment remains
a trusted-process, single-scheduler cluster; this slice does not restore full
Mesos or Thermos feature parity.
