# Comparative review: modernized and pre-modern Aurora

The next three hardening items—journal compaction, application health and task
logs—are tracked in the [follow-up status](HARDENING02_IMPLEMENTATION_STATUS.md).
Earlier qualification results below remain tied to their recorded revisions.

Reviewed on 2026-09-12. Baseline: upstream master
`11ebaeeb071cb182c388a40755e84f60dda32260`. Modernized branch:
`codex/in-place-java25`, `0ade4c3fdec123aaa3a1ac687dfcf70ffef4c98f`.
The implementation under the final documentation commit is `b5405842b`.

Subsequent fixes and their qualification are tracked in
[continuous-operation hardening](HARDENING01_IMPLEMENTATION_STATUS.md). This
review and its metrics retain the revision above as their comparison point.

**Assessment.** This branch is a better foundation for developing a small,
self-contained Aurora, with a modern Java toolchain and a substantially simpler
execution deployment. It preserves the original scheduler's policy machinery.
It is materially less capable and less operationally mature than pre-modern
Aurora with correctly configured Mesos/Thermos. It is a working trusted-process
MVP, not yet a feature-equivalent or production-ready replacement.

The review found three reproducible correctness defects, an automatic-backup
regression, and broken sandbox links. These need attention before treating the
remaining work as mostly Java idiom cleanup. Production HA can remain last;
correctness, retention and backups are necessary even for a single scheduler.

**Method and confidence.** Two Astra medium reviewers independently inspected
execution and scheduler/storage; a Luna reviewer examined build, Java, client
and UI changes. The parent reviewed their evidence and reran both isolated
reproduction programs. Comparisons used actual current source, `git show` of
the baseline, source inventories, application wiring, tests and qualification
receipts. This was a focused architectural and correctness review, not a fresh
line-by-line security audit of every source file or an exhaustive compatibility
certification. No production source or live cluster state was changed.

All 1,265 inputs in the INPLACE-08 qualification manifest still match their
recorded hashes. The existing lab's three containers are running, `/health`
returns `OK`, and the two `fixtures/test/push-mvp-demo` instances report RUNNING
on distinct agents. These are fresh read-only checks; PID survival and workload
qualification figures below come from the earlier named receipts.

**Findings requiring fixes.** Priorities below express implementation urgency,
not security vulnerability ratings.

| Finding | Priority | Evidence and practical effect |
| --- | --- | --- |
| Expected quota validation failure latches scheduler write failure | P1 | An invalid quota can return `INVALID_REQUEST` while disabling further writes and requesting execution-driver abort. Reproduced with the actual storage and quota classes. |
| Lifetime agent history can disable reconciliation | P1 | The 129th retained attempt is admitted, but state/watch then return 503. Reproduced with no reservations or unacknowledged observations. Restart preserves the history. |
| Global outbox window hides healthy-agent commands | P2; release blocker for scaling | The first 1,024 pending commands are selected globally before filtering by agent. An unavailable-node backlog can indefinitely hide later Run/Stop commands for healthy nodes. Actual query and predicate reproduced. |
| Automatic backup schedule and retention no longer wired | P1 for unattended operation | Go storage provides on-demand snapshots only, ignores the retained backup settings, writes beside the database and never prunes. Confirmed by wiring and call-site inspection. |
| UI sandbox links still target removed Thermos observer | P2 | Three components link to host port 1338, but the maintained Go deployment has no observer there. Source-confirmed workflow regression. |

**Quota rejection must remain recoverable.**
[`SchedulerThriftInterface.setQuota`](../../src/main/java/org/apache/aurora/scheduler/thrift/SchedulerThriftInterface.java#L545)
catches `QuotaException` outside `storage.write`. Negative quota and quota below
current consumption are normal validation rejections raised before saving quota.
[`SqliteStorage.write`](../../src/main/java/org/apache/aurora/scheduler/storage/sqlite/SqliteStorage.java#L162)
notifies its failure handler for every escaping exception;
[`GoAgentModule`](../../src/main/java/org/apache/aurora/scheduler/execution/go/GoAgentModule.java#L78)
enables the failure latch and binds that handler to asynchronous execution abort.

A disposable probe using `SqliteStorage`, `QuotaManagerImpl` and the production
failure-handler setting produced:

```text
quota_validation=QuotaException
failure_callback_count=1
subsequent_benign_write=A previous write failed; scheduler restart required
```

Fail-stop protects against uncertain commits or corrupted in-memory policy
state. It must distinguish those cases from ordinary validated rejection, while
preserving transaction isolation. Add tests through production-style API/storage
wiring that submit invalid input and then successfully perform valid operations.
Do not solve this by disabling fail-stop for every exception.

**History bounds are an availability cliff, not retention.**
[`Store.Ack`](../../agent/store.go#L486) removes observations while permanently
retaining command deduplication and attempt tombstones.
[`state`](../../agent/transport.go#L208) and
[`watch`](../../agent/watch.go#L178) refuse more than 128 attempts or 1,024 commands.
Admission does not prevent crossing those limits.

A probe invoked actual public Store and HTTP handler APIs in a temporary
directory. It admitted distinct Stop tombstones and acknowledged every
observation. An in-memory HTTP recorder supplied verified-peer metadata; this
tested handler behavior, not a TLS handshake or live workload execution:

```text
admitted=128 attempts=128 commands=128 observations=0 reserved=0
/v1/state?afterCursor=128 => 200
admitted=129 attempts=129 commands=129 observations=0 reserved=0
/v1/state?afterCursor=129 => 503 {"error":"inventory profile limit"}
/v1/watch?afterCursor=129 => 503 {"error":"inventory profile limit"}
```

Completed execution also leaves retained attempts. The demonstrated tombstone
case establishes that concurrent workload count is not the relevant bound.
Implement safe history compaction/pagination and admission backpressure while
preserving replay protection. Simply deleting old deduplication identities can
allow stale commands to execute again. Scheduler receipts, acknowledged outbox
entries and transaction-outcome records also need retention rules.

**Outbox selection needs per-agent fairness.**
[`SqliteEffects.pending`](../../src/main/java/org/apache/aurora/scheduler/storage/sqlite/SqliteEffects.java#L136)
applies SQL `ORDER BY sequence LIMIT ?` globally.
[`GoAgentDriver.selectCommand`](../../src/main/java/org/apache/aurora/scheduler/execution/go/GoAgentDriver.java#L608)
and `hasPending` then filter the result by agent. The probe inserted 1,024 older
commands assigned to an unavailable-node backlog and one later command for a
healthy node. The latter was durable but absent from the driver's selection
window. This is a query-level reproduction, not a 1,025-task cluster test.

Filter by agent before applying the limit, add an appropriate index, and test
that healthy-agent Run and Stop commands progress behind an unavailable-agent
backlog. Multiple virtual threads cannot repair a shared query that hides work.

**Backup creation survived; the operational policy did not.**
Old SchedulerMain installed SnapshotModule and BackupModule, which drove
periodic snapshots, interval-based backup creation and retention. Current
[`GoStorageBackup`](../../src/main/java/org/apache/aurora/scheduler/execution/go/GoStorageBackup.java#L39)
has only on-demand entry points and hardcodes `<database-parent>/backups`.
The old `-backup_dir`, `-backup_interval`, and `-max_saved_backups` options remain
exposed in CliOptions but are not used by this binding. A valid backup primitive
does not establish automatic backup coverage. Restore configurable scheduling,
retention, failure/age metrics and a tested copy off the scheduler's disk.

**The preserved UI still contains obsolete execution links.**
[`TaskDetails`](../../ui/src/main/js/components/TaskDetails.js#L13),
`TaskListItemActions` and `InstanceHistoryItem` point to
`http://<slaveHost>:1338/task/<taskId>`. The observer was removed. Provide a Go
log/sandbox access path and update those components, or explicitly present the
capability as unavailable. Passing component snapshots does not qualify these
links against the current deployment.

**Architecture and product comparison.**

| Area | Pre-modern Aurora | Modernized branch | Assessment |
| --- | --- | --- | --- |
| Scheduler | Original Java policy, state manager, updater, cron, quotas, maintenance | Same application and entry point; execution/storage adapters changed | Strong continuity; the parallel scheduler prototype remains abandoned. |
| Cluster resource supply | Mesos allocator and resource offers, shared cluster infrastructure | Operator-enrolled agents with configured capacity; adapter subtracts reservations and constructs HostOffer | Simpler for a dedicated Aurora cluster; no replacement for Mesos multi-framework allocation. |
| Agent updates | Mesos event/status infrastructure | Persistent scheduler-opened mTLS watch, immediate durable changes, 30-second heartbeats, roughly five-minute snapshots | Good event-driven design; reconnect and reconciliation remain explicit. |
| Execution | Thermos processes and lifecycle, Mesos containerizers | One explicit process command per supported scheduler task, supervised by Go | Large reduction in executable behavior. |
| Placement policy | Resources, quotas, constraints, preemption, update reservations | Original policy largely retained | Code continuity exceeds current exposed capabilities: fresh enrollment supplies only the synthetic host attribute, not a configurable rack/zone/dedicated topology. |
| Resources | CPU/memory/disk isolation when Mesos is properly configured; ports/GPU/revocable features | CPU/memory reservations; scheduler disk accounting; ports/GPU/revocable CPU rejected | Accounting is not enforcement. |
| Storage | Mesos replicated log, original volatile/indexed stores, backups | SQLite stores plus transactional outbox and receipts, local ownership | Simpler and carefully transactional; replication removed. |
| Recovery | Replicated scheduler recovery and legacy executor recovery | Same-state scheduler/daemon restart and supported supervisor recovery | Demonstrated restart recovery is useful; host, namespace and disk loss remain unqualified. |
| HA | Multi-scheduler deployment with replicated persistence and election | Single local storage owner; production HA deferred | Clear current capability loss. ZooKeeper discovery/election code still exists but does not replicate SQLite. |
| Configuration/client | Python/Pystachio executable configuration and Python CLI/plugins | Strict JSON documents, standalone Go CLI using original Thrift JSON API | Much simpler deployment; configuration and authentication workflows require migration. |
| UI | Original React frontend and Thermos links | Same frontend, restored build, obsolete observer links remain | Buildable; product/dependency modernization largely pending. |
| Developer platform | Legacy Gradle/Pants/Python and native Mesos prerequisites | Pinned Java 25/Gradle/Go/Node/Thrift workflow, ARM64 CI | Major reproducibility gain for this Pi; Linux ARM64 is the supported bootstrap platform. |

The old resource-isolation comparison assumes the recommended Mesos isolators,
not every possible upstream installation. See the upstream
[isolation contract](https://aurora.apache.org/documentation/latest/features/resource-isolation/)
and [operator configuration](https://aurora.apache.org/documentation/latest/operations/configuration/).
Upstream [backup recovery](https://aurora.apache.org/documentation/latest/operations/backup-restore/)
also documents its replicated-log recovery model.

**What is genuinely stronger in the implementation.** Assignment and command
intent commit together; delivery occurs after commit and outside SQLite's writer
lock. Observation deduplication and state transitions commit before ACK. Stable
command bodies, Stop tombstones, sequence checks and reservation retention
address real retry/crash hazards. Go watches wake only after durable mutations
and avoid a snapshot/register lost-wakeup window.

SQLite uses WAL and FULL synchronization, local ownership locking, nested
rollback propagation, independent committed read snapshots and explicit handling
of uncertain commit outcomes. All seven original stores have implementations.
Complete backups retain execution receipts, and strict historical import refuses
unsafe active-task adoption. These are substantial engineering strengths; they
do not establish higher distributed reliability than the old replicated stack.

Go supervisors persist process identity and launch intent, gate workload release,
record outcomes, and support daemon recovery. PIDFD/start-time checks reduce
PID-reuse signaling hazards. Transient supervisor IPC failures retry per attempt;
unknown execution retains capacity. Agent mTLS, bounded requests and watch frames,
explicit identity/session checks and checksum-pinned tools improve the new
boundaries. Removing Python execution and Mesos JNI removes dependencies and
failure modes, while also transferring their former responsibilities into this
repository.

**Execution compatibility and service correctness.** The supported scheduler
path rejects Thermos graphs/finalizers, images, volumes, artifact fetching, named
ports, GPUs, revocable CPU and partition-rescheduling profiles. Standalone
`agent/task` code implements more process semantics, but GoTaskFactory does not
submit through it; its presence is not scheduler feature parity.

Most significantly for services, GoTaskFactory always submits
`readiness.kind=none`. The agent marks the process ready when launched. A rolling
update can therefore succeed while the application is alive but unable to serve
traffic. The original updater algorithm remains useful, but its RUNNING input
has a weaker health meaning. Application readiness and continued health need
their own supported contract and failing-health rollback tests.

Workloads share the agent's OS identity/environment. The job user is metadata;
there is no per-task UID switch or cgroup/container isolation. Docker protects
the lab containers as units, not workloads from other workloads inside an agent.
The new profile is appropriate for trusted processes; it does not provide the
old configured multitenant isolation. Logs are capped with dropped-byte counters,
not a replacement for rotating logs, collection and the observer experience.

The Go CLI removes the Python runtime and offers useful job/update/cron/quota/
maintenance commands with strict validation and single-attempt mutations.
However, it lacks the old Kerberos/SPNEGO client plugin and ZooKeeper discovery,
requires an explicit leader/gateway endpoint, and emits raw Thrift structures for
many operations. Existing API security code remains; the private lab disables
HTTP authentication. Agent mTLS is not evidence of end-user API authentication.

Migration requires resolved JSON jobs and a drained historical snapshot with
terminal tasks and completed updates. Arbitrary Python configuration programs
are not automatically converted, and running Thermos workloads cannot be
adopted. Schema compatibility preserves history; it does not make deployment a
drop-in rolling upgrade.

**Availability and scale.** Startup withholds driver registration until every
enrolled journal reconciles, using 90-second waits on initial futures. A single
offline configured node can prevent scheduler startup. This is deliberate
conservative behavior, but needs an eventual design for bringing healthy capacity
online while retaining uncertainty and reservations for unavailable nodes.

Push transport reduces idle network exchanges; it does not remove local polling
or establish scalable scheduling. Agent runtime ticks and supervisor IPC remain
periodic. Agent mutations serialize the entire retained journal snapshot.
SQLite task queries except explicit task IDs deserialize all task records and
filter in Java; the old in-memory store had job/host secondary indexes. Agent
refreshes also scan retained tasks, with state changes waking all agent workers.
This can produce roughly agents-times-retained-tasks work in refresh waves.
Backups hold the writer lock while copying and checking the database.

These are source-based scaling concerns, not measured throughput regressions.
There is no equivalent-workload before/after benchmark supporting a claim that
the new scheduler is faster, uses less RAM, or scales further. Fewer deployed
components are a real operational simplification; their resource savings have
not been quantified against an old Mesos cluster.

**How much Java modernization actually happened.** The toolchain targets Java
25 directly, using Gradle 9.7.1 and updated analysis/coverage tools. Source changes
include owned-resource lifetimes, explicit constructor reflection, interruption
handling, immutable date formatting, `Subject.callAs`, private records, pattern
bindings, selected collection/Optional APIs and virtual threads for agent I/O.
The Java audit implementation records 72 completed local findings, one partial
and two retained, with broader contract/framework work deferred. Those are
historical audit dispositions, not a claim every current Java file was rewritten.

Application frameworks remain substantially older: Jackson 2.5.1, Jetty
9.3.11.v20160721, Shiro 1.4.0, ZooKeeper 3.4.8, Curator 2.12.0, Thrift 0.10.0 and
JUnit 4.12 are still declared. Guice is 6.0.0 and Guava 31.0.1-jre. The UI remains
React 16, Bootstrap 3, Babel 6, Jest 21 and Webpack 4. Java 25 compatibility is
real; broad application/dependency modernization remains unfinished. Dependency
versions alone are not a vulnerability assessment; no fresh SCA/CVE audit was
performed in this review. Security-sensitive HTTP/auth/serialization dependencies
deserve a coordinated qualification before broad deployment.

**Code size, measured against upstream rather than the previous slice.**
The [machine-readable counts](comparative-review-2026-09-12-metrics.json) count
physical lines, including comments and tests; language rows exclude `3rdparty`.
They are not executable-statement counts or feature/value measurements.

| Inventory | Pre-modern | Modernized | Change |
| --- | ---: | ---: | ---: |
| Non-vendored Java files | 634 | 659 | +25 |
| Non-vendored Java lines | 99,202 | 100,985 | +1,783 |
| Non-vendored Python files | 259 | 0 | -259 |
| Non-vendored Python lines | 43,506 | 0 | -43,506 |
| Go files | 0 | 62 | +62 |
| Go lines | 0 | 18,613 | +18,613 |
| Java + Python + Go lines | 142,708 | 119,598 | -23,110 (16.2% smaller) |
| All tracked text files | 1,477 | 1,324 | -153 |
| All tracked text lines | 227,761 | 237,092 | +9,331 (4.1% larger) |

Including vendored files, the upstream tree has 260 Python files, versus zero
now. The prior slice's 275-file retirement count used a different intermediate
baseline containing newly added modernization helpers. It is not the upstream
comparison. Documentation/evidence growth and a 15,475-line npm lockfile explain
why the repository's text total can grow while these implementation languages
shrink. Much of the reduction retires features and their tests, so it cannot be
attributed solely to cleaner code. The Java codebase itself is slightly larger.

**Tests and proof.** Existing qualification at the implementation commit records
1,342 scheduler and 124 commons Java tests, zero failures/errors/skips; 91.39%
instruction and 80.77% branch coverage; 144 UI tests; Java quality/distribution
gates; and uncached Go agent/tools tests and vet. The three named
[CI jobs](https://github.com/jordanly/aurora/actions/runs/34702901597) passed for
`b5405842b`; HEAD adds documentation only. Local artifact build.properties names
the earlier build commit and dirty state; source/artifact hashes establish its
inputs. This review revalidated the 1,265 source hashes, not the complete CI run.

The two-agent lab receipts include smoke execution and updates, three rounds of
scheduler restart/crash and agent crash with preserved identities/PIDs, automatic
rollback, occupied-host drain/replacement, cron execution, a 600-second mixed
workload with 40 batch completions, and 61 standalone-client operations. A
360-second quiet measurement observed 24 heartbeats and two full snapshots with
zero inventory polls or extra commands/ACKs/deltas/reconnects.

The source tests meaningfully exercise uncertain commits, snapshot isolation,
after-commit publication, replay/cancellation, PID identity, supervisor recovery
and watch ordering. This is credible MVP evidence. However, passing retained
Java tests does not establish removed Thermos behavior. The finite soak never
crossed the retained-history failure boundary; mock/component tests miss current
production-wiring interactions such as quota fail-stop and broken observer links.

This review reran the isolated quota/outbox and agent-history probes. It did not
rerun Gradle or the full cluster acceptance sequence. An attempted fresh full
Go run in a restricted subagent sandbox could not finish because test socket
creation was denied; that is an environment limitation, not a newly established
product failure. The previously documented Pi race-detector limitation remains.
No long-duration churn, multi-host power loss, storage loss, production auth,
multi-scheduler fencing or realistic performance comparison is established.

**Recommended implementation order.**

1. Fix recoverable validation handling, inventory/admission limits, and per-agent
   outbox selection. Add regressions through actual production wiring, including
   invalid-then-valid API calls and a mixed reachable/unreachable backlog.
2. Establish indefinite single-owner operation: safe replay-aware compaction,
   automatic backups/retention, restore drills, alerts and sustained churn beyond
   the old history boundary. Test bounded growth without manually resetting
   journals. Clarify partial-agent startup and operator recovery.
3. Complete the chosen service profile: readiness/health, logs and UI links,
   topology attributes, and the resource/isolation contract needed for intended
   workloads. Add ports/artifacts/images only as explicit supported capabilities.
4. Modernize HTTP/auth/serialization/DI dependencies and then broader Java value,
   time, configuration and test-framework boundaries. Preserve API/wire contracts
   and measure concurrency changes. Continue incremental UI/client improvements
   and expand build support beyond ARM64 if that is a deployment requirement.
5. Design and qualify HA last, including shared durable state/replication, fencing,
   partitions, stale owners, node/storage loss and restore cutover. Existing local
   ownership epochs are useful ingredients, not the complete HA design.

The appropriate next milestone is a reliable, continuously operating
single-scheduler cluster. That would turn the current qualified demonstration
into a stronger platform for the remaining Java and product modernization.
