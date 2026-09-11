**SUPERVISE-01, THERMOS-01 and POLICY-01**

Implementation is complete and qualification is in progress. These are three
bounded capabilities on the Java 25 native scheduler and Go agent. The historical
MVP remains on its original binaries; new qualification uses disposable, private
scheduler/two-agent Docker labs. Java 25 remains the default; the Java 26 runtime
and compiled profiles remain supported build targets.

**Surviving attempt supervisors**

`aurora-agent serve --supervise` opts into private per-attempt instances of the
same executable. A supervisor owns the actual workload wait and bounded logs
while the agent daemon can stop and reattach. An inherited gate prevents workload
execution before supervisor identity is durable. Local control version 1 binds
the peer UID/PID, process start, random token, actual boot/namespaces and immutable
Run. Unsupported local versions refuse attachment.

An ordered local execution journal is imported with its cursor and node outbox in
the same transaction. Terminal acknowledgements are replayable after a daemon
crash. Known exit codes/signals survive log-pipe errors; cleanup uncertainty is
separate from a known exit. Supervisor loss uses pinned identities and actual
cleanup proof; missing/corrupt evidence retains the reservation. It never adopts
an arbitrary PID or relaunches consumed work. Stop persists wall and same-boot
monotonic deadlines; redelivery cannot extend them.

Opt-in upgrades agent journals to format 3. Formats 1/2 can enter this mode; the
default legacy runtime refuses format 3. Actual boot or namespace changes still
require external fencing/re-enrollment. Whole-container loss is a separate event
from daemon loss. Trusted process-group containment and reservation accounting
remain the operating model; this slice adds no per-task cgroup enforcement.

**Native task execution and conversion**

An ordinary immutable Run can execute the same agent binary using
`execute-task --manifest-json JSON`. Its default `task-state` directory is under
the attempt's private working directory. The outer assignment owns aggregate
resources and ports. The task runner owns named child executions and records
their process identities, exact exits, retry counts, bounded logs and independent
primary/finalization results. There is no new remote process-level API.

The [task contract](../../agent/task/README.md) specifies success dependencies,
deterministic ordering, concurrency, finite total runs, failed-run budgets,
daemon/ephemeral behavior and finalizers. `native-v1` requires every required
process to succeed. The selected `thermos-v1` planner retains explicitly tested
legacy cases, including immediate positive task-failure thresholds, with documented
changes for finite aggregate runs, successful-only ephemeral dependencies,
immediate primary cleanup and uncertain execution. JSON field names are exact:
case aliases cannot override explicit limits.
Finalizers share the remaining cleanup budget and remain best effort under the
outer Stop deadline. A used, missing or corrupt task execution journal is never a
source of authority to relaunch an old attempt.

`convert-task --document RESOLVED.json --max-runs N` accepts a trusted offline
`thermos-resolved-v1` export and emits a manifest plus conversion findings. It
does not evaluate `.aurora`, Python or Pystachio. All export defaults must be
explicit; unmapped fields are rejected. Source digests and trust assertions are
provenance supplied by a trusted exporter, not proof of trust. Shell commands
remain explicit `/bin/bash -c` argv and need that executable in the chosen image.

The task manifest is limited to 4096 canonical bytes; the enclosing Job also
retains its 4096-byte limit, including JSON escaping and the surrounding template.
This first composition therefore supports small graphs. Larger manifests and a
typed scheduler task API need a separately reviewed wire change.

**Scheduler policy**

`--enable-policy --policy-config /absolute/settings.json` enables the selected
policy profile as a whole. Both flags are required. The settings contain only
static enrolled-node attributes, for example:

```json
{"attributes":{"agent-a":{"rack":["a"]},"agent-b":{"rack":["b"]}}}
```

All mutations use the existing authenticated operator API, an operation ID and
appropriate revision guards. Repeating an identical operation returns its
durable state; changing its body conflicts. The existing allocator remains the
single owner of Run placement and resource accounting.

| Capability | Selected behavior |
| --- | --- |
| Constraints | Static attribute set intersection or negation; a missing positive attribute fails and a missing negated attribute passes; existing `maxPerAgent` remains. |
| Quotas | Role CPU/memory desired-demand admission, old/target update envelopes and a reservation floor until cancelled work is cleaned. New submissions and updates require a role quota. |
| Updates | Fixed-size services, one instance at a time, stop first; per-instance templates and integer `minReady` persist across restart. A committed Stop is excluded from readiness. |
| Rollback | Explicit operator request restores the previous template using a new monotonic revision, including an interrupted mixed-template update. Stopped jobs cannot revive. |
| Drain | Persistent scheduler cordon and service evacuation, destination capacity held until cleanup and replacement readiness. Batch evacuation is rejected/blocked; no force timeout or agent-local admission closure is claimed. |
| Preemption | Explicit candidate revision and exact single victim, same role, lower priority, preemptible service; full fit and availability checks, a durable node hold, and no candidate Run before victim cleanup. |

Routes are `POST /v1/quotas`, `/v1/policy/jobs`, `/v1/jobs/update`,
`/v1/jobs/rollback`, `/v1/nodes/drain`, `/v1/nodes/cordon` and `/v1/preempt`.
`GET /v1/state` includes durable operations and placement reasons. Exact payloads
are exercised by the [physical policy check](../../build-support/native/native-policy-check)
and [controller tests](../../scheduler/native/src/test/java/org/apache/aurora/nativescheduler/NativePolicyTest.java).
The original `/v1/jobs` route uses default policy when the profile is enabled.

Policy mode atomically upgrades SQL schema 2 to schema 3, with an exact catalog
check. Default/older readers refuse schema 3. Enabling this mode is a persisted
format change; disabling the CLI option is not a downgrade path. Schema 3
snapshots reopen only with policy enabled and identical settings; historical
default restore tools deliberately refuse them. Existing schema 2 desired jobs
are grandfathered with default policy and count toward later quota changes.
Operation history is bounded to 64, alongside the existing job/attempt/command
limits; no pruning or automatic multi-victim preemption is added here.

**Qualification**

The [execution check](../../build-support/native/native-execution-check) exercises
daemon-only crashes with live HTTP services, offline exits/logs, replayed Stops
and composed DAG/retry/finalizer execution. The policy check restarts the scheduler
mid-update, drain and preemption and checks real readiness and cleanup. Both use
fresh owned image-backed labs and clean only their recorded resources.

The original 23-case, three-round, ten-minute qualification continues to exercise
the default runtime. Focused Go subprocess and Java store/controller tests cover
failure boundaries that the physical checks cannot schedule deterministically.
ThreadSanitizer cannot execute on this Pi's 47-bit VMA layout; race results are
unavailable, not passing. Ordinary unit tests and physical tests remain required.

The existing gate's two fixtures that require an observed TERM now allow a
three-second grace period. A superseded run with the former 300 ms fixture
deadline recorded SIGKILL without a TERM receipt and failed that assertion.
The fixture change gives durable admission and the signal handler time to run on
the Pi. Runtime deadline behavior is unchanged; forced-stop checks still require
actual SIGKILL, cleanup and exact port reuse.
