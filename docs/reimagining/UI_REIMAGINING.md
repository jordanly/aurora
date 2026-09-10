# Aurora operator console: UI reimagining

Design proposal • 2026-09-09 (America/New_York) • source baseline `e3350f63d446cca17e8f1763ce30cc94346e64cf` on `codex/aurora-context`

## Recommendation

Build an operator console organized around **Overview, Workloads, Deployments, Agents, Resources, and Events**. Make workload → instance → execution attempt → process run the primary debugging path. Put evidence, its age, and the next useful action together. Retain Aurora's valuable instance history, configuration comparison, pending explanations and rollout semantics while replacing the split navigation and browser-to-worker links.

Deliver a separate, read-only console first, using a small typed adapter over the existing scheduler API. Add standalone Go agent telemetry and a same-origin log gateway next. Enable mutations only when the backend can authorize, revalidate and track them reliably. This proposal defines a product/API direction; it does not select the replacement scheduler's persistence or agent transport implementation.

**Evidence boundary:** This is source research, not a running-cluster evaluation or usability study. The existing source is Java/Mesos plus Python/Thermos; a repository search found no Go source or `go.mod`. Standalone Go agents, their enrollment protocol and their runtime semantics are proposed, not existing capabilities. Example names, counts, times and revision IDs in sketches are fictional. Proposed UI labels must never imply new operational guarantees on the legacy backend.

Research ran on remote host `raspberrypi`, architecture `aarch64`, in `/home/jordanly/.codex/worktrees/cbd4/aurora`. HEAD matched the requested commit before research; `AURORA_CONTEXT.md` was read first. No `AGENTS.md` was found in checkout ancestors or the repository. Work is isolated on `codex/ui-reimagining`; only this report is an intended change.

## 1. What the source actually provides

Links below are relative to this report and resolve into the reviewed checkout. Symbols identify the relevant implementation without depending on generated artifacts.

| Area | Observed behavior and evidence | Design consequence |
| --- | --- | --- |
| Routes and navigation | [index.js](../../ui/src/main/js/index.js) registers `/scheduler`, role, environment, job, instance, `task/:taskId`, `update/:uid`, and `/updates`. [Navigation](../../ui/src/main/js/components/Navigation.js) offers the brand/home and updates. [JettyServerModule](../../src/main/java/org/apache/aurora/scheduler/http/JettyServerModule.java) rewrites scheduler/update paths to the React shell. | Preserve old deep links. Add operator navigation without interpreting a numeric instance as an execution ID. |
| UI transport | [scheduler-client.js](../../ui/src/main/js/client/scheduler-client.js) creates a generated `ReadOnlySchedulerClient` at `/api`; a localStorage override can change its host. [ApiBeta](../../src/main/java/org/apache/aurora/scheduler/http/api/ApiBeta.java) exposes POST `/apibeta/{method}` through the annotated admin interface. | Existing UI is read-only. Ordinary JSON RPC is available, but a versioned resource API, cluster registry and capability endpoint would be additions. Do not use arbitrary browser host overrides as cluster management. |
| Discovery and workload views | [Home](../../ui/src/main/js/pages/Home.js) loads role summaries. [Jobs](../../ui/src/main/js/pages/Jobs.js) loads role job summaries and quota, filtering environment locally. [Job](../../ui/src/main/js/pages/Job.js) loads tasks, pending reasons, config groups, cron summary and update details separately. | Retain ownership scope but make role/environment filters rather than mandatory navigation steps. Separate independent panel errors and label mixed-time responses. |
| Refresh and result limits | Most inspected pages fetch on mount. [Update](../../ui/src/main/js/pages/Update.js) schedules a 60-second refresh for active updates. [Updates](../../ui/src/main/js/pages/Updates.js) requests at most 100 summaries per status group without a continuation UI. | Add explicit freshness, cancelable refresh and paging. Never label a limited result as the complete cluster total. |
| Attempts and debugging | [Instance](../../ui/src/main/js/pages/Instance.js) retrieves attempts by instance and RUNNING neighbors on the active task's host. [Task](../../ui/src/main/js/pages/Task.js) distinguishes a missing/pruned task. [TaskDetails](../../ui/src/main/js/components/TaskDetails.js) links `/structdump/task/:id` and `http://host:1338/task/:id`. [TaskListItemActions](../../ui/src/main/js/components/TaskListItemActions.js) links Thermos tasks to observers. | Keep attempt history and neighbors; integrate their context. Missing history is not proof an execution never existed. Worker addresses must cease being the browser's log API. |
| Config and deployment presentation | [ConfigDiff](../../ui/src/main/js/components/ConfigDiff.js), [UpdateInstanceSummary](../../ui/src/main/js/components/UpdateInstanceSummary.js), and [InstanceViz](../../ui/src/main/js/components/InstanceViz.js) already compare config groups and visualize instance progress. InstanceViz uses colored cells with title text and shrinks them above 100/1,000 instances. | Reuse concepts, not necessarily components. Provide a text/table equivalent with explicit state labels and usable targets at large counts. |
| API objects and operations | [api.thrift](../../api/src/main/thrift/org/apache/aurora/gen/api.thrift), especially `TaskQuery`, `ScheduledTask`, `JobUpdateDetails`, and the three service interfaces, defines task/update histories, pagination offsets/limits, quotas, cron, update/restart/kill and maintenance operations. | Avoid rebuilding scheduler decisions in JavaScript. Existing RPC availability does not establish safe browser operation tracking or full authorization configuration. |
| Pending explanations | [ReadOnlySchedulerImpl.getPendingReason](../../src/main/java/org/apache/aurora/scheduler/thrift/ReadOnlySchedulerImpl.java) selects PENDING tasks and joins nearest-fit veto strings. It rejects nonempty host or status restrictions. `getTasksWithoutConfigs` removes executor config data, not all task configuration. | Display the supplied explanation as evidence, not a complete placement simulation. Clone/filter the request for this RPC; don't pass the page's status/host filters through blindly. Request full executor data only deliberately. |
| Infrastructure diagnostics | [Agents](../../src/main/java/org/apache/aurora/scheduler/http/Agents.java) serves HTML from stored host attributes: host, Mesos ID, maintenance mode and attributes. [LeaderHealth](../../src/main/java/org/apache/aurora/scheduler/http/LeaderHealth.java) returns leadership status, including 503 for a follower. [Offers](../../src/main/java/org/apache/aurora/scheduler/http/Offers.java) exposes retained offers. | Known host does not mean recently connected agent. Follower 503 does not establish cluster failure. Offers are transient scheduling inputs, not physical inventory. |
| Resources | [RoleQuota](../../ui/src/main/js/components/RoleQuota.js) shows CPU/RAM/disk quota categories. [Utilization](../../src/main/java/org/apache/aurora/scheduler/http/Utilization.java) uses [ResourceCounter](../../src/main/java/org/apache/aurora/scheduler/stats/ResourceCounter.java), whose consumption totals aggregate active task configurations. | Label these as configured requests/accounting. They are not sampled process CPU utilization or available physical capacity. Preserve tier/dedicated distinctions. |
| Process logs | [observer JSON routes](../../src/main/python/apache/thermos/observer/http/json.py) expose task/process views. [file_browser.py](../../src/main/python/apache/thermos/observer/http/file_browser.py) exposes log chunks by task/process/run/type plus offset/length and sandbox browsing. | Existing host-local process/run evidence is useful. Central search, durable retention and a Go agent log protocol are additions. |
| Security and events | [HttpSecurityModule](../../src/main/java/org/apache/aurora/scheduler/http/api/security/HttpSecurityModule.java) supports NONE/BASIC/NEGOTIATE, defaulting to NONE. [PubsubEventModule](../../src/main/java/org/apache/aurora/scheduler/events/PubsubEventModule.java) uses asynchronous event buses. [Context](../../AURORA_CONTEXT.md#rules-to-preserve-and-remaining-unknowns) records event-before-outer-commit and non-isolated-read constraints. | A new UI must not assume authentication or a durable replayable event stream. Preserve server authorization and design a committed read model before promising coherent live snapshots. |

Existing tests provide seams, not proof of a modern contract: [Job-test](../../ui/src/main/js/pages/__tests__/Job-test.js) checks API invocation and update visibility; [InstanceHistory-test](../../ui/src/main/js/components/__tests__/InstanceHistory-test.js) checks history ordering/empty inputs. [test-setup.js](../../ui/test-setup.js) manually defines Thrift globals and omits PARTITIONED from its task enum fixture even though the schema contains it. Contract fixtures should be generated or validated against the real schema. No tests were executed in this design pass.

## 2. Product model and information architecture

### Identities and language

Use **workload** as the umbrella label, with explicit Service, Batch or Cron kind. Preserve the job key `(role, environment, name)` and add cluster to every UI identity. Role retains its ownership/quota meaning; renaming it to team would imply an unverified organizational mapping.

An **instance** is a logical replica/shard. An **execution attempt** is the concrete scheduled task ID; a **process run** is a retry inside that attempt. A **deployment** initially maps to an Aurora job update, not every restart, cron firing or process retry. A future revision is a normalized immutable configuration identity, distinct from a deployment operation. Legacy `ancestorId` is useful where present, but update replacements need not populate it; never fabricate an ancestry edge from temporal proximity.

An **agent** needs a stable ID and separate boot/session ID. Its connection status, scheduling eligibility, maintenance intent, runtime health and workload readiness are separate fields. Reusing a hostname cannot transfer an old execution's identity to a new agent process.

### Navigation and proposed URLs

Mount the new console at `/console/c/:cluster`. Cluster comes from an authorized server registry (initially one configured cluster). Every detail URL includes cluster; filter state, selected tab, sort and fixed time range are shareable query parameters. Time travel is available only where retained evidence supports it.

| Navigation | Screens under the cluster prefix | Primary question |
| --- | --- | --- |
| Overview | `/overview` | What needs attention, and how current is the evidence? |
| Workloads | `/workloads`, `/workloads/:role/:env/:name`; children `/instances/:instance`, `/attempts/:attempt` | What should be running, what actually is, and why do they differ? |
| Deployments | `/deployments`, `/deployments/:role/:env/:name/:id` | What is changing, what is blocked, and what happens next? |
| Agents | `/agents`, `/agents/:agentId` | Which machines can accept work and which need intervention? |
| Resources | `/resources?groupBy=role` with pool/tier/agent filters | Is the bottleneck quota, placement or physical capacity? |
| Events | `/events` and a filtered Failures view | What changed across workload, deployment and agent boundaries? |

Logs live on attempt/process detail and can open a full-width scoped viewer. The Events page can pivot to logs for a specific attempt; global log search is deferred until indexed storage exists. Failures are a saved Events view plus an Overview queue, not a second incident system with a separate truth. Settings contains cluster identity, access and integration configuration. Legacy raw diagnostics remain under an Advanced link during migration.

Default landing is the last authorized cluster's Overview. Global search initially resolves exact job keys, task/update IDs and hostnames within that cluster; broad indexed search is a later capability. Search results say what entity will open. Back restores filters, scroll and selection. Links to inaccessible objects must not leak names or counts from other roles.

## 3. Operator workflows and screen sketches

All sketches describe the target experience. `[N]` marks a new backend capability; unmarked legacy fields still require an adapter. Use a prominent unavailable/unknown state when an `[N]` field has no provider. Sketch text and numbers are illustrative, not measurements.

### A. Triage cluster health

Overview prioritizes workload impact, then links to the evidence behind each issue. Keep control-plane availability separate from workload availability. Aggregate only data the viewer can access and state the population and sample time. A disconnected console preserves its last successful values with a stale banner; it must not turn them into zeros or a reassuring green status.

```text
Aurora   [cluster: east-prod v]  [Search workload / attempt / agent]   User
Overview | Workloads | Deployments | Agents | Resources | Events
-----------------------------------------------------------------------
Overview                         Observed 14:32:08 UTC  [Live v] [Pause]
Control plane [N]: Available      Agents [N]: 41 connected / 1 unknown
Services: 2 need attention        Deployments: 1 paused
Evidence: workloads 2s old | agents 3s old | metrics unavailable

Needs attention             Scope             Evidence          Open
! 3 instances pending       payments/api      memory fit veto   [Explain]
? Agent connection unknown  agent-42          last seen 90s [N] [Agent]
|| Rollout paused           search/index      batch complete    [Deploy]

Recent changes              Actor / source     Time             Open
Deployment u-17 paused      scheduler          14:31:12         [Event]
Instance 7 -> FAILED        task event         14:30:59         [Attempt]
-----------------------------------------------------------------------
Capacity by pool [N]        [Allocated requests] [Measured usage] [Table]
```

Selecting the pending issue preserves cluster, workload and instance filters. The explanation shows the raw legacy reason and its availability limits; the new structured version lists evaluation time, candidate pool, resources including overhead, named ports and constraint failures. Suggested remedies link to configuration/agent evidence. The UI does not claim that editing one constraint guarantees placement.

### B. Inspect and operate a workload

The workload header names owner, kind, desired count and observed state. During a rollout show old/new config groups; do not treat every non-target instance as unhealthy. For legacy services without a durable desired-spec object, label the provenance of the count (update instructions or active configurations) rather than inventing desired state after every task disappears.

```text
Workloads / payments / prod / api       Service    [Deploy...] [Actions v]
Desired: 12 [source: deployment u-17]    Running: 9   Pending: 3
Readiness [N]: unavailable              Config groups: 2
[Instances] [Configuration] [Deployments] [Events] [Resources]
-----------------------------------------------------------------------
Status [Pending v]   Revision [All v]   Host [All v]       [Clear filters]
Instance  Latest attempt  Task state  Ready [N]  Revision  Agent    Age
7         task-7b         PENDING     --         r-18      --       4m
8         task-8b         PENDING     --         r-18      --       4m
9         task-9b         PENDING     --         r-18      --       3m
Showing 3 matches                         [Previous] [Next]
-----------------------------------------------------------------------
Instance 7 selected                                       [Open detail]
Reason: insufficient RAM (scheduler nearest-fit evidence)
Request: 2 CPU / 8 GiB + executor overhead; dedicated pool constraint
[Explain placement] [Compare configuration] [View earlier attempt]
```

Selecting a row opens a contextual drawer with a full-page link; keyboard Enter follows the main detail link. The instance page displays active/uncertain attempts and retained history. Filters and pagination must not silently exclude an uncertain competing execution. Preserve `PARTITIONED`/unknown as uncertainty; a scheduler replacement does not prove the previous remote process stopped.

Service actions include deploy, scale and restart selected instances. Batch views instead emphasize completion, failure budgets and submitted executions; Cron adds template, schedule, collision policy and trigger history where available. Do not label `startCronJob` as replaying a known missed schedule slot. A durable run ID and scheduled-time ledger are new backend requirements. Config entry accepts a normalized validated manifest from tooling; it does not evaluate uploaded `.aurora` Python in the browser or gateway.

### C. Plan and observe a deployment

Flow: choose workload/revision → validate → inspect config and resource delta → choose instance subset and strategy → review impact → submit → observe operation and deployment. Before mutations exist, users can inspect existing updates and a clearly labeled nonbinding diff. Existing `getJobUpdateDiff` returns add/remove/update/unchanged groups and validates configuration; it is not an atomic admission reservation or a dry-run guarantee of placement.

```text
Deployment u-17 / payments/prod/api              PAUSED: batch complete
Current r-17 -> Desired r-18 [N revision IDs]     [Resume] [Roll back...]
Started by alex 14:24 UTC                        [Abort...] [Copy link]
[Progress] [Config diff] [Policy] [Events]
-----------------------------------------------------------------------
Stable on target: 6 / 12     Updating: 0    Waiting: 6    Failed: 0
Batch: 2 of 4 [N explicit controller field]      Next: instances 6-8
Gate: operator resume       Stability window: 30 seconds
Readiness checks [N]: unavailable     Pulse/SLA gate detail [N]: unknown
Instance   Target config   Task state   Deployment result   Evidence
0-5        r-18            RUNNING      Stable              [Expand]
6-11       r-17            RUNNING      Waiting             [Expand]
-----------------------------------------------------------------------
Review resume: advances next batch; up to 3 instances may be disrupted
Reason [Change ticket / operational note                           ]
                              [Cancel] [Resume deployment]
```

Show configured pulse interval, SLA policy and stability timer separately from observed gate decisions. Where legacy events do not explain a stall, say “gate detail unavailable”; do not infer SLA denial from elapsed time. Current state, controller intent and pending operator action appear above the timeline. Compare baseline → desired and actual config groups → desired independently.

Pause stops further progression according to controller semantics; it need not cancel in-flight task transitions. Abort ends the update without promising restoration of the previous configuration. Rollback of an active update uses its initial configuration mapping; redeploying an older completed revision is a new operation. Surface the exact supported action set for the current backend/state. Never keep a coordinated rollout alive with hidden browser pulses: its coordinator must survive closing the tab.

### D. Maintain a standalone agent

Agents list: stable ID, hostname, runtime/version/capabilities, connection status, heartbeat age, scheduling eligibility, maintenance state, active workload count and capacity pressure. Group by pool/rack/architecture only when advertised and validated. Missing version or architecture is unknown, not incompatible.

```text
Agents / agent-42    host: pi-worker-4    Go runtime [N]   session: b-9 [N]
Connection: UNKNOWN [N]  Last received heartbeat: 90s [N]
Scheduling: ineligible [N]   Maintenance: drain requested
[Overview] [Workloads] [Resources] [Events] [Diagnostics]
-----------------------------------------------------------------------
Drain operation [N] op-83: waiting for termination confirmation
Affected: 6 instances / 2 workloads     Replacement fit: not guaranteed
Blocker: payments/api availability policy [N authoritative decision]
Task-7a: last reported RUNNING; shutdown unconfirmed
Desired action: stop task-7a       Last agent report: 14:30:38 UTC [N]
[Inspect workload] [Inspect operation] [Download diagnostics...]
-----------------------------------------------------------------------
Capacity [N]: allocatable / assigned / reserved / measured (sample time)
```

Drain review lists affected workloads, replacement capacity estimate, policy gates and timeout/force behavior. Existing `slaDrainHosts` can force drain on timeout; the review must state this. For new agents, separate “stop assigning work” from “evacuate workloads”; this is a proposed contract, not a direct synonym for legacy `startMaintenance`. Ending maintenance does not resurrect previous attempts. A disconnected agent remains unconfirmed until reconciliation/fencing policy establishes the outcome. Force controls belong to a privileged advanced path with explicit scope and reason, never the default retry action.

### E. Follow a failure into logs and process runs

```text
payments/prod/api / instance 7 / attempt task-7a        FAILED
Scheduler event: health check failed   Agent: agent-42   [Copy evidence]
[Summary] [Processes] [Logs] [Events] [Files] [Configuration]
-----------------------------------------------------------------------
Process [server v]  Run [3 v]  Stream [stderr v]  Range [14:28-14:31 UTC]
Source: agent-42/session b-9 [N]       Available locally; retention [N]
[Find in loaded text] [Wrap] [Timestamps] [Follow: off] [Download range]
14:30:55 ... connection refused
14:30:56 ... readiness probe failed
---- source connection lost at 14:30:58; log completeness unknown ----
                         214 new lines buffered [Jump to latest]
-----------------------------------------------------------------------
Related evidence
14:30:59 Scheduler: FAILED, health check message
14:30:58 Agent observation [N]: process exit, code 1
14:30:57 Probe [N]: check failed, consecutive failure 3
[Instance history] [Deployment u-17] [Other tasks on agent]
```

A process retry changes run selection within the same attempt; task replacement opens a different attempt. “Follow latest attempt” is explicit and inserts a visible boundary when it switches. Scrolling or text selection pauses follow. Keep line buffers bounded, render text safely, and distinguish empty, rotated, deleted, not retained, denied, disconnected and unavailable logs. Legacy output may lack timestamps: show stream order/offset and never synthesize event times. Unified Events retains source timestamps plus received timestamps and flags clock skew; chronological proximity is not a proven causal relation.

The Failures view groups by evidence such as reason code, revision or agent, with drilldown to individual events. Initially it can group retained scheduler events and display coverage limits. Durable acknowledgment, ownership assignment and incident deduplication are deferred unless an incident integration supplies those semantics. Debug export is a scoped, redacted evidence bundle with cluster/IDs/time range/schema versions and missing-source notes; arbitrary remote shell is outside the first release.

### F. Explain resources

```text
Resources  [Pool: shared v] [Tier: all v] [Group: role v] [Time: now v]
View: [Requests/accounting] [Physical capacity N] [Measured usage N]
Role       CPU request/quota   RAM request/quota   Pending   Explain
payments   24 / 40 cores       96 / 128 GiB        3         [Fit]
search     32 / 60 cores      160 / 256 GiB        0         [Details]
Coverage: scheduler snapshot; measured usage unavailable
```

Never sum CPU and memory into a generic “percent full.” Keep requested, reserved, allocatable and measured series distinct, with units, denominators, sample age and missing coverage. Use MiB/GiB consistently with the legacy `ramMb`/`diskMb` conversion documented in context. Tier, revocable capacity, dedicated pools, overhead and ports can prevent placement even with quota headroom. A new placement explanation may show “3 candidate agents excluded by rack constraint” only if the backend actually evaluated and reports that scope. Capacity planning is an estimate, not a promise of admission.

## 4. Shared interaction and visual system

Use a calm, dense console: neutral canvas, clear panel boundaries, one restrained blue accent for navigation/actions, and semantic status accents accompanied by text and icons. Use a readable system sans-serif for controls and monospaced IDs/logs. Draft dimensions: 16px base text, 14px dense data, 40px standard rows, 32px compact rows, and an 8px spacing rhythm. Offer light/dark preferences after both are contrast-tested; do not encode priority through brightness alone. Reserve animation for user-directed transitions, honor reduced motion, and avoid pulsing failure badges.

Target WCAG 2.2 AA: text contrast of at least 4.5:1 (3:1 for qualifying large text), relevant non-text contrast of 3:1, visible unobscured focus, keyboard operation, and minimum 24×24 CSS-pixel targets subject to the standard's exceptions. Use native tables and buttons, labeled tabs, announced status messages and a pause mechanism for auto-updating content. Validate reflow at 320 CSS pixels; wide data tables may have their own scroll region. These requirements come from the [W3C WCAG 2.2 Recommendation](https://www.w3.org/TR/WCAG22/), especially 1.4.3, 1.4.10, 1.4.11, 2.1.1, 2.2.2, 2.4.7, 2.4.11, 2.5.8 and 4.1.3.

Product-specific behavior proposed here:

- Give instance grids an equivalent sortable table; status filters expose counts and labels. Do not create 10,000 tiny tab stops. Charts include a table/download alternative.
- Drawers restore focus to the invoking row; dialogs trap focus appropriately, support Cancel/Escape before submission, and announce validation next to fields. A narrow screen uses full-page details, with cluster and action scope always visible.
- Loading is panel-scoped. Empty results explain filters; denied data says access denied; stale data retains the last successful sample. Unknown enum values render as “Unknown state (raw value)” and disable dependent actions.
- Live lists preserve selection and row order while being examined; show a “new results” affordance instead of inserting rows under the pointer. Save UTC instants in URLs and allow local display with timezone visible.
- Begin with visibility-aware polling (proposed 10s for active detail, 30s for summaries, slower on errors). Deduplicate requests, abort on navigation, and back off with jitter. Actual intervals depend on measured backend cost. Show last successful observation, not just the browser's last refresh attempt.
- Mutations use review → submit → accepted/pending → completed or failed. Show exact cluster, object, instance selection, impact, actor and reason. All-page bulk selection is distinct from visible-page selection. Permissions and state are rechecked server-side at submission.
- A timeout after submission means “outcome unknown,” not “failed, retry.” Track an operation ID/idempotency key where supported. Legacy ambiguous calls need a read/reconciliation path and explicit operator resolution; an adapter cannot guarantee exactly-once behavior by retrying.

## 5. Required API and event capabilities

### Capability ledger

**E** = existing scheduler RPC or diagnostic; **A** = adapter/UI work using existing evidence; **N** = new authoritative backend capability. Endpoint names below are proposals, not endpoints found in source.

| Experience | Available now | Required delta |
| --- | --- | --- |
| Workload and attempts | **E:** role/job summaries; task queries with offset/limit; config groups; task histories and optional ancestry. | **A:** stable UI identities, normalization, scoped errors. **N:** durable desired spec/revisions, stable snapshot cursors, global filtered aggregates and retained attempt tombstones. |
| Deployment inspect/control | **E:** update summaries/details/diff, settings, pause/resume/abort/rollback/start RPCs. | **A:** map job updates to deployment UI. **N:** authoritative current batch/gates/blocked reasons, admission preview with revision precondition, durable operations/idempotency. |
| Placement | **E:** nearest-fit reason strings; retained offers diagnostic. | **A:** scoped explanation panel without host/status restrictions in pending RPC. **N:** typed reason codes, evaluated candidates, overhead/reservations and observation version. |
| Cluster health | **E:** leaderhealth and separate diagnostics. | **A:** correctly interpret follower/unknown leader results. **N:** authorized cluster capability/status summary, component conditions, read-model lag and workload impact aggregates. |
| Agents/maintenance | **E:** stored Mesos host attributes as HTML; maintenance and SLA drain RPCs. | **N:** typed inventory, stable agent/session identity, enrollment status, heartbeat lease, version/capabilities, desired/reported state, drain progress, reconciliation/fencing evidence. Legacy HTML remains a link rather than being scraped. |
| Logs/processes/files | **E:** Thermos host-local JSON/chunk/file routes. | **A/N:** authenticated gateway and adapter; **N:** standalone-agent process/run schema, resumable log cursors, explicit retention/rotation, bounded export, optional archived logs. |
| Resources | **E:** quota categories/tier configs and configured task resource accounting. | **A:** accurate labels/conversions. **N:** allocatable inventory, reservations/overhead accounting, sampled usage with timestamps/coverage, retained time series. |
| Events/failures | **E:** per-task and per-update event arrays, not a public global durable stream. | **A:** scoped merged history with source labels. **N:** retained queryable event store, committed sequence, stable correlation IDs, cursor replay and gap reporting. |
| Access and operations | **E:** annotated manager/admin authorization when configured. | **N:** user/capability discovery, object-specific allowed actions, complete operation audit and secure gateway sessions. UI hiding is never enforcement. |
| Cron runs | **E:** templates/schedule/collision policy, immediate trigger and task history. | **N:** durable schedule-slot/run ledger, skipped/missed trigger evidence and explicit retry semantics. |

### Proposed contract shape

A browser-facing `/console-api/v1` serves bounded JSON resources. A typed adapter maps legacy Thrift responses into this contract and advertises unsupported fields. A new scheduler can implement the same contract directly. The gateway is an aggregation/authentication boundary, not a second scheduling controller. Version it independently from agent wire protocols; use generated or schema-validated clients, explicit enums with unknown handling, and lossless encoding for 64-bit counters/cursors.

Minimum resources: `clusters/{id}/capabilities`, `overview`, `workloads`, workload instances/attempts/revisions, `deployments`, `agents`, `resources`, `events`, and `operations/{id}`. Each response carries cluster ID, schema version, observation time, completeness/continuation and per-source freshness. New coherent snapshots need an actual committed revision; legacy multi-RPC joins must say “best-effort, non-atomic” rather than mint a fake revision.

Every mutation accepts a typed target, requested action, reason, expected object revision and idempotency key; it returns an operation reference. Operations preserve target membership, actor, requested/accepted/finished times and per-target outcomes. An acceptance response does not claim workloads already stopped. Revision conflicts return the changed facts for a new review. Enforce authority at both the gateway and underlying operation boundary; preserve legacy role/admin restrictions when adapting RPCs. Do not expose storage recovery or force-state RPCs as generic console actions.

For agents, propose `agentId`, `sessionId`, architecture/runtime/capabilities, last heartbeat received by server, lease policy, desired generation, reported generation, active attempts, resource sample time and diagnostic capabilities. Distinguish agent-reported time from server time. Enrollment tokens/credentials are secret material, not URL parameters or diagnostics. Missing heartbeats mark connectivity uncertain; replacement/fencing decisions remain scheduler policy. The UI must expose the policy and observed confirmation rather than pretending a timeout is physical termination.

### Durable events, streaming and logs

Propose an event envelope with `eventId`, cluster, committed sequence/cursor, object type/ID/version, event type, occurredAt, receivedAt, actor/source, operation/deployment/attempt correlation, reason code/message and visibility scope. Produce externally authoritative state events from a committed outbox/read model or an equivalent durability boundary. Forwarding the legacy in-process bus alone would overstate persistence and replay guarantees. Agent observations can arrive before a scheduler decision and must remain labeled observations.

Use snapshot + resumable Server-Sent Events for console state notifications; use ordinary requests for actions. SSE defines reconnection and `Last-Event-ID`, but application retention, deduplication, authorization and delivery guarantees remain backend responsibilities. See the [WHATWG server-sent events specification](https://html.spec.whatwg.org/multipage/server-sent-events.html). This is a design choice favoring one-way notifications; bidirectional interactive terminals would require a separate design.

Subscribe using a snapshot cursor without a fetch/subscribe gap; deduplicate by event ID, apply only newer object versions, and refetch on gaps or expired cursors. Define leader-epoch handling explicitly. Coalesce high-volume updates and bound queues; on overflow send resync-required rather than silently drop events. Reauthorize long-lived connections when access changes. Keep log streams separate from state events to prevent noisy output from delaying health changes.

The same-origin log gateway authorizes cluster/workload/attempt/process/run and routes only to registered agent identities, never a user-supplied arbitrary URL. Bound bytes, concurrent tails and export size; scope filesystem access to the attempt sandbox. A cursor includes source/session/file generation and byte offset so rotation or agent restart cannot silently splice unrelated output. Logs remain plain untrusted text, with secrets redacted according to a defined policy; archived logs require their own retention/access policy. External metrics/log tools may remain deep links until these services exist.

## 6. Implementation choices and tradeoffs

| Choice | Recommendation and tradeoff |
| --- | --- |
| Evolve existing pages vs parallel console | Ship `/console` beside `/scheduler`; retain domain logic as reference and explicit compatibility tests. A separate shell/asset build permits modernization without rewriting the Java/Python toolchain first, but temporarily doubles navigation support. The [webpack plugin seam](../../ui/webpack.config.js) can support a limited shell experiment; it does not solve API or observer gaps. |
| Frontend stack | Retain React as the conceptual model, move new code to typed components and schema-validated data access with a pinned lockfile. Choose supported versions during implementation after toolchain verification; this report does not assert current package versions. Split pages by route and avoid importing generated browser globals into presentation components. |
| Aggregating gateway vs direct browser APIs | Prefer one same-origin adapter/gateway, eliminating host reachability requirements and centralizing access/freshness. It adds a service boundary and possible failure point; deploy redundantly and keep scheduling functional when the UI/gateway fails. |
| Unified observability vs replacing observability products | Integrate task/process context and bounded local logs first; link existing metrics/log systems. Full indexing increases retention, storage and access-control work and should follow measured demand. |
| Push vs polling | Polling is the honest compatibility path; SSE becomes useful only after cursor/replay semantics exist. Avoid building a live appearance on stale or uncommitted data. |
| Rich visualizations vs operator density | Use tables as the default, with compact instance grids and resource charts as secondary views. This trades spectacle for comparable values and keyboard access. |
| Form wizard vs arbitrary DSL editor | Start with read-only normalized config, diff and imported validated revisions. A schema-guided editor can follow. Embedded executable Python would expand trust and compatibility scope substantially. |

Assumptions to resolve with product/backend owners: who can view cross-role data; single versus multiple clusters; target cluster/attempt scale; process-DAG parity in Go agents; container/native runtime support; readiness/discovery semantics; partitions/fencing policy; metrics/log retention; and whether deployments must integrate an existing delivery system. Defaults here are one cluster, existing role boundaries, read-only first, explicit unsupported capabilities and external tooling for submission. None blocks the first read-only slice.

## 7. Phased migration and validation gates

Phases are ordered deliverables, not calendar estimates. Advance when the exit criteria pass; UI completion alone cannot waive backend dependencies.

| Phase | Deliverable and dependencies | Exit criteria / rollback |
| --- | --- | --- |
| 0 — Contract and workflow prototype | Route map, adapter schemas, representative legacy fixtures, static sketches and a narrow interactive prototype during implementation. Agree on desired-state/agent/readiness identities. | Operators can explain instance vs attempt vs process retry, find a pending reason and identify unsupported data. Contract fixtures cover error responses and PARTITIONED. No production mutation. |
| 1 — Read-only console | New shell, Workloads/attempt history/config/deployment inspection, quota accounting, scoped Events, legacy diagnostics links; bounded polling. Uses E/A capabilities only. | Old route mapping preserves role/environment/name/instance/task/update plus `jobView`/`taskView` where meaningful. Required views have loading/empty/error/denied/stale states and accurate coverage. Feature flag returns users to legacy pages. |
| 2 — Standalone agent visibility | Typed inventory/capabilities, heartbeat conditions, new readiness/process evidence, gateway logs and resource samples. Requires corresponding Go agent/controller contracts. | Test reconnect, agent restart with same hostname, missing metrics, rotated/pruned logs and mixed legacy/new agents. No unknown agent shown healthy, no log cursor splices sessions, and no browser needs worker network access. Keep legacy observer links as fallback only where supported. |
| 3 — Controlled operations | Deploy review, pause/resume/rollback, scoped restart/scale and drain with durable operations, server authorization and revision guards. | Conflict, denied action, response loss after acceptance, partial bulk failure and stale preview tests pass. Closing the browser does not stop controller progress. Disable operation capability flags to roll back UI exposure; retain operation records and reconciliation. |
| 4 — Unified evidence and cutover | Committed event feed, retained history/cron run ledger, placement explanations, indexed cross-object search; retire legacy routes only after parity. | Replay/gap/leader-failover tests pass, retained-data limits are visible, and agreed operators complete the full triage/drain/deployment exercises. Redirect old bookmarks server-side; keep reversible routing until acceptance. Remove old assets/dependencies in a separate cleanup change. |

Suggested measurable acceptance targets (proposed, not achieved):

1. **Operator task trials:** with 5–8 representative workload owners/on-call operators, at least 80% independently find the evidence for a stuck instance within two minutes, distinguish readiness from rollout stability, and identify the impact of a proposed drain. Observe mistakes, not only speed; revise thresholds after the first baseline session.
2. **Semantic correctness:** fixtures include multiple config groups, absent ancestry, PARTITIONED, successful batch completion, service replacement, paused rollback, missing pulse evidence, stale leader response, pruned attempt and cron collision. Assertions test labels/actions and retained uncertainty, not just snapshots of markup.
3. **Contracts:** run focused adapter tests against generated Thrift bindings and representative serialized responses. Verify `getPendingReason` filter restrictions, optional result/error envelopes, unknown enums, 64-bit values and offset pagination under changing data. Stable cursor guarantees are tested only on the new API.
4. **Operations:** transport loss after backend acceptance yields one durable operation and a recoverable status; double-click/retry does not create duplicate mutations on the new contract. Role-scoped denials, expired sessions and concurrent updates must fail on the server even with a handcrafted request.
5. **Accessibility:** keyboard-only and screen-reader walkthroughs cover tables, drawers, tabs, live updates, logs and review dialogs; test contrast, zoom/reflow and both themes. Automated checks supplement manual workflow testing.
6. **Performance:** initially test fixtures of 10,000 active attempts, 1,000 agents and 100 concurrent viewers (planning assumptions to replace with real scale). Target usable first results within 2 seconds on the agreed test network, at most 100 result rows per page, bounded log buffers, and no whole-cluster task fan-out per viewer. Profile backend scan/read-lock cost before fixing polling intervals. Report measurement environment and p95 values.
7. **Failure recovery:** delay/out-of-order/duplicate events, expire cursors, restart gateway/leader/agent and revoke access midstream. Verify resync, visible stale/partial data and no cross-role leakage. Scheduler operation must continue when every browser and the console gateway are down.

## 8. Research checks and handoff

Performed: host/architecture verification; exact baseline SHA and clean-start status check; ancestor/repository instruction search; context-first read; targeted inspection of route/page/component/API/diagnostic/observer sources and representative UI tests; Go source/module search; primary-source checks of WCAG 2.2 and SSE. No dependency installation, builds, application tests, runtime services, deployment, push or merge were needed for this report. Document checks are recorded after writing below.

Document verification: all 34 local source links resolve; six text sketches have balanced code fences; repository status shows only the report directory as untracked on the isolated branch. `git diff --check` passed for tracked changes (the new report was separately checked for trailing whitespace). No runtime or accessibility conformance is claimed.

Usage policy: initial shared main-weekly usage was 4%; conservative task stop is 11% total used (seven points above initial), with the shared early ceiling of 50% taking precedence. Successful checks before research and between major phases all returned 4% and ordinary usage allowed. The final check did not return and was terminated; research stopped and only report verification/handoff continued. Final usage is therefore unavailable. No subagent was needed, no resets or credits were consumed, and no automatic continuation across reset is authorized. Usage is a sampled shared account figure, not an attribution of this task's cost.

Next review should settle three decisions: accept the workload/instance/attempt vocabulary; approve read-only parallel-console delivery as the first slice; and assign ownership of the agent telemetry, operation and event contracts. Those decisions make the implementation backlog concrete without requiring a full UI rewrite up front.
