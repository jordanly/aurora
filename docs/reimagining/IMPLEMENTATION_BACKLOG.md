**Ordered implementation backlog**

Companion to the [unified roadmap](UNIFIED_AURORA_ROADMAP.md) and [Docker lab](PI_CONTAINER_LAB.md). These rows define intended deliverables and acceptance contracts, not created issues. The [native cluster guide](CLUSTER_MVP.md) records the integrated scheduler/two-agent implementation and its bounded operating profile. The [process runtime status](PROCESS_RUNTIME_STATUS.md), [durable core status](DURABLE_CORE_STATUS.md) and [first foundation status](FIRST_SLICE_STATUS.md) preserve the preceding increments and evidence. Keep each PR focused; split a row if its reviewable diff becomes too large. Dependencies are gate dependencies, not a requirement for one enormous PR per stage.

The integrated SLICE-01 MVP has passed its three-round and ten-minute gate.
CUT-01 has passed its [packaged Pi qualification](CUT01_STATUS.md), with a [native packaging lane](../../build-support/native/README.md):
pinned standalone builds, production and lab images, exact runtime content gates,
and an image-backed qualification command. The [evidence ledger](cut01-evidence.json)
records all 23 passing cases and verified cleanup. [JAVA-01](JAVA01_STATUS.md)
now advances the retained native scheduler/protocol profile to Java 25 and
Gradle 9, with reproducible archives, Java 8/25 state and TLS compatibility, and
the same complete Pi gate. [Its evidence](java01-evidence.json) records all 23
passing cases and cleanup. JAVA-02 is next; retired legacy build targets are
outside the native JAVA-01 completion claim.

**First work and independent tracks**

Start with PLAN-01, BUILD-01 and CONTRACT-01. Go reducers/protocol fixtures can proceed before Java build reconstruction finishes. Once contracts stabilize, native backend, Go agent and container harness can advance independently. Java behavior changes need executable focused tests. Real launch requires storage plus agent guarantees; full Java modernization waits for the standalone artifact's Mesos dependency removal.

| Order / ID | Deliverable | Depends on | Acceptance |
| --- | --- | --- | --- |
| P0 / PLAN-01 | Baseline/provenance, first batch and HTTP service, capability ledger, architecture decisions and work ownership. | Current upstream | Research applies to source; accepted/changed/rejected scope is explicit. Planning artifacts are now drafted; decisions remain proposed. |
| P1 / BUILD-01 | Temporary Java toolchain, explicit Thrift compiler, bounded generator comparison and Python 3 compatibility. | PLAN-01 | Generated Java/API artifacts compile; actual tool/dependency failures recorded; no broad JVM upgrade. |
| P1 / BUILD-02 | Independent generation, scheduler unit/integration, frontend and full-package targets; retained full-suite coverage; capped Pi workers. | BUILD-01 | Selected scheduler test executes without npm or whole-suite coverage false failure; full checks retain their gates. |
| P1 / CONTRACT-01 | Strict Job/execution schemas, identity/counter model, canonical hashes, command/observation/reconciliation fixtures. | PLAN-01 | Java/Go encode/decode identical semantics; command body differs from delivery-authority envelope; unknown/conflicting versions reject. |
| P1 / LAB-01 | Host preflight, owned run roots, image toolchain manifests, Compose/config rendering and cleanup skeleton. | PLAN-01 | Docker daemon access actually works; architecture/page size/cgroup/runtime recorded; parse and native image smoke separate. Host changes are separately scoped. |
| P1 / ACCESS-01 | Lab CA/static enrollment, node identity checks, test-user authorization, TLS names and secret-redacted evidence. | CONTRACT-01 | Unknown node, wrong cluster and unauthorized job action reject; proxies preserve verified end-to-end TLS. No production PKI platform required. |
| P2 / CORE-01 | Neutral execution/inventory/allocation types; quarantine Mesos conversions; narrow native module assembly. | BUILD-02, CONTRACT-01 | Core policy does not expose Mesos protobufs; native profile disables unsupported APIs/background controllers explicitly. |
| P2 / AGENT-01 | One local-store choice, durable admission reducer, reservations, immutable attempts, observation outbox and compact tombstones. | CONTRACT-01 | Restart/replay/corruption, stop-before-run, conflicting IDs and concurrent admission pass; no false durable acceptance. |
| P2 / AGENT-02 | Preinstalled process runtime, launch gate/identity, readiness, bounded logs, exact ports, stop/cleanup and explicit MVP recovery. | AGENT-01 | Real ARM64 batch/service; no duplicate local execution after lost replies; unresolved outcomes remain reserved until cleanup. |
| P2 / STORE-01 | Native SQL transaction boundary and every store needed by enabled paths; durable jobs/revisions/instances, tasks and nodes. | CORE-01, BUILD-02 | Atomic multi-store writes and rollback-only nesting; consistent reads; no committed cache leak. No silent no-op/volatile stores. |
| P2 / STORE-02 | Durable allocations, commands, operations, observation dedupe/cursors, initialization/recovery and backup hooks. | STORE-01, CONTRACT-01 | Crash before/after commit and uncertain commit tests; contiguous ACK cannot cross an uncommitted gap; pending effects survive restart. |
| P2 / LAB-02 | Scheduler/agent/proxy/test images, keeper/probes, generated certificates/config and native fixture programs. | LAB-01, ACCESS-01, AGENT-02; scheduler build for real scheduler image | Services and bound state persist/recreate as designed; networks prevent bypass; keeper exposes verified daemon-only fault control. |
| P3 / INTAKE-01 | Minimal native submit/read/stop/restart; idempotent operations; durable desired membership; one replacement owner. | STORE-02, CONTRACT-01, ACCESS-01 | Zero-active service still has desired state; completed batch stays completed; duplicate submit is harmless; cancel cannot resurrect service. |
| P3 / CORE-02 | Native recovered-state startup, inventory/placement, minimal one-instance-per-agent rule and admission-disable/cordon. | CORE-01, STORE-02 | Two fixture instances spread; unavailable agents receive no placement; unknown allocations retained; no Mesos registration/ZooKeeper needed. |
| P3 / EXEC-01 | Committed launch/stop dispatch and observation reduction; reconnect snapshots/watermarks; fresh authority on stable command replay. | CORE-02, AGENT-02, STORE-02 | Actual effects follow commit; duplicates/reorders/old commands cannot regress state; failed writes produce no observation ACK. |
| P3 / SLICE-01 | Complete two-agent scenario runner, small read/debug API, physical evidence and isolated restore. | LAB-02, INTAKE-01, EXEC-01 | Batch/service + scheduler restart + proxy interruption + stop/reconnect + MVP agent crash + backup restore pass three times, then bounded mixed run. |
| P4 / CUT-01 | Native distribution dependency cleanup and build profile; separate legacy adapter/importer classpaths. | SLICE-01 | Native dependency report/image/startup excludes Mesos jars/JNI/native log/Python worker; schema names alone are documented exceptions. Rerun SLICE-01. |
| P5 / JAVA-01 | Complete for retained native profile: Gradle 9, Java 25 compiler/test/JavaExec toolchains and deterministic package tasks. Retired buildSrc/Thrift/JMH targets excluded after CUT-01 isolation. | CUT-01 | 34 Java tests, identical Java archives/installed trees, Java 8/25 state and TLS compatibility, verified production images, full three-round/ten-minute Pi gate; [recorded evidence](java01-evidence.json). |
| P5 / JAVA-02 | Coherent DI, serialization, networking, logging and JDBC/native compatibility upgrades. | JAVA-01 | Focused/full relevant Java and SQL/recovery checks pass, including exact ARM64 native libraries and no unexplained broad access flags. |
| P5 / JAVA-03 | HTTP/auth stack and distribution/launcher qualification; modern-JVM two-agent regression run. | JAVA-02 | API/auth/storage fixtures plus same SLICE-01 corpus pass; measured resource/latency changes documented. Full Java modernization gate complete. |
| P6 / SUPERVISE-01 | Same-binary surviving attempt supervisors, local protocol/versioning and daemon reattachment. | AGENT-02, SLICE-01; integrate on modern native baseline | Kill only daemon while children exit/log; preserve exact outcomes and execution; supervisor loss differs from container loss. |
| P6 / THERMOS-01 | Selected DAG/retry/daemon/ephemeral/finalizer behavior and trusted offline manifest conversion. | SUPERVISE-01, behavior corpus | Retained legacy cases match; intentional changes/rejections explicit; no live Python-checkpoint adoption. |
| P6 / POLICY-01 | Broader constraints/quotas, updates/rollback, preemption and workload evacuation/SLA drain, with real stores/controllers. | JAVA-03, required agent capabilities | Restart mid-update and drain; revalidate victims; release capacity only on cleanup. Enable features individually. |
| P6 / CRON-01 | Selected cron template/collision/restart behavior and any explicitly required run-slot evidence. | POLICY-01, durable job model | Completed runs not regenerated accidentally; missed/duplicate-trigger semantics documented and tested. No exactly-once claim. |
| P1/P3/P6 / CONSOLE-01 | Typed schemas/fixtures early; small read-only workload/attempt/agent/log views with the lab; expanded navigation after native API stabilizes. | CONTRACT-01 for fixtures; SLICE-01 for real evidence | PARTITIONED/unknown, pending reasons, retry versus replacement, freshness, denied/pruned/rotated logs and legacy links behave correctly. |
| P6 / CONSOLE-02 | Authorized operation review/submission/status; revision guards; deployment/drain controls. | POLICY-01, durable operations, CONSOLE-01 | Double-submit/response loss reconciles by operation ID; authorization remains server-side; closing browser does not interrupt controllers. |
| P6+ / EVENTS-01 | Committed event cursor, snapshot/replay handoff, retention and gap/resync; optional SSE. | STORE-02, stable read model | No fetch/subscribe gap; expired/recovery-generation cursor forces resync; logs cannot starve state events. |
| Separate gate / LIMITS-01 | Outer CPU/PID proofs, then delegated per-task limits and memory after host capability verification. | LAB-02; enabled/delegated controllers for each test | Advertised enforcement matches measured behavior; no global cgroup exposure; native-systemd fallback if safe container delegation unavailable. |
| P7 / MIGRATE-01 | Workload cohort inventory, offline export/import, discovery transition, semantic state comparisons, drain/cutover/reverse migration. | Required P6 capabilities | Complete retained-state coverage, one execution owner, deliberate config changes accepted, rollback after native writes exercised. |
| P7 / RETIRE-01 | Remove old runtime/UI/assets/build glue after migration/retention; retain bounded offline readers where required. | MIGRATE-01 | No remaining consumer needs deleted paths; old bookmarks/retention commitments handled. |
| P8 / PROD-01 | PostgreSQL conformance/HA, election/database fencing, independent-host tests, enrollment/rotation, OCI/runtime and operations. | Declared production requirements, stable native profile | Measured RPO/RTO within stated failures; correct workload fencing and security/runtime guarantees. |

**Proposed code boundaries**

Paths below are proposed additions unless already present; creating empty package trees is not a milestone.

| Boundary | Suggested location and ownership |
| --- | --- |
| Schemas/fixtures | `protocol/`: native Job/execution/observation schema, version policy, canonicalization and cross-language golden scenarios. One contract owner. |
| Native scheduler wiring | `src/main/java/org/apache/aurora/scheduler/app/NativeSchedulerMain.java`: supported module graph and explicit readiness; keep legacy main separately buildable while needed. |
| Execution domain | Scheduler `execution/` package: neutral identities, inventory, reservations and backend. Mesos conversion remains in its quarantined adapter. |
| SQL/effects | Scheduler `storage/sql/` and narrow dispatch integration: one owner for transaction, outbox, cursor and cache-publication semantics. |
| Agent | `agent/` Go module with `cmd/aurora-agent` and narrow spec/protocol/admission/runtime/store/health/log packages. Keeper/proxy/test tools stay in lab support. |
| Lab | `build-support/lab/`: Dockerfiles, implemented Compose/config generator, keeper/probes, host harness and scenario fixtures. Ignore run state/caches; docs blueprint remains design history. |
| Console | Separately built typed frontend plus scheduler-hosted read/log API initially. Presentation code receives normalized data, not generated legacy globals. |

Keep the current task state machine and update controller as policy authorities where behavior is retained. Avoid a generic command bus, general plugin system, new consensus layer or a second independent replica controller. The command outbox is a concrete durable dispatch mechanism, not justification for replacing every domain API.

**Review and acceptance discipline**

Each PR identifies its enabled behavior, touched compatibility boundaries, executed checks and remaining runtime limitations. Review write/effect ordering and identity/recovery changes together. Do not weaken full-suite coverage or regenerate compatibility fixtures solely to make a change pass.

Keep a small gate ledger: artifact/commit, config/schema versions, executed test names/counts, evidence path, failures/waivers and next dependency. A waived legacy comparison remains an explicit risk; it cannot silently turn into verified equivalence. Run only relevant checks until new failures or changes justify broader repetition.

The retained P3/P4 artifact is the baseline for P5 Java comparisons. The same two-agent scenarios are rerun after runtime/dependency changes, agent supervision changes, policy additions and release packaging. Save failures and cleanup evidence; passing synthetic tests does not waive real process/fault gates.

**Useful stopping points**

BUILD-02 leaves a testable repository; SLICE-01 delivers a working standalone lab; CUT-01 removes Mesos from its runtime; JAVA-03 completes modernization on that smaller system; selected P6 items cover useful workloads and operator actions. Migration and production HA extend the result only when their requirements are known. Re-estimate after BUILD-02 and SLICE-01 rather than attaching unsupported calendar commitments now.
