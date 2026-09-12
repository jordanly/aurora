# INPLACE-05 through INPLACE-07 implementation status

This checkpoint records the bounded original-scheduler integration currently
present on `codex/in-place-java25`. It keeps the original
`org.apache.aurora.scheduler.app.SchedulerMain`, Thrift API, generated entities,
and public field names. It does not claim completion of the broader INPLACE-06
Thermos compatibility work, production high availability, or Mesos retirement.

## Implemented slice

`SchedulerMain` selects the Go execution cohort when `-go_agent_config` is
provided. `GoAgentModule` binds the existing scheduler execution interfaces to
`GoAgentDriver`, `GoTaskFactory`, and the enrolled Go agents. The adapter keeps
the original scheduling, offer, state-transition, and resource-accounting
contracts while sending a narrow process profile over the native-v1alpha1
transport.

The supported cohort is a single-owner, trusted host-process profile. It uses
explicit argv and environment, authenticated agent transport, durable command
and observation receipts, bounded inventory polling, and the existing Aurora
state machine. `GoTaskFactory` validates the process profile and creates stable
wire identities. `GoAgentDriver` performs enrollment, inventory reconciliation,
launch, kill, abort, offer publication, and state updates. `GoEventModule` and
`SqliteStorage.transactionalEventSink` publish policy events after the enclosing
SQLite transaction commits; publication failures leave the operation requiring
restart/reconciliation rather than replaying the policy callback.

`build-support/lab/build-agents` builds static Linux ARM64 agent and lab-helper
binaries with the pinned Go 1.27.1 archive. It writes source and binary
provenance records consumed by `build-support/lab/inplace-cluster`. The lab
stages the original scheduler distribution and two agents in a private,
non-root, capability-dropped Docker network. It does not launch a replacement
Java scheduler.

## Deliberate boundaries

The supported profile is intentionally smaller than Aurora's historical
executor surface. It currently has the following constraints:

- Partition rescheduling is disabled for this profile.
- Images, volumes, ports, GPUs, and fetcher URIs are unsupported.
- Job role, environment, and name keys must be lowercase ASCII values of at
  most 64 characters.
- CPU reservations must resolve to whole millicores; memory and CPU values are
  reservations, not resource-isolation claims.
- The agent bounds inventory at 128 attempts and 1,024 command results and
  retains history. Reaching the bound refuses further inventory rather than silently
  truncating it; retention/compaction is still needed for long-lived use.
- Backups include the complete local SQLite database. Legacy online snapshot
  editing/restore is refused because it omits command and observation state.
  Coordinated offline restore with agent fencing is not qualified by this lab.
- Escaping outer-write failures stop the scheduler, since policy caches may
  already have changed. Restart reconstructs them from committed state.
  Production failover, media loss and power-loss recovery remain unqualified.

The original public Aurora API and Thrift schemas remain unchanged. This slice
does not provide general Thermos compatibility, arbitrary executor payloads,
production HA, distributed leadership fencing, or a Mesos removal. Those are
separate qualification and migration boundaries.

## Build and test evidence

The pinned compiler-only Thrift bootstrap now passes `--disable-plugin`, avoiding
Ubuntu/Boost auto-enablement of optional plugin support that requires
`libthrift.la`. The focused bootstrap tests pass, and a fresh offline compiler
build from the verified Thrift 0.10.0 source archive reports exactly
`Thrift version 0.10.0`.

The Go agent unit suite and the static ARM64 build helper have been run against
the current source tree. The generated binaries' provenance records match their
source-tree and binary SHA256 values. The GitHub Actions Java 25 jobs for
compiler fix `390fe6b25` are green:

<https://github.com/jordanly/aurora/actions/runs/34672906447>

The final working-tree CI result is **pending**. Live scheduler/agent lab
qualification is **pending**. Parent-level test totals, coverage, and
acceptance evidence are **pending** and must be filled from the final clean
checkout and isolated-lab runs; this document does not infer completion from
local compilation or fixture tests.
