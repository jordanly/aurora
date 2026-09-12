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

The implementation commit is
[`02850c964`](https://github.com/jordanly/aurora/commit/02850c964eb37c23a76ddfb24aa160bb7cb12e67).
All three [implementation CI jobs passed](https://github.com/jordanly/aurora/actions/runs/34674334138):
original Java behavior/distribution, original quality gates, and Go tests/vet/provenance.
The policy acceptance extension is committed separately as `3a8a8fa3a`.

Local qualification passed:

- 1,465 scheduler and 124 commons Java tests, with no failures, errors or skips.
- 144 UI tests in 33 suites, frontend lint/build, and original installed launchers.
- 38 Python build-helper tests; uncached tests and vet for all five Go packages.
- Checkstyle, PMD and SpotBugs for main, test and JMH; 106 PMD and 76 SpotBugs
  migration fixtures; original license gates.
- Instruction coverage 89.42%, branch coverage 80.34%. The existing 87%/79%
  thresholds remain unchanged and include the original production classes.

The Java build used the implementation working tree before its commit and reused
unchanged Gradle/dependency outputs. Its `build.properties` therefore names the
preceding commit with a dirty marker. This is an incremental local qualification,
not a claim of a fresh local checkout. CI separately checked the committed source.
The [input manifest](inplace05-07-inputs.json), [Java suite ledger](inplace05-07-tests.jsonl),
and [qualification evidence](inplace05-07-evidence.json) bind source, report and
installed artifact hashes. Earlier receipts have not been rewritten.

## Live original cluster

The final owned Pi lab is `.pi-lab/original-qualified`. It runs the original
`SchedulerMain` on Java 25, a single SQLite owner, and two supervised Go agents
in separate Docker containers. Its private UI is available from the Pi at
`http://172.19.0.4:8081/scheduler`; `/leaderhealth` returns HTTP 200. No host port
is published. In-process ZooKeeper supports the original lab lifecycle. The
acceptance runner runs on the Pi host, rather than in a fourth container.

Executed acceptance through the original `/api` Thrift interface:

- Two-agent placement, completed batch work, service execution, successful
  rolling update, termination, quota and cron/maintenance API operations.
- Three rounds of graceful scheduler restart, scheduler SIGKILL and agent daemon
  SIGKILL: nine faults total. Original task IDs, host assignments and physical
  workload PIDs survived. Agent containers/namespaces remained stable.
- A 600.11-second mixed workload: two continuously running services, twenty
  two-instance batches (40 completed tasks), and 140 checks of service identity
  and physical PIDs.
- A failing service update automatically rolled back, restoring the exact
  original executor configuration. An occupied host reached DRAINED; its
  replacement stayed pending until maintenance ended, then ran. A manual cron
  trigger executed two tasks to completion through the original cron controller.

After acceptance, SQLite integrity reported `ok` and every command was
acknowledged. The two small `fixtures/test/mvp-demo` instances are left running,
one per agent, for inspection. They reserve 100 millicores and 32 MiB each and
run `/bin/sleep infinity`. Stop them through the original Aurora API/UI, or remove
only this lab with its ownership-checked `inplace-cluster down` action.

The old experimental lab and Home Assistant were left intact. Superseded debug
containers created for this integration were removed; their local evidence remains.

This completes the corrected cluster MVP for the declared process cohort. It
closes neither the full executor compatibility ledger nor every INPLACE-07
matrix row. Container/namespace-loss adoption, active-update restart, network
partition/fencing, storage fault/power-loss recovery, full restore/migration,
production HA and remaining Mesos removal still need their own qualification.
