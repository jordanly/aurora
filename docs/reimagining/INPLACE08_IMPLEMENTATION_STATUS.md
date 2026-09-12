# Push transport, Python retirement and Mesos cleanup

This checkpoint modernizes the original Aurora application in place. Its entry
point remains `org.apache.aurora.scheduler.app.SchedulerMain`; the existing
placement, task state machine, quotas, updater, cron, maintenance, Thrift API and
UI remain the behavior owners. Java 25 and SQLite run the scheduler; Go agents
run the supported process profile. Production multi-scheduler HA is deferred.

The separate scheduler prototype remains abandoned. Public Thrift identifiers
and stored historical records have not been renamed to disguise their history.

## Resource availability and task updates

Each configured agent has an authenticated, persistent mTLS watch connection.
The scheduler opens the connection; the agent sends state changes immediately
over that stream. Idle operation no longer polls full inventory every second.

```mermaid
sequenceDiagram
    participant Policy as Original Aurora policy
    participant DB as SQLite
    participant Driver as GoAgentDriver
    participant Agent as Go agent
    Driver->>Agent: Open authenticated watch
    Agent-->>Driver: Initial inventory and observation cursor
    Driver->>Policy: Publish available resources as HostOffer
    Policy->>DB: Commit assignment and command intent
    Driver->>Agent: Deliver committed command
    Agent->>Agent: Persist admission and execute process
    Agent-->>Driver: Push durable state change
    Driver->>DB: Commit original state transition and cursor
    Driver->>Agent: Acknowledge committed observations
    loop While connected
        Agent-->>Driver: Lightweight heartbeat every 30 seconds
        Agent-->>Driver: Full reconciliation every 5 minutes, jittered ±30 seconds
    end
```

Heartbeats contain identity/session and cursor information, without rereading
inventory. Durable agent mutations wake the stream; acknowledgements do not
produce a feedback loop. Full snapshots also occur on initial connection and
reconnection. Frames and observation pages are bounded; cursor gaps, changed
authority and inconsistent reservations fail closed.

The scheduler merges these observations into its existing state and resource
model. It retains reservations and withdraws offers when an agent cannot be
reconciled. Independent virtual threads and bounded exchanges prevent one
unresponsive agent from holding the global SQLite writer during network I/O.
Command selection and acknowledgement use short transactions on either side of
delivery. The existing local offer-expiry refresh is separate from network
inventory traffic; pending commands retain a bounded retry path.

Transport counters are exported through the original `/vars.json` endpoint:
`go_agent_watch_connections`, `go_agent_watch_snapshots`, `go_agent_watch_deltas`,
`go_agent_watch_heartbeats`, `go_agent_inventory_requests`,
`go_agent_command_requests` and `go_agent_ack_requests`. Connections count opens
cumulatively, rather than currently connected sockets. `/offers` now serializes
neutral offer fields: offer/agent identity, hostname, available resources,
revocable resources, ports, maintenance and unavailability. It no longer needs
a Mesos protobuf JSON module.

## Python and Mesos retirement

The [retirement ledger](inplace08-retirements.json) identifies every removed
tracked file, its previous Git blob and content hash, and its replacement or
retirement reason. It is an audit record, not a coverage exclusion list or a
claim of full Thermos feature equivalence.

All 275 tracked Python files are removed. The maintained client, tool bootstrap,
Thrift entity generator, validation helpers, Docker lab and API acceptance runner
are implemented in Go with small shell launchers. The generator preserves the
original generated Java bytes. Pants, PEX, executable `.aurora` examples, the
Python CLI and Thermos runner/executor/observer are retired, together with their
obsolete Vagrant, Packer and Mesos deployment paths. Unrelated software installed
on the Pi is outside this repository cleanup.

JSON job documents and the [Go client](../reference/go-client.md) provide the
supported submission and operations path through the original `/api` endpoint.
The standalone client needs neither Python nor a Go compiler at runtime. It
rejects duplicate/unknown configuration fields, sends mutations once, checks
original RPC response codes and reports malformed responses as failures.
Arbitrary executable Python configurations do not have an automatic conversion.

Mesos drivers, JNI loading, the replicated-log implementation, executor loaders,
protobuf conversions and runtime dependencies are removed. Neutral fixtures
exercise retained policy behavior; only tests specific to removed implementations
are retired. The scheduler requires Go agent configuration. Mesos/Thermos names
remaining in public Thrift fields, historical fixtures and documentation preserve
wire/storage compatibility; they do not load or contact Mesos. Packaging checks
reject Mesos runtime classes and Python/PEX entries in the original application
jars.

## Recovery and supported execution

The [offline recovery tool](../operations/backup-restore.md) restores a complete
SQLite backup without altering its source bytes, including all seven stores,
outbox, observation receipts and ownership metadata. Historical Thrift snapshot
import uses the original backup reader, loader and backfill in one transaction.
It requires terminal tasks, completed updates and supported cron definitions;
it cannot adopt running Thermos processes. Strict schema validation and exclusive
publication refuse data loss and replacement of an existing database.

The supported executor runs explicit trusted host processes with resource
reservations. It does not implement Thermos process graphs, finalizers, health
checks, service announcement, images, volumes, artifact fetching, named ports,
GPUs or revocable CPU. The user field is metadata, not an OS identity switch.
Memory and disk accounting do not establish cgroup or container isolation.
CPU requests use integer millicores; conversion tolerates only floating-point
representation error, without accepting genuinely fractional millicores.

Agent history remains bounded at 128 attempts and 1,024 command results. Retention
or compaction is needed for indefinite operation. Restart recovery does not
qualify container/namespace-loss adoption, stale-backup adoption, power-loss
recovery, production fencing or multi-scheduler HA. Dependency and UI framework
modernization remain subsequent work in the retained application.

## Qualification

Final build, cluster and client receipts are being collected for this checkpoint.
Earlier [INPLACE-05–07 evidence](INPLACE05_07_IMPLEMENTATION_STATUS.md) remains
unchanged and establishes only its named source and artifact versions.
