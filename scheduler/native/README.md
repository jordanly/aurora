<!-- Licensed under the Apache License, Version 2.0. See LICENSE. -->
# Native Java scheduler

This isolated Java 8 daemon uses the native protocol validator and SQLite core.
It does not load Mesos or legacy scheduler stores. Build with cached dependencies:

```sh
build-support/java/gradle-local -p scheduler/native test installDist --offline
```

Distribution: `.pi-tools/native-scheduler-dist/install/aurora-native-scheduler`.
The wrapper selects the repository Java 8 installation and two Gradle workers.
The build verifies the protocol runtime dependency manifest and SQLite JDBC hash.

```sh
aurora-native-scheduler --config /lab/config.json --state /lab/state \
  --listen 0.0.0.0:8443 --tls-keystore /lab/tls/scheduler.p12 \
  --tls-truststore /lab/tls/ca.p12 --tls-password-file /lab/tls/password
```

Configuration is immutable across restarts (including node order):

```json
{"cluster":"lab","incarnation":"recovery-a","nodes":[
  {"node":"agent-a","journal":"journal-a","boot":"boot-a","runtime":"runtime-a","url":"https://agent-a:9443","cpuMillis":1000,"memoryBytes":536870912,"network":"agent-container","portStart":18080},
  {"node":"agent-b","journal":"journal-b","boot":"boot-b","runtime":"runtime-b","url":"https://agent-b:9443","cpuMillis":1000,"memoryBytes":536870912,"network":"agent-container","portStart":18080}
]}
```

Each node has a 16-port range beginning at `portStart`. Reservations sum CPU and
memory across all jobs and exclude occupied ports. `maxPerAgent` applies per job.
Unknown agent reservations block new placement on that node. Unreachable nodes
retain every allocation. No heartbeat timeout releases resources. Memory is an
admission reservation, not a kernel memory limit.

The mutually authenticated HTTPS operator API requires a verified leaf DNS SAN
`operator`. Agent connections retain Java hostname verification against each
configured node DNS name, use the scheduler client certificate, reject redirects,
and have bounded connect/read deadlines. Only the configured CA is trusted.
An operator POST must use `Content-Type: application/json`.

- `GET /v1/state`: `{cluster,incarnation,epoch,jobs,attempts,commands,nodes,limits}`.
  Jobs include the validated body and desired instance names. Attempts include
  identity, immutable Run, node, state, cleanup, readiness, reservation and sequence.
  Readiness is false when the node is unreachable; allocation state is retained.
- `POST /v1/jobs`: raw native Job. Returns 201 for a new durable job or 200 for an
  identical body. A conflicting body returns 409. Replaying a cancelled job does
  not restore its desired membership. This API does not update jobs.
- `POST /v1/jobs/stop`: `{role,environment,name}`. Clears desired membership and
  persists Stops atomically. Pending Runs are suppressed before Stop dispatch.
- `POST /v1/backup`: `{name:"snapshot-a"}`. Writes
  `STATE/backups/snapshot-a/scheduler.db`; existing destinations and symlinks reject.
  Success follows a consistent SQLite snapshot plus file/directory synchronization.

A scheduler startup durably increments its uint64 epoch before contacting agents.
Each pending immutable command is wrapped in the new epoch/session. Receipt loss
therefore replays the same command identity and body. Agent session admission
provides stale-session rejection for this single configured scheduler; it does
not implement a distributed leader election protocol.

Observation pages must be contiguous from the last committed cursor. Receipt,
sequence reduction and cursor advancement share a SQL transaction. ACK is sent
only after commit. Full agent execution inventory reconciles by attempt identity
and sequence but cannot advance the ACK cursor. Cursor retention gaps, changed
identity, conflicting sequences and incomplete inventory fail closed.

Batch membership remains durable after completion and never launches a second
attempt. Services replace only after a terminal outcome with explicit complete
cleanup and a one-second delay. Unknown cleanup stays reserved indefinitely.
Terminal outcomes become immutable once cleanup is complete. Before cleanup
completes, agent recovery may report Lost while preserving the reservation.
Cancellation survives restart. Stop-only tombstones require a durable execution
outcome before resources can be released.

This MVP intentionally retains history: at most 64 jobs (each input at most 4 KiB)
and 48 allocated attempts per scheduler database. Reaching the attempt limit stops
new placement, preserving current desired membership and reservations. There is
no history compaction, automatic enrollment, job update API, HA failover, resource
overcommit, arbitrary descendant containment, or container-loss cleanup proof.
State `limits.attemptHistoryFull` explicitly reports the placement ceiling.
Operator output is capped at 1 MiB and fails closed if exceeded.

For isolated recovery inspection, copy a successfully completed snapshot directory
to a separate writable directory and run:

```sh
aurora-native-scheduler --inspect-only --config /lab/config.json --state /lab/restored
```

This verifies/open-locks the database and prints durable state without incrementing
the epoch or making agent requests. It creates the restored directory's local lock
and SQLite sidecars. The original config and cluster/incarnation are required.
Do not start a restored scheduler against a live old cluster as a takeover method.
Failed/interrupted backups are not success artifacts and are never overwritten.

## Portable build output and runtime boundary

A supplied absolute build root moves **both** isolated Java projects' outputs:

```sh
gradle -p scheduler/native -PnativeBuildRoot=/absolute/build test installDist
```

The distribution is `/absolute/build/scheduler/install/aurora-native-scheduler`;
protocol build output is `/absolute/build/protocol`. Relative roots reject. Without
this property, the existing `.pi-tools/native-scheduler-dist` and
`.pi-tools/protocol-java-dist` defaults remain. Java 8 and Gradle 4.10.2 remain the
build baseline. This project includes only `protocol/java`; it does not configure
or invoke the legacy root build, Thrift generators, frontend, or Mesos tasks.

`test`, `check`, and `installDist` require the native runtime boundary gate.
`installDist` also verifies the actual copied `lib` directory after installation.
The gate admits exactly seven external runtime artifacts, identified by both name
and complete SHA-256 in `build-support/native/verify-boundary`: Jackson annotations,
core and databind 2.18.4; networknt JSON Schema validator 2.0.4; SLF4J API and nop
2.0.17; and SQLite JDBC 3.53.4.0. It scans every JAR entry and every class constant
pool. Renaming a foreign JAR, repackaging Mesos/ZooKeeper/Curator/executor references,
or including Python workers cannot satisfy the gate. The pinned SQLite JAR's
platform JNI libraries are explicitly permitted.

Own JARs admit only the enumerated native scheduler, SQL core and native protocol
classes, plus the protocol schema and JAR manifests. Qualification-only
`NativeStoreTool` is compiled for existing SQL tests but excluded from production.
Own classes must target Java 8. This is an artifact/content boundary check; it does
not replace behavioral tests or prove safety of arbitrary dynamically supplied code.

JSON dependency reports are generated at:

- `scheduler/reports/native-runtime.json`: staged runtime, before tests.
- `scheduler/reports/installed-native-runtime.json`: actual distribution runtime.

Each report includes actual JAR hashes, coordinates, byte sizes, class/reference
counts, native-library counts and the own-class inventory. Root packaging must
recheck the final copied runtime independently:

```sh
python3 build-support/native/verify-boundary \
  --lib-dir /absolute/package/lib --report /absolute/package/runtime-dependencies.json
```

Nonzero exit rejects the package. A successful report has `ok: true`; a validation
failure replaces any old success report with `ok: false`. The checker uses Python
standard libraries and has no `.pi-tools`, Java, Gradle or network dependency.
Run its offline unit tests with:

```sh
python3 -m unittest discover -s build-support/native/tests -p test_boundary.py
```
