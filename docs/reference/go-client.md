# Go client and JSON jobs

The Go client uses the original scheduler's `/api` Thrift JSON endpoint. The
Java 25 scheduler still owns placement, quotas, state transitions, rolling
updates, cron and maintenance. The client contains no scheduling engine.

From a checkout on the supported Linux ARM64 platform:

```sh
./aurora --help
./aurora job validate examples/jobs/process-service.json
export AURORA_SCHEDULER=http://LEADER:8081
./aurora job check examples/jobs/process-service.json
./aurora job create examples/jobs/process-service.json
./aurora job status fixtures/test/process-service
./aurora update start examples/jobs/process-service.json
./aurora job kill fixtures/test/process-service
```

`./aurora` bootstraps the pinned Go compiler and executes the client. It requires
no Python. `--offline` prevents bootstrap downloads when the verified archive is
already seeded. Build a standalone client with its adjacent provenance receipt:

```sh
build-support/bootstrap-go build-client --output "$PWD/.cache/client/aurora"
```

The resulting `aurora` binary needs no source checkout, compiler or Python at
runtime. Add `--offline` to the build command when the verified Go archive is cached.

Global options precede the command. Set `--scheduler` explicitly or use
`AURORA_SCHEDULER`; the URL must identify the leader's `/api` endpoint. HTTPS uses
normal hostname verification, with optional `--ca` and paired `--cert`/`--key`.
The current client does not implement the retired Python Kerberos/SPNEGO plugin
or ZooKeeper discovery. Use the original API's supported authentication gateway
and leader endpoint for such deployments; the isolated Pi lab uses private HTTP.
TLS configuration is not a substitute for enabling authentication on the server.

Every request has a timeout and a 1 MiB request/reply limit. Redirects are refused.
Mutations are sent once: if a connection fails after submission, inspect current
job or update state before retrying. `--message` records the caller's audit
message for supported mutations. Exit zero means success; failures use nonzero.

`job status` emits task IDs, hostnames, instance IDs and original numeric task
statuses. Other remote commands emit the original Thrift response struct as JSON.
`rpc METHOD FILE` supports the retained API's numeric-field argument format for
operations without a convenience command. It does not bypass server validation
or authorization. Files can be `-` for standard input.

## Job document

`version`, `job`, `resources` and `process.argv` are required. See the complete
[service](../../examples/jobs/process-service.json),
[batch](../../examples/jobs/process-batch.json) and
[cron](../../examples/jobs/process-cron.json) examples.

| Field | Meaning / default |
| --- | --- |
| `version` | `aurora-job-v1` |
| `job` | `role`, `environment`, `name`; each matches `[a-z][a-z0-9-]{0,63}` |
| `instances` | Positive instance count, default 1 |
| `service` | Restart through the original service policy, default false |
| `user` | Original API identity metadata, default job role |
| `priority` | Original scheduler priority, default 0 |
| `maxTaskFailures` | Original scheduler failure policy, default 1 |
| `tier` | Original scheduler tier, default `preferred`; revocable CPU unsupported |
| `resources` | Positive integer `cpuMillis`, `ramMb`, `diskMb` |
| `process.argv` | 1–64 literal arguments; executable must be an absolute path |
| `process.env` | Explicit string environment, default `{}` |
| `process.graceMillis` | Stop grace 0–60000 ms, default 1000 |
| `constraints` | Original attribute constraints, default `[]` |
| `contactEmail`, `metadata` | Optional original UI metadata |
| `cronSchedule` | Optional original cron expression; cron jobs cannot be services |
| `cronCollisionPolicy` | `KILL_EXISTING` (default) or `CANCEL_NEW` |
| `update` | Batch size, failure limits, minimum running time and rollback policy |

Constraints use either `{"name":"rack","values":["rack-a"],"negated":false}`
or `{"name":"host","limit":1}`. Names and value sets cannot contain duplicates.
Update defaults are one instance per batch, zero tolerated failures, 1000 ms
minimum running time and automatic rollback. `update start` requires a service;
`update diff` asks the existing updater for its proposed changes.

`job validate` checks the document locally; `job check` also calls the scheduler's
`populateJobConfig` to validate current tiers and configuration policies.
Unknown fields and duplicate JSON keys are rejected. Configuration is data:
there is no Python evaluation, template expansion, implicit shell or environment
substitution. An explicit `/bin/sh -c ...` command is a user-selected process.

## Migration from Python / Thermos

Executable `.aurora` files, the Python CLI, Thermos runner/executor/observer,
Pants and PEX workflows are retired. Convert the desired task into one explicit
process and materialize its arguments, environment and resource values in JSON.
There is no automatic conversion of arbitrary Python programs.

The supported Go executor runs trusted host processes in the agent's environment.
The `user` field does not switch operating-system users. Memory is reserved for
placement, not enforced by cgroups; disk is scheduler accounting. This profile
does not provide Thermos process graphs, finalizers, ephemeral daemons, service
announcement, HTTP health checks, images, volume mounts, artifact fetching,
named ports, GPUs or revocable CPU. Unsupported execution configurations fail
validation. The retained empty `MesosContainer` wire value is a compatibility
sentinel and does not load or contact Mesos.

Drain old workloads before importing historical backups; running Thermos tasks
cannot be adopted by the Go agent. Follow [offline recovery](../operations/backup-restore.md).
The lab's persistent agent journals and SQLite receipts support restart recovery;
production multi-scheduler HA is a later slice.
