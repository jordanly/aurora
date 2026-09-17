# Offline SQLite recovery and historical snapshot import

The Go-agent scheduler creates a consistent SQLite backup on a fixed delay
using `-backup_interval` (default one hour) and keeps up to
`-max_saved_backups` (default 48). `-backup_dir` selects the backup directory;
when omitted, backups are written to a `backups` directory beside the configured
SQLite database. Manual backup requests use the same directory and retention
policy. Scheduled failures are logged and retried on the next interval; monitor
`scheduler_backup_success`, `scheduler_backup_failed`,
`scheduler_backup_last_success_ms`, and `scheduler_backup_last_success_age_ms`.
The last-success timestamp and age are `-1` until the first successful backup;
retention cleanup errors have a separate `scheduler_backup_retention_failed`
counter because the new backup remains available.
Retention preserves the backup just published, then removes older regular files
named `backup-<UUID>.db` by modification time and filename until the configured
limit is met. This keeps the new recovery point when an older backup has a future
timestamp. It leaves symbolic links, temporary and unrecognized files, and the
live database untouched.

The recovery tool publishes a new database file. It never overwrites an existing
database or opens its source as a scheduler storage owner. Stop the scheduler
before selecting a recovery database and keep the source backup unchanged.
The destination parent directory must already exist on the local filesystem and
be controlled by the scheduler administrator. Recovery does not provide HA
handoff, agent enrollment changes, or adoption of historical Thermos processes.

## Restore a complete SQLite backup

Use a standalone backup produced by `SqliteStorage.backup`, which takes a
consistent SQLite snapshot including all seven stores, transaction outcomes,
command outbox entries (including acknowledgements), observation receipts, and
ownership metadata. A copy of a live database file is not a backup: its WAL may
contain committed data that the main file does not contain.

```sh
recovery-tool -from=SQLITE -to=SQLITE \
  -backup=/var/backups/aurora/scheduler.backup \
  -sqlite-destination=/var/lib/aurora/recovered.db
```

The source must be a regular nonsymlink file without `.owner`, `-wal`, `-shm`, or
`-journal` sidecars. The tool checks source identity, size, and modification time
around the copy and rejects changes. It checks database integrity, the supported
schema/table set, and every stored Thrift record on a disposable validation copy.
Complete schema-version 3 and 4 backups are supported. A version-3 copy is
validated through the version-4 migration; publication still preserves the
original version-3 bytes, and the scheduler migrates them when it opens the
destination. Version 4 also preserves ticket retirement fences and receipt
watermarks. Earlier incomplete schemas are refused.
Validation never changes the source ownership epoch. Publication preserves the
original complete backup bytes; opening the recovered database as the scheduler
subsequently acquires a new local ownership epoch.

Choose a backup consistent with the intended enrollment and agent journals.
A historical database does not erase later agent command identities or permit
reusing them. Agent reconnect and reconciliation still enforce the retained
journal, command, session, and receipt checks; recovery does not claim that a
stale database can safely adopt arbitrary workloads.

## Import a historical Thrift snapshot

A historical scheduler snapshot contains the seven original stores but does not
contain the native command outbox or observation receipts. It therefore cannot
establish ownership of an active attempt, even if that task uses a native
executor. The import profile requires:

* Every task is terminal. `PENDING`, assigned, running, killing, and other active
  tasks are refused. Drain/terminate legacy execution using its original system
  and obtain the resulting snapshot first; the tool does not kill workloads or
  rewrite task status to pretend cleanup occurred.
* Every job update is completed or aborted. Paused and blocked updates are still
  active and are refused.
* Every cron definition uses the supported `go-process` executor and passes the
  shared process-profile validation. Convert legacy executable definitions before
  import; terminal Thermos task records and completed update history remain intact.
  Deployment-specific tier/enrollment feasibility is still checked by the scheduler.

```sh
recovery-tool -from=BACKUP -to=SQLITE \
  -backup=/var/backups/aurora/scheduler-backup-2026-09-12-00-00 \
  -sqlite-destination=/var/lib/aurora/imported.db
```

The retained `BackupReader`, `SnapshotterImpl`, `Loader`, and `ThriftBackfill`
interpret the original serialized schema. The import writes scheduler metadata,
cron jobs, tasks, quotas, host attributes, job updates with their events, and host
maintenance requests in one transaction. Duplicate record identities, records
that would otherwise be discarded, malformed/unsupported fields or enum values,
and trailing bytes are refused. No generated Thrift schema or compatibility
golden is rewritten. Historical imports are limited to 256 MiB, nesting depth 64,
and one million entries per collection; unsupported older fields require a
separately reviewed conversion instead of silent data loss.

## Publication and failures

The destination database and all sidecars must be absent, including empty files.
The tool reserves the stable `.owner` lock pathname and holds its lock throughout
staging and publication, so a scheduler cannot initialize that path concurrently.
It validates a temporary database, fsyncs the result, and publishes it with an
atomic filesystem link that cannot replace a concurrently created destination.
Failures before publication leave no partial database.

An empty `.owner` reservation can remain after a failed attempt. Retry at a fresh
destination pathname; the tool deliberately does not unlink ownership lock files
that another process might already have opened. After successful recovery, stop
any prior owner, point the scheduler's SQLite configuration at the recovered
path, and start the single scheduler. Keep the original backup for inspection.
`recovery-tool --help` lists the supported endpoints and options; the old `LOG`
endpoint and Mesos/ZooKeeper recovery flags are not supported.
