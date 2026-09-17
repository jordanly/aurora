# Aurora scheduler storage

The original `SchedulerMain` uses one local SQLite database for scheduler metadata,
cron jobs, tasks, quotas, host attributes, job updates and host maintenance. The
same database holds Go-agent command intents, observation receipts and retirement
fences. Mesos replicated-log, H2 and MyBatis storage are no longer used by this
scheduler. This deployment has one storage owner; replication and scheduler HA
remain separate work.

## Ownership and transactions

Startup acquires the database's stable `.owner` file lock and advances a durable
ownership epoch. A second owner is refused. Each transaction verifies that epoch
and session before accessing data. Use a local filesystem and keep the database,
lock file and SQLite sidecars together under a trusted administrator's control.

SQLite runs in WAL mode with `synchronous=FULL`. Each outer read sees a committed
snapshot. An outer write commits all store changes, command intents and observation
receipts together. Nested writes join that transaction; a nested failure marks it
rollback-only even if the caller catches the exception. Read-to-write promotion is
refused. Store handles and JDBC resources must not escape their callback.

Task and host events publish only after commit, in commit order. Publication is
volatile; startup reconstructs policy subscribers from durable task state. Failed
writes or event publication stop further scheduler writes because policy caches
may have changed. Callbacks are never automatically replayed. Restart and reconcile
from durable state before resuming operation.

The owner retains one idle connection until shutdown so opening a transaction does
not race the last connection's WAL cleanup. It does not keep an open transaction
or prevent checkpoint progress. Connection cleanup failure retains ownership until
cleanup succeeds.

## Transaction outcome retention

Routine `Storage.write(work)` calls use reserved `aurora-auto:` operation IDs and
one durable outcome receipt. The next successful automatic write replaces that
receipt atomically with its own effects. A commit acknowledgment failure blocks
subsequent writes until the uncertain operation is reconciled; it cannot be hidden
by a newer receipt.

An automatic ID cannot be submitted through the explicit-ID write API. If its
receipt has expired, `isCommitted` throws rather than treating the operation as
uncommitted. After a process restart, an absent automatic receipt is likewise
unknown: inspect domain state rather than retrying the old callback. A retained
receipt still proves commitment across restart.

The explicit `SqliteStorage.write(id, work)` API preserves durable replay
protection for caller-chosen IDs. A repeated committed ID is rejected before work
runs. New IDs must contain 1–256 characters. At 4,096 explicit receipts, further
new explicit-ID writes are refused; automatic scheduler writes continue. Legacy
receipts are preserved on upgrade, including databases already above that limit.
There is no age-based deletion that could make an old explicit ID executable again.

## Agent history and storage capacity

Schema version 4 adds per-node attempt tickets, compact retired-ticket intervals,
receipt watermarks and the automatic transaction receipt. Retirement is coordinated
with the agent after terminal cleanup, command acknowledgment and supervisor
acknowledgment. The scheduler keeps 64 completed attempts per node by default;
`-Daurora.go.retained-completed=0..896` configures that window. Older logs become
unavailable after retirement. See the [agent retention contract](../../agent/README.md).

Upgrading an existing node requires a one-time drain of legacy tasks before ticket
mode activates. Pending or uncertain legacy work blocks the transition. Existing
node incarnation/journal scope cannot be changed inside the same database; ordinary
daemon and scheduler restarts retain that identity.

Retiring records bounds future execution-history growth; it does not shrink an
already allocated database file. SQLite can reuse freed pages. The agent's offline
`compact` command can publish a smaller validated journal copy. Live task/job
configuration, retained scheduler task/update history, backup files and externally
managed data have their own capacity and retention policies. Monitor disk usage;
these execution bounds are not a fixed byte limit for the entire installation.

## Backup and recovery

Use the scheduler's consistent backup facility. Copying only a live main database
can omit committed data still in its WAL. Backups include ownership, all seven
stores, command/receipt state and retirement fences. Restore to a new path and keep
the source unchanged. Older backups cannot erase later agent replay fences or
establish ownership of unknown workloads.

See [backup, restore and historical snapshot import](backup-restore.md) for commands,
retention settings, validation and recovery limits.
