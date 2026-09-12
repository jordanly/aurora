# INPLACE-04 storage contract — preparation only

This [INPLACE-04](IN_PLACE_MODERNIZATION_PLAN.md) preparation closes no implementation or HA gate.

## Ownership and durability scope

The initial backend is SQLite on local persistent storage, owned exclusively by
one scheduler on this Pi. Acquire an OS ownership lock before opening storage;
retain it until connections close and reject a second scheduler at startup.
Use one database for all seven stores and transactional metadata.
Require zero acknowledged-commit loss after process crash with intact storage.
Host loss, media failure and power loss need separate qualification.

Production availability, numeric RPO/RTO, recovery capacity and covered failure
domains remain unspecified. Production backend
selection, replication, promotion eligibility and fencing require a separate
decision before implementation of that profile and before HA cutover. A local
ownership lock does not replace the Mesos replicated log's availability contract.

## Existing contracts and deliberate corrections

[Storage.java](../../src/main/java/org/apache/aurora/scheduler/storage/Storage.java)
exposes scheduler metadata, cron jobs, tasks, quotas, host attributes, job updates
and host maintenance through one provider. Preserve every store operation,
query, identity, uniqueness and ordering contract, task events, update history
and recovery metadata. Retain public Thrift fields and immutable entities.

[DurableStorage](../../src/main/java/org/apache/aurora/scheduler/storage/durability/DurableStorage.java)
collects nested operations and persists them after applying delegated mutations.
[WriteRecorder](../../src/main/java/org/apache/aurora/scheduler/storage/durability/WriteRecorder.java)
also posts host-attribute events during mutation. Moving only the operation log
into SQL does not make those memory changes or events transactional.

[StorageTransactionTest](../../src/test/java/org/apache/aurora/scheduler/storage/mem/StorageTransactionTest.java)'s
`testWritesUnderTransaction` and `testOperations`
retain quota/task changes after failures, including nested failures. `Storage.read`
documents dirty reads. Correcting these is an intentional change requiring new
conformance assertions; the existing implementation lacks rollback/isolation.

## Required transaction behavior

- One outer `Storage.write` commits all seven stores, outbox intents and receipts
  together or changes none. Initialize/recover before serving policy operations;
  preserve initialization writes and reject unsupported schema versions.
- Nested writes join the same transaction and connection. Any escaping nested
  failure marks the outer transaction rollback-only, even if the caller catches
  it. Outer completion must then fail; savepoint rollback must not enable a
  partial outer commit. Rollback covers checked exceptions and unchecked failures.
- Each independent `read` callback sees one committed snapshot across stores;
  concurrent readers can proceed independently. Reads nested inside a write see
  its own changes on that transaction. No mutable store or cursor may escape its
  transaction lifetime. Read-to-write promotion must have an explicit tested
  contract; never silently open an unrelated writer or replay policy callbacks.
- Release resources and roll back on failed work. Preserve the primary failure
  if rollback/close also fails. Bounded busy handling must distinguish a retryable
  database operation from rerunning non-idempotent work. SQLite does not nest
  `BEGIN` transactions, and some errors leave a transaction active; the adapter
  must track and resolve that state. [SQLite transactions](https://sqlite.org/lang_transaction.html).
- A commit exception or lost acknowledgment is an uncertain result, not proof of
  rollback. Recover using a stable operation identity and a transactional outcome
  record before retrying; committed intents must remain dispatchable after restart.
- Persist stable command IDs, task/agent identity, ordering, ownership epoch,
  payload version and delivery state. Persist observation receipts with their state transition;
  duplicate observations must not reapply counters or enqueue duplicate commands.
- An ownership epoch check must serialize with ownership changes and the guarded
  writes in the same database transaction; a separate check-then-write is not
  fencing. External command/agent fencing remains part of the later HA contract.
- INPLACE-05 must buffer events and dispatch commands only after outermost durable
  commit, preserving policy ordering. Failed transactions publish nothing. Durable
  replay handles the commit-to-publication crash window; consumers deduplicate.

## SQLite, schema and recovery prerequisites

Verify WAL mode and `synchronous=FULL` on applicable connections at startup;
FULL syncs the WAL at commit. Use local storage, monitor checkpoint progress and
bound transaction lifetimes. [SQLite WAL](https://sqlite.org/wal.html),
[synchronous settings](https://sqlite.org/pragma.html#pragma_synchronous).
Qualify/pin JDBC and its native SQLite version on ARM64, including the WAL-reset
corruption fix identified in the WAL docs. No version is selected here.

Record schema/payload versions and migration history; reject unsupported schemas.
Test interrupted migrations before writes. Use a consistent backup snapshot or
a verified quiesced procedure, never by copying only the live main database and
discarding its WAL. Restore into an isolated location and check all seven stores,
outbox and receipts. [Backup API](https://sqlite.org/backup.html), [WAL handling](https://sqlite.org/wal.html#the_wal_file).

## Ordered acceptance increments

1. Qualify dependency/ARM64 loading, settings, ownership exclusion and schema startup.
2. Build the shared transaction harness; prove rollback-only nesting, snapshot reads,
   commit uncertainty handling and resource cleanup with bounded failure tests.
3. Implement scheduler/cron/quota/attribute/maintenance stores, then task and update
   stores; run existing contracts and corrected transaction expectations.
4. Prove actual controller cross-store invariants with uniqueness conflicts,
   disk-full/I/O failure, process termination around commit, and restart recovery.
5. Add atomic outbox/receipt conformance and consistent backup/restore/migration
   tests; connect postcommit delivery only with INPLACE-05 acceptance evidence.
6. Keep production HA cutover blocked until declared objectives, storage ownership,
   fencing, eligible promotion and measured failure/recovery evidence are approved.
