# Native SQL foundation

`NativeSqlStore` is a bounded native scheduler persistence component, separate
from legacy `Storage`; no unsupported legacy store is implemented or silently
made volatile. It uses one SQLite database per state directory and an exclusive
OS file lock for cooperating scheduler processes. This is a single-host owner
lock, not distributed fencing or an HA database.

`write` commits all job/membership/attempt/allocation/command/observation changes
atomically. Nested writes reuse the transaction; an escaping checked exception,
runtime exception **or Error** permanently poisons the outer write even if its
caller catches it. All failed mutations, including validation and outcome conflicts, poison it too. Independent reads use
separate WAL snapshots and cannot observe uncommitted writes. Transaction views
reject use after callback exit or on another thread; JDBC and mutable caches
are not exposed. Reads inside a write see that write's transaction with a read-only scope; even a
captured outer view cannot mutate during that scope. A failed nested read also
poisons the outer transaction.

Jobs and desired instance membership survive attempt completion. Cancellation
clears membership while preserving attempt and allocation history. Allocation
FKs require an attempt; command FKs require an allocation. Commands compare
immutable body and attempt on ID replay, retain their original body, and have a
separate pending flag. Observation cursors dedupe by node/journal within the
store's persisted cluster/recovery identity. Gap receipts persist without
advancing the contiguous cursor. Counters use validated decimal strings and
BigInteger, never floating point or signed 64-bit storage.

Callers must perform protocol/admission validation before submitting opaque
canonical body strings. This component is not an intake validator or placement policy. The isolated
`scheduler/native` daemon provides the controller; SQL exposes its durable epoch
and monotonic attempt projection.
A transport may use a cursor only after the outer transaction returns
successfully; a value read inside a callback is not permission to ACK. Observation reduction shares the same write as receipt in the native daemon. No connection/session
or execution fencing is inferred from storage identity.

Every connection requires `journal_mode=WAL`, `synchronous=FULL`, foreign keys,
and read-uncommitted disabled. Schema version 2 and application ID identify the
file; unknown versions, existing empty/unidentified databases, a deleted database in a
previously owned directory, missing metadata, and
changed schema definitions reject. Startup also checks SQLite integrity and
foreign keys. Metadata is initialized only with a new database. State and owner
symlinks reject, and initial creation forces the parent directory to storage. Version 1 is verified before transactional migration to version 2. The migration
adds scheduler configuration/epoch and attempt observation projections.
`snapshot(newDirectory)` uses SQLite [VACUUM INTO](https://www.sqlite.org/lang_vacuum.html)
under the store writer lock, followed by file and directory synchronization.
Existing destinations and symlink ancestors reject. Restored copies are checked
by the normal constructor; this is not a live-cluster takeover mechanism. Commit I/O failures are surfaced; their
outcomes require reopen/reconciliation before external effects. Tests cover
process crash boundaries, not injected uncertain-commit I/O failure, media
corruption, physical power loss or uncertain distributed ownership.

## Pinned native dependency and executed evidence

Pinned `org.xerial:sqlite-jdbc:3.53.4.0`. The upstream
[tagged POM](https://raw.githubusercontent.com/xerial/sqlite-jdbc/3.53.4.0/pom.xml)
sets compiler release 8 and packages Linux natives; the
[upstream release](https://github.com/xerial/sqlite-jdbc/releases/tag/3.53.4.0)
is the version source. No other dependency modernization was made.

Executed on 2026-09-10, aarch64 Raspberry Pi, kernel
`6.18.39+rpt-rpi-2712`, `getconf PAGESIZE` = `16384`, Temurin `1.8.0_462`:

- Java-only standalone compile plus CLI using only the JDBC jar succeeded:
  `NATIVE_SQL_OK sqlite-jdbc=3.53.4.0 WAL FULL reopen rollback-only arch=aarch64 java=1.8.0_462`.
- JDBC jar SHA-256:
  `bcb1f51e36f940867e83342f9efbf5968ac44a6bef4d397bb4af7b17b45cd2fb`.
- Embedded `org/sqlite/native/Linux/aarch64/libsqlitejdbc.so` SHA-256:
  `4e253e3f886f8da539e6d8fbd92c3a281f50e395bcba568ced953865e8bea5be`.
- `readelf -l` shows both LOAD segments aligned to `0x10000`;
  actual JNI execution above is the compatibility proof on this kernel.
- `build-support/java/gradle-local focusedTest --tests '*sql.NativeSqlStoreTest'`
  runs the focused suite using Java 8 / Gradle 4.10.2 with at most two workers.
  The isolated scheduler build also includes this SQL suite, with migration and
  snapshot regressions in addition to the original eleven tests. Tests cover atomic rollback, nested Exception/Error, reader snapshot/no dirty
  read, terminal batch/cancelled membership persistence, immutable command
  conflicts, dedupe/cursor gaps and scope, ownership/schema rejection, escaped
  views, uint64 boundary and actual subprocess halt before/after commit.

## Standalone CLI for native image qualification

Compile only this directory's Java sources (no Mesos classpath):

```sh
javac -d CLASSES src/main/java/org/apache/aurora/scheduler/storage/sql/*.java
java -cp 'CLASSES:sqlite-jdbc-3.53.4.0.jar' \
  org.apache.aurora.scheduler.storage.sql.NativeStoreTool self-check STATE_DIR
```

The supplied directory is durably modified with a fixed lab test identity and
job. Repeating self-check is supported. `crash-before` and `crash-after` are
subprocess-test modes that halt with status 71/72 without closing the store.
This is native JDBC and persistence qualification, not an integrated scheduler.
