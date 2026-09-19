# Historical storage compatibility boundary

BACK-004 retains packaged writable legacy storage for extension compatibility. The required import surface is documented below; the separate BACK-009 fix extends replay through the final historical golden. Writable legacy storage has **not** been removed.

The original `SchedulerMain` installs `GoAgentModule`, which binds SQLite. Its normal production path does not install `DurableStorageModule`, `LogPersistenceModule`, `SnapshotModule`, or `BackupModule`. This establishes that those legacy writers are unused by the supplied entrypoint; it does not establish that their public classes are unused by external modules.

The offline historical-import path in `SqliteRecovery.importHistorical` requires:

- `StrictSnapshot` to reject unsupported or malformed snapshot input before import.
- `BackupReader` and `backup.Recovery.load` to decode historical snapshot files.
- `SnapshotterImpl.asStream` to produce historical operations, retaining the host-agent-ID check.
- `Loader` and `ThriftBackfill` to restore all seven store contracts and backfill supported old forms.
- `SnapshotterImpl.from` and snapshot counts to reject duplicate identities or discarded records.
- Historical Thrift schemas, golden resources, `DataCompatibilityTest`, and SQLite recovery tests.

This work keeps those semantics and the offline-only restrictions of the Go adapter. The corrected compatibility replay includes every prefix through the final golden.

`MoreModules.instantiate` supports reflectively selected public modules with a default constructor or a `CliOptions` constructor. `DurableStorageModule` is public and default-constructible. An external custom module can also install the other public legacy modules using their existing options constructors. Gradle still places these classes in the main output, runtime artifact, and source archive; they are not isolated in a separate compatibility artifact. Moving or deleting them would therefore change the packaged extension surface even though the default application does not instantiate them.

A later removal should define that packaged extension contract, separate read/import dependencies from writable log framing, recording, scheduled snapshots, and editable online recovery, then remove only the exclusive writer surface. It must update source-inventory/retired-path metadata, command-line options and applicable packaging tests together. Original-entrypoint injection, complete SQLite restoration, offline historical import, and the full golden replay remain required checks. Passing the historical goldens alone does not establish compatibility for external custom modules.
