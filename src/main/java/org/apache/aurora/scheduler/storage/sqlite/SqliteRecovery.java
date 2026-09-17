/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.storage.sqlite;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.aurora.common.util.BuildInfo;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.execution.go.GoTaskFactory;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.backup.BackupReader;
import org.apache.aurora.scheduler.storage.durability.Loader;
import org.apache.aurora.scheduler.storage.durability.Persistence.PersistenceException;
import org.apache.aurora.scheduler.storage.durability.ThriftBackfill;
import org.apache.aurora.scheduler.storage.log.SnapshotterImpl;
import org.apache.aurora.scheduler.updater.Updates;

/**
 * Offline recovery at a fresh path. Sources never become storage owners; complete
 * SQLite backups retain every table, including durable commands and receipt history.
 */
public final class SqliteRecovery {
  private static final long MAX_HISTORICAL_BYTES = 256L * 1024 * 1024;
  private static final List<String> SIDECARS = List.of(".owner", "-wal", "-shm", "-journal");
  private static final Set<String> VERSION_THREE_TABLES = Set.of(
      "storage_owner", "storage_transactions", "scheduler_metadata", "cron_jobs", "quotas",
      "attributes", "host_maintenance", "tasks", "job_updates", "command_outbox",
      "observation_receipts", "sqlite_sequence");
  private static final Set<String> RETENTION_TABLES = Set.of(
      "agent_retention", "attempt_retention", "receipt_watermarks", "automatic_outcome");

  private SqliteRecovery() { }

  public static void importSnapshot(Path source, Path destination) {
    recover(source, destination, true);
  }

  public static void restoreBackup(Path source, Path destination) {
    recover(source, destination, false);
  }

  private static void recover(Path source, Path destination, boolean historical) {
    Path workspace = null;
    Throwable primary = null;
    try {
      Path input = source.toAbsolutePath().normalize();
      Path requested = destination.toAbsolutePath().normalize();
      Path parent = Objects.requireNonNull(requested.getParent(),
          "Recovery destination requires a parent directory").toRealPath();
      Path target = parent.resolve(Objects.requireNonNull(requested.getFileName(),
          "Recovery destination requires a filename"));
      require(!Files.exists(target, LinkOption.NOFOLLOW_LINKS),
          "Recovery destination must be a new file");
      noSidecars(target);
      require(Files.isRegularFile(input, LinkOption.NOFOLLOW_LINKS),
          "Recovery source must be a regular, nonsymlink backup file");
      if (!historical) {
        noSidecars(input);
      }
      // Reserve the stable lock pathname before doing work. A scheduler cannot
      // initialize this destination concurrently. Never unlink an ownership lock:
      // another process might already have opened that inode while waiting on it.
      Path owner = target.resolveSibling(target.getFileName() + ".owner");
      try (FileChannel channel = FileChannel.open(owner, StandardOpenOption.CREATE_NEW,
               StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
           FileLock lock = channel.tryLock()) {
        require(lock != null, "Recovery destination already has an owner");
        workspace = Files.createTempDirectory(parent, ".aurora-recovery-");
        Path inputCopy = workspace.resolve("input");
        copySource(input, inputCopy, historical);
        Path payload = workspace.resolve("recovered.db");
        if (historical) {
          importHistorical(inputCopy, workspace.resolve("import.db"), payload);
        } else {
          Files.move(inputCopy, payload);
          validateDatabase(payload, workspace.resolve("validation.db"));
        }
        Files.setPosixFilePermissions(payload, PosixFilePermissions.fromString("rw-------"));
        try (FileChannel file = FileChannel.open(payload, StandardOpenOption.WRITE)) {
          file.force(true);
        }
        // Atomic no-replace publication. Failure before here leaves no database;
        // the empty .owner reservation remains, so retries use a fresh pathname.
        Files.createLink(target, payload);
        Files.delete(payload);
        try (FileChannel directory = FileChannel.open(parent, StandardOpenOption.READ)) {
          directory.force(true);
        }
      }
    } catch (IOException | SQLException e) {
      primary = new StorageException(
          "Offline recovery failed; use a fresh destination for retry", e);
      throw (StorageException) primary;
    } catch (RuntimeException e) {
      primary = new StorageException("Offline recovery rejected the backup or destination", e);
      throw (StorageException) primary;
    } catch (Error e) {
      primary = e;
      throw e;
    } finally {
      if (workspace != null) {
        try (Stream<Path> files = Files.list(workspace)) {
          for (Path file : files.toList()) {
            Files.deleteIfExists(file);
          }
          Files.delete(workspace);
        } catch (IOException e) {
          if (primary != null) {
            primary.addSuppressed(e);
          } else {
            throw new StorageException("Unable to remove recovery staging directory", e);
          }
        }
      }
    }
  }

  private static void noSidecars(Path path) {
    for (String suffix : SIDECARS) {
      require(!Files.exists(path.resolveSibling(path.getFileName() + suffix),
          LinkOption.NOFOLLOW_LINKS),
          "Require a standalone offline backup/new destination without sidecars: " + path);
    }
  }

  private static void copySource(Path source, Path copy, boolean historical) throws IOException {
    BasicFileAttributes before = Files.readAttributes(source, BasicFileAttributes.class,
        LinkOption.NOFOLLOW_LINKS);
    require(before.isRegularFile() && before.size() > 0, "Empty or invalid backup file");
    require(!historical || before.size() <= MAX_HISTORICAL_BYTES,
        "Historical snapshot exceeds the 256 MiB import profile");
    try (FileChannel input = FileChannel.open(source, StandardOpenOption.READ,
             LinkOption.NOFOLLOW_LINKS);
         FileChannel output = FileChannel.open(copy, StandardOpenOption.CREATE_NEW,
             StandardOpenOption.WRITE)) {
      long offset = 0;
      while (offset < before.size()) {
        long copied = input.transferTo(offset, before.size() - offset, output);
        require(copied > 0, "Backup changed during copy");
        offset += copied;
      }
      require(input.size() == before.size(), "Backup changed during copy");
      output.force(true);
    }
    BasicFileAttributes after = Files.readAttributes(source, BasicFileAttributes.class,
        LinkOption.NOFOLLOW_LINKS);
    require(Objects.equals(before.fileKey(), after.fileKey())
        && before.size() == after.size()
        && before.lastModifiedTime().equals(after.lastModifiedTime()),
        "Backup identity or content changed during copy");
    if (!historical) {
      noSidecars(source);
    }
  }

  private static void importHistorical(Path input, Path staging, Path payload) throws IOException {
    StrictSnapshot.validate(Files.readAllBytes(input));
    final Snapshot[] captured = new Snapshot[1];
    SnapshotterImpl snapshotter = new SnapshotterImpl(new BuildInfo(), Clock.SYSTEM_CLOCK) {
      @Override
      public Stream<Op> asStream(Snapshot snapshot) {
        captured[0] = snapshot;
        if (snapshot.getHostAttributes() != null) {
          snapshot.getHostAttributes().forEach(host -> require(host.isSetSlaveId(),
              "Historical host attributes without agent IDs would be discarded"));
        }
        return super.asStream(snapshot);
      }
    };
    BackupReader reader = new BackupReader(input.toFile(), snapshotter);
    reader.prepare();
    try (SqliteStorage storage = SqliteStorage.open(staging);
         var edits = reader.recover()) {
      storage.write("historical-snapshot-import", stores -> {
        Loader.load(stores, new ThriftBackfill(), edits);
        validateStores(stores, true);
        Snapshot loaded = snapshotter.from(stores);
        Snapshot original = captured[0];
        require(original != null && counts(original).equals(counts(loaded)),
            "Historical snapshot contains duplicate identities or discarded records");
        return null;
      });
      storage.backup(payload);
    } catch (PersistenceException e) {
      throw new StorageException("Unable to recover historical snapshot", e);
    }
  }

  private static List<Integer> counts(Snapshot snapshot) {
    return List.of(snapshot.getHostAttributesSize(), snapshot.getTasksSize(),
        snapshot.getCronJobsSize(), snapshot.getQuotaConfigurationsSize(),
        snapshot.getJobUpdateDetailsSize(), snapshot.getHostMaintenanceRequestsSize(),
        snapshot.isSetSchedulerMetadata() && snapshot.getSchedulerMetadata().isSetFrameworkId()
            ? 1 : 0);
  }

  private static void validateStores(Storage.StoreProvider stores, boolean historical) {
    stores.getSchedulerStore().fetchFrameworkId();
    stores.getQuotaStore().fetchQuotas();
    stores.getAttributeStore().getHostAttributes();
    stores.getHostMaintenanceStore().getHostMaintenanceRequests();
    for (var task : stores.getTaskStore().fetchTasks(Query.unscoped())) {
      require(task.getStatus() != null && task.getAssignedTask() != null
          && task.getAssignedTask().getTask() != null
          && task.getAssignedTask().getTaskId() != null
          && !task.getAssignedTask().getTaskId().isEmpty(), "Malformed task record");
      if (historical) {
        require(Tasks.isTerminated(task.getStatus()),
            "Historical import requires drained terminal tasks, including PENDING tasks; "
                + "active Thermos/native attempts cannot be adopted without durable receipts");
      } else if (!Tasks.isTerminated(task.getStatus())) {
        validateExecutable(task.getAssignedTask().getTask());
      }
    }
    for (var cron : stores.getCronJobStore().fetchJobs()) {
      validateExecutable(cron.getTaskConfig());
    }
    for (var details : stores.getJobUpdateStore().fetchJobUpdates(JobUpdateStore.MATCH_ALL)) {
      var update = details.getUpdate();
      var status = update.getSummary().getState().getStatus();
      require(status != null, "Job update requires a known status and event history");
      if (historical) {
        require(!Updates.ACTIVE_JOB_UPDATE_STATES.contains(status),
            "Historical import requires completed/aborted job updates; paused updates are active");
      } else if (Updates.ACTIVE_JOB_UPDATE_STATES.contains(status)) {
        if (update.getInstructions().isSetDesiredState()) {
          validateExecutable(update.getInstructions().getDesiredState().getTask());
        }
        update.getInstructions().getInitialState()
            .forEach(instance -> validateExecutable(instance.getTask()));
      }
    }
  }

  private static void validateExecutable(
      org.apache.aurora.scheduler.storage.entities.ITaskConfig task) {
    try {
      GoTaskFactory.validateForImport(task);
    } catch (TaskDescriptionException | RuntimeException e) {
      throw new StorageException("Unsupported executable configuration: convert legacy cron/active "
          + "definitions to the Go process profile before import; no Thermos adoption is provided",
          e);
    }
  }

  private static void validateDatabase(Path payload, Path validation)
      throws SQLException, IOException {
    // Read-only verification does not acquire/bump a source ownership epoch.
    try (var connection = DriverManager.getConnection(
             "jdbc:sqlite:" + payload.toUri().toASCIIString() + "?mode=ro&immutable=1");
         var statement = connection.createStatement()) {
      try (var rows = statement.executeQuery("PRAGMA integrity_check")) {
        if (!rows.next() || !"ok".equals(rows.getString(1)) || rows.next()) {
          throw new StorageException("SQLite backup failed integrity verification");
        }
      }
      int version;
      try (var rows = statement.executeQuery("PRAGMA user_version")) {
        if (!rows.next()) {
          throw new StorageException("Missing SQLite backup schema version");
        }
        version = rows.getInt(1);
        require(version == 3 || version == SqliteDatabase.SCHEMA_VERSION,
            "Unsupported SQLite backup schema version");
      }
      Set<String> tables = new HashSet<>();
      try (var rows = statement.executeQuery("SELECT name,type FROM sqlite_master")) {
        while (rows.next()) {
          String type = rows.getString(2);
          require(!"trigger".equals(type) && !"view".equals(type),
              "Unsupported SQLite backup schema object");
          if ("table".equals(type)) {
            tables.add(rows.getString(1));
          }
        }
      }
      Set<String> expectedTables = new HashSet<>(VERSION_THREE_TABLES);
      if (version >= 4) {
        expectedTables.addAll(RETENTION_TABLES);
      }
      require(expectedTables.equals(tables), "Incomplete or unsupported SQLite backup tables");
    }
    // Decode all seven stores on a separate copy. Opening bumps only this
    // disposable validation copy's epoch; publish the original complete bytes.
    Files.copy(payload, validation);
    try (SqliteStorage storage = SqliteStorage.open(validation)) {
      storage.read(stores -> {
        validateStores(stores, false);
        storage.effects().pending(Integer.MAX_VALUE);
        return null;
      });
    }
  }

  private static void require(boolean accepted, String message) {
    if (!accepted) {
      throw new StorageException(message);
    }
  }
}
