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
package org.apache.aurora.scheduler.execution.go;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import jakarta.inject.Inject;

import com.google.common.util.concurrent.AbstractScheduledService;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.backup.BackupModule;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.sqlite.SqliteStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Complete SQLite backups include execution intentions and receipt cursors. */
final class GoStorageBackup extends AbstractScheduledService
    implements StorageBackup, Recovery, SnapshotStore {
  private static final Logger LOG = LoggerFactory.getLogger(GoStorageBackup.class);
  private static final Pattern BACKUP_NAME = Pattern.compile(
      "backup-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.db");
  private final SqliteStorage storage;
  private final Path database;
  private final Path directory;
  private final long intervalMillis;
  private final int maxSavedBackups;
  private final AtomicLong lastSuccessMillis = new AtomicLong();
  private final AtomicLong successes;
  private final AtomicLong failures;
  private final AtomicLong retentionFailures;

  @Inject
  GoStorageBackup(SqliteStorage storage, GoAgentConfig config, BackupModule.Options options,
                  StatsProvider statsProvider) {
    this.storage = Objects.requireNonNull(storage);
    database = config.database().toAbsolutePath().normalize();
    Path defaultDirectory = Objects.requireNonNull(database.getParent(),
        "Database must name a file").resolve("backups");
    directory = options.backupDir == null
        ? defaultDirectory : options.backupDir.toPath().toAbsolutePath().normalize();
    TimeAmount interval = Objects.requireNonNull(options.backupInterval);
    intervalMillis = interval.as(org.apache.aurora.common.quantity.Time.MILLISECONDS);
    maxSavedBackups = options.maxSavedBackups;
    if (intervalMillis <= 0) {
      throw new IllegalArgumentException("Backup interval must be positive");
    }
    if (maxSavedBackups < 1) {
      throw new IllegalArgumentException("Maximum saved backups must be at least one");
    }
    StatsProvider stats = Objects.requireNonNull(statsProvider);
    successes = stats.makeCounter("scheduler_backup_success");
    failures = stats.makeCounter("scheduler_backup_failed");
    retentionFailures = stats.makeCounter("scheduler_backup_retention_failed");
    stats.makeGauge("scheduler_backup_last_success_ms",
        () -> lastSuccessMillis.get() == 0 ? -1 : lastSuccessMillis.get());
    stats.makeGauge("scheduler_backup_last_success_age_ms", () -> {
      long lastSuccess = lastSuccessMillis.get();
      return lastSuccess == 0 ? -1 : Math.max(0, System.currentTimeMillis() - lastSuccess);
    });
  }

  @Override
  protected void runOneIteration() {
    try {
      backupNow();
    } catch (RuntimeException e) {
      LOG.error("Scheduled SQLite backup failed; a later attempt will retry", e);
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public void snapshot() {
    backupNow();
  }

  @Override
  public void snapshotWith(Snapshot snapshot) {
    throw offlineOnly();
  }

  @Override
  public synchronized void backupNow() {
    Path published = directory.resolve("backup-" + UUID.randomUUID() + ".db");
    try {
      Files.createDirectories(directory);
      storage.backup(published);
      successes.incrementAndGet();
      lastSuccessMillis.set(System.currentTimeMillis());
    } catch (IOException | RuntimeException e) {
      failures.incrementAndGet();
      throw new RecoveryException("Unable to write SQLite backup", e);
    }
    try {
      pruneBackups(published);
    } catch (IOException | RuntimeException e) {
      retentionFailures.incrementAndGet();
      LOG.error("SQLite backup was published, but retention cleanup failed", e);
      throw new RecoveryException("SQLite backup was published, but retention cleanup failed", e);
    }
  }

  @Override
  public Set<String> listBackups() {
    if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
      return Set.of();
    }
    try (var files = Files.list(directory)) {
      return files.filter(this::isBackupFile)
          .map(path -> Objects.requireNonNull(path.getFileName()).toString())
          .collect(Collectors.toUnmodifiableSet());
    } catch (IOException e) {
      throw new RecoveryException("Unable to list SQLite backups", e);
    }
  }

  private boolean isBackupFile(Path path) {
    String name = Objects.requireNonNull(path.getFileName()).toString();
    if (!BACKUP_NAME.matcher(name).matches()
        || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
        || path.toAbsolutePath().normalize().equals(database)) {
      return false;
    }
    try {
      return !Files.exists(database, LinkOption.NOFOLLOW_LINKS)
          || !Files.isSameFile(path, database);
    } catch (IOException e) {
      return false;
    }
  }

  private void pruneBackups(Path justPublished) throws IOException {
    try (var files = Files.list(directory)) {
      var backups = files.filter(this::isBackupFile)
          .map(path -> new BackupFile(path, modified(path)))
          .filter(item -> !item.path().equals(justPublished))
          .sorted(Comparator.comparing(BackupFile::modified)
              .thenComparing(item -> Objects.requireNonNull(item.path().getFileName()).toString()))
          .toList();
      int excess = backups.size() + 1 - maxSavedBackups;
      for (int i = 0; i < excess; i++) {
        Files.delete(backups.get(i).path());
      }
    }
  }

  private static FileTime modified(Path path) {
    try {
      return Files.getLastModifiedTime(path, LinkOption.NOFOLLOW_LINKS);
    } catch (IOException e) {
      throw new RecoveryException("Unable to read backup modification time: " + path, e);
    }
  }

  private record BackupFile(Path path, FileTime modified) { }

  private static RecoveryException offlineOnly() {
    return new RecoveryException("SQLite restore requires an offline full-database recovery with "
        + "agent fencing; legacy Thrift snapshot editing does not include execution receipts");
  }

  @Override
  public void stage(String backupName) {
    throw offlineOnly();
  }
  @Override
  public Iterable<IScheduledTask> query(Query.Builder query) {
    throw offlineOnly();
  }
  @Override
  public void deleteTasks(Query.Builder query) {
    throw offlineOnly();
  }
  @Override
  public void unload() {
    throw offlineOnly();
  }
  @Override
  public void commit() {
    throw offlineOnly();
  }
}
