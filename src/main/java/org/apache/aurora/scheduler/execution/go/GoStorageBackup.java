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
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.sqlite.SqliteStorage;

/** Complete SQLite backups include execution intentions and receipt cursors. */
final class GoStorageBackup implements StorageBackup, Recovery, SnapshotStore {
  private final SqliteStorage storage;
  private final Path directory;

  @Inject
  GoStorageBackup(SqliteStorage storage, GoAgentConfig config) {
    this.storage = storage;
    directory = Objects.requireNonNull(config.database().toAbsolutePath().getParent(),
        "Database must name a file").resolve("backups");
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
  public void backupNow() {
    try {
      Files.createDirectories(directory);
      storage.backup(directory.resolve("backup-" + UUID.randomUUID() + ".db"));
    } catch (IOException e) {
      throw new RecoveryException("Unable to write SQLite backup", e);
    }
  }

  @Override
  public Set<String> listBackups() {
    if (!Files.exists(directory)) {
      return Set.of();
    }
    try (var files = Files.list(directory)) {
      return files.filter(Files::isRegularFile)
          .map(path -> Objects.requireNonNull(path.getFileName()).toString())
          .filter(name -> name.startsWith("backup-") && name.endsWith(".db"))
          .collect(Collectors.toSet());
    } catch (IOException e) {
      throw new RecoveryException("Unable to list SQLite backups", e);
    }
  }

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
