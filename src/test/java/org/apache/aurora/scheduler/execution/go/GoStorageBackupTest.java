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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.backup.BackupModule;
import org.apache.aurora.scheduler.storage.backup.Recovery.RecoveryException;
import org.apache.aurora.scheduler.storage.sqlite.SqliteEffects.Command;
import org.apache.aurora.scheduler.storage.sqlite.SqliteEffects.ReceiptKey;
import org.apache.aurora.scheduler.storage.sqlite.SqliteStorage;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GoStorageBackupTest {
  @Rule
  public TemporaryFolder temporary = new TemporaryFolder();
  private final FakeStatsProvider statsProvider = new FakeStatsProvider();

  @Test
  public void testConfiguredDirectoryAndRestoresTaskOutboxAndReceipts() throws Exception {
    Path root = temporary.getRoot().toPath();
    Path db = root.resolve("scheduler.db");
    Path configured = root.resolve("configured-backups");
    try (SqliteStorage storage = SqliteStorage.open(db)) {
      storage.write((NoResult.Quiet) stores -> {
        stores.getUnsafeTaskStore().saveTasks(ImmutableSet.of(
            TaskTestUtil.makeTask("task", TaskTestUtil.JOB)));
        storage.effects().enqueue(new Command("run-1", "agent", "task", "Run", 1,
            new byte[] {1, 2}));
        storage.effects().recordReceipt(new ReceiptKey("agent", "boot", 0), 1,
            new byte[] {3, 4});
      });

      GoStorageBackup backup = backup(storage, db, configured, 10, 2);
      assertEquals(-1, statsProvider.getLongValue("scheduler_backup_last_success_ms"));
      assertEquals(-1, statsProvider.getLongValue("scheduler_backup_last_success_age_ms"));
      backup.backupNow();
      long lastSuccess = statsProvider.getLongValue("scheduler_backup_last_success_ms");
      assertTrue(lastSuccess > 0);
      Thread.sleep(5);
      assertTrue(statsProvider.getLongValue("scheduler_backup_last_success_age_ms") > 0);
      assertEquals(1, backup.listBackups().size());
      Path created = configured.resolve(backup.listBackups().iterator().next());
      try (SqliteStorage restored = SqliteStorage.open(created)) {
        restored.read(stores -> {
          assertTrue(stores.getTaskStore().fetchTask("task").isPresent());
          assertEquals("run-1", restored.effects().pending("agent", 10).get(0)
              .command().id());
          assertTrue(restored.effects().hasReceipt(new ReceiptKey("agent", "boot", 0)));
          return null;
        });
      }
    }
  }

  @Test
  public void testRetentionUsesOwnedRegularFilesAndPreservesLiveDatabase() throws Exception {
    Path root = temporary.getRoot().toPath();
    String liveName = "backup-" + UUID.randomUUID() + ".db";
    Path db = root.resolve(liveName);
    Path old = root.resolve("backup-" + UUID.randomUUID() + ".db");
    Path unrelated = root.resolve("backup-foreign.db");
    Path temp = root.resolve("backup-" + UUID.randomUUID() + ".db.tmp");
    Path symlink = root.resolve("backup-" + UUID.randomUUID() + ".db");
    Files.write(old, new byte[] {1});
    Files.setLastModifiedTime(old, FileTime.fromMillis(1));
    Files.write(unrelated, new byte[] {2});
    Files.write(temp, new byte[] {3});
    Files.createSymbolicLink(symlink, unrelated);
    try (SqliteStorage storage = SqliteStorage.open(db)) {
      GoStorageBackup backup = backup(storage, db, root, 1, 1);
      backup.backupNow();
      assertTrue(Files.exists(db));
      assertFalse(Files.exists(old));
      assertTrue(Files.exists(unrelated));
      assertTrue(Files.exists(temp));
      assertTrue(Files.isSymbolicLink(symlink));
      assertEquals(1, backup.listBackups().size());
    }
  }

  @Test
  public void testRetentionKeepsNewBackupWhenExistingBackupHasFutureMtime() throws Exception {
    Path root = temporary.getRoot().toPath();
    Path db = root.resolve("scheduler.db");
    Path directory = root.resolve("backups");
    try (SqliteStorage storage = SqliteStorage.open(db)) {
      storage.write((NoResult.Quiet) stores -> stores.getUnsafeTaskStore().saveTasks(
          ImmutableSet.of(TaskTestUtil.makeTask("before", TaskTestUtil.JOB))));
      GoStorageBackup backup = backup(storage, db, directory, 1000, 1);
      backup.backupNow();
      Path prior = directory.resolve(backup.listBackups().iterator().next());
      Files.setLastModifiedTime(prior, FileTime.fromMillis(System.currentTimeMillis() + 60_000));

      storage.write((NoResult.Quiet) stores -> stores.getUnsafeTaskStore().saveTasks(
          ImmutableSet.of(TaskTestUtil.makeTask("after", TaskTestUtil.JOB))));
      backup.backupNow();

      assertEquals(1, backup.listBackups().size());
      String freshName = backup.listBackups().iterator().next();
      assertNotEquals(prior.getFileName().toString(), freshName);
      try (SqliteStorage restored = SqliteStorage.open(directory.resolve(freshName))) {
        assertTrue(restored.read(stores -> stores.getTaskStore().fetchTask("after").isPresent()));
      }
    }
  }

  @Test
  public void testScheduledFailureDoesNotStopRetryAndServiceStops() throws Exception {
    Path root = temporary.getRoot().toPath();
    Path db = root.resolve("scheduler.db");
    Path directory = root.resolve("backups");
    Files.write(directory, new byte[] {1});
    try (SqliteStorage storage = SqliteStorage.open(db)) {
      GoStorageBackup backup = backup(storage, db, directory, 25, 2);
      long failures = statsProvider.getLongValue("scheduler_backup_failed");
      try {
        backup.startAsync().awaitRunning(10, TimeUnit.SECONDS);
        eventually(() -> statsProvider.getLongValue("scheduler_backup_failed") > failures);
        Files.delete(directory);
        eventually(() -> !backup.listBackups().isEmpty());
        backup.stopAsync().awaitTerminated(10, TimeUnit.SECONDS);
        var filesAtStop = backup.listBackups();
        Thread.sleep(80);
        assertEquals(filesAtStop, backup.listBackups());
        assertTrue(statsProvider.getLongValue("scheduler_backup_success") > 0);
        assertTrue(statsProvider.getLongValue("scheduler_backup_last_success_ms") > 0);
        assertTrue(statsProvider.getLongValue("scheduler_backup_last_success_age_ms") >= 0);
      } finally {
        backup.stopAsync().awaitTerminated(10, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  public void testManualBackupFailurePropagatesAndInvalidSettingsAreRejected() throws Exception {
    Path root = temporary.getRoot().toPath();
    Path db = root.resolve("scheduler.db");
    Path directory = root.resolve("not-a-directory");
    Files.write(directory, new byte[] {1});
    try (SqliteStorage storage = SqliteStorage.open(db)) {
      GoStorageBackup backup = backup(storage, db, directory, 100, 1);
      try {
        backup.backupNow();
        fail("Expected backup failure");
      } catch (RecoveryException expected) {
        assertTrue(expected.getMessage().contains("Unable to write"));
      }
      assertInvalid(storage, db, root, 0, 1);
      assertInvalid(storage, db, root, 1, 0);
    }
  }

  private void assertInvalid(SqliteStorage storage, Path db, Path directory,
                             long interval, int maxBackups) {
    try {
      backup(storage, db, directory, interval, maxBackups);
      fail("Expected invalid backup settings");
    } catch (IllegalArgumentException expected) {
      // Expected validation at service construction.
    }
  }

  private GoStorageBackup backup(SqliteStorage storage, Path db, Path directory,
                                 long intervalMillis, int maxBackups) {
    BackupModule.Options options = new BackupModule.Options();
    options.backupDir = directory.toFile();
    options.backupInterval = new TimeAmount(intervalMillis, Time.MILLISECONDS);
    options.maxSavedBackups = maxBackups;
    return new GoStorageBackup(storage, config(db), options, statsProvider);
  }

  private static GoAgentConfig config(Path db) {
    return new GoAgentConfig("cluster", "incarnation", db, null, "", null, "", List.of());
  }

  private static void eventually(java.util.function.BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue("Condition was not reached before timeout", condition.getAsBoolean());
  }
}
