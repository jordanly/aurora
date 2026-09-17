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
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.resources.ResourceTestUtil;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.sqlite.SqliteEffects.Command;
import org.apache.aurora.scheduler.storage.sqlite.SqliteEffects.PendingCommand;
import org.apache.aurora.scheduler.storage.sqlite.SqliteEffects.ReceiptKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SqliteEffectsTest {
  private static final Command FIRST = new Command(
      "first", "agent", "task", "LAUNCH", 1, new byte[] {1, 2});
  private static final ReceiptKey RECEIPT = new ReceiptKey("agent", "incarnation", 0);

  @Rule
  public TemporaryFolder temporary = new TemporaryFolder();

  private Path path;
  private SqliteStorage storage;

  @Before
  public void setUp() {
    path = temporary.getRoot().toPath().resolve("effects.db");
    storage = SqliteStorage.open(path);
  }

  @After
  public void tearDown() {
    storage.close();
  }

  @Test
  public void testTicketRetirementSurvivesRestartAndKeepsAllocator() {
    storage.write((NoResult.Quiet) stores -> {
      storage.effects().beginRetention("agent", "scope");
      storage.effects().confirmRetention("agent", "[]");
      assertEquals(1, storage.effects().allocateTicket("agent", "live"));
      assertEquals(2, storage.effects().allocateTicket("agent", "done"));
      storage.effects().enqueue(new Command("done-run", "agent", "done", "Run", 1, new byte[0]));
      storage.effects().completeTicket("agent", 2);
      assertTrue(storage.effects().prepareRetirement("agent", 0).isEmpty());
      storage.effects().acknowledge("done-run");
    });
    reopen();
    storage.write((NoResult.Quiet) stores -> {
      var selected = storage.effects().prepareRetirement("agent", 0);
      assertEquals(List.of(new SqliteEffects.Retiring(2, "done")), selected);
      assertTrue(storage.effects().retiringTask("done"));
    });
    reopen();
    storage.write((NoResult.Quiet) stores -> {
      assertTrue(storage.effects().retiringTask("done"));
      storage.effects().finishRetirement("agent", "scope",
          new SqliteEffects.Retiring(2, "done"), "attempt-done");
      assertTrue(storage.effects().command("done-run").isEmpty());
      assertEquals(3, storage.effects().allocateTicket("agent", "next"));
    });
  }

  @Test
  public void testReceiptPayloadPruningPreservesCursorAndRejectsOldReplay() {
    var first = new ReceiptKey("agent", "scope", 1);
    storage.write((NoResult.Quiet) stores -> {
      assertTrue(storage.effects().recordReceipt(first, 1, new byte[] {1}));
      storage.effects().pruneReceipts("agent", "scope");
      assertFalse(storage.effects().hasReceipt(first));
      assertEquals(1, storage.effects().committedCursor("agent", "scope"));
    });
    reopen();
    storage.write((NoResult.Quiet) stores -> {
      assertEquals(1, storage.effects().committedCursor("agent", "scope"));
      assertTrue(storage.effects().recordReceipt(new ReceiptKey("agent", "scope", 2),
          1, new byte[] {2}));
      assertEquals(2, storage.effects().committedCursor("agent", "scope"));
    });
    try {
      storage.write((NoResult.Quiet) stores ->
          storage.effects().recordReceipt(first, 1, new byte[] {1}));
      org.junit.Assert.fail("Pruned receipt replay accepted");
    } catch (StorageException expected) {
      assertTrue(expected.getMessage().contains("watermark"));
    }
  }

  @Test
  public void testCommandCopiesPayloadAndComparesContents() {
    byte[] body = {1, 2};
    Command command = new Command("first", "agent", "task", "LAUNCH", 1, body);
    body[0] = 9;
    command.payload()[1] = 9;
    assertArrayEquals(new byte[] {1, 2}, command.payload());
    assertEquals(FIRST, command);
    assertEquals(FIRST.hashCode(), command.hashCode());
  }

  @Test
  public void testPendingOrderAcknowledgmentAndDedupSurviveReopen() {
    Command second = new Command("second", "agent", "task", "KILL", 2, new byte[] {3});
    storage.write((NoResult.Quiet) stores -> {
      assertTrue(storage.effects().enqueue(FIRST));
      assertFalse(storage.effects().enqueue(FIRST));
      assertTrue(storage.effects().enqueue(second));
    });
    List<PendingCommand> pending = storage.read(stores -> storage.effects().pending(10));
    assertEquals(List.of(FIRST, second), pending.stream().map(PendingCommand::command).toList());
    assertTrue(pending.get(1).sequence() > pending.get(0).sequence());
    assertEquals(pending.get(0).ownerEpoch(), pending.get(1).ownerEpoch());
    assertEquals(List.of(pending.get(0)), storage.read(stores -> storage.effects().pending(1)));
    storage.write((NoResult.Quiet) stores -> {
      assertTrue(storage.effects().acknowledge(FIRST.id()));
      assertFalse(storage.effects().acknowledge(FIRST.id()));
    });
    reopen();
    storage.write((NoResult.Quiet) stores -> assertFalse(storage.effects().enqueue(FIRST)));
    assertEquals(List.of(pending.get(1)), storage.read(stores -> storage.effects().pending(10)));
    Command third = new Command("third", "agent", "task", "KILL", 1, new byte[0]);
    storage.write((NoResult.Quiet) stores -> storage.effects().enqueue(third));
    List<PendingCommand> after = storage.read(stores -> storage.effects().pending(10));
    assertTrue(after.get(1).sequence() > pending.get(1).sequence());
    assertTrue(after.get(1).ownerEpoch() > pending.get(1).ownerEpoch());
  }

  @Test
  public void testAgentPendingFiltersBeforeApplyingLimit() {
    storage.write((NoResult.Quiet) stores -> {
      for (int i = 0; i < 1025; i++) {
        assertTrue(storage.effects().enqueue(new Command("backlog-" + i, "unavailable",
            "task-" + i, "Run", 1, new byte[0])));
      }
      assertTrue(storage.effects().enqueue(FIRST));
    });

    assertEquals("backlog-0", storage.read(stores -> storage.effects().pending(1).get(0)
        .command().id()));
    assertEquals(List.of(FIRST), storage.read(stores -> storage.effects().pending("agent", 1)
        .stream().map(PendingCommand::command).toList()));
  }

  @Test
  public void testCommandConflictsRejectEveryChangedBodyField() throws Exception {
    storage.write((NoResult.Quiet) stores -> storage.effects().enqueue(FIRST));
    List<Command> conflicts = List.of(
        new Command("first", "other", "task", "LAUNCH", 1, new byte[] {1, 2}),
        new Command("first", "agent", "other", "LAUNCH", 1, new byte[] {1, 2}),
        new Command("first", "agent", "task", "KILL", 1, new byte[] {1, 2}),
        new Command("first", "agent", "task", "LAUNCH", 2, new byte[] {1, 2}),
        new Command("first", "agent", "task", "LAUNCH", 1, new byte[] {1, 3}));
    for (Command conflict : conflicts) {
      expectFailure(StorageException.class, () -> storage.write((NoResult.Quiet) stores ->
          storage.effects().enqueue(conflict)));
    }
    expectFailure(StorageException.class, () -> storage.write((NoResult.Quiet) stores ->
        storage.effects().acknowledge("unknown")));
    assertEquals(List.of(FIRST), storage.read(stores -> storage.effects().pending(10))
        .stream().map(PendingCommand::command).toList());
  }

  @Test
  public void testReceiptReplayConflictsAndIncarnationSeparation() throws Exception {
    storage.write((NoResult.Quiet) stores -> {
      byte[] payload = {4, 5};
      assertTrue(storage.effects().recordReceipt(RECEIPT, 1, payload));
      payload[0] = 9;
      assertFalse(storage.effects().recordReceipt(RECEIPT, 1, new byte[] {4, 5}));
      assertTrue(storage.effects().recordReceipt(new ReceiptKey("agent", "next", 0), 1, payload));
      assertTrue(storage.effects().recordReceipt(new ReceiptKey("other", "incarnation", 0), 1,
          payload));
    });
    expectFailure(StorageException.class, () -> storage.write((NoResult.Quiet) stores ->
        storage.effects().recordReceipt(RECEIPT, 2, new byte[] {4, 5})));
    expectFailure(StorageException.class, () -> storage.write((NoResult.Quiet) stores ->
        storage.effects().recordReceipt(RECEIPT, 1, new byte[] {4, 6})));
    reopen();
    assertTrue(storage.read(stores -> storage.effects().hasReceipt(RECEIPT)));
    storage.write((NoResult.Quiet) stores -> assertFalse(
        storage.effects().recordReceipt(RECEIPT, 1, new byte[] {4, 5})));
  }

  @Test
  public void testEffectsAndTaskQuotaCommitOrRollbackTogether() throws Exception {
    IOException failure = new IOException("abort all mutations");
    assertSame(failure, expectFailure(IOException.class, () -> storage.write("failed", stores -> {
      stores.getQuotaStore().saveQuota("role", ResourceTestUtil.aggregate(2, 512, 100));
      stores.getUnsafeTaskStore().saveTasks(ImmutableSet.of(
          TaskTestUtil.makeTask("task", TaskTestUtil.JOB)));
      storage.effects().enqueue(FIRST);
      storage.effects().recordReceipt(RECEIPT, 1, new byte[] {7});
      throw failure;
    })));
    assertFalse(storage.isCommitted("failed"));
    storage.read(stores -> {
      assertFalse(stores.getQuotaStore().fetchQuota("role").isPresent());
      assertFalse(stores.getTaskStore().fetchTask("task").isPresent());
      assertEquals(List.of(), storage.effects().pending(10));
      assertFalse(storage.effects().hasReceipt(RECEIPT));
      return null;
    });
    storage.write("committed", stores -> {
      stores.getQuotaStore().saveQuota("role", ResourceTestUtil.aggregate(2, 512, 100));
      stores.getUnsafeTaskStore().saveTasks(ImmutableSet.of(
          TaskTestUtil.makeTask("task", TaskTestUtil.JOB)));
      storage.effects().enqueue(FIRST);
      storage.effects().recordReceipt(RECEIPT, 1, new byte[] {7});
      return null;
    });
    reopen();
    assertTrue(storage.isCommitted("committed"));
    storage.read(stores -> {
      assertTrue(stores.getQuotaStore().fetchQuota("role").isPresent());
      assertTrue(stores.getTaskStore().fetchTask("task").isPresent());
      assertEquals(FIRST, storage.effects().pending(10).get(0).command());
      assertTrue(storage.effects().hasReceipt(RECEIPT));
      return null;
    });
  }

  @Test
  public void testReceiptDedupGuardsStateMutation() {
    storage.write((NoResult.Quiet) stores -> {
      if (storage.effects().recordReceipt(RECEIPT, 1, new byte[] {1})) {
        stores.getQuotaStore().saveQuota("role", ResourceTestUtil.aggregate(2, 512, 100));
      }
    });
    storage.write((NoResult.Quiet) stores -> {
      if (storage.effects().recordReceipt(RECEIPT, 1, new byte[] {1})) {
        stores.getQuotaStore().saveQuota("role", ResourceTestUtil.aggregate(9, 512, 100));
      }
    });
    assertEquals(ResourceTestUtil.aggregate(2, 512, 100),
        storage.read(stores -> stores.getQuotaStore().fetchQuota("role").get()));
  }

  @Test
  public void testEffectsRequireScopeAndRejectInvalidIdentityVersionOrSequence() throws Exception {
    expectFailure(IllegalStateException.class, () -> storage.effects().pending(1));
    expectFailure(IllegalStateException.class, () -> storage.effects().enqueue(FIRST));
    expectFailure(IllegalStateException.class, () -> storage.read(stores ->
        storage.effects().recordReceipt(RECEIPT, 1, new byte[0])));
    expectFailure(IllegalArgumentException.class, () -> new ReceiptKey("agent", "incarnation", -1));
    expectFailure(IllegalArgumentException.class, () -> new Command(
        "id", "agent", "task", "LAUNCH", 0, new byte[0]));
    expectFailure(IllegalArgumentException.class, () -> storage.read(stores ->
        storage.effects().pending(0)));
  }

  @Test
  public void testBackupRestoresConsistentStoresEffectsAndOutcomes() throws Exception {
    storage.write("backup-operation", stores -> {
      stores.getQuotaStore().saveQuota("role", ResourceTestUtil.aggregate(2, 512, 100));
      storage.effects().enqueue(FIRST);
      storage.effects().recordReceipt(RECEIPT, 1, new byte[] {7});
      return null;
    });
    Path backup = temporary.getRoot().toPath().resolve("backup.db");
    storage.backup(backup);
    storage.write((NoResult.Quiet) stores -> storage.effects().acknowledge(FIRST.id()));
    try (SqliteStorage restored = SqliteStorage.open(backup)) {
      assertTrue(restored.isCommitted("backup-operation"));
      restored.read(stores -> {
        assertTrue(stores.getQuotaStore().fetchQuota("role").isPresent());
        assertEquals(FIRST, restored.effects().pending(10).get(0).command());
        assertTrue(restored.effects().hasReceipt(RECEIPT));
        return null;
      });
    }
    assertEquals(List.of(), storage.read(stores -> storage.effects().pending(10)));
  }

  @Test
  public void testBackupRejectsExistingSourceAndSymlinkDestinationsAndActiveWork()
      throws Exception {
    expectFailure(StorageException.class, () -> storage.backup(path));
    Path existing = temporary.newFile("existing.db").toPath();
    expectFailure(StorageException.class, () -> storage.backup(existing));
    assertEquals(0, Files.size(existing));
    Path symlink = path.resolveSibling("alias.db");
    Files.createSymbolicLink(symlink, path);
    expectFailure(StorageException.class, () -> storage.backup(symlink));
    Path dangling = path.resolveSibling("dangling.db");
    Files.createSymbolicLink(dangling, path.resolveSibling("missing.db"));
    expectFailure(StorageException.class, () -> storage.backup(dangling));
    expectFailure(IllegalStateException.class, () -> storage.read(stores -> {
      storage.backup(path.resolveSibling("inside.db"));
      return null;
    }));
    assertFalse(Files.exists(path.resolveSibling("inside.db")));
  }

  @Test
  public void testSchemaTwoMigrationRetainsStoresAndOutcomes() throws Exception {
    storage.write("before-migration", stores -> {
      stores.getQuotaStore().saveQuota("role", ResourceTestUtil.aggregate(2, 512, 100));
      return null;
    });
    storage.close();
    try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
         Statement statement = connection.createStatement()) {
      statement.execute("DROP TABLE command_outbox");
      statement.execute("DROP TABLE observation_receipts");
      statement.execute("DROP TABLE automatic_outcome");
      statement.execute("DROP TABLE agent_retention");
      statement.execute("DROP TABLE attempt_retention");
      statement.execute("DROP TABLE receipt_watermarks");
      statement.execute("PRAGMA user_version=2");
    }
    storage = SqliteStorage.open(path);
    assertTrue(storage.isCommitted("before-migration"));
    storage.write((NoResult.Quiet) stores -> {
      assertTrue(stores.getQuotaStore().fetchQuota("role").isPresent());
      assertTrue(storage.effects().enqueue(FIRST));
      assertTrue(storage.effects().recordReceipt(RECEIPT, 1, new byte[0]));
    });
    try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
         Statement statement = connection.createStatement();
         ResultSet rows = statement.executeQuery("PRAGMA user_version")) {
      assertTrue(rows.next());
      assertEquals(4, rows.getInt(1));
    }
  }

  @Test
  public void testProcessCrashBeforeCommitRollsBackAllState() throws Exception {
    checkProcessCrash("before", false, 17);
  }

  @Test
  public void testProcessCrashAfterCommitRecoversAllAcknowledgedState() throws Exception {
    checkProcessCrash("after", true, 18);
  }

  private void checkProcessCrash(String point, boolean committed, int expectedExit)
      throws Exception {
    Path childDatabase = temporary.getRoot().toPath().resolve("crash-" + point + ".db");
    Path output = temporary.getRoot().toPath().resolve("crash-" + point + ".log");
    String classpath = System.getProperty(
        "aurora.test.classpath", System.getProperty("java.class.path"));
    Process child = new ProcessBuilder(
        Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-cp", classpath, SqliteCrashProbe.class.getName(), childDatabase.toString(), point)
        .redirectErrorStream(true).redirectOutput(output.toFile()).start();
    try {
      assertTrue("Crash probe did not terminate", child.waitFor(30, TimeUnit.SECONDS));
      assertEquals(Files.readString(output), expectedExit, child.exitValue());
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        assertTrue(child.waitFor(10, TimeUnit.SECONDS));
      }
    }
    try (SqliteStorage recovered = SqliteStorage.open(childDatabase)) {
      assertEquals(committed, recovered.isCommitted("crash-operation"));
      recovered.read(stores -> {
        assertEquals(committed, stores.getQuotaStore().fetchQuota("crash-role").isPresent());
        assertEquals(committed ? 1 : 0, recovered.effects().pending(10).size());
        assertEquals(committed, recovered.effects().hasReceipt(RECEIPT));
        return null;
      });
    }
  }

  private void reopen() {
    storage.close();
    storage = SqliteStorage.open(path);
  }

  @FunctionalInterface
  private interface CheckedAction {
    void run() throws Exception;
  }

  private static <T extends Throwable> T expectFailure(Class<T> expected, CheckedAction action)
      throws Exception {
    try {
      action.run();
    } catch (Throwable failure) {
      if (expected.isInstance(failure)) {
        return expected.cast(failure);
      }
      throw new AssertionError("Expected " + expected.getName() + " but got " + failure, failure);
    }
    throw new AssertionError("Expected " + expected.getName());
  }
}
