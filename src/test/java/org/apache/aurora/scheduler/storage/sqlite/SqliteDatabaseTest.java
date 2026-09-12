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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SqliteDatabaseTest {
  @Rule
  public TemporaryFolder temporary = new TemporaryFolder();

  private Path path;
  private SqliteDatabase database;
  private ExecutorService executor;

  @Before
  public void setUp() throws Exception {
    path = temporary.getRoot().toPath().resolve("scheduler.db");
    database = SqliteDatabase.open(path);
    executor = Executors.newFixedThreadPool(2);
    database.write("schema", () -> {
      execute("CREATE TABLE test_values (id INTEGER PRIMARY KEY, value TEXT NOT NULL)");
      execute("INSERT INTO test_values VALUES (1, 'before')");
      return null;
    });
  }

  @After
  public void tearDown() throws Exception {
    executor.shutdownNow();
    try {
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    } finally {
      database.close();
    }
  }

  @Test
  public void testVersionSettingsAndReopen() throws Exception {
    database.read(() -> {
      assertEquals("3.53.4", scalar("SELECT sqlite_version()"));
      assertEquals("wal", scalar("PRAGMA journal_mode"));
      assertEquals("2", scalar("PRAGMA synchronous"));
      assertEquals("1", scalar("PRAGMA foreign_keys"));
      assertEquals("3", scalar("PRAGMA user_version"));
      return null;
    });
    database.close();
    database = SqliteDatabase.open(path);
    assertTrue(database.isCommitted("schema"));
    assertEquals("before", database.read(this::value));
    assertEquals("2", database.read(() -> scalar("SELECT epoch FROM storage_owner")));
  }

  @Test
  public void testExclusiveOwnershipAndClosedAccess() throws Exception {
    expectFailure(StorageException.class, () -> SqliteDatabase.open(path));
    Path alias = path.resolveSibling("alias.db");
    java.nio.file.Files.createSymbolicLink(alias, path);
    expectFailure(StorageException.class, () -> SqliteDatabase.open(alias));
    database.close();
    expectFailure(StorageException.class, () -> database.read(() -> null));
    database.close();
    database = SqliteDatabase.open(path);
  }

  @Test
  public void testHardLinkAliasesCannotCreateASecondOwner() throws Exception {
    Path alias = path.resolveSibling("hard-link.db");
    java.nio.file.Files.createLink(alias, path);
    try {
      expectFailure(StorageException.class, () -> SqliteDatabase.open(alias));
      expectFailure(StorageException.class, () -> SqliteDatabase.open(path));
      assertEquals("before", database.read(this::value));
    } finally {
      java.nio.file.Files.delete(alias);
    }
    database.close();
    database = SqliteDatabase.open(path);
    assertEquals("before", database.read(this::value));
  }

  @Test
  public void testDanglingSymbolicLinkCannotCreateAnAliasedOwner() throws Exception {
    Path missing = path.resolveSibling("missing.db");
    Path alias = path.resolveSibling("dangling.db");
    java.nio.file.Files.createSymbolicLink(alias, missing);
    expectFailure(StorageException.class, () -> SqliteDatabase.open(alias));
    assertFalse(java.nio.file.Files.exists(missing));
    try (SqliteDatabase separate = SqliteDatabase.open(missing)) {
      expectFailure(StorageException.class, () -> SqliteDatabase.open(alias));
      assertEquals(null, separate.currentOperationId());
    }
  }

  @Test
  public void testJdbcOptionsInFilenameRemainLiteral() throws Exception {
    Path literal = path.resolveSibling("literal db?busy_timeout=1");
    Path withoutOptions = path.resolveSibling("literal db");
    try (SqliteDatabase special = SqliteDatabase.open(literal);
         SqliteDatabase independent = SqliteDatabase.open(withoutOptions)) {
      assertTrue(java.nio.file.Files.isRegularFile(literal));
      assertTrue(java.nio.file.Files.isRegularFile(withoutOptions));
      assertFalse(java.nio.file.Files.isSameFile(literal, withoutOptions));
      special.write("literal", () -> null);
      assertTrue(special.isCommitted("literal"));
      assertFalse(independent.isCommitted("literal"));
      expectFailure(StorageException.class, () -> SqliteDatabase.open(literal));
    }
  }

  @Test
  public void testUnsupportedAndUnversionedSchemasReleaseOwnership() throws Exception {
    database.close();
    try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
         Statement statement = connection.createStatement()) {
      statement.execute("PRAGMA user_version=99");
    }
    expectFailure(StorageException.class, () -> SqliteDatabase.open(path));
    try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
         Statement statement = connection.createStatement()) {
      statement.execute("PRAGMA user_version=0");
    }
    expectFailure(StorageException.class, () -> SqliteDatabase.open(path));
    try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
         Statement statement = connection.createStatement()) {
      statement.execute("PRAGMA user_version=3");
    }
    database = SqliteDatabase.open(path);
  }

  @Test
  public void testCommitReadOwnWriteAndDuplicateRejection() throws Exception {
    assertEquals(null, database.currentOperationId());
    database.write("update", () -> {
      assertEquals("update", database.currentOperationId());
      database.requireWrite();
      execute("UPDATE test_values SET value='after'");
      assertEquals("after", database.read(this::value));
      database.write("update", () -> {
        execute("INSERT INTO test_values VALUES (2, 'nested')");
        return null;
      });
      return null;
    });
    assertTrue(database.isCommitted("update"));
    expectFailure(SqliteDatabase.AlreadyCommittedException.class,
        () -> database.write("update", () -> {
          fail("A duplicate operation must not run its callback");
          return null;
        }));
    assertEquals("2", database.read(() -> scalar("SELECT count(*) FROM test_values")));
    expectFailure(IllegalStateException.class, () -> database.connection());
    expectFailure(IllegalStateException.class, () -> database.requireWrite());
  }

  @Test
  public void testSchemaOneMigrationPreservesOwnershipAndOutcomes() throws Exception {
    Path oldPath = temporary.getRoot().toPath().resolve("version-one.db");
    createVersionOneDatabase(oldPath);
    try (SqliteDatabase migrated = SqliteDatabase.open(oldPath)) {
      assertTrue(migrated.isCommitted("old-operation"));
      migrated.read(() -> {
        try (Statement statement = migrated.connection().createStatement();
             ResultSet rows = statement.executeQuery("SELECT epoch FROM storage_owner")) {
          assertTrue(rows.next());
          assertEquals(8, rows.getLong(1));
        }
        try (Statement statement = migrated.connection().createStatement();
             ResultSet rows = statement.executeQuery("PRAGMA user_version")) {
          assertTrue(rows.next());
          assertEquals(3, rows.getInt(1));
        }
        return null;
      });
      migrated.write("new-operation", () -> {
        try (Statement statement = migrated.connection().createStatement()) {
          statement.execute("INSERT INTO scheduler_metadata VALUES (1, 'framework')");
          for (String table : new String[] {
              "cron_jobs", "quotas", "attributes", "host_maintenance", "tasks", "job_updates"}) {
            statement.execute("INSERT INTO " + table + " VALUES ('key', 1, X'00')");
          }
        }
        return null;
      });
    }
    try (SqliteDatabase reopened = SqliteDatabase.open(oldPath)) {
      assertTrue(reopened.isCommitted("old-operation"));
      assertTrue(reopened.isCommitted("new-operation"));
    }
  }

  @Test
  public void testFailedMigrationRollsBackSchemaAndOwnership() throws Exception {
    Path legacyPath = temporary.getRoot().toPath().resolve("failed-migration.db");
    createVersionOneDatabase(legacyPath);
    AtomicBoolean failCommit = new AtomicBoolean(true);
    expectFailure(StorageException.class, () -> SqliteDatabase.open(legacyPath,
        url -> interceptCommit(DriverManager.getConnection(url), failCommit, false)));
    assertFalse("Failure must occur at migration commit", failCommit.get());

    try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + legacyPath);
         Statement statement = connection.createStatement()) {
      try (ResultSet rows = statement.executeQuery("PRAGMA user_version")) {
        assertTrue(rows.next());
        assertEquals(1, rows.getInt(1));
      }
      try (ResultSet rows = statement.executeQuery("SELECT epoch, session_id FROM storage_owner")) {
        assertTrue(rows.next());
        assertEquals(7, rows.getLong(1));
        assertEquals("old-owner", rows.getString(2));
      }
      try (ResultSet rows = statement.executeQuery("SELECT count(*) FROM sqlite_schema"
          + " WHERE name NOT LIKE 'sqlite_%'"
          + " AND name NOT IN ('storage_owner', 'storage_transactions')")) {
        assertTrue(rows.next());
        assertEquals(0, rows.getInt(1));
      }
    }
    // Failed initialization must release ownership and leave the old database retryable.
    try (SqliteDatabase recovered = SqliteDatabase.open(legacyPath)) {
      assertTrue(recovered.isCommitted("old-operation"));
      recovered.write("after-migration-retry", () -> null);
      assertTrue(recovered.isCommitted("after-migration-retry"));
    }
  }

  private static void createVersionOneDatabase(Path legacyPath) throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + legacyPath);
         Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE storage_owner (singleton INTEGER PRIMARY KEY"
          + " CHECK(singleton=1), epoch INTEGER NOT NULL, session_id TEXT NOT NULL)");
      statement.execute("INSERT INTO storage_owner VALUES (1, 7, 'old-owner')");
      statement.execute("CREATE TABLE storage_transactions (operation_id TEXT PRIMARY KEY"
          + " NOT NULL, owner_epoch INTEGER NOT NULL)");
      statement.execute("INSERT INTO storage_transactions VALUES ('old-operation', 7)");
      statement.execute("PRAGMA user_version=1");
    }
  }

  @Test
  public void testCheckedAndUncheckedFailuresRollback() throws Exception {
    IOException checked = new IOException("callback failed");
    assertSame(checked, expectFailure(IOException.class, () -> database.write("checked", () -> {
      execute("UPDATE test_values SET value='failed'");
      throw checked;
    })));
    AssertionError error = new AssertionError("callback error");
    assertSame(error, expectFailure(AssertionError.class, () -> database.write("error", () -> {
      execute("DELETE FROM test_values");
      throw error;
    })));
    assertEquals("before", database.read(this::value));
    assertFalse(database.isCommitted("checked"));
    assertFalse(database.isCommitted("error"));
  }

  @Test
  public void testCaughtNestedFailureMarksOuterRollbackOnly() throws Exception {
    IOException nested = new IOException("nested");
    StorageException failure = expectFailure(StorageException.class,
        () -> database.write("outer", () -> {
          execute("UPDATE test_values SET value='outer'");
          try {
            database.write("outer", () -> {
              execute("INSERT INTO test_values VALUES (2, 'inner')");
              throw nested;
            });
          } catch (IOException expected) {
            assertSame(nested, expected);
          }
          return null;
        }));
    assertSame(nested, failure.getCause());
    assertEquals("before", database.read(this::value));
    assertEquals("1", database.read(() -> scalar("SELECT count(*) FROM test_values")));
    assertFalse(database.isCommitted("outer"));
  }

  @Test
  public void testReadOnlyAndPromotionRejection() throws Exception {
    expectFailure(SQLException.class, () -> database.read(() -> {
      execute("DELETE FROM test_values");
      return null;
    }));
    expectFailure(StorageException.class, () -> database.read(() ->
        database.write("promotion", () -> null)));
    expectFailure(StorageException.class, () -> database.write("outer", () ->
        database.write("different", () -> null)));
    assertFalse(database.isCommitted("outer"));
    assertEquals("before", database.read(this::value));
  }

  @Test
  public void testCaughtNestedArgumentFailuresAlsoMarkRollbackOnly() throws Exception {
    CheckedAction[] invalidWrites = {
        () -> database.write(null, () -> null),
        () -> database.write("", () -> null),
        () -> database.write("invalid", null),
        () -> database.read(null)
    };
    for (CheckedAction invalid : invalidWrites) {
      expectFailure(StorageException.class, () -> database.write("invalid", () -> {
        execute("UPDATE test_values SET value='failed'");
        expectFailure(RuntimeException.class, invalid);
        return null;
      }));
      assertFalse(database.isCommitted("invalid"));
      assertEquals("before", database.read(this::value));
    }
  }

  @Test
  public void testReadersKeepSnapshotAndDoNotBlockOtherReadersOrWriter() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Future<String> reader = executor.submit(() -> database.read(() -> {
      // The snapshot is established on callback entry, before the first application query.
      started.countDown();
      assertTrue(release.await(30, TimeUnit.SECONDS));
      return value();
    }));
    try {
      assertTrue(started.await(30, TimeUnit.SECONDS));
      Future<String> independent = executor.submit(() -> database.read(this::value));
      assertEquals("before", independent.get(30, TimeUnit.SECONDS));
      database.write("concurrent", () -> {
        execute("UPDATE test_values SET value='after'");
        return null;
      });
      assertEquals("after", database.read(this::value));
    } finally {
      release.countDown();
    }
    assertEquals("before", reader.get(30, TimeUnit.SECONDS));
  }

  @Test
  public void testReadersDoNotSeeUncommittedFailedWrite() throws Exception {
    CountDownLatch mutated = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Future<?> failedWriter = executor.submit(() -> expectFailure(IOException.class,
        () -> database.write("uncommitted", () -> {
          execute("UPDATE test_values SET value='uncommitted'");
          mutated.countDown();
          assertTrue(release.await(30, TimeUnit.SECONDS));
          throw new IOException("rollback");
        })));
    try {
      assertTrue(mutated.await(30, TimeUnit.SECONDS));
      assertEquals("before", database.read(this::value));
    } finally {
      release.countDown();
    }
    failedWriter.get(30, TimeUnit.SECONDS);
    assertEquals("before", database.read(this::value));
  }

  @Test
  public void testOwnerEpochIsValidatedBeforeMutation() throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
         Statement statement = connection.createStatement()) {
      statement.execute("UPDATE storage_owner SET epoch=epoch+1");
    }
    expectFailure(StorageException.class, () -> database.write("stale", () -> {
      fail("Stale owner must not execute work");
      return null;
    }));
  }

  @Test
  public void testCloseWaitsForReadersAndRejectsCallbackClose() throws Exception {
    database.read(() -> {
      expectFailure(IllegalStateException.class, () -> database.close());
      return null;
    });
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Future<?> reader = executor.submit(() -> database.read(() -> {
      started.countDown();
      assertTrue(release.await(30, TimeUnit.SECONDS));
      return null;
    }));
    Future<?> closing = null;
    try {
      assertTrue(started.await(30, TimeUnit.SECONDS));
      closing = executor.submit(() -> database.close());
      Future<?> pendingClose = closing;
      expectFailure(TimeoutException.class, () -> pendingClose.get(100, TimeUnit.MILLISECONDS));
      expectFailure(StorageException.class, () -> SqliteDatabase.open(path));
    } finally {
      release.countDown();
    }
    reader.get(30, TimeUnit.SECONDS);
    closing.get(30, TimeUnit.SECONDS);
    database = SqliteDatabase.open(path);
  }

  @Test
  public void testCommitFailureBeforeCommitRequiresReconciliation() throws Exception {
    testCommitUncertainty(false);
  }

  @Test
  public void testCommitFailureAfterCommitDoesNotReplayOrClaimRollback() throws Exception {
    testCommitUncertainty(true);
  }

  @Test
  public void testBusyWriterFailsWithinBoundWithoutRunningWork() throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
         Statement statement = connection.createStatement()) {
      statement.execute("BEGIN IMMEDIATE");
      try {
        Future<?> blocked = executor.submit(() -> expectFailure(StorageException.class,
            () -> database.write("busy", () -> {
              fail("Contended write must not run its callback");
              return null;
            })));
        blocked.get(10, TimeUnit.SECONDS);
      } finally {
        statement.execute("ROLLBACK");
      }
    }
    assertFalse(database.isCommitted("busy"));
    database.write("after-busy", () -> null);
  }

  @Test
  public void testRootPathsRejectedBeforeDatabaseAccess() throws Exception {
    expectFailure(StorageException.class, () -> SqliteDatabase.open(path.getRoot()));
    expectFailure(StorageException.class, () -> database.backup(path.getRoot()));
    assertEquals("before", database.read(this::value));
  }

  @Test
  public void testCaughtDiskFullCannotAutocommitLaterStoreMutations() throws Exception {
    SqliteSchedulerStore scheduler = new SqliteSchedulerStore(database);
    expectFailure(StorageException.class, () -> database.write("disk-full", () -> {
      String pages = scalar("PRAGMA page_count");
      execute("PRAGMA max_page_count=" + pages);
      execute("UPDATE test_values SET value='must roll back'");
      StorageException full = expectFailure(StorageException.class,
          () -> scheduler.saveFrameworkId("x".repeat(1024 * 1024)));
      assertTrue(full.getCause() instanceof SQLException);
      assertEquals(13, ((SQLException) full.getCause()).getErrorCode());
      // SQLITE_FULL can cancel BEGIN itself. Neither a read nor a second mutation
      // may now execute outside the original transaction after its exception is caught.
      expectFailure(StorageException.class, scheduler::fetchFrameworkId);
      expectFailure(StorageException.class, () -> scheduler.saveFrameworkId("must not commit"));
      return null;
    }));
    assertFalse(database.isCommitted("disk-full"));
    assertEquals("before", database.read(this::value));
    assertFalse(database.read(scheduler::fetchFrameworkId).isPresent());
    database.write("after-full", () -> {
      scheduler.saveFrameworkId("recovered");
      return null;
    });
    assertEquals("recovered", database.read(scheduler::fetchFrameworkId).get());
  }

  @Test
  public void testCleanupFailurePreservesPrimaryAndRetainsOwnership() throws Exception {
    database.close();
    AtomicBoolean failClose = new AtomicBoolean();
    database = SqliteDatabase.open(path, url -> interceptClose(
        DriverManager.getConnection(url), failClose));
    IOException primary = new IOException("work failure");
    failClose.set(true);
    assertSame(primary, expectFailure(IOException.class, () -> database.write("cleanup", () -> {
      execute("UPDATE test_values SET value='failed'");
      throw primary;
    })));
    assertEquals(1, primary.getSuppressed().length);
    expectFailure(StorageException.class, () -> database.read(() -> null));
    expectFailure(StorageException.class, () -> SqliteDatabase.open(path));
    database.close();
    database = SqliteDatabase.open(path);
    assertFalse(database.isCommitted("cleanup"));
    assertEquals("before", database.read(this::value));
  }

  @Test
  public void testCommittedCleanupFailureResolvesAfterReopen() throws Exception {
    database.close();
    AtomicBoolean failClose = new AtomicBoolean();
    database = SqliteDatabase.open(path, url -> interceptClose(
        DriverManager.getConnection(url), failClose));
    failClose.set(true);
    expectFailure(SqliteDatabase.CommitUncertainException.class,
        () -> database.write("committed-cleanup", () -> {
          execute("UPDATE test_values SET value='after'");
          return null;
        }));
    expectFailure(StorageException.class, () -> database.isCommitted("committed-cleanup"));
    database.close();
    database = SqliteDatabase.open(path);
    assertTrue(database.isCommitted("committed-cleanup"));
    assertEquals("after", database.read(this::value));
  }

  @Test
  public void testInitializationCleanupFailureReleasesOwnershipAfterRetry() throws Exception {
    database.close();
    AtomicBoolean failClose = new AtomicBoolean(true);
    expectFailure(StorageException.class, () -> SqliteDatabase.open(path,
        url -> interceptClose(DriverManager.getConnection(url), failClose)));
    database = SqliteDatabase.open(path);
    assertEquals("before", database.read(this::value));
  }

  private void testCommitUncertainty(boolean commitFirst) throws Exception {
    database.close();
    AtomicBoolean failCommit = new AtomicBoolean();
    database = SqliteDatabase.open(path, url -> interceptCommit(
        DriverManager.getConnection(url), failCommit, commitFirst));
    failCommit.set(true);
    List<String> published = new ArrayList<>();
    SqliteDatabase.CommitUncertainException failure = expectFailure(
        SqliteDatabase.CommitUncertainException.class, () -> database.write("uncertain", () -> {
          database.afterCommit(() -> published.add("committed"));
          execute("UPDATE test_values SET value='after'");
          return null;
        }));
    assertEquals("uncertain", failure.getOperationId());
    expectFailure(StorageException.class, () -> database.write("blocked", () -> null));
    assertTrue(published.isEmpty());
    assertEquals(commitFirst, database.isCommitted("uncertain"));
    assertEquals(commitFirst ? List.of("committed") : List.of(), published);
    assertEquals(commitFirst ? "after" : "before", database.read(this::value));
    database.write("next", () -> null);
    database.close();
    database = SqliteDatabase.open(path);
    assertEquals(commitFirst, database.isCommitted("uncertain"));
  }

  private static Connection interceptCommit(
      Connection delegate, AtomicBoolean failCommit, boolean commitFirst) {
    return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
        new Class<?>[] {Connection.class}, (proxy, method, args) -> {
          Object result = invoke(delegate, method, args);
          if (!"createStatement".equals(method.getName())) {
            return result;
          }
          Statement statement = (Statement) result;
          return Proxy.newProxyInstance(
              Statement.class.getClassLoader(),
              new Class<?>[] {Statement.class},
              (statementProxy, statementMethod, statementArgs) -> {
                if (!"execute".equals(statementMethod.getName())
                    || !"COMMIT".equals(statementArgs[0])
                    || !failCommit.getAndSet(false)) {
                  return invoke(statement, statementMethod, statementArgs);
                }
                if (commitFirst) {
                  invoke(statement, statementMethod, statementArgs);
                }
                throw new SQLException("Injected commit acknowledgment failure");
              });
        });
  }

  private static Connection interceptClose(Connection delegate, AtomicBoolean failClose) {
    return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
        new Class<?>[] {Connection.class}, (proxy, method, args) -> {
          if ("close".equals(method.getName()) && failClose.getAndSet(false)) {
            throw new SQLException("Injected connection close failure");
          }
          return invoke(delegate, method, args);
        });
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private void execute(String sql) throws SQLException {
    try (Statement statement = database.connection().createStatement()) {
      statement.execute(sql);
    }
  }

  private String scalar(String sql) throws SQLException {
    try (Statement statement = database.connection().createStatement();
         ResultSet result = statement.executeQuery(sql)) {
      assertTrue(result.next());
      return result.getString(1);
    }
  }

  private String value() throws SQLException {
    return scalar("SELECT value FROM test_values WHERE id=1");
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
