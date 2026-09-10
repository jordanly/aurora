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
package org.apache.aurora.scheduler.storage.sql;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class NativeSqlStoreTest {
  @Rule public TemporaryFolder temporary = new TemporaryFolder();
  private final NativeSqlStore.JobKey job = new NativeSqlStore.JobKey("r", "e", "j");
  private final NativeSqlStore.Journal journal = new NativeSqlStore.Journal("c", "i", "n", "j");
  private NativeSqlStore open(Path dir) throws Exception { return new NativeSqlStore(dir, "c", "i"); }
  private void seed(NativeSqlStore store) throws Exception {
    store.write(tx -> {
      tx.createJob(job, "18446744073709551615", "batch", "immutable-job");
      tx.addInstance(job, "0"); tx.createAttempt("a", job, "0");
      tx.allocate("a", "n", "allocation"); tx.command("cmd", "a", "run");
      return null;
    });
  }
  @Test public void versionOneMigratesAtomicallyAndPreservesDurableJobs() throws Exception {
    Path dir = temporary.newFolder().toPath();
    try (NativeSqlStore store = open(dir)) { seed(store); }
    try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dir.resolve("scheduler.db"))) {
      connection.createStatement().execute("DROP TABLE attempt_observations");
      connection.createStatement().execute("DROP TABLE scheduler_state");
      connection.createStatement().execute("PRAGMA user_version=1");
    }
    try (NativeSqlStore store = open(dir)) {
      assertEquals("immutable-job", store.read(tx -> tx.jobBody(job)));
      assertEquals("1", store.write(tx -> tx.startScheduler("configuration")));
      store.write(tx -> {
        tx.reduceAttempt("a", "18446744073709551615", "lost", "unknown", false, 10);
        return null;
      });
    }
    try (NativeSqlStore store = open(dir)) {
      assertEquals("2", store.write(tx -> tx.startScheduler("configuration")));
      assertEquals("18446744073709551615", store.read(tx -> tx.attempts().get(0).sequence));
      assertTrue(store.read(tx -> tx.attempts().get(0).reserved()));
      rejects(() -> store.write(tx -> tx.startScheduler("different")));
      assertEquals("2", store.read(NativeSqlStore.Tx::schedulerEpoch));
    }
  }
  @Test public void snapshotRetainsEpochProjectionAndRefusesOverwriteOrSymlink() throws Exception {
    Path dir = temporary.newFolder().toPath();
    Path copy = temporary.getRoot().toPath().resolve("snapshot");
    try (NativeSqlStore store = open(dir)) {
      seed(store);
      store.write(tx -> {
        tx.startScheduler("config"); tx.reduceAttempt("a", "1", "succeeded", "complete", false, 10);
        return null;
      });
      store.snapshot(copy); rejects(() -> store.snapshot(copy));
      Path link = temporary.getRoot().toPath().resolve("link");
      java.nio.file.Files.createSymbolicLink(link, dir);
      rejects(() -> store.snapshot(link.resolve("child")));
    }
    try (NativeSqlStore store = open(copy)) {
      assertEquals("1", store.read(NativeSqlStore.Tx::schedulerEpoch));
      assertFalse(store.read(tx -> tx.attempts().get(0).reserved()));
      assertTrue(store.read(tx -> tx.hasInstance(job, "0")));
    }
  }
  @Test public void completedTerminalOutcomeIsImmutableAndConflictRollsBackReceipt() throws Exception {
    Path dir = temporary.newFolder().toPath();
    try (NativeSqlStore store = open(dir)) {
      seed(store);
      store.write(tx -> {
        tx.reduceAttempt("a", "1", "succeeded", "pending", false, 1);
        tx.observe(journal, "1", "pending"); return null;
      });
      store.write(tx -> {
        tx.reduceAttempt("a", "2", "succeeded", "complete", false, 2);
        tx.observe(journal, "2", "complete"); return null;
      });
      for (String outcome : new String[] {"failed", "stopped", "lost"}) {
        rejects(() -> store.write(tx -> {
          tx.observe(journal, "3", "conflicting");
          // Catching the mutator failure must still poison the complete write.
          rejects(() -> tx.reduceAttempt("a", "3", outcome, "complete", false, 3));
          return null;
        }));
        assertEquals("2", store.read(tx -> tx.committedCursor(journal)));
        assertEquals("succeeded", store.read(tx -> tx.attempts().get(0).state));
      }
      store.write(tx -> {
        tx.reduceAttempt("a", "3", "succeeded", "complete", false, 3);
        tx.observe(journal, "3", "same terminal outcome"); return null;
      });
      assertEquals("3", store.read(tx -> tx.committedCursor(journal)));
    }
  }
  private interface Failing { void run() throws Exception; }
  private void rejects(Failing work) throws Exception {
    try { work.run(); fail("Expected rejection"); } catch (Exception expected) { }
  }
  @Test public void durableTerminalBatchAndOutbox() throws Exception {
    Path dir = temporary.newFolder().toPath();
    try (NativeSqlStore store = open(dir)) {
      seed(store);
      store.write(tx -> { tx.completeAttempt("a", "succeeded"); return null; });
    }
    try (NativeSqlStore store = open(dir)) {
      store.read(tx -> {
        assertEquals("immutable-job", tx.jobBody(job)); assertTrue(tx.hasInstance(job, "0"));
        assertEquals("18446744073709551615", tx.jobRevision(job));
        assertEquals("succeeded", tx.attemptState("a"));
        assertEquals("allocation", tx.allocation("a"));
        assertEquals("run", tx.pendingCommands().get(0));
        rejects(() -> tx.pendingCommands().clear());
        return null;
      });
    }
  }
  @Test public void cancelledMembershipRetainsAttemptHistory() throws Exception {
    Path dir = temporary.newFolder().toPath();
    try (NativeSqlStore store = open(dir)) {
      seed(store);
      store.write(tx -> { tx.removeInstance(job, "0"); return null; });
    }
    try (NativeSqlStore store = open(dir)) {
      store.read(tx -> {
        assertFalse(tx.hasInstance(job, "0"));
        assertEquals("pending", tx.attemptState("a"));
        assertEquals("allocation", tx.allocation("a"));
        return null;
      });
    }
  }
  @Test public void nestedExceptionAndErrorPoisonWholeWrite() throws Exception {
    try (NativeSqlStore store = open(temporary.newFolder().toPath())) {
      for (boolean error : new boolean[] {false, true}) {
        rejects(() -> store.write(tx -> {
          tx.createJob(job, "0", "batch", "job");
          try {
            store.write(inner -> {
              inner.addInstance(job, "0");
              if (error) { throw new AssertionError("fail"); }
              throw new Exception("fail");
            });
          } catch (Exception | AssertionError expected) { }
          return null;
        }));
        assertNull(store.read(tx -> tx.jobBody(job)));
      }
      rejects(() -> store.write(tx -> {
        tx.createJob(job, "0", "batch", "job");
        tx.addInstance(job, "0"); tx.createAttempt("a", job, "0");
        tx.allocate("a", "n", "allocation"); tx.command("cmd", "missing", "run");
        return null;
      }));
      assertNull(store.read(tx -> tx.jobBody(job)));
    }
  }
  @Test public void independentReaderHasSnapshotAndNoDirtyRead() throws Exception {
    try (NativeSqlStore store = open(temporary.newFolder().toPath())) {
      ExecutorService executor = Executors.newFixedThreadPool(2);
      CountDownLatch written = new CountDownLatch(1);
      CountDownLatch read = new CountDownLatch(1);
      CountDownLatch committed = new CountDownLatch(1);
      try {
        Future<?> writer = executor.submit(() -> {
          try {
            store.write(tx -> {
              tx.createJob(job, "0", "batch", "job"); written.countDown();
              assertTrue(read.await(10, TimeUnit.SECONDS)); return null;
            });
            committed.countDown();
          } catch (Exception e) { throw new RuntimeException(e); }
        });
        assertTrue(written.await(10, TimeUnit.SECONDS));
        store.read(tx -> {
          assertNull(tx.jobBody(job)); read.countDown();
          assertTrue(committed.await(10, TimeUnit.SECONDS));
          assertNull(tx.jobBody(job)); return null;
        });
        writer.get(10, TimeUnit.SECONDS);
        assertEquals("job", store.read(tx -> tx.jobBody(job)));
      } finally { read.countDown(); executor.shutdownNow(); }
    }
  }
  @Test public void gapDedupeAndScopeIsolation() throws Exception {
    Path dir = temporary.newFolder().toPath();
    try (NativeSqlStore store = open(dir)) {
      store.write(tx -> { tx.observe(journal, "2", "two"); return null; });
      assertEquals("0", store.read(tx -> tx.committedCursor(journal)));
      rejects(() -> store.write(tx -> {
        tx.observe(journal, "1", "one"); throw new Exception("crash before commit");
      }));
      assertEquals("0", store.read(tx -> tx.committedCursor(journal)));
      store.write(tx -> { tx.observe(journal, "1", "one"); tx.observe(journal, "2", "two");
        tx.observe(journal, "18446744073709551615", "max"); return null; });
      rejects(() -> store.write(tx -> { tx.observe(journal, "2", "conflict"); return null; }));
      rejects(() -> store.write(tx -> { tx.observe(
          new NativeSqlStore.Journal("other", "i", "n", "j"), "3", "bad"); return null; }));
      assertEquals("0", store.read(tx -> tx.committedCursor(
          new NativeSqlStore.Journal("c", "i", "n", "new-journal"))));
    }
    try (NativeSqlStore store = open(dir)) {
      assertEquals("2", store.read(tx -> tx.committedCursor(journal)));
    }
  }
  @Test public void immutableCommandConflictRollsBackAndEscapesReject() throws Exception {
    try (NativeSqlStore store = open(temporary.newFolder().toPath())) {
      seed(store);
      store.write(tx -> { tx.command("cmd", "a", "run"); return null; });
      rejects(() -> store.write(tx -> {
        tx.acknowledgeCommand("cmd");
        try { tx.command("cmd", "a", "different"); } catch (Exception expected) { }
        return null;
      }));
      assertEquals(1, (int) store.read(tx -> tx.pendingCommands().size()));
      NativeSqlStore.Tx escaped = store.read(tx -> tx);
      rejects(() -> escaped.jobBody(job));
      rejects(() -> escaped.addInstance(job, "1"));
      rejects(() -> store.read(tx -> { tx.addInstance(job, "1"); return null; }));
    }
  }
  @Test public void ownerAndSchemaAndCounterReject() throws Exception {
    Path dir = temporary.newFolder().toPath();
    try (NativeSqlStore store = open(dir)) { rejects(() -> open(dir)); }
    rejects(() -> new NativeSqlStore(dir, "other", "i"));
    try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + dir.resolve("scheduler.db"))) {
      c.createStatement().execute("PRAGMA user_version=999");
    }
    rejects(() -> open(dir));
    for (String invalid : new String[] {"-1", "00", "18446744073709551616", "1.0", ""}) {
      rejects(() -> NativeSqlStore.counter(invalid));
    }
  }
  @Test public void existingStateNeverReseededAndSchemaTamperingRejects() throws Exception {
    for (String damage : new String[] {"truncate", "missing", "metadata", "table", "constraint"}) {
      Path dir = temporary.newFolder().toPath();
      try (NativeSqlStore store = open(dir)) { seed(store); }
      if ("truncate".equals(damage)) {
        java.nio.file.Files.write(dir.resolve("scheduler.db"), new byte[0]);
      } else if ("missing".equals(damage)) {
        java.nio.file.Files.delete(dir.resolve("scheduler.db"));
      } else {
        try (Connection c = DriverManager.getConnection(
            "jdbc:sqlite:" + dir.resolve("scheduler.db"))) {
          if ("metadata".equals(damage)) { c.createStatement().execute("DELETE FROM metadata"); }
          if ("table".equals(damage)) { c.createStatement().execute("DROP TABLE observations"); }
          if ("constraint".equals(damage)) {
            c.createStatement().execute("DROP TABLE observations");
            c.createStatement().execute("CREATE TABLE observations(scope TEXT,cursor TEXT,body TEXT)");
          }
        }
      }
      rejects(() -> open(dir));
      if ("metadata".equals(damage)) {
        try (Connection c = DriverManager.getConnection(
            "jdbc:sqlite:" + dir.resolve("scheduler.db"));
            java.sql.ResultSet rows = c.createStatement().executeQuery("SELECT count(*) FROM metadata")) {
          assertTrue(rows.next()); assertEquals(0, rows.getInt(1));
        }
      }
    }
  }
  @Test public void nestedReadCannotMutateAndCaughtMutatorFailuresPoison() throws Exception {
    try (NativeSqlStore store = open(temporary.newFolder().toPath())) {
      seed(store);
      rejects(() -> store.write(tx -> {
        tx.acknowledgeCommand("cmd");
        try {
          store.read(inner -> { tx.addInstance(job, "1"); return null; });
        } catch (Exception expected) { }
        return null;
      }));
      assertFalse(store.read(tx -> tx.hasInstance(job, "1")));
      assertEquals(1, (int) store.read(tx -> tx.pendingCommands().size()));
      for (boolean conflict : new boolean[] {false, true}) {
        rejects(() -> store.write(tx -> {
          tx.acknowledgeCommand("cmd");
          try {
            if (conflict) { tx.completeAttempt("missing", "succeeded"); }
            else { tx.addInstance(job, "invalid/token"); }
          } catch (Exception expected) { }
          return null;
        }));
        assertEquals(1, (int) store.read(tx -> tx.pendingCommands().size()));
      }
    }
  }
  @Test public void symlinkedStateRefuses() throws Exception {
    Path real = temporary.newFolder().toPath();
    Path link = temporary.getRoot().toPath().resolve("link");
    java.nio.file.Files.createSymbolicLink(link, real);
    rejects(() -> open(link));
    java.nio.file.Files.createSymbolicLink(real.resolve("owner.lock"),
        temporary.newFile().toPath());
    rejects(() -> open(real));
  }
  @Test public void killedProcessRecoversBeforeAndAfterCommit() throws Exception {
    String java = System.getProperty("java.home") + "/bin/java";
    String classpath = System.getProperty("java.class.path");
    for (String mode : new String[] {"crash-before", "crash-after"}) {
      Path dir = temporary.newFolder().toPath();
      Process child = new ProcessBuilder(java, "--enable-native-access=ALL-UNNAMED",
          "--illegal-native-access=deny", "-cp", classpath,
          NativeStoreTool.class.getName(), mode, dir.toString()).inheritIO().start();
      assertTrue(child.waitFor(20, TimeUnit.SECONDS));
      assertEquals("crash-before".equals(mode) ? 71 : 72, child.exitValue());
      try (NativeSqlStore store = new NativeSqlStore(dir, "lab", "recovery-1")) {
        String body = store.read(tx -> tx.jobBody(
            new NativeSqlStore.JobKey("lab", "test", "qualification")));
        if ("crash-before".equals(mode)) { assertNull(body); } else { assertEquals("job", body); }
      }
    }
  }
}
