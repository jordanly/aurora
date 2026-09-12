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
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.HostMaintenanceRequest;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IHostMaintenanceRequest;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SqliteStorageTest {
  @Rule
  public TemporaryFolder temporary = new TemporaryFolder();

  private Path path;
  private SqliteStorage storage;

  @Before
  public void setUp() {
    path = temporary.getRoot().toPath().resolve("storage.db");
    storage = SqliteStorage.open(path);
  }

  @After
  public void tearDown() {
    storage.close();
  }

  private static void populate(Storage.MutableStoreProvider stores) {
    var task = TaskTestUtil.makeTask("task", TaskTestUtil.JOB);
    var config = task.getAssignedTask().getTask();
    stores.getSchedulerStore().saveFrameworkId("framework");
    stores.getCronJobStore().saveAcceptedJob(IJobConfiguration.build(new JobConfiguration()
        .setKey(TaskTestUtil.JOB.newBuilder()).setTaskConfig(config.newBuilder())
        .setInstanceCount(1).setCronSchedule("0 * * * *")));
    stores.getUnsafeTaskStore().saveTasks(Set.of(task));
    stores.getQuotaStore().saveQuota("role", IResourceAggregate.build(new ResourceAggregate()
        .setResources(Set.of(Resource.numCpus(2), Resource.ramMb(1024), Resource.diskMb(1024)))));
    stores.getAttributeStore().saveHostAttributes(IHostAttributes.build(new HostAttributes()
        .setHost("host")
        .setSlaveId("agent")
        .setMode(MaintenanceMode.NONE)
        .setAttributes(Set.of())));
    stores.getJobUpdateStore().saveJobUpdate(IJobUpdate.build(new JobUpdate()
        .setSummary(new JobUpdateSummary().setUser("user")
            .setKey(new JobUpdateKey(TaskTestUtil.JOB.newBuilder(), "update")))
        .setInstructions(new JobUpdateInstructions().setInitialState(Set.of())
            .setDesiredState(new InstanceTaskConfig().setTask(config.newBuilder())
                .setInstances(Set.of(new Range(0, 0))))
            .setSettings(new JobUpdateSettings()))));
    stores.getHostMaintenanceStore().saveHostMaintenanceRequest(IHostMaintenanceRequest.build(
        new HostMaintenanceRequest().setHost("host").setCreatedTimestampMs(1).setTimeoutSecs(30)));
  }

  private static List<Object> snapshot(Storage.StoreProvider stores) {
    return List.of(stores.getSchedulerStore().fetchFrameworkId(),
        ImmutableList.copyOf(stores.getCronJobStore().fetchJobs()),
        ImmutableSet.copyOf(stores.getTaskStore().fetchTasks(Query.unscoped())),
        stores.getQuotaStore().fetchQuotas(), stores.getAttributeStore().getHostAttributes(),
        stores.getJobUpdateStore().fetchJobUpdates(JobUpdateStore.MATCH_ALL),
        stores.getHostMaintenanceStore().getHostMaintenanceRequests());
  }

  private static void clear(Storage.MutableStoreProvider stores) {
    stores.getSchedulerStore().saveFrameworkId("changed");
    stores.getCronJobStore().deleteJobs();
    stores.getUnsafeTaskStore().deleteAllTasks();
    stores.getQuotaStore().deleteQuotas();
    stores.getAttributeStore().deleteHostAttributes();
    stores.getJobUpdateStore().deleteAllUpdates();
    stores.getHostMaintenanceStore().deleteHostMaintenanceRequests();
  }

  @Test
  public void testAllSevenStoresCommitAndFailedDeleteSurviveReopen() throws Exception {
    storage.write("populate", (NoResult.Quiet) SqliteStorageTest::populate);
    List<Object> expected = storage.read(SqliteStorageTest::snapshot);
    IOException failure = new IOException("rollback all stores");
    try {
      storage.write("delete", stores -> {
        clear(stores);
        throw failure;
      });
      fail("Failed write committed");
    } catch (IOException actual) {
      assertSame(failure, actual);
    }
    storage.close();
    storage = SqliteStorage.open(path);
    assertEquals(expected, storage.read(SqliteStorageTest::snapshot));
    assertTrue(storage.isCommitted("populate"));
    assertFalse(storage.isCommitted("delete"));
    storage.write("clear", (NoResult.Quiet) SqliteStorageTest::clear);
    List<Object> cleared = storage.read(SqliteStorageTest::snapshot);
    storage.close();
    storage = SqliteStorage.open(path);
    assertEquals(cleared, storage.read(SqliteStorageTest::snapshot));
  }

  @Test
  public void testCaughtNestedFailureRollsBackAllStores() {
    List<Object> before = storage.read(SqliteStorageTest::snapshot);
    expectFailure(StorageException.class, () -> storage.write("outer", stores -> {
      populate(stores);
      try {
        storage.write((NoResult<IOException>) nested -> {
          clear(nested);
          throw new IOException("nested failure");
        });
      } catch (IOException expected) {
        // The outer callback returning normally must not allow a commit.
      }
      return null;
    }));
    assertEquals(before, storage.read(SqliteStorageTest::snapshot));
    assertFalse(storage.isCommitted("outer"));
  }

  @Test
  public void testAllStoresShareAReadSnapshotDuringCommit() throws Exception {
    List<Object> before = storage.read(SqliteStorageTest::snapshot);
    CountDownLatch snapshotEstablished = new CountDownLatch(1);
    CountDownLatch committed = new CountDownLatch(1);
    var executor = Executors.newSingleThreadExecutor();
    try {
      var reader = executor.submit(() -> storage.read(stores -> {
        assertEquals(before.getFirst(), stores.getSchedulerStore().fetchFrameworkId());
        snapshotEstablished.countDown();
        assertTrue(committed.await(30, TimeUnit.SECONDS));
        return snapshot(stores);
      }));
      assertTrue(snapshotEstablished.await(30, TimeUnit.SECONDS));
      storage.write("populate", (NoResult.Quiet) SqliteStorageTest::populate);
      committed.countDown();
      assertEquals(before, reader.get(30, TimeUnit.SECONDS));
      assertNotEquals(before, storage.read(SqliteStorageTest::snapshot));
    } finally {
      committed.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testStoreHandlesRequireAnActiveTransactionAndCannotPromoteReads() {
    QuotaStore.Mutable escaped = storage.write(stores -> stores.getQuotaStore());
    expectFailure(IllegalStateException.class, escaped::fetchQuotas);
    expectFailure(IllegalStateException.class, escaped::deleteQuotas);
    storage.read(stores -> {
      expectFailure(IllegalStateException.class, escaped::deleteQuotas);
      return null;
    });
    expectFailure(StorageException.class, () -> storage.read(stores ->
        storage.write((NoResult.Quiet) SqliteStorageTest::populate)));
  }

  @Test
  public void testUnsupportedAndCorruptPayloadsFailClosed() throws Exception {
    storage.write((NoResult.Quiet) SqliteStorageTest::populate);
    storage.close();
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + path);
         var statement = connection.createStatement()) {
      statement.executeUpdate("UPDATE quotas SET payload_version=99");
    }
    storage = SqliteStorage.open(path);
    expectFailure(StorageException.class, () -> storage.read(s -> s.getQuotaStore().fetchQuotas()));
    storage.close();
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + path);
         var statement = connection.createStatement()) {
      statement.executeUpdate("UPDATE quotas SET payload_version=1,payload=X'FF'");
    }
    storage = SqliteStorage.open(path);
    expectFailure(StorageException.class, () -> storage.read(s -> s.getQuotaStore().fetchQuotas()));
  }

  private static void expectFailure(Class<? extends Throwable> type, Runnable operation) {
    try {
      operation.run();
      fail("Expected " + type.getSimpleName());
    } catch (Throwable failure) {
      if (!type.isInstance(failure)) {
        throw failure;
      }
    }
  }
}
