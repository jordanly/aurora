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

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.aurora.common.util.BuildInfo;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.BatchJobUpdateStrategy;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.HostMaintenanceRequest;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateStrategy;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.PartitionPolicy;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IHostMaintenanceRequest;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.log.SnapshotterImpl;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SqliteRecoveryTest {
  @Rule
  public TemporaryFolder temporary = new TemporaryFolder();

  private Path path(String name) {
    return temporary.getRoot().toPath().resolve(name);
  }

  private static SnapshotterImpl snapshotter() {
    return new SnapshotterImpl(new BuildInfo(), Clock.SYSTEM_CLOCK);
  }

  private static void populate(Storage.MutableStoreProvider stores) {
    var terminal = TaskTestUtil.makeTask("historical-task", TaskTestUtil.JOB).newBuilder()
        .setStatus(ScheduleStatus.FINISHED);
    // Keep the historical executor bytes for terminal history. Only the cron,
    // which could execute after startup, is explicitly converted to Go.
    terminal.getAssignedTask().getTask().setExecutorConfig(new ExecutorConfig("thermos", "legacy"));
    var config = terminal.getAssignedTask().getTask().deepCopy()
        .setExecutorConfig(new ExecutorConfig("go-process", "{\"version\":\"aurora-process-v1\","
            + "\"argv\":[\"/bin/true\"],\"env\":{},\"graceMillis\":1000}"))
        .setResources(Set.of(Resource.numCpus(1), Resource.ramMb(1), Resource.diskMb(1)))
        .setContainer(Container.mesos(new MesosContainer()))
        .setMesosFetcherUris(Set.of())
        .setPartitionPolicy(new PartitionPolicy().setReschedule(false));
    stores.getSchedulerStore().saveFrameworkId("historical-framework");
    stores.getCronJobStore().saveAcceptedJob(IJobConfiguration.build(new JobConfiguration()
        .setKey(TaskTestUtil.JOB.newBuilder()).setTaskConfig(config)
        .setInstanceCount(1).setCronSchedule("0 * * * *")));
    stores.getUnsafeTaskStore().saveTasks(Set.of(IScheduledTask.build(terminal)));
    stores.getQuotaStore().saveQuota("role", IResourceAggregate.build(new ResourceAggregate()
        .setResources(Set.of(Resource.numCpus(2), Resource.ramMb(1024), Resource.diskMb(1024)))));
    stores.getAttributeStore().saveHostAttributes(IHostAttributes.build(new HostAttributes()
        .setHost("host").setSlaveId("agent").setMode(MaintenanceMode.DRAINED)
        .setAttributes(Set.of())));
    var key = new JobUpdateKey(TaskTestUtil.JOB.newBuilder(), "historical-update");
    stores.getJobUpdateStore().saveJobUpdate(IJobUpdate.build(new JobUpdate()
        .setSummary(new JobUpdateSummary().setUser("user").setKey(key))
        .setInstructions(new JobUpdateInstructions().setInitialState(Set.of())
            .setDesiredState(new InstanceTaskConfig().setTask(config.deepCopy())
                .setInstances(Set.of(new Range(0, 0))))
            .setSettings(new JobUpdateSettings().setUpdateStrategy(JobUpdateStrategy.batchStrategy(
                new BatchJobUpdateStrategy().setGroupSize(1)))))));
    stores.getJobUpdateStore().saveJobUpdateEvent(IJobUpdateKey.build(key),
        IJobUpdateEvent.build(new JobUpdateEvent().setStatus(JobUpdateStatus.ROLLED_FORWARD)
            .setTimestampMs(100).setUser("user").setMessage("complete")));
    stores.getJobUpdateStore().saveJobInstanceUpdateEvent(IJobUpdateKey.build(key),
        IJobInstanceUpdateEvent.build(new JobInstanceUpdateEvent().setInstanceId(0)
            .setTimestampMs(90).setAction(JobUpdateAction.INSTANCE_UPDATED)));
    stores.getHostMaintenanceStore().saveHostMaintenanceRequest(IHostMaintenanceRequest.build(
        new HostMaintenanceRequest().setHost("host").setCreatedTimestampMs(1).setTimeoutSecs(30)));
  }

  private static List<Object> sevenStores(Storage.StoreProvider stores) {
    return List.of(stores.getSchedulerStore().fetchFrameworkId(),
        com.google.common.collect.ImmutableList.copyOf(stores.getCronJobStore().fetchJobs()),
        Set.copyOf(stores.getTaskStore().fetchTasks(Query.unscoped())),
        stores.getQuotaStore().fetchQuotas(), stores.getAttributeStore().getHostAttributes(),
        stores.getJobUpdateStore().fetchJobUpdates(JobUpdateStore.MATCH_ALL),
        stores.getHostMaintenanceStore().getHostMaintenanceRequests());
  }

  private Snapshot historical() {
    try (SqliteStorage source = SqliteStorage.open(path("seed-" + System.nanoTime() + ".db"))) {
      source.write((NoResult.Quiet) SqliteRecoveryTest::populate);
      return source.read(snapshotter()::from);
    }
  }

  private Path backup(Snapshot snapshot) throws Exception {
    Path backup = path("snapshot-" + System.nanoTime());
    Files.write(backup, new TSerializer(new TBinaryProtocol.Factory()).serialize(snapshot));
    return backup;
  }

  @Test
  public void versionThreeBackupMigratesOnlyWhenRestoredOwnerOpens() throws Exception {
    Path source = path("version-three.db");
    try (SqliteStorage storage = SqliteStorage.open(path("live.db"))) {
      storage.write("legacy-operation", stores -> {
        populate(stores);
        return null;
      });
      storage.backup(source);
    }
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + source);
         var statement = connection.createStatement()) {
      for (String table : List.of("agent_retention", "attempt_retention", "receipt_watermarks",
          "automatic_outcome")) {
        statement.execute("DROP TABLE " + table);
      }
      statement.execute("PRAGMA user_version=3");
    }
    byte[] original = Files.readAllBytes(source);
    Path restored = path("restored-version-three.db");
    SqliteRecovery.restoreBackup(source, restored);
    assertArrayEquals(original, Files.readAllBytes(source));
    assertArrayEquals(original, Files.readAllBytes(restored));
    try (SqliteStorage storage = SqliteStorage.open(restored)) {
      assertTrue(storage.isCommitted("legacy-operation"));
      assertEquals(java.util.Optional.of("historical-framework"),
          storage.read(stores -> stores.getSchedulerStore().fetchFrameworkId()));
      assertTrue(storage.read(stores -> storage.effects().retention("agent").isEmpty()));
      storage.write(stores -> null);
    }
    assertArrayEquals(original, Files.readAllBytes(source));
  }

  @Test
  public void historicalSnapshotPreservesContentsAndCountsAcrossAllSevenStores() throws Exception {
    List<Object> expected;
    Snapshot snapshot;
    try (SqliteStorage source = SqliteStorage.open(path("seed.db"))) {
      source.write((NoResult.Quiet) SqliteRecoveryTest::populate);
      expected = source.read(SqliteRecoveryTest::sevenStores);
      snapshot = source.read(snapshotter()::from);
    }
    Path backup = backup(snapshot);
    byte[] original = Files.readAllBytes(backup);
    Path destination = path("import.db");
    SqliteRecovery.importSnapshot(backup, destination);
    assertArrayEquals(original, Files.readAllBytes(backup));
    try (SqliteStorage imported = SqliteStorage.open(destination)) {
      assertEquals(expected, imported.read(SqliteRecoveryTest::sevenStores));
      assertTrue(imported.isCommitted("historical-snapshot-import"));
      assertTrue(imported.read(stores -> imported.effects().pending(100).isEmpty()));
      Snapshot restored = imported.read(snapshotter()::from);
      assertEquals(1, restored.getTasksSize());
      assertEquals(1, restored.getCronJobsSize());
      assertEquals(1, restored.getQuotaConfigurationsSize());
      assertEquals(1, restored.getHostAttributesSize());
      assertEquals(1, restored.getJobUpdateDetailsSize());
      assertEquals(1, restored.getHostMaintenanceRequestsSize());
      assertEquals("historical-framework", restored.getSchedulerMetadata().getFrameworkId());
      assertEquals("thermos", restored.getTasks().iterator().next()
          .getAssignedTask().getTask().getExecutorConfig().getName());
    }
  }

  @Test
  public void completeSqliteRestorePreservesExactBackupAndDurableEffects() throws Exception {
    Path backup = path("complete.backup");
    var pending = new SqliteEffects.Command("pending", "agent", "task", "Run", 2, new byte[]{1, 2});
    var done = new SqliteEffects.Command("done", "agent", "task", "Stop", 2, new byte[]{3});
    var receipt = new SqliteEffects.ReceiptKey("agent", "journal", 3);
    List<Object> expected;
    try (SqliteStorage source = SqliteStorage.open(path("source.db"))) {
      source.write("original-operation", stores -> {
        populate(stores);
        source.effects().enqueue(pending);
        source.effects().enqueue(done);
        source.effects().acknowledge("done");
        source.effects().recordReceipt(receipt, 2, new byte[]{4, 5});
        return null;
      });
      expected = source.read(SqliteRecoveryTest::sevenStores);
      source.backup(backup);
    }
    byte[] original = Files.readAllBytes(backup);
    Path destination = path("restored.db");
    SqliteRecovery.restoreBackup(backup, destination);
    assertArrayEquals(original, Files.readAllBytes(backup));
    assertArrayEquals(original, Files.readAllBytes(destination));
    assertFalse(Files.exists(path("complete.backup.owner")));
    try (SqliteStorage restored = SqliteStorage.open(destination)) {
      assertEquals(expected, restored.read(SqliteRecoveryTest::sevenStores));
      assertTrue(restored.isCommitted("original-operation"));
      restored.write(stores -> {
        assertEquals(pending, restored.effects().pending(10).getFirst().command());
        assertEquals(1, restored.effects().pending(10).size());
        assertEquals(done, restored.effects().command("done").orElseThrow());
        assertFalse(restored.effects().acknowledge("done"));
        assertFalse(restored.effects().recordReceipt(receipt, 2, new byte[]{4, 5}));
        assertEquals(3, restored.effects().committedCursor("agent", "journal"));
        return null;
      });
    }
  }

  @Test
  public void historicalImportRejectsPendingTasksAndActiveTasksOrUpdates() throws Exception {
    for (ScheduleStatus status : List.of(ScheduleStatus.RUNNING, ScheduleStatus.PENDING)) {
      Snapshot snapshot = historical();
      snapshot.getTasks().iterator().next().setStatus(status);
      assertImportRejected(snapshot);
    }
    Snapshot update = historical();
    update.getJobUpdateDetails().iterator().next().getDetails().getUpdateEvents().getFirst()
        .setStatus(JobUpdateStatus.ROLL_FORWARD_PAUSED);
    assertImportRejected(update);
  }

  @Test
  public void historicalImportRejectsThermosCronAndMalformedLaterRecords() throws Exception {
    Snapshot cron = historical();
    cron.getCronJobs().iterator().next().getJobConfiguration().getTaskConfig()
        .setExecutorConfig(new ExecutorConfig("thermos", "legacy"));
    assertImportRejected(cron);
    Snapshot quota = historical();
    quota.getQuotaConfigurations().iterator().next().getQuota().setResources(Set.of());
    assertImportRejected(quota);
    Snapshot host = historical();
    host.getHostAttributes().iterator().next().unsetSlaveId();
    assertImportRejected(host);
    Snapshot duplicate = historical();
    var first = duplicate.getTasks().iterator().next();
    duplicate.setTasks(Set.of(first, first.deepCopy().setStatus(ScheduleStatus.FAILED)));
    assertImportRejected(duplicate);
  }

  private void assertImportRejected(Snapshot snapshot) throws Exception {
    Path input = backup(snapshot);
    byte[] original = Files.readAllBytes(input);
    Path destination = path("rejected-" + System.nanoTime());
    expectFailure(() -> SqliteRecovery.importSnapshot(input, destination));
    assertFalse(Files.exists(destination));
    assertArrayEquals(original, Files.readAllBytes(input));
    try (var files = Files.list(temporary.getRoot().toPath())) {
      assertFalse(files.anyMatch(file ->
          java.util.Objects.requireNonNull(file.getFileName()).toString()
              .startsWith(".aurora-recovery-")));
    }
  }

  @Test
  public void strictSnapshotRejectsTrailingAndUnknownFieldsWithoutPublishing() throws Exception {
    byte[] original = Files.readAllBytes(backup(historical()));
    byte[] trailing = Arrays.copyOf(original, original.length + 1);
    Path input = path("trailing");
    Files.write(input, trailing);
    expectFailure(() -> SqliteRecovery.importSnapshot(input, path("trailing.db")));
    assertFalse(Files.exists(path("trailing.db")));
    // Replace root STOP with an unknown i32 field followed by STOP.
    byte[] unknown = Arrays.copyOf(original, original.length + 7);
    byte[] field = {8, 0, 99, 0, 0, 0, 1, 0};
    System.arraycopy(field, 0, unknown, original.length - 1, field.length);
    Files.write(path("unknown"), unknown);
    expectFailure(() -> SqliteRecovery.importSnapshot(path("unknown"), path("unknown.db")));
    assertFalse(Files.exists(path("unknown.db")));
  }

  @Test
  public void refusesExistingDestinationsAndLiveOrUnsupportedSqliteSources() throws Exception {
    Path backup = path("standalone");
    try (SqliteStorage source = SqliteStorage.open(path("live.db"))) {
      source.write((NoResult.Quiet) SqliteRecoveryTest::populate);
      source.backup(backup);
      expectFailure(() -> SqliteRecovery.restoreBackup(path("live.db"), path("unsafe.db")));
      assertFalse(Files.exists(path("unsafe.db")));
    }
    Files.write(path("existing"), new byte[]{7, 8});
    expectFailure(() -> SqliteRecovery.restoreBackup(backup, path("existing")));
    assertArrayEquals(new byte[]{7, 8}, Files.readAllBytes(path("existing")));
    Files.write(path("locked.owner"), new byte[0]);
    expectFailure(() -> SqliteRecovery.restoreBackup(backup, path("locked")));
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + backup);
         var statement = connection.createStatement()) {
      statement.execute("PRAGMA user_version=99");
    }
    expectFailure(() -> SqliteRecovery.restoreBackup(backup, path("future.db")));
    assertFalse(Files.exists(path("future.db")));
  }

  private static void expectFailure(Runnable operation) {
    try {
      operation.run();
      fail("Expected fail-closed recovery");
    } catch (StorageException expected) {
      // No partial destination may be published.
    }
  }
}
