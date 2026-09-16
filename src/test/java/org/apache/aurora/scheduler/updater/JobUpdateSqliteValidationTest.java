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
package org.apache.aurora.scheduler.updater;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Range;
import org.apache.aurora.scheduler.SchedulerModule.TaskEventBatchWorker;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.sqlite.SqliteStorage;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.aurora.scheduler.updater.JobUpdateController.AuditData;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobUpdateSqliteValidationTest {
  private static final IJobUpdateKey KEY = IJobUpdateKey.build(
      new JobUpdateKey(TaskTestUtil.JOB.newBuilder(), "update"));
  private static final AuditData AUDIT = new AuditData("user", Optional.empty());

  @Rule
  public TemporaryFolder temporary = new TemporaryFolder();

  private JobUpdateControllerImpl controller(SqliteStorage storage, UpdateFactory factory) {
    return new JobUpdateControllerImpl(factory, storage,
        createMock(ScheduledExecutorService.class), createMock(StateManager.class),
        createMock(UpdateAgentReserver.class), new FakeClock(), createMock(Lifecycle.class),
        createMock(TaskEventBatchWorker.class), new FakeStatsProvider(),
        createMock(SlaKillController.class));
  }

  private static IJobUpdate update() {
    return IJobUpdate.build(new JobUpdate()
        .setSummary(new JobUpdateSummary().setKey(KEY.newBuilder()).setUser("user"))
        .setInstructions(new JobUpdateInstructions().setInitialState(Set.of())
            .setDesiredState(new InstanceTaskConfig()
                .setTask(TaskTestUtil.makeTask("task", TaskTestUtil.JOB)
                    .getAssignedTask().getTask().newBuilder())
                .setInstances(Set.of(new Range(0, 0))))
            .setSettings(new JobUpdateSettings())));
  }

  private static void saveUpdate(SqliteStorage storage, JobUpdateStatus status) {
    storage.write(stores -> {
      stores.getJobUpdateStore().saveJobUpdate(update());
      stores.getJobUpdateStore().saveJobUpdateEvent(KEY, IJobUpdateEvent.build(
          new JobUpdateEvent().setStatus(status).setTimestampMs(1)));
      return null;
    });
  }

  private interface Request {
    void run() throws UpdateStateException;
  }

  private static void assertRejected(SqliteStorage storage, Request request) {
    assertTrue(storage.write(stores -> {
      try {
        request.run();
        return false;
      } catch (UpdateStateException expected) {
        return true;
      }
    }));
    storage.write(stores -> {
      stores.getSchedulerStore().saveFrameworkId("still-writable");
      return null;
    });
  }

  @Test
  public void missingAndTerminalUpdatesDoNotPoisonEnclosingWrite() {
    try (var storage = SqliteStorage.open(temporary.getRoot().toPath().resolve("updates.db"))) {
      var failures = new AtomicInteger();
      storage.setWriteFailureHandler(failure -> failures.incrementAndGet());
      var controller = controller(storage, createMock(UpdateFactory.class));
      assertRejected(storage, () -> controller.pause(KEY, AUDIT));
      assertRejected(storage, () -> controller.resume(KEY, AUDIT));
      assertRejected(storage, () -> controller.abort(KEY, AUDIT));
      assertRejected(storage, () -> controller.rollback(KEY, AUDIT));
      saveUpdate(storage, JobUpdateStatus.ROLLED_FORWARD);
      var before = storage.read(stores -> stores.getJobUpdateStore().fetchJobUpdate(KEY));
      assertRejected(storage, () -> controller.pause(KEY, AUDIT));
      assertRejected(storage, () -> controller.resume(KEY, AUDIT));
      assertRejected(storage, () -> controller.abort(KEY, AUDIT));
      assertRejected(storage, () -> controller.rollback(KEY, AUDIT));
      assertEquals(before, storage.read(stores -> stores.getJobUpdateStore().fetchJobUpdate(KEY)));
      assertEquals(0, failures.get());
    }
  }

  @Test
  public void activeUpdateCollisionDoesNotPoisonEnclosingWrite() {
    try (var storage = SqliteStorage.open(temporary.getRoot().toPath().resolve("collision.db"))) {
      var failures = new AtomicInteger();
      storage.setWriteFailureHandler(failure -> failures.incrementAndGet());
      saveUpdate(storage, JobUpdateStatus.ROLLING_FORWARD);
      UpdateFactory factory = createMock(UpdateFactory.class);
      expect(factory.newUpdate(anyObject(), eq(true))).andReturn(null);
      replay(factory);
      var controller = controller(storage, factory);
      var before = storage.read(stores -> stores.getJobUpdateStore().fetchJobUpdate(KEY));
      assertRejected(storage, () -> controller.start(update(), AUDIT));
      assertEquals(before, storage.read(stores -> stores.getJobUpdateStore().fetchJobUpdate(KEY)));
      assertEquals(0, failures.get());
      verify(factory);
    }
  }
}
