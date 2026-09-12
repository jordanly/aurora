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
package org.apache.aurora.scheduler;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.execution.ExecutionControl;
import org.apache.aurora.scheduler.execution.TaskObservation;
import org.apache.aurora.scheduler.execution.TaskUpdate;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.easymock.Capture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.TaskStatusHandlerImpl.statName;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TaskStatusHandlerImplTest extends EasyMockTest {
  private static final String TASK_ID_A = "task_id_a";
  private StateManager stateManager;
  private StorageTestUtil storageUtil;
  private ExecutionControl driver;
  private FakeStatsProvider stats;
  private TaskStatusHandlerImpl statusHandler;

  @Before
  public void setUp() {
    stateManager = createMock(StateManager.class);
    storageUtil = new StorageTestUtil(this);
    driver = createMock(ExecutionControl.class);
    stats = new FakeStatsProvider();
    statusHandler = new TaskStatusHandlerImpl(storageUtil.storage, stateManager, stats,
        driver, new LinkedBlockingQueue<>(), 1000, new CachedCounters(stats));
    statusHandler.startAsync();
  }

  @After
  public void after() {
    if (statusHandler.state() != com.google.common.util.concurrent.Service.State.FAILED) {
      statusHandler.stopAsync().awaitTerminated();
    }
  }

  @Test
  public void testForwardsStatusUpdates() throws Exception {
    assertForwarded(Optional.of("fake message"));
  }

  @Test
  public void testForwardsAbsentMessage() throws Exception {
    assertForwarded(Optional.empty());
  }

  private void assertForwarded(Optional<String> message) throws Exception {
    TaskObservation observation = new TaskObservation(TASK_ID_A, RUNNING,
        Optional.of("reconciliation"), message);
    TaskUpdate update = createMock(TaskUpdate.class);
    expect(update.observe()).andReturn(observation);
    storageUtil.expectWrite();
    expect(stateManager.changeState(storageUtil.mutableStoreProvider, TASK_ID_A,
        Optional.empty(), RUNNING, message)).andReturn(StateChangeResult.SUCCESS);
    CountDownLatch latch = new CountDownLatch(1);
    update.acknowledge();
    waitAndAnswer(latch);
    control.replay();
    statusHandler.statusUpdate(update);
    assertTrue(latch.await(5L, TimeUnit.SECONDS));
    assertEquals(1L, stats.getValue(statName(observation, StateChangeResult.SUCCESS)));
  }

  @Test
  public void testFailedStatusUpdate() throws Exception {
    TaskUpdate update = createMock(TaskUpdate.class);
    expect(update.observe()).andReturn(new TaskObservation(TASK_ID_A, RUNNING,
        Optional.empty(), Optional.of("fake message")));
    storageUtil.expectWrite();
    CountDownLatch latch = new CountDownLatch(1);
    expect(stateManager.changeState(storageUtil.mutableStoreProvider, TASK_ID_A, Optional.empty(),
        RUNNING, Optional.of("fake message"))).andAnswer(() -> {
          latch.countDown();
          throw new StorageException("Injected error");
        });
    // No acknowledgement is permitted when the storage write fails.
    control.replay();
    statusHandler.statusUpdate(update);
    assertTrue(latch.await(5L, TimeUnit.SECONDS));
  }

  @Test
  public void testThreadFailure() throws Exception {
    statusHandler.stopAsync().awaitTerminated();
    BlockingQueue<TaskUpdate> queue = createMock(new Clazz<BlockingQueue<TaskUpdate>>() { });
    statusHandler = new TaskStatusHandlerImpl(storageUtil.storage, stateManager, stats,
        driver, queue, 1000, new CachedCounters(stats));
    TaskUpdate update = createMock(TaskUpdate.class);
    expect(queue.add(update)).andReturn(true);
    expect(queue.take()).andThrow(new RuntimeException());
    CountDownLatch latch = new CountDownLatch(1);
    driver.abort();
    waitAndAnswer(latch);
    control.replay();
    statusHandler.startAsync();
    statusHandler.statusUpdate(update);
    assertTrue(latch.await(5L, TimeUnit.SECONDS));
    try {
      statusHandler.awaitTerminated(5, TimeUnit.SECONDS);
      org.junit.Assert.fail("Queue failure must fail the status service");
    } catch (IllegalStateException expected) {
      assertEquals(com.google.common.util.concurrent.Service.State.FAILED, statusHandler.state());
    }
  }

  @Test
  public void testBatchAcknowledgesOnlyAfterCommitInArrivalOrder() throws Exception {
    assertBatchAcknowledgement(false);
  }

  @Test
  public void testFailedBatchAcknowledgesNeitherUpdate() throws Exception {
    assertBatchAcknowledgement(true);
  }

  private void assertBatchAcknowledgement(boolean failSecond) throws Exception {
    statusHandler.stopAsync().awaitTerminated();
    BlockingQueue<TaskUpdate> queue = new LinkedBlockingQueue<>();
    statusHandler = new TaskStatusHandlerImpl(storageUtil.storage, stateManager, stats,
        driver, queue, 1000, new CachedCounters(stats));
    TaskUpdate first = createMock(TaskUpdate.class);
    TaskUpdate second = createMock(TaskUpdate.class);
    queue.add(first);
    queue.add(second);
    AtomicBoolean inTransaction = new AtomicBoolean();
    AtomicBoolean committed = new AtomicBoolean();
    CountDownLatch completed = new CountDownLatch(1);
    Capture<Storage.MutateWork<Void, RuntimeException>> work = createCapture();
    control.checkOrder(true);
    expect(storageUtil.storage.<Void, RuntimeException>write(capture(work))).andAnswer(() -> {
      inTransaction.set(true);
      try {
        work.getValue().apply(storageUtil.mutableStoreProvider);
        committed.set(true);
        return null;
      } finally {
        inTransaction.set(false);
        if (failSecond) {
          completed.countDown();
        }
      }
    });
    expect(first.observe()).andAnswer(() -> {
      assertTrue(inTransaction.get());
      return new TaskObservation("first", RUNNING, Optional.empty(), Optional.empty());
    });
    expect(stateManager.changeState(storageUtil.mutableStoreProvider, "first", Optional.empty(),
        RUNNING, Optional.empty())).andReturn(StateChangeResult.SUCCESS);
    expect(second.observe()).andAnswer(() -> {
      assertTrue(inTransaction.get());
      if (failSecond) {
        throw new IllegalArgumentException("unsupported observation");
      }
      return new TaskObservation("second", FAILED, Optional.empty(), Optional.empty());
    });
    if (!failSecond) {
      expect(stateManager.changeState(storageUtil.mutableStoreProvider, "second", Optional.empty(),
          FAILED, Optional.empty())).andReturn(StateChangeResult.SUCCESS);
      first.acknowledge();
      expectLastCall().andAnswer(() -> {
        assertTrue(committed.get());
        assertFalse(inTransaction.get());
        return null;
      });
      second.acknowledge();
      expectLastCall().andAnswer(() -> {
        assertTrue(committed.get());
        completed.countDown();
        return null;
      });
    }

    control.replay();
    statusHandler.startAsync();
    assertTrue(completed.await(5L, TimeUnit.SECONDS));
    statusHandler.stopAsync().awaitTerminated();
    assertEquals(!failSecond, committed.get());
  }

  private static void waitAndAnswer(CountDownLatch latch) {
    expectLastCall().andAnswer(() -> {
      latch.countDown();
      return null;
    });
  }
}
