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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Service;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.BackoffStrategy;
import org.apache.aurora.scheduler.BatchWorker.Result;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BatchWorkerTest extends EasyMockTest {
  private static final String SERVICE_NAME = "TestWorker";
  private static final String BATCH_STAT = SERVICE_NAME + "_batches_processed";
  private static final long TIMEOUT_SECONDS = 10L;
  private FakeStatsProvider statsProvider;
  private StorageTestUtil storageUtil;
  private BatchWorker<Boolean> batchWorker;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    statsProvider = new FakeStatsProvider();
    batchWorker = new BatchWorker<Boolean>(storageUtil.storage, statsProvider, 2) {
      @Override
      protected String serviceName() {
        return SERVICE_NAME;
      }
    };
    addTearDown(() -> {
      if (batchWorker.state() != Service.State.FAILED) {
        batchWorker.stopAsync().awaitTerminated(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
      assertTrue(batchWorker.scheduledExecutor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    });
  }

  @Test
  public void testExecuteBeforeStartPreservesPollRequeueOrder() throws Exception {
    control.replay();
    List<Integer> order = Collections.synchronizedList(new ArrayList<>());
    CompletableFuture<Boolean> result1 = batchWorker.execute(store -> order.add(1));
    CompletableFuture<Boolean> result2 = batchWorker.execute(store -> order.add(2));
    CompletableFuture<Boolean> result3 = batchWorker.execute(store -> order.add(3));
    assertFalse(result1.isDone());
    batchWorker.startAsync().awaitRunning(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertTrue(result1.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertTrue(result2.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertTrue(result3.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertEquals(Arrays.asList(2, 3, 1), order);
  }

  @Test
  public void testExecuteThrowsFailsActiveAndQueuedResults() throws Exception {
    control.replay();
    RuntimeException failure = new IllegalArgumentException("work failed");
    CountDownLatch active = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    batchWorker.startAsync().awaitRunning(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    CompletableFuture<Boolean> result = batchWorker.execute(store -> {
      active.countDown();
      await(release);
      throw failure;
    });
    try {
      await(active);
      CompletableFuture<Boolean> queued = batchWorker.execute(store -> {
        throw new AssertionError("Queued work must not run after failure");
      });
      release.countDown();
      assertFailure(result, failure);
      assertFailure(queued, failure);
      assertEquals(Service.State.FAILED, batchWorker.state());
      assertRejected();
    } finally {
      release.countDown();
    }
  }

  @Test
  public void testExecuteWithReplay() throws Exception {
    BackoffStrategy backoff = createMock(BackoffStrategy.class);
    expect(backoff.calculateBackoffMs(EasyMock.anyLong())).andReturn(0L).anyTimes();
    control.replay();

    batchWorker.startAsync().awaitRunning(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    CompletableFuture<Boolean> result = batchWorker.executeWithReplay(
        backoff,
        store -> statsProvider.getValue(BATCH_STAT).longValue() > 1L
            ? new Result<>(true, true)
            : new Result<>(false, false));
    assertTrue(result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  public void testStopBeforeStartCancelsAndRejectsWork() throws Exception {
    control.replay();
    CompletableFuture<Boolean> queued = batchWorker.execute(store -> true);
    CompletableFuture<Boolean> retry = batchWorker.executeWithReplay(
        constantBackoff(0L), store -> new Result<>(false, false));

    batchWorker.stopAsync().awaitTerminated(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertTrue(queued.isCancelled());
    assertTrue(retry.isCancelled());
    assertRejected();
    assertEquals(0, statsProvider.getValue(SERVICE_NAME + "_queue_size").intValue());
  }

  @Test
  public void testStopFinishesActiveBatchAndCancelsQueuedWork() throws Exception {
    control.replay();
    CountDownLatch active = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    // Poll moves the head to the tail, leaving the next two items in the active batch.
    CompletableFuture<Boolean> queued = batchWorker.execute(store -> {
      throw new AssertionError("Queued work must not start after stop");
    });
    CompletableFuture<Boolean> first = batchWorker.execute(store -> {
      active.countDown();
      await(release);
      return true;
    });
    CompletableFuture<Boolean> second = batchWorker.execute(store -> {
      // No item may report success before the entire batch commits.
      assertFalse(first.isDone());
      return true;
    });
    batchWorker.startAsync().awaitRunning(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    try {
      await(active);
      batchWorker.stopAsync();
      assertEquals(Service.State.STOPPING, batchWorker.state());
      assertFalse(first.isDone());
      assertFalse(queued.isDone());
      assertRejected();
      release.countDown();
      assertTrue(first.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertTrue(second.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      batchWorker.awaitTerminated(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertCancelled(queued);
    } finally {
      release.countDown();
    }
  }

  @Test
  public void testStopCancelsDelayedRetryAndTerminatesRetryThread() throws Exception {
    control.replay();
    CountDownLatch attempted = new CountDownLatch(1);
    AtomicInteger attempts = new AtomicInteger();
    batchWorker.startAsync().awaitRunning(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    CompletableFuture<Boolean> retry = batchWorker.executeWithReplay(
        constantBackoff(TimeUnit.DAYS.toMillis(1)), store -> {
          attempts.incrementAndGet();
          attempted.countDown();
          return new Result<>(false, false);
        });
    await(attempted);
    // This work can complete only after the retry has been scheduled by the previous batch.
    assertTrue(batchWorker.execute(store -> true).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertTrue(batchWorker.scheduledExecutor.submit(() -> true)
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS));

    batchWorker.stopAsync().awaitTerminated(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertCancelled(retry);
    assertTrue(batchWorker.scheduledExecutor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertEquals(1, attempts.get());
    assertEquals(0, statsProvider.getValue(SERVICE_NAME + "_queue_size").intValue());
  }

  @Test
  public void testFailureFailsDelayedRetryAndTerminatesRetryThread() throws Exception {
    control.replay();
    CountDownLatch attempted = new CountDownLatch(1);
    batchWorker.startAsync().awaitRunning(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    CompletableFuture<Boolean> retry = batchWorker.executeWithReplay(
        constantBackoff(TimeUnit.DAYS.toMillis(1)), store -> {
          attempted.countDown();
          return new Result<>(false, false);
        });
    await(attempted);
    assertTrue(batchWorker.execute(store -> true).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertTrue(batchWorker.scheduledExecutor.submit(() -> true)
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    RuntimeException failure = new IllegalStateException("subsequent work failed");
    CompletableFuture<Boolean> failed = batchWorker.execute(store -> {
      throw failure;
    });

    assertFailure(failed, failure);
    assertFailure(retry, failure);
    assertTrue(batchWorker.scheduledExecutor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertEquals(0, statsProvider.getValue(SERVICE_NAME + "_queue_size").intValue());
  }

  @Test
  public void testEarlierResultFailsWhenSameBatchFails() throws Exception {
    control.replay();
    RuntimeException failure = new IllegalArgumentException("second item failed");
    // The poll/requeue order runs the second submission first.
    CompletableFuture<Boolean> failed = batchWorker.execute(store -> {
      throw failure;
    });
    CompletableFuture<Boolean> succeeded = batchWorker.execute(store -> true);
    batchWorker.startAsync();

    assertFailure(failed, failure);
    assertFailure(succeeded, failure);
  }

  @Test
  public void testStartupFailureFailsAcceptedWork() throws Exception {
    batchWorker.stopAsync();
    RuntimeException failure = new IllegalStateException("startup failed");
    batchWorker = new BatchWorker<Boolean>(storageUtil.storage, statsProvider, 2) {
      @Override
      protected void startUp() {
        throw failure;
      }
    };
    control.replay();
    CompletableFuture<Boolean> result = batchWorker.execute(store -> true);
    batchWorker.startAsync();

    assertFailure(result, failure);
    assertEquals(Service.State.FAILED, batchWorker.state());
  }

  @Test
  public void testStorageFailureFailsAcceptedWork() throws Exception {
    batchWorker.stopAsync();
    Storage storage = createMock(Storage.class);
    RuntimeException failure = new IllegalStateException("storage write failed");
    expect(storage.write(EasyMock.<Storage.MutateWork<Void, RuntimeException>>anyObject()))
        .andThrow(failure);
    batchWorker = new BatchWorker<>(storage, statsProvider, 2);
    control.replay();
    CompletableFuture<Boolean> result = batchWorker.execute(store -> true);
    batchWorker.startAsync();

    assertFailure(result, failure);
    assertEquals(Service.State.FAILED, batchWorker.state());
  }

  @Test
  public void testFailureAfterCallbackFailsStagedResultsAndDoesNotScheduleRetry() throws Exception {
    batchWorker.stopAsync();
    Storage storage = createMock(Storage.class);
    RuntimeException failure = new IllegalStateException("commit failed");
    expect(storage.write(EasyMock.<Storage.MutateWork<Void, RuntimeException>>anyObject()))
        .andAnswer(() -> {
          Storage.MutateWork<Void, RuntimeException> work = EasyMock.getCurrentArgument(0);
          work.apply(storageUtil.mutableStoreProvider);
          assertEquals(0, batchWorker.scheduledExecutor.submit(() -> 0).get().intValue());
          throw failure;
        });
    batchWorker = new BatchWorker<>(storage, statsProvider, 2);
    control.replay();
    AtomicInteger retries = new AtomicInteger();
    CompletableFuture<Boolean> retry = batchWorker.executeWithReplay(constantBackoff(0), store -> {
      retries.incrementAndGet();
      return new Result<>(false, false);
    });
    CompletableFuture<Boolean> result = batchWorker.execute(store -> true);
    batchWorker.startAsync();

    assertFailure(result, failure);
    assertFailure(retry, failure);
    assertEquals(1, retries.get());
  }

  @Test
  public void testCleanupCallbacksCanSubmitFromAnotherThread() throws Exception {
    control.replay();
    CompletableFuture<Boolean> result = batchWorker.execute(store -> true);
    CompletableFuture<Boolean> callback = result.handle((value, failure) -> {
      CompletableFuture<Boolean> submitted = CompletableFuture.supplyAsync(() -> {
        assertRejected();
        return true;
      });
      try {
        return submitted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    });

    batchWorker.stopAsync();
    assertTrue(callback.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  public void testSubmissionRacingStopLeavesNoAcceptedResultPending() throws Exception {
    control.replay();
    CountDownLatch start = new CountDownLatch(1);
    CompletableFuture<List<CompletableFuture<Boolean>>> submissions =
        CompletableFuture.supplyAsync(() -> {
          await(start);
          List<CompletableFuture<Boolean>> accepted = new ArrayList<>();
          for (int i = 0; i < 1000; i++) {
            try {
              accepted.add(batchWorker.execute(store -> true));
            } catch (RejectedExecutionException e) {
              break;
            }
          }
          return accepted;
        });
    CompletableFuture<Boolean> beforeStop = batchWorker.execute(store -> true);
    start.countDown();
    batchWorker.stopAsync().awaitTerminated(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertCancelled(beforeStop);
    for (CompletableFuture<Boolean> result : submissions.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
      assertCancelled(result);
    }
  }

  private static BackoffStrategy constantBackoff(long delayMillis) {
    return new BackoffStrategy() {
      @Override
      public long calculateBackoffMs(long lastBackoffMs) {
        return delayMillis;
      }

      @Override
      public boolean shouldContinue(long lastBackoffMs) {
        return true;
      }
    };
  }

  private void assertRejected() {
    try {
      batchWorker.execute(store -> true);
      fail("Stopped worker accepted work");
    } catch (RejectedExecutionException e) {
      // Expected.
    }
    try {
      batchWorker.executeWithReplay(constantBackoff(0L), store -> new Result<>(true, true));
      fail("Stopped worker accepted repeatable work");
    } catch (RejectedExecutionException e) {
      // Expected.
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static void assertFailure(CompletableFuture<?> result, Throwable failure)
      throws Exception {
    try {
      result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      fail("Expected failed work");
    } catch (ExecutionException e) {
      assertSame(failure, e.getCause());
    }
  }

  private static void assertCancelled(CompletableFuture<?> result) throws Exception {
    try {
      result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      fail("Expected cancelled work");
    } catch (java.util.concurrent.CancellationException e) {
      assertTrue(result.isCancelled());
    }
  }
}
