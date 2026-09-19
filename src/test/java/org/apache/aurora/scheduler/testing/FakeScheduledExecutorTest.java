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
package org.apache.aurora.scheduler.testing;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FakeScheduledExecutorTest extends EasyMockTest {
  @Test
  public void testCancellationRemovesDeferredWork() {
    ScheduledExecutorService mock = createMock(ScheduledExecutorService.class);
    FakeScheduledExecutor clock = FakeScheduledExecutor.scheduleExecutor(mock);
    control.replay();
    AtomicInteger runs = new AtomicInteger();
    ScheduledFuture<?> future = mock.schedule((Runnable) runs::incrementAndGet, 10,
        TimeUnit.MILLISECONDS);
    assertEquals(1, clock.countDeferredWork());
    assertTrue(future.cancel(false));
    assertFalse(future.cancel(false));
    assertTrue(future.isDone());
    assertTrue(future.isCancelled());
    clock.assertEmpty();
    clock.advance(Amount.of(10L, Time.MILLISECONDS));
    assertEquals(0, runs.get());
    assertThrows(CancellationException.class, future::get);
  }

  @Test
  public void testCallableCompletionAndDelay() throws Exception {
    ScheduledExecutorService mock = createMock(ScheduledExecutorService.class);
    FakeScheduledExecutor clock = FakeScheduledExecutor.fromScheduledExecutorService(mock);
    control.replay();
    ScheduledFuture<String> future = mock.schedule(() -> "done", 10, TimeUnit.MILLISECONDS);
    assertEquals(10, future.getDelay(TimeUnit.MILLISECONDS));
    assertThrows(TimeoutException.class, () -> future.get(0, TimeUnit.NANOSECONDS));
    clock.advance(Amount.of(5L, Time.MILLISECONDS));
    assertEquals(5, future.getDelay(TimeUnit.MILLISECONDS));
    assertFalse(future.isDone());
    clock.advance(Amount.of(5L, Time.MILLISECONDS));
    assertEquals("done", future.get());
    assertTrue(future.isDone());
    assertFalse(future.cancel(false));
    clock.assertEmpty();
  }

  @Test
  public void testFailureIsReportedByFuture() {
    ScheduledExecutorService mock = createMock(ScheduledExecutorService.class);
    FakeScheduledExecutor clock = FakeScheduledExecutor.scheduleExecutor(mock);
    control.replay();
    RuntimeException failure = new RuntimeException("work failed");
    ScheduledFuture<?> future = mock.schedule((Callable<Void>) () -> {
      throw failure;
    }, 1, TimeUnit.MILLISECONDS);
    clock.advance(Amount.of(1L, Time.MILLISECONDS));
    assertSame(failure, assertThrows(ExecutionException.class, future::get).getCause());
    clock.assertEmpty();
  }

  @Test
  public void testPeriodicCancellationRemovesEveryOccurrence() {
    ScheduledExecutorService mock = createMock(ScheduledExecutorService.class);
    FakeScheduledExecutor clock = FakeScheduledExecutor.scheduleAtFixedRateExecutor(mock, 3);
    control.replay();
    AtomicInteger runs = new AtomicInteger();
    ScheduledFuture<?> future = mock.scheduleAtFixedRate(runs::incrementAndGet, 1, 1,
        TimeUnit.MILLISECONDS);
    assertEquals(4, clock.countDeferredWork());
    clock.advance(Amount.of(1L, Time.MILLISECONDS));
    assertEquals(1, runs.get());
    assertFalse(future.isDone());
    assertTrue(future.cancel(false));
    clock.assertEmpty();
    clock.advance(Amount.of(10L, Time.MILLISECONDS));
    assertEquals(1, runs.get());
  }

  @Test
  public void testPeriodicFailureSuppressesLaterOccurrences() {
    ScheduledExecutorService mock = createMock(ScheduledExecutorService.class);
    FakeScheduledExecutor clock = FakeScheduledExecutor.scheduleAtFixedRateExecutor(mock, 3);
    control.replay();
    AtomicInteger runs = new AtomicInteger();
    RuntimeException failure = new RuntimeException("periodic failure");
    ScheduledFuture<?> future = mock.scheduleAtFixedRate(() -> {
      runs.incrementAndGet();
      throw failure;
    }, 1, 1, TimeUnit.MILLISECONDS);
    clock.advance(Amount.of(10L, Time.MILLISECONDS));
    assertEquals(1, runs.get());
    assertSame(failure, assertThrows(ExecutionException.class, future::get).getCause());
    clock.assertEmpty();
  }

  @Test
  public void testCancellationAlsoStopsWorkAlreadySelectedForExecution() throws Exception {
    ScheduledExecutorService mock = createMock(ScheduledExecutorService.class);
    FakeScheduledExecutor clock = FakeScheduledExecutor.scheduleExecutor(mock);
    control.replay();
    AtomicReference<ScheduledFuture<?>> second = new AtomicReference<>();
    AtomicInteger runs = new AtomicInteger();
    ScheduledFuture<?> first = mock.schedule((Runnable) () -> second.get().cancel(false), 1,
        TimeUnit.MILLISECONDS);
    second.set(mock.schedule((Runnable) runs::incrementAndGet, 1, TimeUnit.MILLISECONDS));
    clock.advance(Amount.of(1L, Time.MILLISECONDS));
    assertNull(first.get());
    assertTrue(second.get().isCancelled());
    assertEquals(0, runs.get());
    clock.assertEmpty();
  }
}
