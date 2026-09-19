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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.testing.FakeClock;
import org.easymock.EasyMock;

import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

/**
 * A simulated scheduled executor that records scheduled work and executes it when the clock is
 * advanced past its execution time. Scheduled work reports completion through its future, including
 * failures. execute() retains its synchronous behavior.
 */
public final class FakeScheduledExecutor extends FakeClock {
  private record DeferredWork(long deadlineMillis, ScheduledWork<?> work) { }

  private final List<DeferredWork> deferredWork = new ArrayList<>();

  private FakeScheduledExecutor() { }

  // TODO(wfarner): Rename to fromScheduledExecutor().
  public static FakeScheduledExecutor scheduleExecutor(ScheduledExecutorService mock) {
    FakeScheduledExecutor executor = new FakeScheduledExecutor();
    recordScheduling(mock, executor, false);
    mock.execute(EasyMock.anyObject());
    expectLastCall().andAnswer(() -> {
      ((Runnable) EasyMock.getCurrentArguments()[0]).run();
      return null;
    }).anyTimes();
    return executor;
  }

  public static FakeScheduledExecutor fromScheduledExecutorService(ScheduledExecutorService mock) {
    return scheduleExecutor(mock);
  }

  private static void recordScheduling(
      ScheduledExecutorService mock, FakeScheduledExecutor executor, boolean immediate) {
    mock.schedule(EasyMock.<Runnable>anyObject(), EasyMock.anyLong(), EasyMock.anyObject());
    expectLastCall().andAnswer(() -> {
      Runnable work = EasyMock.getCurrentArgument(0);
      long delay = EasyMock.getCurrentArgument(1);
      TimeUnit unit = EasyMock.getCurrentArgument(2);
      return executor.schedule(Executors.callable(work, null), unit.toMillis(delay), immediate);
    }).anyTimes();
    mock.schedule(EasyMock.<Callable<Object>>anyObject(), EasyMock.anyLong(), EasyMock.anyObject());
    expectLastCall().andAnswer(() -> {
      Callable<Object> work = EasyMock.getCurrentArgument(0);
      long delay = EasyMock.getCurrentArgument(1);
      TimeUnit unit = EasyMock.getCurrentArgument(2);
      return executor.schedule(work, unit.toMillis(delay), immediate);
    }).anyTimes();
  }

  private <V> ScheduledWork<V> schedule(Callable<V> work, long delayMillis, boolean immediate) {
    ScheduledWork<V> future = new ScheduledWork<>(work, nowMillis() + delayMillis, false);
    if (immediate) {
      future.run();
    } else {
      addDelayedWork(delayMillis, future);
    }
    return future;
  }

  public static FakeScheduledExecutor scheduleAtFixedRateExecutor(
      ScheduledExecutorService mock, int maxInvocations) {
    return scheduleAtFixedRateExecutor(mock, 1, maxInvocations);
  }

  public static FakeScheduledExecutor scheduleAtFixedRateExecutor(
      ScheduledExecutorService mock, int maxSchedules, int maxInvocations) {
    FakeScheduledExecutor executor = new FakeScheduledExecutor();
    // Preserve this fixture's immediate one-shot scheduling alongside capped periodic work.
    recordScheduling(mock, executor, true);
    mock.scheduleAtFixedRate(EasyMock.anyObject(), EasyMock.anyLong(), EasyMock.anyLong(),
        EasyMock.anyObject());
    expectLastCall().andAnswer(() -> {
      Runnable work = EasyMock.getCurrentArgument(0);
      long initialDelay = EasyMock.getCurrentArgument(1);
      long period = EasyMock.getCurrentArgument(2);
      TimeUnit unit = EasyMock.getCurrentArgument(3);
      long firstDelayMillis = unit.toMillis(initialDelay);
      ScheduledWork<Void> future = executor.new ScheduledWork<>(
          Executors.callable(work, null), executor.nowMillis() + firstDelayMillis, true);
      // Keep the existing initial occurrence plus maxInvocations repeated occurrences.
      for (int i = 0; i <= maxInvocations; i++) {
        executor.addDelayedWork(firstDelayMillis + i * unit.toMillis(period), future);
      }
      return future;
    }).times(maxSchedules);
    return executor;
  }

  private void addDelayedWork(long delayMillis, ScheduledWork<?> future) {
    Preconditions.checkArgument(delayMillis > 0);
    synchronized (deferredWork) {
      deferredWork.add(new DeferredWork(nowMillis() + delayMillis, future));
    }
  }

  private void removePending(ScheduledWork<?> future) {
    synchronized (deferredWork) {
      deferredWork.removeIf(entry -> entry.work().equals(future));
    }
  }

  private final class ScheduledWork<V> extends FutureTask<V> implements ScheduledFuture<V> {
    private final long initialDeadlineMillis;
    private final boolean periodic;

    ScheduledWork(Callable<V> work, long initialDeadlineMillis, boolean periodic) {
      super(work);
      this.initialDeadlineMillis = initialDeadlineMillis;
      this.periodic = periodic;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      if (cancelled) {
        removePending(this);
      }
      return cancelled;
    }

    @Override
    public void run() {
      if (periodic) {
        if (!runAndReset()) {
          removePending(this);
        }
      } else {
        super.run();
      }
    }

    @Override
    public long getDelay(TimeUnit unit) {
      synchronized (deferredWork) {
        long deadline = deferredWork.stream().filter(entry -> entry.work().equals(this))
            .mapToLong(DeferredWork::deadlineMillis).min().orElse(initialDeadlineMillis);
        return unit.convert(deadline - nowMillis(), TimeUnit.MILLISECONDS);
      }
    }

    @Override
    public int compareTo(Delayed other) {
      return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
    }
  }

  @Override
  public void setNowMillis(long nowMillis) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void advance(Amount<Long, Time> period) {
    super.advance(period);
    List<ScheduledWork<?>> toExecute = new ArrayList<>();
    synchronized (deferredWork) {
      Iterator<DeferredWork> entries = deferredWork.iterator();
      while (entries.hasNext()) {
        DeferredWork next = entries.next();
        if (next.deadlineMillis() <= nowMillis()) {
          entries.remove();
          toExecute.add(next.work());
        }
      }
    }
    for (ScheduledWork<?> work : toExecute) {
      work.run();
    }
  }

  public int countDeferredWork() {
    synchronized (deferredWork) {
      return deferredWork.size();
    }
  }

  public void assertEmpty() {
    assertEquals(0, countDeferredWork());
  }
}
