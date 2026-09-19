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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Service;

import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.sqlite.SqliteStorage;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class BatchWorkerSqliteTest {
  @Rule
  public TemporaryFolder temporary = new TemporaryFolder();

  private SqliteStorage storage;
  private BatchWorker<Boolean> worker;

  @Before
  public void setUp() {
    storage = SqliteStorage.open(temporary.getRoot().toPath().resolve("batch.db"));
    storage.write((Storage.MutateWork.NoResult.Quiet) stores ->
        stores.getSchedulerStore().saveFrameworkId("original"));
    worker = new BatchWorker<>(storage, new FakeStatsProvider(), 2);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (worker.state() != Service.State.FAILED) {
        worker.stopAsync().awaitTerminated(10, TimeUnit.SECONDS);
      }
      assertTrue(worker.scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS));
    } finally {
      storage.close();
    }
  }

  @Test
  public void testRolledBackBatchNeverCompletesEarlierMutation() throws Exception {
    RuntimeException failure = new IllegalStateException("later mutation failed");
    // Poll moves the first submission to the end of this two-item batch.
    CompletableFuture<Boolean> failed = worker.execute(stores -> {
      throw failure;
    });
    CompletableFuture<Boolean> earlier = worker.execute(stores -> {
      stores.getSchedulerStore().saveFrameworkId("uncommitted");
      return true;
    });
    AtomicInteger callbacks = new AtomicInteger();
    earlier.thenRun(callbacks::incrementAndGet);
    worker.startAsync();

    assertSame(failure, failureOf(failed));
    assertSame(failure, failureOf(earlier));
    assertEquals(0, callbacks.get());
    assertEquals("original", storage.read(
        stores -> stores.getSchedulerStore().fetchFrameworkId().orElseThrow()));
  }

  @Test
  public void testSwallowedNestedFailureCannotCompleteBatch() throws Exception {
    CompletableFuture<Boolean> result = worker.execute(stores -> {
      stores.getSchedulerStore().saveFrameworkId("uncommitted");
      assertThrows(IllegalStateException.class, () -> storage.write(nested -> {
        throw new IllegalStateException("nested failed");
      }));
      return true;
    });
    worker.startAsync();

    assertTrue(failureOf(result) instanceof Storage.StorageException);
    assertEquals("original", storage.read(
        stores -> stores.getSchedulerStore().fetchFrameworkId().orElseThrow()));
  }

  @Test
  public void testCallbackObservesCommitFromAnotherThread() throws Exception {
    CompletableFuture<Boolean> result = worker.execute(stores -> {
      storage.write((Storage.MutateWork.NoResult.Quiet) nested ->
          nested.getSchedulerStore().saveFrameworkId("committed"));
      return true;
    });
    CompletableFuture<String> observed = result.thenApply(value -> {
      // The callback waits for an independent transaction, making pre-commit completion observable.
      try {
        return CompletableFuture.supplyAsync(() -> storage.read(
            stores -> stores.getSchedulerStore().fetchFrameworkId().orElseThrow()))
            .get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    });
    worker.startAsync();

    assertEquals("committed", observed.get(10, TimeUnit.SECONDS));
  }

  @Test
  public void testPublicationFailureDoesNotReplayCommittedMutation() throws Exception {
    RuntimeException deliveryFailure = new IllegalStateException("publication failed");
    var sink = storage.transactionalEventSink(event -> {
      throw deliveryFailure;
    });
    AtomicInteger attempts = new AtomicInteger();
    CompletableFuture<Boolean> result = worker.execute(stores -> {
      attempts.incrementAndGet();
      stores.getSchedulerStore().saveFrameworkId("committed");
      sink.post(new PubsubEvent() { });
      return true;
    });
    worker.startAsync();

    Throwable failure = failureOf(result);
    assertTrue(failure instanceof SqliteStorage.PostCommitException);
    assertSame(deliveryFailure, failure.getCause());
    assertFalse(result.isCancelled());
    assertEquals(1, attempts.get());
    assertEquals("committed", storage.read(
        stores -> stores.getSchedulerStore().fetchFrameworkId().orElseThrow()));
  }

  private static Throwable failureOf(CompletableFuture<?> result) {
    return assertThrows(ExecutionException.class,
        () -> result.get(10, TimeUnit.SECONDS)).getCause();
  }
}
