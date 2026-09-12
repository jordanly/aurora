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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.events.PubsubEvent.HostAttributesChanged;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
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

public class SqliteEventSinkTest {
  @Rule
  public TemporaryFolder temporary = new TemporaryFolder();

  private final List<PubsubEvent> delivered = new ArrayList<>();
  private SqliteStorage storage;
  private EventSink sink;

  private record Event(int value) implements PubsubEvent { }

  @Before
  public void setUp() {
    storage = SqliteStorage.open(temporary.getRoot().toPath().resolve("events.db"), delivered::add);
    sink = storage.transactionalEventSink(delivered::add);
  }

  @After
  public void tearDown() {
    storage.close();
  }

  @Test
  public void testNestedEventsPublishOnlyAfterCommitWithFreshRead() {
    EventSink observing = storage.transactionalEventSink(event -> {
      assertEquals(Optional.of("committed"), storage.read(stores ->
          stores.getSchedulerStore().fetchFrameworkId()));
      delivered.add(event);
    });
    storage.write("outer", stores -> {
      stores.getSchedulerStore().saveFrameworkId("committed");
      observing.post(new Event(1));
      storage.write(nested -> {
        observing.post(new Event(2));
        assertTrue(delivered.isEmpty());
        return null;
      });
      observing.post(new Event(3));
      assertTrue(delivered.isEmpty());
      return null;
    });
    assertEquals(List.of(new Event(1), new Event(2), new Event(3)), delivered);
    assertTrue(storage.isCommitted("outer"));
  }

  @Test
  public void testRollbackDiscardsEventsAndMutations() {
    expectFailure(IllegalArgumentException.class, () -> storage.write("rollback", stores -> {
      stores.getSchedulerStore().saveFrameworkId("rolled-back");
      sink.post(new Event(1));
      throw new IllegalArgumentException("abort");
    }));
    assertTrue(delivered.isEmpty());
    assertFalse(storage.isCommitted("rollback"));
  }

  @Test
  public void testCaughtNestedFailureDiscardsWholeBatch() {
    expectFailure(StorageException.class, () -> storage.write("nested", stores -> {
      sink.post(new Event(1));
      expectFailure(IllegalArgumentException.class, () -> storage.write(nested -> {
        sink.post(new Event(2));
        throw new IllegalArgumentException("abort nested");
      }));
      return null;
    }));
    assertTrue(delivered.isEmpty());
    assertFalse(storage.isCommitted("nested"));
  }

  @Test
  public void testReentrantSubscriberWriteCannotOvertakeEarlierEvents() {
    EventSink reentrant = storage.transactionalEventSink(event -> {
      delivered.add(event);
      if (event.equals(new Event(1))) {
        storage.write("subscriber", stores -> {
          sink.post(new Event(3));
          return null;
        });
      }
    });
    storage.write("outer", stores -> {
      reentrant.post(new Event(1));
      reentrant.post(new Event(2));
      return null;
    });
    assertEquals(List.of(new Event(1), new Event(2), new Event(3)), delivered);
    assertTrue(storage.isCommitted("subscriber"));
  }

  @Test
  public void testPublicationFailureReportsCommittedOutcomeAndBlocksWrites() {
    IllegalStateException rejected = new IllegalStateException("executor rejected");
    EventSink broken = storage.transactionalEventSink(event -> {
      throw rejected;
    });
    SqliteStorage.PostCommitException failure = expectFailure(
        SqliteStorage.PostCommitException.class, () -> storage.write("committed", stores -> {
          stores.getSchedulerStore().saveFrameworkId("saved");
          broken.post(new Event(1));
          sink.post(new Event(2));
          return null;
        }));
    assertEquals("committed", failure.getOperationId());
    assertSame(rejected, failure.getCause());
    assertTrue(storage.isCommitted("committed"));
    assertEquals(Optional.of("saved"),
        storage.read(stores -> stores.getSchedulerStore().fetchFrameworkId()));
    expectFailure(SqliteStorage.PostCommitException.class, () -> storage.write("later", stores -> {
      fail("A publication failure requires subscriber reconstruction before more writes");
      return null;
    }));
    assertTrue(delivered.isEmpty());
  }

  @Test
  public void testLifecycleEventsImmediateAndReadEventsRejected() {
    sink.post(new Event(1));
    assertEquals(List.of(new Event(1)), delivered);
    expectFailure(IllegalStateException.class, () -> storage.read(stores -> {
      sink.post(new Event(2));
      return null;
    }));
    assertEquals(List.of(new Event(1)), delivered);
  }

  @Test
  public void testHostAttributeEventsMatchChangedResultAndRollback() {
    IHostAttributes attrs = IHostAttributes.build(new HostAttributes()
        .setHost("host").setMode(MaintenanceMode.NONE).setAttributes(Set.of()));
    storage.write("attributes", stores -> {
      assertTrue(stores.getAttributeStore().saveHostAttributes(attrs));
      assertFalse(stores.getAttributeStore().saveHostAttributes(attrs));
      assertTrue(delivered.isEmpty());
      return null;
    });
    assertEquals(List.of(new HostAttributesChanged(attrs)), delivered);
    expectFailure(IllegalArgumentException.class,
        () -> storage.write("abort-attributes", stores -> {
          stores.getAttributeStore().saveHostAttributes(IHostAttributes.build(
              attrs.newBuilder().setMode(MaintenanceMode.DRAINING)));
          throw new IllegalArgumentException("abort");
        }));
    assertEquals(List.of(new HostAttributesChanged(attrs)), delivered);
  }

  @Test
  public void testOptInFailureHandlerStopsWritesAfterRollback() {
    List<Throwable> failures = new ArrayList<>();
    storage.setWriteFailureHandler(failures::add);
    IllegalArgumentException original = new IllegalArgumentException(
        "policy cache may have changed");
    assertSame(original, expectFailure(IllegalArgumentException.class,
        () -> storage.write("failed", stores -> {
          throw original;
        })));
    assertEquals(List.of(original), failures);
    expectFailure(StorageException.class, () -> storage.write("later", stores -> {
      fail("Failed policy work requires scheduler restart");
      return null;
    }));
    assertFalse(storage.isCommitted("failed"));
  }

  private static <T extends Throwable> T expectFailure(Class<T> type, Runnable work) {
    try {
      work.run();
    } catch (Throwable failure) {
      if (type.isInstance(failure)) {
        return type.cast(failure);
      }
      throw new AssertionError("Unexpected failure", failure);
    }
    throw new AssertionError("Expected " + type.getSimpleName());
  }
}
