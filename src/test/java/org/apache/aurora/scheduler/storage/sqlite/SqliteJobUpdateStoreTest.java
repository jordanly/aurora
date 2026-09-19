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
import java.util.List;

import com.google.inject.AbstractModule;
import com.google.inject.Module;

import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.AbstractJobUpdateStoreTest;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_UPDATED;
import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class SqliteJobUpdateStoreTest extends AbstractJobUpdateStoreTest {
  @Rule
  public TemporaryFolder temporary = new TemporaryFolder();

  private Path path;
  private SqliteStorage sqlite;

  @Override
  protected Module getStorageModule() {
    path = temporary.getRoot().toPath().resolve("updates.db");
    sqlite = SqliteStorage.open(path);
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(Storage.class).toInstance(sqlite);
      }
    };
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      super.tearDown();
    } finally {
      sqlite.close();
    }
  }

  @Test
  public void testStableEventTiesOutOfOrderHistoryAndReopen() {
    IJobUpdateKey key = makeKey(JobKeys.from("role", "env", "job"), "ties");
    saveUpdate(makeJobUpdate(key));
    IJobUpdateEvent earlier = IJobUpdateEvent.build(new JobUpdateEvent()
        .setStatus(ROLLING_FORWARD).setTimestampMs(100));
    IJobUpdateEvent firstTie = IJobUpdateEvent.build(new JobUpdateEvent()
        .setStatus(ROLLING_BACK).setTimestampMs(200));
    IJobUpdateEvent lastTie = IJobUpdateEvent.build(new JobUpdateEvent()
        .setStatus(ABORTED).setTimestampMs(200));
    IJobInstanceUpdateEvent instance = IJobInstanceUpdateEvent.build(new JobInstanceUpdateEvent()
        .setInstanceId(0).setAction(INSTANCE_UPDATED).setTimestampMs(300));
    storage.write((NoResult.Quiet) stores -> {
      JobUpdateStore.Mutable updates = stores.getJobUpdateStore();
      updates.saveJobUpdateEvent(key, firstTie);
      updates.saveJobUpdateEvent(key, lastTie);
      updates.saveJobUpdateEvent(key, earlier);
      updates.saveJobInstanceUpdateEvent(key, instance);
      updates.saveJobInstanceUpdateEvent(key, instance);
    });
    IJobUpdateDetails expected = fetch(key);
    assertEquals(earlier, expected.getUpdateEvents().get(0));
    assertEquals(List.of(firstTie, lastTie), expected.getUpdateEvents().subList(2, 4));
    assertEquals(List.of(instance, instance), expected.getInstanceEvents());
    assertEquals(ABORTED, expected.getUpdate().getSummary().getState().getStatus());
    assertEquals(100, expected.getUpdate().getSummary().getState().getCreatedTimestampMs());
    assertEquals(300, expected.getUpdate().getSummary().getState().getLastModifiedTimestampMs());
    reopen();
    assertEquals(expected, fetch(key));
  }

  @Test
  public void testFailedHistoryMutationDoesNotLeakIntoStoredUpdate() throws Exception {
    IJobUpdateKey key = makeKey(JobKeys.from("role", "env", "job"), "rollback");
    saveUpdate(makeJobUpdate(key));
    IJobUpdateDetails before = fetch(key);
    IOException failure = new IOException("abort history mutation");
    try {
      storage.write(stores -> {
        stores.getJobUpdateStore().saveJobUpdateEvent(key,
            IJobUpdateEvent.build(new JobUpdateEvent().setStatus(ABORTED).setTimestampMs(999)));
        throw failure;
      });
      fail("Expected failed transaction");
    } catch (IOException expected) {
      assertSame(failure, expected);
    }
    assertEquals(before, fetch(key));
    reopen();
    assertEquals(before, fetch(key));
  }

  @Test
  public void testOverwritingUpdateResetsHistoryAndSynthesizedState() {
    IJobUpdateKey key = makeKey(JobKeys.from("role", "env", "job"), "overwrite");
    IJobUpdateDetails update = makeJobUpdate(key);
    saveUpdate(update);
    storage.write((NoResult.Quiet) stores -> stores.getJobUpdateStore()
        .saveJobUpdate(update.getUpdate()));
    IJobUpdateDetails replaced = fetch(key);
    assertEquals(List.of(), replaced.getUpdateEvents());
    assertEquals(List.of(), replaced.getInstanceEvents());
    assertFalse(replaced.getUpdate().getSummary().getState().isSetStatus());
    assertEquals(0, replaced.getUpdate().getSummary().getState().getLastModifiedTimestampMs());
  }

  @Test
  public void testMissingUpdateEventDoesNotCreateHistory() {
    IJobUpdateKey key = makeKey(JobKeys.from("role", "env", "job"), "missing");
    try {
      storage.write((NoResult.Quiet) stores -> stores.getJobUpdateStore().saveJobUpdateEvent(key,
          IJobUpdateEvent.build(new JobUpdateEvent().setStatus(ABORTED).setTimestampMs(1))));
      fail("Expected missing update rejection");
    } catch (StorageException expected) {
      assertEquals("Update not found: " + key, expected.getMessage());
    }
    assertFalse(storage.read(stores -> stores.getJobUpdateStore().fetchJobUpdate(key)).isPresent());
  }

  @Test
  public void testNegativeOffsetAndUnlimitedNegativeLimitMatchOriginalQueries() {
    IJobUpdateKey key = makeKey(JobKeys.from("role", "env", "job"), "pagination");
    saveUpdate(makeJobUpdate(key));
    assertEquals(List.of(fetch(key)), storage.read(stores -> stores.getJobUpdateStore()
        .fetchJobUpdates(IJobUpdateQuery.build(new JobUpdateQuery().setLimit(-1)))));
    try {
      storage.read(stores -> stores.getJobUpdateStore()
          .fetchJobUpdates(IJobUpdateQuery.build(new JobUpdateQuery().setOffset(-1))));
      fail("Expected negative offset rejection");
    } catch (IllegalArgumentException expected) {
      // Stream pagination has the same rejection in the original memory store.
    }
  }

  @Test
  public void testExactKeyStillAppliesOtherFiltersAndPagination() {
    IJobUpdateKey key = makeKey(JobKeys.from("role", "env", "job"), "keyed");
    saveUpdate(makeJobUpdate(key));
    JobUpdateQuery query = new JobUpdateQuery().setKey(key.newBuilder());
    assertEquals(List.of(fetch(key)), fetch(query));
    assertEquals(List.of(), fetch(query.deepCopy().setRole("other-role")));
    assertEquals(List.of(), fetch(query.deepCopy().setUser("other-user")));
    assertEquals(List.of(), fetch(query.deepCopy().setJobKey(
        JobKeys.from("other", "env", "job").newBuilder())));
    assertEquals(List.of(), fetch(query.deepCopy().setOffset(1)));
    var partial = key.newBuilder();
    partial.unsetId();
    assertEquals(List.of(), fetch(query.deepCopy().setKey(partial)));
    assertEquals(List.of(fetch(key)), fetch(query.deepCopy().setLimit(1)));
    assertEquals(List.of(), fetch(query.deepCopy().setKey(
        makeKey(JobKeys.from("role", "env", "job"), "missing").newBuilder())));
  }

  private List<IJobUpdateDetails> fetch(JobUpdateQuery query) {
    return storage.read(stores -> stores.getJobUpdateStore()
        .fetchJobUpdates(IJobUpdateQuery.build(query)));
  }

  private IJobUpdateDetails fetch(IJobUpdateKey key) {
    return storage.read(stores -> stores.getJobUpdateStore().fetchJobUpdate(key).get());
  }

  private void reopen() {
    sqlite.close();
    sqlite = SqliteStorage.open(path);
    storage = sqlite;
    storage.prepare();
  }
}
