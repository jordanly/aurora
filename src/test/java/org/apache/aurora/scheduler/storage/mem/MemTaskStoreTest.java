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
package org.apache.aurora.scheduler.storage.mem;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.AbstractTaskStoreTest;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MemTaskStoreTest extends AbstractTaskStoreTest {

  private FakeStatsProvider statsProvider;

  @Override
  protected Module getStorageModule() {
    statsProvider = new FakeStatsProvider();
    return Modules.combine(
        new MemStorageModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(StatsProvider.class).toInstance(statsProvider);
          }
        });
  }

  @Test
  public void testOverwriteReleasesOldConfigAndIndexes() {
    MemTaskStore store = new MemTaskStore(statsProvider, Amount.of(1L, Time.SECONDS));
    var original = TASK_A.newBuilder();
    original.getAssignedTask().setSlaveHost("old-host");
    IScheduledTask oldTask = IScheduledTask.build(original);
    var replacement = oldTask.newBuilder();
    replacement.getAssignedTask().setSlaveHost("new-host");
    replacement.getAssignedTask().getTask().getJob().setName("different-job");
    IScheduledTask newTask = IScheduledTask.build(replacement);
    store.saveTasks(ImmutableSet.of(oldTask));
    store.saveTasks(ImmutableSet.of(newTask));
    assertEquals(ImmutableSet.of(Tasks.getJob(newTask)), store.getJobKeys());
    assertFalse(store.isConfigInterned(Tasks.getConfig(oldTask)));
    assertTrue(store.isConfigInterned(Tasks.getConfig(newTask)));
    assertEquals(1L, statsProvider.getLongValue(MemTaskStore.getIndexSizeStatName("host")));
    store.mutateTask(Tasks.id(newTask), task -> oldTask);
    assertFalse(store.isConfigInterned(Tasks.getConfig(newTask)));
    assertEquals(ImmutableSet.of(Tasks.getJob(oldTask)), store.getJobKeys());
    store.deleteTasks(Tasks.ids(oldTask));
    assertFalse(store.isConfigInterned(Tasks.getConfig(oldTask)));
    assertTrue(store.getJobKeys().isEmpty());
    assertEquals(0L, statsProvider.getLongValue(MemTaskStore.getIndexSizeStatName("host")));
    assertEquals(0L, statsProvider.getLongValue(MemTaskStore.getIndexSizeStatName("job")));
  }

  @Test
  public void testSecondaryIndexConsistency() {
    storage.write((NoResult.Quiet) storeProvider -> {
    // Test for regression of AURORA-1305.
      TaskStore.Mutable taskStore = storeProvider.getUnsafeTaskStore();
      taskStore.saveTasks(ImmutableSet.of(TASK_A));
      taskStore.deleteTasks(Tasks.ids(TASK_A));
      assertEquals(0L, statsProvider.getLongValue(MemTaskStore.getIndexSizeStatName("job")));
    });
  }
}
