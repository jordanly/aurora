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
package org.apache.aurora.scheduler.stats;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.resources.ResourceTestUtil;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TaskStatCalculatorTest extends EasyMockTest {
  @Test
  public void testSingleSnapshotAndDisappearingRolesReset() {
    StorageTestUtil storage = new StorageTestUtil(this);
    storage.expectOperations();
    IScheduledTask task = TaskTestUtil.makeTask("task", TaskTestUtil.JOB);
    expect(storage.taskStore.fetchTasks(Query.unscoped().active()))
        .andReturn(ImmutableSet.of(task));
    expect(storage.quotaStore.fetchQuotas())
        .andReturn(ImmutableMap.of("role", ResourceTestUtil.aggregate(2, 4, 8)));
    expect(storage.taskStore.fetchTasks(Query.unscoped().active())).andReturn(ImmutableSet.of());
    expect(storage.quotaStore.fetchQuotas()).andReturn(ImmutableMap.of());
    FakeStatsProvider stats = new FakeStatsProvider();
    TaskStatCalculator calculator = new TaskStatCalculator(
        new ResourceCounter(storage.storage), new CachedCounters(stats));
    control.replay();

    calculator.run();
    assertTrue(stats.getAllValues().values().stream().anyMatch(value -> value.longValue() > 0));
    assertTrue(stats.getAllValues().keySet().stream()
        .anyMatch(name -> name.startsWith("quota_per_role_")));
    calculator.run();
    stats.getAllValues().forEach((name, value) -> assertEquals(name, 0L, value.longValue()));
  }
}
