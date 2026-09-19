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

import java.time.Duration;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeTicker;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TaskVarsLifetimeTest extends EasyMockTest {
  @Test
  public void testBoundedHistoryRetirementAndServiceRestart() {
    StorageTestUtil storage = new StorageTestUtil(this);
    FakeStatsProvider stats = new FakeStatsProvider();
    FakeTicker ticker = new FakeTicker();
    TaskVars vars = new TaskVars(storage.storage, stats, ticker, 2, Duration.ofHours(1));
    control.replay();
    vars.startAsync().awaitRunning();
    IScheduledTask first = lost("first");
    vars.taskChangedState(TaskStateChange.initialized(first));
    vars.taskChangedState(TaskStateChange.initialized(lost("second")));
    vars.taskChangedState(TaskStateChange.initialized(lost("third")));
    vars.taskChangedState(TaskStateChange.initialized(lost("fourth")));
    assertEquals(2L, stats.getLongValue("task_vars_dynamic_job_LOST_overflow"));
    assertEquals(1L, stats.getLongValue(TaskVars.jobStatName(first, ScheduleStatus.LOST)));

    ticker.advance(Amount.of(61L, Time.MINUTES));
    vars.taskChangedState(TaskStateChange.initialized(lost("fifth")));
    assertFalse(stats.getAllValues().containsKey(TaskVars.jobStatName(first, ScheduleStatus.LOST)));
    assertEquals(5L, stats.getLongValue(TaskVars.getVarName(ScheduleStatus.LOST)));
    vars.taskChangedState(TaskStateChange.initialized(first));
    assertEquals(1L, stats.getLongValue(TaskVars.jobStatName(first, ScheduleStatus.LOST)));
    vars.stopAsync().awaitTerminated();
    assertTrue(stats.getAllValues().isEmpty());

    TaskVars replacement = new TaskVars(storage.storage, stats);
    replacement.startAsync().awaitRunning();
    assertEquals(0L, stats.getLongValue(TaskVars.getVarName(ScheduleStatus.LOST)));
    replacement.stopAsync().awaitTerminated();
    assertTrue(stats.getAllValues().isEmpty());
  }

  private IScheduledTask lost(String name) {
    return IScheduledTask.build(TaskTestUtil.makeTask(name, JobKeys.from("role", "env", name))
        .newBuilder().setStatus(ScheduleStatus.LOST));
  }
}
