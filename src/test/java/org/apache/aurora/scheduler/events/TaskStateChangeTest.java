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
package org.apache.aurora.scheduler.events;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TaskStateChangeTest {
  // Captured from the original serializer before replacing its access to Optional internals.
  private static final String TASK_JSON =
      "{\"cachedHashCode\":0,\"status\":\"RUNNING\",\"failureCount\":0,"
          + "\"timesPartitioned\":0,\"taskEvents\":[]}";
  private static final IScheduledTask TASK =
      IScheduledTask.build(new ScheduledTask().setStatus(ScheduleStatus.RUNNING));

  @Test
  public void testInitializedJsonRetainsEmptyOldState() {
    assertEquals("{\"task\":" + TASK_JSON + ",\"oldState\":{}}",
        TaskStateChange.initialized(TASK).toJson());
  }

  @Test
  public void testTransitionJsonRetainsOldStateValue() {
    for (ScheduleStatus state : ScheduleStatus.values()) {
      assertEquals("{\"task\":" + TASK_JSON + ",\"oldState\":{\"value\":\"" + state + "\"}}",
          TaskStateChange.transition(TASK, state).toJson());
    }
  }
}
