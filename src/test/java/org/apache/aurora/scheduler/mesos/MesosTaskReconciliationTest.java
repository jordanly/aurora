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
package org.apache.aurora.scheduler.mesos;

import java.util.Collection;
import java.util.List;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.execution.ReconciliationTarget;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.TaskStatus;
import org.easymock.Capture;
import org.junit.Test;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

public class MesosTaskReconciliationTest extends EasyMockTest {
  @Test
  public void testConvertsTargetsToNativeStatuses() {
    Driver driver = createMock(Driver.class);
    Capture<Collection<TaskStatus>> statuses = createCapture();
    driver.reconcileTasks(capture(statuses));
    expectLastCall();

    control.replay();

    new MesosTaskReconciliation(driver).reconcileTasks(List.of(
        new ReconciliationTarget("task-id", "agent-id")));

    Collection<TaskStatus> result = statuses.getValue();
    TaskStatus status = result.iterator().next();
    assertEquals(1, result.size());
    assertEquals(Protos.TaskState.TASK_RUNNING, status.getState());
    assertEquals("task-id", status.getTaskId().getValue());
    assertEquals("agent-id", status.getAgentId().getValue());
  }

  @Test
  public void testPreservesEmptyReconciliation() {
    Driver driver = createMock(Driver.class);
    Capture<Collection<TaskStatus>> statuses = createCapture();
    driver.reconcileTasks(capture(statuses));
    expectLastCall();

    control.replay();

    new MesosTaskReconciliation(driver).reconcileTasks(List.of());

    assertEquals(List.of(), statuses.getValue());
  }
}
