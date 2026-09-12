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

import java.util.Optional;

import com.google.protobuf.ByteString;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.execution.TaskObservation;
import org.apache.mesos.v1.Protos;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.junit.Assert.assertEquals;

public class MesosTaskUpdateTest extends EasyMockTest {
  private static Protos.TaskStatus.Builder status() {
    return Protos.TaskStatus.newBuilder()
        .setTaskId(Protos.TaskID.newBuilder().setValue("observed-task"))
        .setState(Protos.TaskState.TASK_FAILED);
  }

  @Test
  public void testAbsentReasonAndMessageRemainAbsent() {
    Driver driver = createMock(Driver.class);
    control.replay();
    assertEquals(new TaskObservation("observed-task", FAILED, Optional.empty(), Optional.empty()),
        new MesosTaskUpdate(status().build(), driver).observe());
  }

  @Test
  public void testExplicitEmptyMessageIsNotReplacedByDefaultExplanation() {
    Driver driver = createMock(Driver.class);
    control.replay();
    assertEquals(Optional.of(""), new MesosTaskUpdate(status()
        .setReason(Protos.TaskStatus.Reason.REASON_CONTAINER_LIMITATION_MEMORY)
        .setMessage("").build(), driver).observe().message());
  }

  @Test
  public void testAcknowledgementRetainsNativeIdentityAndDeliveryMetadata() {
    Driver driver = createMock(Driver.class);
    Protos.TaskStatus nativeStatus = status()
        .setAgentId(Protos.AgentID.newBuilder().setValue("native-agent"))
        .setUuid(ByteString.copyFromUtf8("original-ack-uuid"))
        .setTimestamp(123.125).build();
    driver.acknowledgeStatusUpdate(nativeStatus);

    control.replay();
    new MesosTaskUpdate(nativeStatus, driver).acknowledge();
  }
}
