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
package org.apache.aurora.scheduler.resources;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.scheduler.execution.TestOffer;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.junit.Test;

import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.resources.ResourceMapper.PORT_MAPPER;
import static org.junit.Assert.assertEquals;

public class PortMapperTest {
  @Test
  public void testAssignNoPorts() {
    AssignedTask builder = makeTask("id", JOB).newBuilder().getAssignedTask();
    builder.getTask().unsetResources();
    builder.unsetAssignedPorts();
    IAssignedTask task = IAssignedTask.build(builder);

    assertEquals(task, PORT_MAPPER.mapAndAssign(
        TestOffer.builder("offer").resources(ResourceBag.EMPTY).build(), task));
  }

  @Test(expected = IllegalStateException.class)
  public void testPortRangeScarcity() {
    PORT_MAPPER.mapAndAssign(TestOffer.builder("offer").resources(ResourceBag.EMPTY).build(),
        makeTask("id", JOB).getAssignedTask());
  }

  @Test
  public void testPortRangeAbundance() {
    TestOffer offer = TestOffer.builder("offer").resources(ResourceBag.EMPTY)
        .ports(1, 2, 3, 4, 5).build();
    assertEquals(
        1,
        PORT_MAPPER.mapAndAssign(offer, makeTask("id", JOB).getAssignedTask())
            .getAssignedPorts().size());
  }

  @Test
  public void testPortRangeExact() {
    TestOffer offer = TestOffer.builder("offer").resources(ResourceBag.EMPTY).ports(1).build();
    assertEquals(
        1,
        PORT_MAPPER.mapAndAssign(offer, makeTask("id", JOB).getAssignedTask())
            .getAssignedPorts().size());
  }
}
