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

import com.google.common.collect.ImmutableList;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.execution.ExecutionOffer;
import org.apache.aurora.scheduler.execution.PreparedTask;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.mesos.v1.Protos;
import org.junit.Test;

import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.offer;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class MesosExecutionAdapterTest extends EasyMockTest {
  @Test
  public void testResourceViewsPreserveMixedRevocabilityAndNativePayload() {
    ResourceType.initializeEmptyCliArgsForTest();
    Protos.Offer nativeOffer = offer(
        mesosScalar(CPUS, 2, false), mesosScalar(CPUS, 1, true),
        mesosScalar(RAM_MB, 64), mesosRange(PORTS, 1000, 1001));
    MesosOffer adapted = new MesosOffer(nativeOffer);

    control.replay();
    assertSame(nativeOffer, MesosOffer.toMesos(adapted));
    assertEquals(3.0, adapted.getTotalResources().valueOf(CPUS), 0.0);
    assertEquals(2.0, adapted.getResources(false).valueOf(CPUS), 0.0);
    assertEquals(1.0, adapted.getResources(true).valueOf(CPUS), 0.0);
    assertEquals(64.0, adapted.getResources(false).valueOf(RAM_MB), 0.0);
    assertEquals(64.0, adapted.getResources(true).valueOf(RAM_MB), 0.0);
    assertEquals(2.0, adapted.getResources(true).valueOf(PORTS), 0.0);
  }

  @Test
  public void testPortViewRetainsRangeOrderOverlapsAndRevocableEntries() {
    Protos.Resource first = Protos.Resource.newBuilder()
        .setName("ports").setType(Protos.Value.Type.RANGES)
        .setRanges(Protos.Value.Ranges.newBuilder()
            .addRange(Protos.Value.Range.newBuilder().setBegin(3).setEnd(4))
            .addRange(Protos.Value.Range.newBuilder().setBegin(1).setEnd(3)))
        .build();
    Protos.Resource second = mesosRange(PORTS, 3).toBuilder()
        .setRevocable(Protos.Resource.RevocableInfo.getDefaultInstance()).build();
    MesosOffer adapted = new MesosOffer(offer(first, second));

    control.replay();
    assertEquals(ImmutableList.of(3, 4, 1, 2, 3, 3), adapted.getAvailablePorts());
  }

  @Test
  public void testHostOfferKeepsIndependentMemoizedViews() {
    ExecutionOffer offer = createMock(ExecutionOffer.class);
    expect(offer.getTotalResources()).andReturn(ResourceBag.EMPTY);
    expect(offer.getResources(true)).andReturn(ResourceBag.EMPTY);
    expect(offer.getResources(false)).andReturn(ResourceBag.EMPTY);

    control.replay();
    HostOffer host = new HostOffer(offer, IHostAttributes.build(new HostAttributes()));
    assertFalse(host.hasCpuAndMem());
    assertSame(ResourceBag.EMPTY, host.getResourceBag(true));
    assertSame(ResourceBag.EMPTY, host.getResourceBag(true));
    assertSame(ResourceBag.EMPTY, host.getResourceBag(false));
    assertSame(ResourceBag.EMPTY, host.getResourceBag(false));
  }

  @Test
  public void testPrepareEagerlyRetainsOriginalTaskInfo() {
    MesosTaskFactory nativeFactory = createMock(MesosTaskFactory.class);
    IAssignedTask task = makeTask("adapter-task", JOB).getAssignedTask();
    Protos.Offer nativeOffer = offer();
    Protos.TaskInfo taskInfo = Protos.TaskInfo.newBuilder()
        .setName("native-task").setTaskId(Protos.TaskID.newBuilder().setValue(task.getTaskId()))
        .setAgentId(nativeOffer.getAgentId())
        .addResources(mesosScalar(CPUS, 2, true)).build();
    expect(nativeFactory.createFrom(task, nativeOffer, true)).andReturn(taskInfo);

    control.replay();
    PreparedTask prepared = new MesosTaskFactoryAdapter(nativeFactory)
        .prepare(task, new MesosOffer(nativeOffer), true);
    assertSame(taskInfo, ((MesosPreparedTask) prepared).getTaskInfo());
  }

  @Test
  public void testPreparePropagatesOriginalExceptionImmediately() {
    MesosTaskFactory nativeFactory = createMock(MesosTaskFactory.class);
    IAssignedTask task = makeTask("adapter-task", JOB).getAssignedTask();
    Protos.Offer nativeOffer = offer();
    SchedulerException failure = new SchedulerException("encoding failed");
    expect(nativeFactory.createFrom(task, nativeOffer, false)).andThrow(failure);

    control.replay();
    try {
      new MesosTaskFactoryAdapter(nativeFactory).prepare(task, new MesosOffer(nativeOffer), false);
      fail("Expected original preparation failure");
    } catch (SchedulerException e) {
      assertSame(failure, e);
    }
  }
}
