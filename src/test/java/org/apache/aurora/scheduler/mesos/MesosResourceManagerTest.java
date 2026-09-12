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

import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.Offer;
import org.apache.mesos.v1.Protos.Value.Scalar;
import org.junit.Test;

import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.apache.mesos.v1.Protos.Value.Type.SCALAR;
import static org.junit.Assert.assertEquals;

public class MesosResourceManagerTest {
  @Test
  public void testGetOfferResources() {
    ResourceType.initializeEmptyCliArgsForTest();
    Protos.Resource resource1 = Protos.Resource.newBuilder()
        .setType(SCALAR)
        .setName(MesosResourceType.getMesosName(CPUS))
        .setScalar(Scalar.newBuilder().setValue(2.0).build())
        .build();

    Protos.Resource resource2 = Protos.Resource.newBuilder()
        .setType(SCALAR)
        .setName(MesosResourceType.getMesosName(CPUS))
        .setRevocable(Protos.Resource.RevocableInfo.getDefaultInstance())
        .setScalar(Scalar.newBuilder().setValue(1.0).build())
        .build();

    Protos.Resource resource3 = Protos.Resource.newBuilder()
        .setType(SCALAR)
        .setName(MesosResourceType.getMesosName(RAM_MB))
        .setScalar(Scalar.newBuilder().setValue(64).build())
        .build();

    Offer offer = Offer.newBuilder()
        .setId(Protos.OfferID.newBuilder().setValue("offer-id"))
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework-id"))
        .setAgentId(Protos.AgentID.newBuilder().setValue("slave-id"))
        .setHostname("hostname")
        .addAllResources(ImmutableSet.of(resource1, resource2, resource3)).build();

    assertEquals(
        ImmutableSet.of(resource1, resource2),
        ImmutableSet.copyOf(MesosResourceManager.getOfferResources(offer, CPUS)));
    assertEquals(
        resource3,
        Iterables.getOnlyElement(MesosResourceManager.getOfferResources(offer, RAM_MB)));
    assertEquals(
        ImmutableSet.of(resource1, resource3),
        ImmutableSet.copyOf(MesosResourceManager.getNonRevocableOfferResources(offer)));
    assertEquals(
        ImmutableSet.of(resource2, resource3),
        ImmutableSet.copyOf(MesosResourceManager.getRevocableOfferResources(offer)));
    assertEquals(
        ImmutableSet.of(resource1, resource3),
        ImmutableSet.copyOf(MesosResourceManager.getOfferResources(offer, false)));
    assertEquals(
        ImmutableSet.of(resource2, resource3),
        ImmutableSet.copyOf(MesosResourceManager.getOfferResources(offer, true)));
  }

  @Test
  public void testMesosResourceQuantity() {
    Set<Protos.Resource> resources = ImmutableSet.of(
        mesosScalar(CPUS, 3.0),
        mesosScalar(CPUS, 4.0),
        mesosScalar(RAM_MB, 64),
        mesosRange(PORTS, 1, 3));

    assertEquals(7.0, MesosResourceManager.quantityOfMesosResource(resources, CPUS), 0.0);
    assertEquals(64, MesosResourceManager.quantityOfMesosResource(resources, RAM_MB), 0.0);
    assertEquals(0.0, MesosResourceManager.quantityOfMesosResource(resources, DISK_MB), 0.0);
    assertEquals(2, MesosResourceManager.quantityOfMesosResource(resources, PORTS), 0.0);
  }

  @Test
  public void testBagFromMesosResources() {
    assertEquals(
        ImmutableMap.of(CPUS, 3.0),
        MesosResourceManager.bagFromMesosResources(ImmutableSet.of(mesosScalar(CPUS, 3.0)))
            .getResourceVectors());
  }

  @Test
  public void testBagFromMesosResourcesUnsupportedResources() {
    Protos.Resource unsupported = Protos.Resource.newBuilder()
        .setName("unknown")
        .setType(SCALAR)
        .setScalar(Scalar.newBuilder().setValue(1.0).build()).build();
    assertEquals(
        ImmutableMap.of(CPUS, 3.0),
        MesosResourceManager.bagFromMesosResources(
            ImmutableSet.of(mesosScalar(CPUS, 3.0), unsupported)).getResourceVectors());
  }
}
