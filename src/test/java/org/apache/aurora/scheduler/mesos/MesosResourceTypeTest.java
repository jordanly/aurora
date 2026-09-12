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

import java.util.EnumSet;
import java.util.Map;

import org.apache.aurora.scheduler.resources.ResourceType;
import org.junit.Test;

import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.GPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class MesosResourceTypeTest {
  @Test
  public void testFindByMesosResource() {
    assertEquals(RAM_MB, MesosResourceType.fromResource(mesosScalar(RAM_MB, 1.0)));
  }

  @Test
  public void testEveryNeutralTypeRetainsItsNativeNameAndConverter() {
    Map<ResourceType, String> expected = Map.of(
        CPUS, "cpus", RAM_MB, "mem", DISK_MB, "disk", PORTS, "ports", GPUS, "gpus");
    assertEquals(EnumSet.allOf(ResourceType.class), expected.keySet());
    expected.forEach((type, name) -> {
      assertEquals(name, MesosResourceType.getMesosName(type));
      assertEquals(type, MesosResourceType.BY_MESOS_NAME.get(name));
      assertSame(type == PORTS ? MesosResourceConverter.RANGES : MesosResourceConverter.SCALAR,
          MesosResourceType.getMesosResourceConverter(type));
    });
  }

  @Test(expected = NullPointerException.class)
  public void testUnknownMesosResourceRejected() {
    MesosResourceType.fromResource(mesosScalar(CPUS, 1.0).toBuilder().setName("unknown").build());
  }
}
