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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.mesos.v1.Protos.Resource;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.mesos.MesosResourceConverter.RANGES;
import static org.apache.aurora.scheduler.mesos.MesosResourceConverter.SCALAR;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.GPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;

/** Native Mesos names and converters keyed by the scheduler's neutral resource enum. */
public final class MesosResourceType {
  private record Definition(String name, MesosResourceConverter converter) {
  }

  private static final ImmutableMap<ResourceType, Definition> TYPES = ImmutableMap.of(
      CPUS, new Definition("cpus", SCALAR),
      RAM_MB, new Definition("mem", SCALAR),
      DISK_MB, new Definition("disk", SCALAR),
      PORTS, new Definition("ports", RANGES),
      GPUS, new Definition("gpus", SCALAR));

  public static final ImmutableMap<String, ResourceType> BY_MESOS_NAME =
      Maps.uniqueIndex(EnumSet.allOf(ResourceType.class), MesosResourceType::getMesosName);

  private MesosResourceType() {
  }

  public static String getMesosName(ResourceType type) {
    return requireNonNull(TYPES.get(type)).name();
  }

  public static MesosResourceConverter getMesosResourceConverter(ResourceType type) {
    return requireNonNull(TYPES.get(type)).converter();
  }

  public static ResourceType fromResource(Resource resource) {
    ResourceType type = BY_MESOS_NAME.get(resource.getName());
    if (type == null) {
      throw new NullPointerException("Unknown Mesos resource: " + resource);
    }
    return type;
  }
}
