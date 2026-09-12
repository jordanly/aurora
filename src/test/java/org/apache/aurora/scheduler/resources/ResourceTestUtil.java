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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.execution.TestOffer;
import org.apache.aurora.scheduler.storage.entities.IResource;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

/** Neutral resource fixtures used by scheduler tests. */
public final class ResourceTestUtil {
  private ResourceTestUtil() { }

  public record TestResource(
      ResourceType type, double value, boolean revocable, List<Integer> ports) {
    public TestResource {
      type = java.util.Objects.requireNonNull(type);
      ports = List.copyOf(ports);
    }
  }

  public static TestResource scalar(ResourceType type, double value) {
    return scalar(type, value, false);
  }

  public static TestResource scalar(ResourceType type, double value, boolean revocable) {
    return new TestResource(type, value, revocable, List.of());
  }

  public static TestResource range(ResourceType type, Integer... values) {
    return range(type, Arrays.asList(values));
  }

  public static TestResource range(ResourceType type, Iterable<Integer> values) {
    List<Integer> ports = new ArrayList<>();
    java.util.TreeSet<Integer> sorted = new java.util.TreeSet<>();
    values.forEach(sorted::add);
    ports.addAll(sorted);
    return new TestResource(type, ports.size(), false, ports);
  }

  public static ResourceBag bag(Map<ResourceType, Double> resources) {
    return new ResourceBag(resources);
  }

  public static ResourceBag bag(double numCpus, long ramMb, long diskMb) {
    return ResourceManager.bagFromAggregate(aggregate(numCpus, ramMb, diskMb));
  }

  public static List<TestResource> scalarFromBag(ResourceBag bag) {
    return resourcesFromBag(bag);
  }

  public static List<TestResource> resourcesFromBag(ResourceBag bag) {
    return bag.streamResourceVectors()
        .map(entry -> scalar(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  public static TestOffer offer(TestResource... resources) {
    return offer("slave-id", resources);
  }

  public static TestOffer offer(String agentId, TestResource... resources) {
    Map<ResourceType, Double> total = values(resources, false, false);
    Map<ResourceType, Double> normal = values(resources, false, true);
    Map<ResourceType, Double> revocable = values(resources, true, false);
    List<Integer> ports = Arrays.stream(resources)
        .flatMap(resource -> resource.ports().stream()).collect(Collectors.toList());
    return TestOffer.builder("offer-id-" + agentId).agentId(agentId).hostname("hostname")
        .total(bag(total)).nonRevocable(bag(normal)).revocable(bag(revocable))
        .ports(ports).build();
  }

  private static Map<ResourceType, Double> values(
      TestResource[] resources, boolean onlyRevocable, boolean normalOnly) {
    return Arrays.stream(resources)
        .filter(resource -> !normalOnly || !resource.revocable())
        .filter(resource -> !onlyRevocable || resource.revocable())
        .collect(Collectors.groupingBy(TestResource::type,
            Collectors.summingDouble(TestResource::value)));
  }

  public static ResourceBag bagFromTestResources(Iterable<TestResource> resources) {
    Map<ResourceType, Double> values = new java.util.EnumMap<>(ResourceType.class);
    resources.forEach(resource -> values.merge(resource.type(), resource.value(), Double::sum));
    return bag(values);
  }

  public static IResourceAggregate aggregate(double numCpus, long ramMb, long diskMb) {
    return IResourceAggregate.build(new ResourceAggregate(ImmutableSet.of(
        Resource.numCpus(numCpus), Resource.ramMb(ramMb), Resource.diskMb(diskMb))));
  }

  public static ITaskConfig resetPorts(ITaskConfig config, Set<String> portNames) {
    TaskConfig builder = config.newBuilder();
    builder.getResources().removeIf(e -> ResourceType.fromResource(IResource.build(e))
        .equals(ResourceType.PORTS));
    portNames.forEach(e -> builder.addToResources(Resource.namedPort(e)));
    return ITaskConfig.build(builder);
  }

  public static ITaskConfig resetResource(ITaskConfig config, ResourceType type, Double value) {
    TaskConfig builder = config.newBuilder();
    builder.getResources().removeIf(e ->
        ResourceType.fromResource(IResource.build(e)).equals(type));
    builder.addToResources(IResource.newBuilder(
        type.getValue(), type.getAuroraResourceConverter().valueOf(value)));
    return ITaskConfig.build(builder);
  }
}
