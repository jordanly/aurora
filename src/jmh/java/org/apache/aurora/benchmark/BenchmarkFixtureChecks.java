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
package org.apache.aurora.benchmark;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.resources.ResourceType;

/** Executable fixture checks; these validate workload shape and report no performance result. */
public final class BenchmarkFixtureChecks {
  private BenchmarkFixtureChecks() { }

  public static void main(String[] args) {
    var hosts = new Hosts.Builder().setNumHostsPerRack(2).build(6);
    Map<String, Integer> racks = new HashMap<>();
    hosts.forEach(host -> host.getAttributes().stream()
        .filter(attribute -> "rack".equals(attribute.getName()))
        .forEach(attribute ->
            racks.merge(attribute.getValues().iterator().next(), 1, Integer::sum)));
    require(hosts.size() == 6 && racks.equals(Map.of("rack-0", 2, "rack-1", 2, "rack-2", 2)),
        "Host rack cardinality");
    var offers = new Offers.Builder().setPorts(3).build(hosts);
    require(offers.size() == 6 && offers.stream().allMatch(
        offer -> offer.getResourceBag(false).valueOf(ResourceType.PORTS) == 3),
        "Offer port cardinality");
    Set<String> ids = new HashSet<>();
    ids.addAll(Tasks.ids(new org.apache.aurora.benchmark.Tasks.Builder().build(4)));
    ids.addAll(Tasks.ids(new org.apache.aurora.benchmark.Tasks.Builder().build(4)));
    require(ids.size() == 8, "Independent task fixture IDs");

    SchedulingBenchmarks.FillClusterBenchmark benchmark =
        new SchedulingBenchmarks.FillClusterBenchmark();
    benchmark.numHosts = 20;
    benchmark.setUpBenchmark();
    benchmark.verifyResetCardinality();
    for (int invocation = 0; invocation < 5; invocation++) {
      benchmark.runBenchmark();
      long assigned = benchmark.storage.read(stores -> stores.getTaskStore()
          .fetchTasks(Query.unscoped()).stream()
          .filter(task -> task.getStatus() == ScheduleStatus.ASSIGNED).count());
      require(assigned == 10, "Successful scheduling batch cardinality");
      benchmark.restoreCluster();
      benchmark.verifyResetCardinality();
    }
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}
