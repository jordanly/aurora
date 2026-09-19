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
package org.apache.aurora.scheduler.stats;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import jakarta.inject.Inject;

import com.google.common.base.Joiner;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.stats.ResourceCounter.Metric;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Calculates and exports aggregate stats about resources consumed by active tasks.
 */
class TaskStatCalculator implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TaskStatCalculator.class);

  private final CachedCounters counters;
  private final ResourceCounter resourceCounter;
  private Set<String> previousNames = Set.of();

  @Inject
  TaskStatCalculator(ResourceCounter resourceCounter, CachedCounters counters) {
    this.resourceCounter = requireNonNull(resourceCounter);
    this.counters = requireNonNull(counters);
  }

  private void collect(Map<String, Long> values, String prefix, Metric metric) {
    metric.getBag().streamResourceVectors().forEach(r -> {
      ResourceType type = r.getKey();
      String metricName =
          Joiner.on("_").join(prefix, type.getAuroraName(), type.getAuroraStatUnit())
              .toLowerCase(Locale.ROOT);
      values.put(metricName, (long) metric.getBag().valueOf(type));
    });
  }

  @Timed("task_stat_calculator_run")
  @Override
  public void run() {
    try {
      ResourceCounter.Snapshot snapshot = resourceCounter.computeSnapshot();
      Map<String, Long> values = new HashMap<>();
      for (Metric metric : snapshot.consumption()) {
        collect(values, "resources_" + metric.type.name(), metric);
      }
      snapshot.consumptionByRole().forEach((name, metric) ->
          collect(values, "resources_per_role_" + name, metric));
      collect(values, "resources_allocated_quota", snapshot.quota());
      snapshot.quotaByRole().forEach((role, metric) ->
          collect(values, "quota_per_role_" + role, metric));
      // A role or a sparse resource vector can disappear entirely between successful samples.
      for (String name : previousNames) {
        if (!values.containsKey(name)) {
          counters.get(name).set(0);
        }
      }
      values.forEach((name, value) -> counters.get(name).set(value));
      previousNames = new HashSet<>(values.keySet());
    } catch (StorageException e) {
      LOG.debug("Unable to fetch metrics, storage is likely not ready.");
    }
  }
}
