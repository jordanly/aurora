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
package org.apache.aurora.common.stats;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A repository for time series data.
 *
 * @author William Farner
 */
public interface TimeSeriesRepository {

  /**
   * Fetches the names of all available time series.
   *
   * @return Available time series, which can then be obtained by calling {@link #get(String)}.
   */
  Set<String> getAvailableSeries();

  /**
   * Fetches a time series by name.
   *
   * @param name The name of the time series to fetch.
   * @return The time series registered with the given name, or {@code null} if no such time series
   *     has been registered.
   */
  TimeSeries get(String name);

  /**
   * Gets an ordered iterable of the timestamps that all timeseries were sampled at.
   *
   * @return All current timestamps.
   */
  Iterable<Number> getTimestamps();
  /** Returns one detached sample of the requested series and their shared timestamps. */
  Snapshot snapshot(List<String> names);

  /** Missing series are omitted from the map; each present column belongs to these timestamps. */
  record Snapshot(List<Number> timestamps, Map<String, List<Number>> series) {
    public Snapshot {
      timestamps = List.copyOf(timestamps);
      Map<String, List<Number>> copied = new LinkedHashMap<>();
      series.forEach((name, values) -> copied.put(name, List.copyOf(values)));
      series = Map.copyOf(copied);
    }
  }

}
