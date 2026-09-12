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
package org.apache.aurora.scheduler.configuration.executor;

import java.util.Map;

import org.apache.aurora.scheduler.resources.ResourceTestUtil;
import org.apache.aurora.scheduler.resources.ResourceType;

/** Common executor-accounting fixtures for scheduler policy tests. */
public final class TaskExecutors {
  private TaskExecutors() { }

  public static final ExecutorSettings NO_OVERHEAD_EXECUTOR =
      TestExecutorSettings.thermosOnlyWithOverhead(ResourceTestUtil.bag(Map.of()));
  public static final ExecutorSettings SOME_OVERHEAD_EXECUTOR =
      TestExecutorSettings.thermosOnlyWithOverhead(ResourceTestUtil.bag(Map.of(
          ResourceType.CPUS, 0.01, ResourceType.RAM_MB, 256.0)));
}
