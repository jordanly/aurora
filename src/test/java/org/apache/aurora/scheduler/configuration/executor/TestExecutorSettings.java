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

import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceTestUtil;
import org.apache.aurora.scheduler.resources.ResourceType;

/** Test fixtures for backend-neutral executor accounting. */
public final class TestExecutorSettings {
  private TestExecutorSettings() { }

  public static final String THERMOS_TASK_PREFIX = "thermos-";
  public static final ExecutorConfig THERMOS_CONFIG =
      new ExecutorConfig(ResourceTestUtil.bag(Map.of(
          ResourceType.CPUS, 0.25, ResourceType.RAM_MB, 128.0)));
  public static final ExecutorSettings THERMOS_EXECUTOR = new ExecutorSettings(
      Map.of(apiConstants.AURORA_EXECUTOR_NAME, THERMOS_CONFIG));

  public static ExecutorSettings thermosOnlyWithOverhead(ResourceBag overhead) {
    return new ExecutorSettings(Map.of(
        apiConstants.AURORA_EXECUTOR_NAME, new ExecutorConfig(overhead)));
  }
}
