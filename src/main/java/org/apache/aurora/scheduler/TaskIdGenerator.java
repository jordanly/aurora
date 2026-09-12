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
package org.apache.aurora.scheduler;

import java.util.UUID;

import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * A function that generates universally-unique (not guaranteed, but highly confident) task IDs.
 */
public interface TaskIdGenerator {

  /**
   * Generates a universally-unique ID for the task.  This is not necessarily a repeatable
   * operation, two subsequent invocations with the same object need not return the same value.
   *
   * @param task Configuration of the task to create an ID for.
   * @param instanceId Instance ID for the task.
   * @return A universally-unique ID for the task.
   */
  String generate(ITaskConfig task, int instanceId);

  class TaskIdGeneratorImpl implements TaskIdGenerator {
    @Override
    public String generate(ITaskConfig task, int instanceId) {
      String sep = "-";
      return (task.getJob().getRole() + sep
          + task.getJob().getEnvironment() + sep
          + task.getJob().getName() + sep
          + instanceId + sep
          + UUID.randomUUID()).replaceAll("[^\\w-]", sep);  // Constrain character set.
    }
  }
}
