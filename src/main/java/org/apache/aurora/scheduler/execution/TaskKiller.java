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
package org.apache.aurora.scheduler.execution;

/**
 * Requests termination of an existing Aurora task through the execution backend.
 * A returned call does not establish a terminal task state or durable delivery.
 * State transitions and retry policy remain owned by the existing scheduler controllers.
 */
@FunctionalInterface
public interface TaskKiller {
  /**
   * Requests termination using the existing Aurora task identifier.
   *
   * @param taskId ID of the task to kill.
   */
  void killTask(String taskId);
}
