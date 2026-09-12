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

import org.apache.aurora.scheduler.execution.PreparedTask;
import org.apache.mesos.v1.Protos.TaskInfo;

import static java.util.Objects.requireNonNull;

/** Eagerly encoded task; dispatch must not repeat or defer preparation. */
public final class MesosPreparedTask implements PreparedTask {
  private final TaskInfo taskInfo;

  public MesosPreparedTask(TaskInfo taskInfo) {
    this.taskInfo = requireNonNull(taskInfo);
  }

  TaskInfo getTaskInfo() {
    return taskInfo;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof MesosPreparedTask prepared
        && taskInfo.equals(prepared.taskInfo);
  }

  @Override
  public int hashCode() {
    return taskInfo.hashCode();
  }
}
