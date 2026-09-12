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

import javax.inject.Inject;

import org.apache.aurora.scheduler.execution.ExecutionOffer;
import org.apache.aurora.scheduler.execution.PreparedTask;
import org.apache.aurora.scheduler.execution.TaskFactory;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;

import static java.util.Objects.requireNonNull;

/** Keeps native task preparation at the original point in the assignment transaction. */
public final class MesosTaskFactoryAdapter implements TaskFactory {
  private final MesosTaskFactory taskFactory;

  @Inject
  public MesosTaskFactoryAdapter(MesosTaskFactory taskFactory) {
    this.taskFactory = requireNonNull(taskFactory);
  }

  @Override
  public PreparedTask prepare(IAssignedTask task, ExecutionOffer offer, boolean revocable) {
    return new MesosPreparedTask(
        taskFactory.createFrom(task, MesosOffer.toMesos(offer), revocable));
  }
}
