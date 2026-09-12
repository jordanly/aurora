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

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;

import org.apache.aurora.scheduler.execution.ReconciliationTarget;
import org.apache.aurora.scheduler.execution.TaskReconciliation;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.TaskStatus;

import static java.util.Objects.requireNonNull;

/** Adapts neutral reconciliation targets to the native Mesos reconciliation API. */
public class MesosTaskReconciliation implements TaskReconciliation {
  @VisibleForTesting
  static final Function<ReconciliationTarget, TaskStatus> TARGET_TO_PROTO = target ->
      TaskStatus.newBuilder()
          // Mesos requires a state in this legacy API but ignores it for reconciliation.
          .setState(Protos.TaskState.TASK_RUNNING)
          .setAgentId(Protos.AgentID.newBuilder().setValue(target.agentId()).build())
          .setTaskId(Protos.TaskID.newBuilder().setValue(target.taskId()).build())
          .build();

  private final Driver driver;

  @Inject
  public MesosTaskReconciliation(Driver driver) {
    this.driver = requireNonNull(driver);
  }

  @Override
  public void reconcileTasks(Collection<ReconciliationTarget> targets) {
    List<TaskStatus> statuses = targets.stream()
        .map(TARGET_TO_PROTO)
        .toList();
    driver.reconcileTasks(statuses);
  }
}
