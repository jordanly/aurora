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
package org.apache.aurora.benchmark.fakes;

import java.util.Collection;

import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.aurora.scheduler.execution.ExecutionDriver;
import org.apache.aurora.scheduler.execution.ExecutionOffer;
import org.apache.aurora.scheduler.execution.OfferTransport;
import org.apache.aurora.scheduler.execution.PreparedTask;
import org.apache.aurora.scheduler.execution.ReconciliationTarget;
import org.apache.aurora.scheduler.execution.TaskFactory;
import org.apache.aurora.scheduler.execution.TaskReconciliation;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;

/** No external work: benchmarks measure the original policy and state machinery. */
public class FakeDriver extends AbstractIdleService
    implements ExecutionDriver, OfferTransport, TaskFactory, TaskReconciliation {
  public record Launch(IAssignedTask task) implements PreparedTask { }

  @Override
  public PreparedTask prepare(IAssignedTask task, ExecutionOffer offer, boolean revocable) {
    return new Launch(task);
  }

  @Override public void blockUntilStopped() { }
  @Override public void launch(String offerId, PreparedTask task, double refuseSeconds) { }
  @Override public void decline(String offerId, double refuseSeconds) { }
  @Override public void killTask(String taskId) { }
  @Override public void abort() { }
  @Override protected void startUp() { }
  @Override protected void shutDown() { }
  @Override public void reconcileTasks(Collection<ReconciliationTarget> targets) { }
}
