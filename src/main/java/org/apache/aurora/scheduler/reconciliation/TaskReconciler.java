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
package org.apache.aurora.scheduler.reconciliation;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import jakarta.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.execution.ReconciliationTarget;
import org.apache.aurora.scheduler.execution.TaskReconciliation;
import org.apache.aurora.scheduler.reconciliation.ReconciliationModule.BackgroundWorker;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.aurora.common.quantity.Time.MINUTES;
import static org.apache.aurora.common.quantity.Time.SECONDS;

/**
 * Periodically requests execution driver reconciliation of all agent tasks (implicit) or
 * scheduler-selected tasks (explicit). The Go execution driver reconciles through watch snapshots
 * independently of these requests.
 */
public class TaskReconciler extends AbstractIdleService {

  @VisibleForTesting
  static final String EXPLICIT_STAT_NAME = "reconciliation_explicit_runs";

  @VisibleForTesting
  static final String IMPLICIT_STAT_NAME = "reconciliation_implicit_runs";

  private final TaskReconcilerSettings settings;
  private final Storage storage;
  private final TaskReconciliation taskReconciliation;
  private final ScheduledExecutorService executor;
  private final AtomicLong explicitRuns;
  private final AtomicLong implicitRuns;

  static class TaskReconcilerSettings {
    private final Amount<Long, Time> explicitInterval;
    private final Amount<Long, Time> implicitInterval;
    private final long explicitDelayMinutes;
    private final long implicitDelayMinutes;
    private final long explicitBatchDelaySeconds;
    private final int explicitBatchSize;

    @VisibleForTesting
    TaskReconcilerSettings(
        Amount<Long, Time> initialDelay,
        Amount<Long, Time> explicitInterval,
        Amount<Long, Time> implicitInterval,
        Amount<Long, Time> scheduleSpread,
        Amount<Long, Time> explicitBatchInterval,
        int explicitBatchSize) {

      this.explicitInterval = requireNonNull(explicitInterval);
      this.implicitInterval = requireNonNull(implicitInterval);
      explicitDelayMinutes = requireNonNull(initialDelay).as(MINUTES);
      implicitDelayMinutes = initialDelay.as(MINUTES) + scheduleSpread.as(MINUTES);
      explicitBatchDelaySeconds = explicitBatchInterval.as(SECONDS);
      this.explicitBatchSize = explicitBatchSize;

      checkArgument(
          explicitDelayMinutes >= 0,
          "Invalid explicit reconciliation delay: %s", explicitDelayMinutes);
      checkArgument(
          implicitDelayMinutes >= 0L,
          "Invalid implicit reconciliation delay: %s", implicitDelayMinutes);
      checkArgument(
          explicitBatchDelaySeconds >= 0L,
          "Invalid explicit batch reconciliation delay: %s", explicitBatchDelaySeconds
      );
    }
  }

  @Inject
  TaskReconciler(
      TaskReconcilerSettings settings,
      Storage storage,
      TaskReconciliation taskReconciliation,
      @BackgroundWorker ScheduledExecutorService executor,
      StatsProvider stats) {

    this.settings = requireNonNull(settings);
    this.storage = requireNonNull(storage);
    this.taskReconciliation = requireNonNull(taskReconciliation);
    this.executor = requireNonNull(executor);
    this.explicitRuns = stats.makeCounter(EXPLICIT_STAT_NAME);
    this.implicitRuns = stats.makeCounter(IMPLICIT_STAT_NAME);
  }

  public void triggerExplicitReconciliation(Optional<Integer> batchSize) {
    doExplicitReconcile(batchSize.orElse(settings.explicitBatchSize));
  }

  public void triggerImplicitReconciliation() {
    doImplicitReconcile();
  }

  @Override
  protected void startUp() {
    scheduleExplicitReconciliation();
    scheduleImplicitReconciliation();
  }

  private void scheduleExplicitReconciliation() {
    executor.scheduleAtFixedRate(
        () -> doExplicitReconcile(settings.explicitBatchSize),
        settings.explicitDelayMinutes,
        settings.explicitInterval.as(MINUTES),
        MINUTES.getTimeUnit());
  }

  private void scheduleImplicitReconciliation() {
    executor.scheduleAtFixedRate(
        () -> doImplicitReconcile(),
        settings.implicitDelayMinutes,
        settings.implicitInterval.as(MINUTES),
        MINUTES.getTimeUnit());
  }

  private void doImplicitReconcile() {
    taskReconciliation.reconcileTasks(ImmutableSet.of());
    implicitRuns.incrementAndGet();
  }

  private void doExplicitReconcile(int batchSize) {
    Iterable<List<IScheduledTask>> activeBatches = Iterables.partition(
        Storage.Util.fetchTasks(storage, Query.unscoped().byStatus(Tasks.SLAVE_ASSIGNED_STATES)),
        batchSize);

    long delay = 0;
    for (List<IScheduledTask> batch : activeBatches) {
      executor.schedule(() -> taskReconciliation.reconcileTasks(
          batch.stream().map(TASK_TO_TARGET).toList()),
          delay,
          SECONDS.getTimeUnit());
      delay += settings.explicitBatchDelaySeconds;
    }
    explicitRuns.incrementAndGet();
  }

  @Override
  protected void shutDown() {
    // Nothing to do - await VM shutdown.
  }

  @VisibleForTesting
  static final Function<IScheduledTask, ReconciliationTarget> TASK_TO_TARGET = t ->
      new ReconciliationTarget(
          t.getAssignedTask().getTaskId(),
          t.getAssignedTask().getSlaveId());
}
