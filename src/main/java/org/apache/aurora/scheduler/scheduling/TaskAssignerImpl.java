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
package org.apache.aurora.scheduler.scheduling;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.updater.UpdateAgentReserver;
import org.apache.mesos.v1.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.common.inject.TimedInterceptor.Timed;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;

public class TaskAssignerImpl implements TaskAssigner {
  private static final Logger LOG = LoggerFactory.getLogger(TaskAssignerImpl.class);

  @VisibleForTesting
  static final Optional<String> LAUNCH_FAILED_MSG =
      Optional.of("Unknown exception attempting to schedule task.");
  @VisibleForTesting
  static final String ASSIGNER_LAUNCH_FAILURES = "assigner_launch_failures";
  @VisibleForTesting
  static final String ASSIGNER_NOT_PENDING_FAILURES = "assigner_not_pending_failures";
  @VisibleForTesting
  static final String ASSIGNER_DIFFERENT_ATTRIBUTES_FAILURES =
      "assigner_different_attributes_failures";
  @VisibleForTesting
  static final String ASSIGNER_OFFER_NO_LONGER_MATCHES =
      "assigner_offer_no_longer_matches_failures";



  private final AtomicLong launchFailures;
  private final AtomicLong notPendingFailures;
  private final AtomicLong differentAttributesFailures;
  private final AtomicLong offerNoLongerMatchesFailures;

  private final StateManager stateManager;
  private final MesosTaskFactory taskFactory;
  private final OfferManager offerManager;
  private final TierManager tierManager;
  private final UpdateAgentReserver updateAgentReserver;
  private final OfferSelector offerSelector;
  private final Storage storage;
  private final ScheduledExecutorService executor;
  private final BlockingQueue<Consumer<MutableStoreProvider>> launchTaskQueue;

  private static final int MAX_TO_LAUNCH = 50;

  @Inject
  public TaskAssignerImpl(
      StateManager stateManager,
      MesosTaskFactory taskFactory,
      OfferManager offerManager,
      TierManager tierManager,
      UpdateAgentReserver updateAgentReserver,
      StatsProvider statsProvider,
      OfferSelector offerSelector,
      Storage storage) {

    this.stateManager = requireNonNull(stateManager);
    this.taskFactory = requireNonNull(taskFactory);
    this.offerManager = requireNonNull(offerManager);
    this.tierManager = requireNonNull(tierManager);
    this.launchFailures = statsProvider.makeCounter(ASSIGNER_LAUNCH_FAILURES);
    this.notPendingFailures = statsProvider.makeCounter(ASSIGNER_NOT_PENDING_FAILURES);
    this.differentAttributesFailures = statsProvider.makeCounter(
        ASSIGNER_DIFFERENT_ATTRIBUTES_FAILURES);
    this.offerNoLongerMatchesFailures = statsProvider.makeCounter(
        ASSIGNER_OFFER_NO_LONGER_MATCHES);
    this.updateAgentReserver = requireNonNull(updateAgentReserver);
    this.offerSelector = requireNonNull(offerSelector);
    this.storage = requireNonNull(storage);
    this.launchTaskQueue = new LinkedBlockingDeque<>();
    this.executor = AsyncUtil.singleThreadLoggingScheduledExecutor("TaskLauncher", LOG);
    executor.scheduleWithFixedDelay(this::drainLaunchTasks, 10, 1, TimeUnit.SECONDS);
  }

  private void drainLaunchTasks() {
    List<Consumer<MutableStoreProvider>> batch = new LinkedList<>();
    launchTaskQueue.drainTo(batch, MAX_TO_LAUNCH);
    storage.write(storeProvider -> {
      for (Consumer<MutableStoreProvider> consumer : batch) {
        consumer.accept(storeProvider);
      }
    });
  }

  @VisibleForTesting
  IAssignedTask mapAndAssignResources(Protos.Offer offer, IAssignedTask task) {
    IAssignedTask assigned = task;
    for (ResourceType type : ResourceManager.getTaskResourceTypes(assigned)) {
      if (type.getMapper().isPresent()) {
        assigned = type.getMapper().get().mapAndAssign(offer, assigned);
      }
    }
    return assigned;
  }

  private Protos.TaskInfo assign(
      MutableStoreProvider storeProvider,
      Protos.Offer offer,
      String taskId,
      boolean revocable) {

    String host = offer.getHostname();
    IAssignedTask assigned = stateManager.assignTask(
        storeProvider,
        taskId,
        host,
        offer.getAgentId(),
        task -> mapAndAssignResources(offer, task));
    LOG.info(
        "Offer on agent {} (id {}) is being assigned task for {}.",
        host, offer.getAgentId().getValue(), taskId);
    return taskFactory.createFrom(assigned, offer, revocable);
  }

  @Timed("assigner_launch_using_offer")
  @VisibleForTesting
  void launchUsingOffer(
      boolean revocable,
      ResourceRequest resourceRequest,
      IAssignedTask task,
      HostOffer offer,
      ImmutableSet.Builder<String> assignmentResult,
      TaskGroupKey groupKey) {

    storage.write((NoResult<OfferManager.LaunchException>) storeProvider -> {
      // TODO(jly): additional checks here on assignment
      // Check that the task is still pending
      Optional<IScheduledTask> optionalExpectedTask =
          storeProvider.getTaskStore().fetchTask(task.getTaskId());
      if (!optionalExpectedTask.isPresent() || optionalExpectedTask.get().getStatus() != PENDING) {
        LOG.error("JLY -- failed on task pending check");
        notPendingFailures.incrementAndGet();
        throw new OfferManager.LaunchException("JLY -- unexpected state not PENDING");
      }

      // Check that the offer still fits -- TODO(jly): question on when this would happen
      AttributeAggregate expectedAggregate =
          AttributeAggregate.getJobActiveState(storeProvider, task.getTask().getJob());
      if (!expectedAggregate.equals(resourceRequest.getJobState())) {
        LOG.error("JLY -- failed on attribute equals");
        differentAttributesFailures.incrementAndGet();
        return null; // TODO(jly): return what
      }

      // Sanity check that the offer matches
      // TODO(jly): pass group key in?
      if (!offerManager.matches(offer, groupKey, resourceRequest, revocable)) {
        LOG.error("JLY -- offer no longer matches");
        offerNoLongerMatchesFailures.incrementAndGet();
      }

      // TODO(jly): we are probably good to launch here
      String taskId = task.getTaskId();
      Protos.TaskInfo taskInfo = assign(storeProvider, offer.getOffer(), taskId, revocable);
      resourceRequest.getJobState().updateAttributeAggregate(offer.getAttributes());
      try {
        offerManager.launchTask(offer.getOffer().getId(), taskInfo);
        assignmentResult.add(taskId);
      } catch (OfferManager.LaunchException e) {
        LOG.warn("Failed to launch task.", e);
        launchFailures.incrementAndGet();

        // The attempt to schedule the task failed, so we need to backpedal on the assignment.
        // It is in the LOST state and a new task will move to PENDING to replace it.
        // Should the state change fail due to storage issues, that's okay.  The task will
        // time out in the ASSIGNED state and be moved to LOST.
        stateManager.changeState(
            storeProvider,
            taskId,
            Optional.of(PENDING),
            LOST,
            LAUNCH_FAILED_MSG);
        throw e;
      }
    });
  }

  private Iterable<IAssignedTask> maybeAssignReserved(
      Iterable<IAssignedTask> tasks,
      boolean revocable,
      ResourceRequest resourceRequest,
      TaskGroupKey groupKey,
      ImmutableSet.Builder<String> assignmentResult) {

    if (!updateAgentReserver.hasReservations(groupKey)) {
      return tasks;
    }

    // Data structure to record which tasks should be excluded from the regular (non-reserved)
    // scheduling loop. This is important because we release reservations once they are used,
    // so we need to record them separately to avoid them being double-scheduled.
    ImmutableSet.Builder<IInstanceKey> excludeBuilder = ImmutableSet.builder();

    for (IAssignedTask task : tasks) {
      IInstanceKey key = InstanceKeys.from(task.getTask().getJob(), task.getInstanceId());
      Optional<String> maybeAgentId = updateAgentReserver.getAgent(key);
      if (maybeAgentId.isPresent()) {
        excludeBuilder.add(key);
        Optional<HostOffer> offer = offerManager.getMatching(
            Protos.AgentID.newBuilder().setValue(maybeAgentId.get()).build(),
            resourceRequest,
            revocable);
        if (offer.isPresent()) {
          // The offer can still be veto'd because of changed constraints, or because the
          // Scheduler hasn't been updated by Mesos yet...
          launchUsingOffer(revocable,
              resourceRequest,
              task,
              offer.get(),
              assignmentResult,
              groupKey);
          LOG.info("Used update reservation for {} on {}", key, maybeAgentId.get());
          updateAgentReserver.release(maybeAgentId.get(), key);
        } else {
          LOG.info(
              "Tried to reuse offer on {} for {}, but was not ready yet.",
              maybeAgentId.get(),
              key);
        }
      }
    }

    // Return only the tasks that didn't have reservations. Offers on agents that were reserved
    // might not have been seen by Aurora yet, so we need to wait until the reservation expires
    // before giving up and falling back to the first-fit algorithm.
    Set<IInstanceKey> toBeExcluded = excludeBuilder.build();
    return Iterables.filter(tasks, t -> !toBeExcluded.contains(
        InstanceKeys.from(t.getTask().getJob(), t.getInstanceId())));
  }

  /**
   * Determine whether or not the offer is reserved for a different task via preemption or
   * update affinity.
   */
  @SuppressWarnings("PMD.UselessParentheses")  // TODO(jly): PMD bug, remove when upgrade from 5.5.3
  private boolean isAgentReserved(HostOffer offer,
                                  TaskGroupKey groupKey,
                                  Map<String, TaskGroupKey> preemptionReservations) {

    String agentId = offer.getOffer().getAgentId().getValue();
    Optional<TaskGroupKey> reservedGroup = Optional.fromNullable(
        preemptionReservations.get(agentId));

    return (reservedGroup.isPresent() && !reservedGroup.get().equals(groupKey))
        || !updateAgentReserver.getReservations(agentId).isEmpty();
  }

  @Timed("assigner_maybe_assign")
  @Override
  public Set<String> maybeAssign(
      ResourceRequest resourceRequest,
      TaskGroupKey groupKey,
      Iterable<IAssignedTask> tasks,
      Map<String, TaskGroupKey> preemptionReservations) {

    if (Iterables.isEmpty(tasks)) {
      return ImmutableSet.of();
    }

    boolean revocable = tierManager.getTier(groupKey.getTask()).isRevocable();
    ImmutableSet.Builder<String> assignmentResult = ImmutableSet.builder();

    // Assign tasks reserved for a specific agent (e.g. for update affinity)
    Iterable<IAssignedTask> nonReservedTasks = maybeAssignReserved(tasks,
        revocable,
        resourceRequest,
        groupKey,
        assignmentResult);

    // Assign the rest of the non-reserved tasks
    for (IAssignedTask task : nonReservedTasks) {
      // Get all offers that will satisfy the given ResourceRequest and that are not reserved
      // for updates or preemption
      FluentIterable<HostOffer> matchingOffers = FluentIterable
          .from(offerManager.getAllMatching(groupKey, resourceRequest, revocable))
          .filter(o -> !isAgentReserved(o, groupKey, preemptionReservations));

      // Determine which is the optimal offer to select for the given request
      Optional<HostOffer> optionalOffer = offerSelector.select(matchingOffers, resourceRequest);

      // If no offer is chosen, continue to the next task
      if (!optionalOffer.isPresent()) {
        continue;
      }

      // Attempt to launch the task using the chosen offer
      HostOffer offer = optionalOffer.get();
      launchUsingOffer(revocable,
          resourceRequest,
          task,
          offer,
          assignmentResult,
          groupKey);

    }

    return assignmentResult.build();
  }
}
