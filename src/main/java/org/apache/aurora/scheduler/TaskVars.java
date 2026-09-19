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

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoGroup;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoType;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAttribute;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * A container that tracks and exports stat counters for tasks.
 */
public class TaskVars extends AbstractIdleService implements EventSubscriber {
  private static final Logger LOG = LoggerFactory.getLogger(TaskVars.class);
  private static final ImmutableSet<ScheduleStatus> TRACKED_JOB_STATES =
      ImmutableSet.of(ScheduleStatus.LOST, ScheduleStatus.FAILED);

  @VisibleForTesting
  static final Map<VetoGroup, String> VETO_GROUPS_TO_COUNTERS = ImmutableMap.of(
      VetoGroup.STATIC, "scheduling_veto_static",
      VetoGroup.DYNAMIC, "scheduling_veto_dynamic",
      VetoGroup.MIXED, "scheduling_veto_mixed"
  );

  @VisibleForTesting
  static final Map<VetoType, String> VETO_TYPE_TO_COUNTERS = ImmutableMap.of(
      VetoType.CONSTRAINT_MISMATCH, "scheduling_veto_constraint_mismatch",
      VetoType.DEDICATED_CONSTRAINT_MISMATCH, "scheduling_veto_dedicated_constraint_mismatch",
      VetoType.INSUFFICIENT_RESOURCES, "scheduling_veto_insufficient_resources",
      VetoType.LIMIT_NOT_SATISFIED, "scheduling_veto_limit_not_satisfied",
      VetoType.MAINTENANCE, "scheduling_veto_maintenance"
  );

  private final LoadingCache<String, Counter> counters;
  private final LoadingCache<String, Counter> untrackedOverflowCounters;
  private final Cache<String, Counter> dynamicCounters;
  private final Cache<String, Counter> untrackedCounters;
  private final StatsProvider trackedProvider;
  private final StatsProvider untrackedProvider;
  private final int dynamicLimit;
  private boolean stopped;
  private final Storage storage;
  private volatile boolean exporting = false;

  @Inject
  TaskVars(Storage storage, StatsProvider statProvider) {
    this(storage, statProvider, Ticker.systemTicker(), 10_000, Duration.ofHours(1));
  }

  @VisibleForTesting
  TaskVars(Storage storage, StatsProvider statProvider, Ticker ticker,
           int dynamicLimit, Duration idleRetention) {
    this.storage = requireNonNull(storage);
    this.trackedProvider = requireNonNull(statProvider);
    this.untrackedProvider = statProvider.untracked();
    if (dynamicLimit < 0) {
      throw new IllegalArgumentException("Dynamic metric limit must be nonnegative");
    }
    this.dynamicLimit = dynamicLimit;
    counters = buildCache(statProvider);
    untrackedOverflowCounters = buildCache(untrackedProvider);
    dynamicCounters = buildDynamicCache(ticker, idleRetention);
    untrackedCounters = buildDynamicCache(ticker, idleRetention);
  }

  private Cache<String, Counter> buildDynamicCache(Ticker ticker, Duration retention) {
    // These are cumulative loss/failure histories, not the fixed active-state gauges. Idle
    // histories retire on subsequent events after one hour and restart at zero on recreation.
    // New names beyond the shared 10,000-name budget accumulate in per-category overflow metrics.
    return CacheBuilder.newBuilder().ticker(ticker).expireAfterAccess(retention)
        .<String, Counter>removalListener(notification -> notification.getValue().close())
        .build();
  }

  private Counter dynamicCounter(String name, String category, boolean untracked) {
    dynamicCounters.cleanUp();
    untrackedCounters.cleanUp();
    Cache<String, Counter> cache = untracked ? untrackedCounters : dynamicCounters;
    Counter existing = cache.getIfPresent(name);
    if (existing != null) {
      return existing;
    }
    if (dynamicCounters.size() + untrackedCounters.size() >= dynamicLimit) {
      return (untracked ? untrackedOverflowCounters : counters)
          .getUnchecked("task_vars_dynamic_" + category + "_overflow");
    }
    Counter counter = new Counter(untracked ? untrackedProvider : trackedProvider);
    if (exporting) {
      counter.exportAs(name);
    }
    cache.put(name, counter);
    return counter;
  }

  private LoadingCache<String, Counter> buildCache(final StatsProvider provider) {
    return CacheBuilder.newBuilder().build(new CacheLoader<String, Counter>() {
      @Override
      public Counter load(String statName) {
        Counter counter = new Counter(provider);
        if (exporting) {
          counter.exportAs(statName);
        }
        return counter;
      }
    });
  }

  @VisibleForTesting
  static String getVarName(ScheduleStatus status) {
    return "task_store_" + status;
  }

  @VisibleForTesting
  static String rackStatName(String rack) {
    return "tasks_lost_rack_" + rack;
  }

  @VisibleForTesting
  static String dedicatedRoleStatName(String role) {
    return "tasks_lost_dedicated_" + role.replace("*", "_");
  }

  @VisibleForTesting
  static String jobStatName(IScheduledTask task, ScheduleStatus status) {
    return String.format(
        "tasks_%s_%s",
        status,
        JobKeys.canonicalString(task.getAssignedTask().getTask().getJob()));
  }

  private static final Predicate<IAttribute> IS_RACK = attr -> "rack".equals(attr.getName());

  private static final Function<IAttribute, String> ATTR_VALUE =
      attr -> Iterables.getOnlyElement(attr.getValues());

  private Counter getCounter(ScheduleStatus status) {
    return counters.getUnchecked(getVarName(status));
  }

  private void incrementCount(ScheduleStatus status) {
    getCounter(status).increment();
  }

  private void decrementCount(ScheduleStatus status) {
    getCounter(status).decrement();
  }

  private void updateHostCounters(IScheduledTask task, ScheduleStatus newState) {
    String host = task.getAssignedTask().getSlaveHost();
    Set<IAttribute> attributes = Strings.isNullOrEmpty(host) ? ImmutableSet.of()
        : storage.read(store ->
            ImmutableSet.copyOf(AttributeStore.Util.attributesOrNone(store, host)));
    Optional<String> rack = attributes.stream().filter(IS_RACK).findFirst().map(ATTR_VALUE);
    Set<String> dedicatedRoles = attributes.stream()
        .filter(attr -> "dedicated".equals(attr.getName())).findFirst()
        .map(IAttribute::getValues).orElse(ImmutableSet.of());

    rack.ifPresent(value -> dynamicCounter(rackStatName(value), "rack", false));
    dedicatedRoles.forEach(role -> dynamicCounter(dedicatedRoleStatName(role), "dedicated", false));
    if (newState == ScheduleStatus.LOST) {
      rack.ifPresentOrElse(
          value -> dynamicCounter(rackStatName(value), "rack", false).increment(),
          () -> LOG.warn("Failed to find rack attribute associated with host " + host));
      dedicatedRoles.forEach(role ->
          dynamicCounter(dedicatedRoleStatName(role), "dedicated", false).increment());
    }
  }

  private void updateJobCounters(IScheduledTask task, ScheduleStatus newState) {
    if (TRACKED_JOB_STATES.contains(newState)) {
      dynamicCounter(jobStatName(task, newState), "job_" + newState, true).increment();
    }
  }

  @Subscribe
  public synchronized void taskChangedState(TaskStateChange stateChange) {
    if (stopped) {
      return;
    }
    IScheduledTask task = stateChange.getTask();
    Optional<ScheduleStatus> previousState = stateChange.getOldState();

    if (stateChange.isTransition() && !previousState.equals(Optional.of(ScheduleStatus.INIT))) {
      decrementCount(previousState.get());
    }
    incrementCount(task.getStatus());

    updateHostCounters(task, task.getStatus());
    updateJobCounters(task, task.getStatus());
  }

  @Override
  protected synchronized void startUp() {
    // Dummy read the counter for each status counter. This is important to guarantee a stat with
    // value zero is present for each state, even if all states are not represented in the task
    // store.
    for (ScheduleStatus status : ScheduleStatus.values()) {
      getCounter(status);
    }

    try {
      exportCounters(counters.asMap());
      exportCounters(untrackedOverflowCounters.asMap());
      exportCounters(dynamicCounters.asMap());
      exportCounters(untrackedCounters.asMap());
    } catch (RuntimeException | Error e) {
      shutDown();
      throw e;
    }
  }

  @Override
  protected synchronized void shutDown() {
    stopped = true;
    exporting = false;
    counters.asMap().values().forEach(Counter::close);
    counters.invalidateAll();
    untrackedOverflowCounters.asMap().values().forEach(Counter::close);
    untrackedOverflowCounters.invalidateAll();
    dynamicCounters.invalidateAll();
    untrackedCounters.invalidateAll();
  }

  private void exportCounters(Map<String, Counter> counterMap) {
    // Initiate export of all counters.  This is not done initially to avoid exporting values that
    // do not represent the entire storage contents.
    exporting = true;
    for (Map.Entry<String, Counter> entry : counterMap.entrySet()) {
      entry.getValue().exportAs(entry.getKey());
    }
  }

  @Subscribe
  public synchronized void tasksDeleted(final TasksDeleted event) {
    if (stopped) {
      return;
    }
    for (IScheduledTask task : event.getTasks()) {
      decrementCount(task.getStatus());
    }
  }

  public synchronized void taskVetoed(Set<Veto> vetoes) {
    if (stopped) {
      return;
    }
    VetoGroup vetoGroup = Veto.identifyGroup(vetoes);
    if (vetoGroup != VetoGroup.EMPTY) {
      counters.getUnchecked(VETO_GROUPS_TO_COUNTERS.get(vetoGroup)).increment();
    }
    for (Veto veto : vetoes) {
      counters.getUnchecked(VETO_TYPE_TO_COUNTERS.get(veto.getVetoType())).increment();
    }
  }

  private static class Counter implements Supplier<Long> {
    private final AtomicLong value = new AtomicLong();
    private StatsProvider.Registration registration;
    private final StatsProvider stats;

    Counter(StatsProvider stats) {
      this.stats = stats;
    }

    @Override
    public Long get() {
      return value.get();
    }

    private synchronized void exportAs(String name) {
      if (registration == null) {
        registration = stats.registerGauge(name, this);
      }
    }

    private synchronized void close() {
      if (registration != null) {
        registration.close();
        registration = null;
      }
    }

    private void increment() {
      value.incrementAndGet();
    }

    private void decrement() {
      value.decrementAndGet();
    }
  }
}
