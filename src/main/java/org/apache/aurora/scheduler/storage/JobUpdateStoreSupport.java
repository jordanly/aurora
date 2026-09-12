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
package org.apache.aurora.scheduler.storage;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateState;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;

import static java.util.Objects.requireNonNull;

/** Shared immutable update-history and query behavior for memory and SQL stores. */
public final class JobUpdateStoreSupport {
  private static final Ordering<IJobUpdateDetails> REVERSE_LAST_MODIFIED_ORDER = Ordering.natural()
      .reverse()
      .onResultOf(u -> u.getUpdate().getSummary().getState().getLastModifiedTimestampMs());
  private static final Ordering<JobUpdateEvent> EVENT_ORDERING = Ordering.natural()
      .onResultOf(JobUpdateEvent::getTimestampMs);
  private static final Ordering<JobInstanceUpdateEvent> INSTANCE_EVENT_ORDERING = Ordering.natural()
      .onResultOf(JobInstanceUpdateEvent::getTimestampMs);

  private JobUpdateStoreSupport() {
  }

  private static void validateInstructions(IJobUpdateInstructions instructions) {
    if (!instructions.isSetDesiredState() && instructions.getInitialState().isEmpty()) {
      throw new IllegalArgumentException(
          "Missing both initial and desired states. At least one is required.");
    }

    if (!instructions.getInitialState().isEmpty()) {
      if (instructions.getInitialState().stream().anyMatch(t -> t.getTask() == null)) {
        throw new NullPointerException("Invalid initial instance state.");
      }
      Preconditions.checkArgument(
          instructions.getInitialState().stream().noneMatch(t -> t.getInstances().isEmpty()),
          "Invalid intial instance state ranges.");
    }

    if (instructions.getDesiredState() != null) {
      MorePreconditions.checkNotBlank(instructions.getDesiredState().getInstances());
      Preconditions.checkNotNull(instructions.getDesiredState().getTask());
    }
  }

  public static IJobUpdateDetails create(IJobUpdate update) {
    requireNonNull(update);
    validateInstructions(update.getInstructions());

    JobUpdateDetails mutable = new JobUpdateDetails()
        .setUpdate(update.newBuilder())
        .setUpdateEvents(ImmutableList.of())
        .setInstanceEvents(ImmutableList.of());
    mutable.getUpdate().getSummary().setState(synthesizeUpdateState(mutable));

    return IJobUpdateDetails.build(mutable);
  }

  public static IJobUpdateDetails appendUpdateEvent(
      IJobUpdateDetails update, IJobUpdateEvent event) {
    JobUpdateDetails mutable = update.newBuilder();
    mutable.addToUpdateEvents(event.newBuilder());
    mutable.setUpdateEvents(EVENT_ORDERING.sortedCopy(mutable.getUpdateEvents()));
    mutable.getUpdate().getSummary().setState(synthesizeUpdateState(mutable));
    return IJobUpdateDetails.build(mutable);
  }

  public static IJobUpdateDetails appendInstanceEvent(
      IJobUpdateDetails update, IJobInstanceUpdateEvent event) {
    JobUpdateDetails mutable = update.newBuilder();
    mutable.addToInstanceEvents(event.newBuilder());
    mutable.setInstanceEvents(INSTANCE_EVENT_ORDERING.sortedCopy(mutable.getInstanceEvents()));
    mutable.getUpdate().getSummary().setState(synthesizeUpdateState(mutable));
    return IJobUpdateDetails.build(mutable);
  }

  private static JobUpdateState synthesizeUpdateState(JobUpdateDetails update) {
    JobUpdateState state = new JobUpdateState();

    JobUpdateEvent firstEvent = Iterables.getFirst(update.getUpdateEvents(), null);
    if (firstEvent != null) {
      state.setCreatedTimestampMs(firstEvent.getTimestampMs());
    }

    JobUpdateEvent lastEvent = Iterables.getLast(update.getUpdateEvents(), null);
    if (lastEvent != null) {
      state.setStatus(lastEvent.getStatus());
      state.setLastModifiedTimestampMs(lastEvent.getTimestampMs());
    }

    JobInstanceUpdateEvent lastInstanceEvent = Iterables.getLast(update.getInstanceEvents(), null);
    if (lastInstanceEvent != null) {
      state.setLastModifiedTimestampMs(
          Longs.max(state.getLastModifiedTimestampMs(), lastInstanceEvent.getTimestampMs()));
    }

    return state;
  }

  public static List<IJobUpdateDetails> query(
      Stream<IJobUpdateDetails> updates, IJobUpdateQuery query) {
    Predicate<IJobUpdateDetails> filter = u -> true;
    if (query.getRole() != null) {
      filter = filter.and(
          u -> u.getUpdate().getSummary().getKey().getJob().getRole().equals(query.getRole()));
    }
    if (query.getKey() != null) {
      filter = filter.and(u -> u.getUpdate().getSummary().getKey().equals(query.getKey()));
    }
    if (query.getJobKey() != null) {
      filter = filter.and(
          u -> u.getUpdate().getSummary().getKey().getJob().equals(query.getJobKey()));
    }
    if (query.getUser() != null) {
      filter = filter.and(u -> u.getUpdate().getSummary().getUser().equals(query.getUser()));
    }
    if (query.getUpdateStatuses() != null && !query.getUpdateStatuses().isEmpty()) {
      filter = filter.and(u -> query.getUpdateStatuses()
          .contains(u.getUpdate().getSummary().getState().getStatus()));
    }

    // TODO(wfarner): Modification time is not a stable ordering for pagination, but we use it as
    // such here.  The behavior is carried over from DbJobupdateStore; determine if it is desired.
    Stream<IJobUpdateDetails> matches = updates
        .filter(filter)
        .sorted(REVERSE_LAST_MODIFIED_ORDER)
        .skip(query.getOffset());

    if (query.getLimit() > 0) {
      matches = matches.limit(query.getLimit());
    }

    return matches.collect(Collectors.toList());
  }
}
