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
package org.apache.aurora.scheduler.base;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.JobStats;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.storage.entities.IJobStats;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

/**
 * Convenience methods related to jobs.
 */
public final class Jobs {

  private Jobs() {
    // Utility class.
  }

  /**
   * States of updates that are blocked on pulses.
   */
  public static final Set<JobUpdateStatus> AWAITING_PULSE_STATES =
      ImmutableSet.copyOf(apiConstants.AWAITNG_PULSE_JOB_UPDATE_STATES);

  /**
   * For a given collection of tasks compute statistics based on the state of the task.
   *
   * @param tasks a collection of tasks for which statistics are sought
   * @return an JobStats object containing the statistics about the tasks.
   */
  public static IJobStats getJobStats(Iterable<IScheduledTask> tasks) {
    JobStats stats = new JobStats();
    for (IScheduledTask task : tasks) {
      updateStats(stats, task.getStatus());
    }
    return IJobStats.build(stats);
  }

  private static void updateStats(JobStats stats, ScheduleStatus status) {
    switch (status) {
      case INIT, PENDING, THROTTLED -> stats.setPendingTaskCount(stats.getPendingTaskCount() + 1);
      case ASSIGNED, STARTING, RESTARTING, RUNNING, KILLING, DRAINING, PARTITIONED, PREEMPTING ->
          stats.setActiveTaskCount(stats.getActiveTaskCount() + 1);
      case KILLED, FINISHED -> stats.setFinishedTaskCount(stats.getFinishedTaskCount() + 1);
      case LOST, FAILED -> stats.setFailedTaskCount(stats.getFailedTaskCount() + 1);
      default -> throw new IllegalArgumentException("Unsupported status: " + status);
    }
  }
}
