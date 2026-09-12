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

import java.util.Optional;

import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.execution.TaskObservation;
import org.apache.aurora.scheduler.execution.TaskUpdate;
import org.apache.mesos.v1.Protos.TaskStatus;

import static java.util.Objects.requireNonNull;

/** Preserves the native receipt for explicit acknowledgement after storage commits. */
public record MesosTaskUpdate(TaskStatus status, Driver driver) implements TaskUpdate {
  public static final String MEMORY_LIMIT_DISPLAY = "Task used more memory than requested.";
  public static final String DISK_LIMIT_DISPLAY = "Task used more disk than requested.";

  public MesosTaskUpdate {
    requireNonNull(status);
    requireNonNull(driver);
  }

  @Override
  public TaskObservation observe() {
    // Conversion can fail: keep it deferred until the worker's storage transaction.
    return new TaskObservation(
        status.getTaskId().getValue(),
        Conversions.convertProtoState(status.getState()),
        status.hasReason() ? Optional.of(status.getReason().name()) : Optional.empty(),
        formatMessage(status));
  }

  @Override
  public void acknowledge() {
    driver.acknowledgeStatusUpdate(status);
  }

  @Override
  public String toString() {
    return status.toString();
  }

  private static Optional<String> formatMessage(TaskStatus status) {
    Optional<String> message = status.hasMessage()
        ? Optional.of(status.getMessage()) : Optional.empty();
    if (!status.hasReason()) {
      return message;
    }
    return switch (status.getReason()) {
      case REASON_CONTAINER_LIMITATION_MEMORY ->
          message.or(() -> Optional.of(MEMORY_LIMIT_DISPLAY));
      case REASON_CONTAINER_LIMITATION_DISK -> message.or(() -> Optional.of(DISK_LIMIT_DISPLAY));
      case REASON_EXECUTOR_UNREGISTERED -> Optional.empty();
      default -> message;
    };
  }
}
