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
package org.apache.aurora.scheduler.storage.sqlite;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.util.Objects.requireNonNull;

/** Uses the original query predicate over a transaction's committed snapshot. */
final class SqliteTaskStore implements TaskStore.Mutable {
  private final SqliteDatabase database;
  private final SqliteRecords<ScheduledTask> records;

  SqliteTaskStore(SqliteDatabase database) {
    this.database = requireNonNull(database);
    records = new SqliteRecords<>(database, SqliteRecords.Table.TASKS, ScheduledTask::new);
  }

  @Override
  public Optional<IScheduledTask> fetchTask(String taskId) {
    return records.get(taskId).map(IScheduledTask::build);
  }

  @Override
  public Collection<IScheduledTask> fetchTasks(Query.Builder query) {
    var filter = TaskStore.Util.queryFilter(requireNonNull(query));
    if (!query.get().getTaskIds().isEmpty()) {
      return query.get().getTaskIds().stream().map(this::fetchTask)
          .flatMap(Optional::stream).filter(filter).toList();
    }
    return records.all().values().stream().map(IScheduledTask::build).filter(filter).toList();
  }

  @Override
  public Set<IJobKey> getJobKeys() {
    return records.all().values().stream().map(IScheduledTask::build)
        .map(task -> task.getAssignedTask().getTask().getJob())
        .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public void saveTasks(Set<IScheduledTask> tasks) {
    database.requireWrite();
    requireNonNull(tasks);
    Preconditions.checkState(Tasks.ids(tasks).size() == tasks.size(),
        "Proposed new tasks would create task ID collision.");
    tasks.forEach(task -> records.put(Tasks.id(task), task.newBuilder()));
  }

  @Override
  public void deleteAllTasks() {
    records.clear();
  }

  @Override
  public void deleteTasks(Set<String> taskIds) {
    database.requireWrite();
    requireNonNull(taskIds).forEach(records::remove);
  }

  @Override
  public Optional<IScheduledTask> mutateTask(
      String taskId, Function<IScheduledTask, IScheduledTask> mutator) {
    database.requireWrite();
    return fetchTask(taskId).map(original -> {
      IScheduledTask mutated = requireNonNull(mutator.apply(original));
      if (!original.equals(mutated)) {
        Preconditions.checkState(Tasks.id(original).equals(Tasks.id(mutated)),
            "A task's ID may not be mutated.");
        records.put(taskId, mutated.newBuilder());
      }
      return mutated;
    });
  }
}
