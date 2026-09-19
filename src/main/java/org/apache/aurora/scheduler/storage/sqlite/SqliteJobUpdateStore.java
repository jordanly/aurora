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

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.JobUpdateStoreSupport;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;

import static java.util.Objects.requireNonNull;

/** Job updates and their ordered histories stored in the caller's SQLite transaction. */
final class SqliteJobUpdateStore implements JobUpdateStore.Mutable {
  private final SqliteDatabase database;
  private final SqliteRecords<JobUpdateDetails> records;

  SqliteJobUpdateStore(SqliteDatabase database) {
    this.database = requireNonNull(database);
    this.records = new SqliteRecords<>(
        database, SqliteRecords.Table.JOB_UPDATES, JobUpdateDetails::new);
  }

  @Override
  public List<IJobUpdateDetails> fetchJobUpdates(IJobUpdateQuery query) {
    IJobUpdateKey key = query.getKey();
    boolean exactKey = key != null && key.getId() != null && key.getJob() != null
        && key.getJob().getRole() != null && key.getJob().getEnvironment() != null
        && key.getJob().getName() != null;
    return JobUpdateStoreSupport.query(
        exactKey
            ? fetchJobUpdate(key).stream()
            : records.all().values().stream().map(IJobUpdateDetails::build),
        query);
  }

  @Override
  public Optional<IJobUpdateDetails> fetchJobUpdate(IJobUpdateKey key) {
    return records.get(key(key)).map(IJobUpdateDetails::build);
  }

  @Override
  public void saveJobUpdate(IJobUpdate update) {
    database.requireWrite();
    IJobUpdateDetails details = JobUpdateStoreSupport.create(update);
    records.put(key(update.getSummary().getKey()), details.newBuilder());
  }

  @Override
  public void saveJobUpdateEvent(IJobUpdateKey key, IJobUpdateEvent event) {
    database.requireWrite();
    records.put(
        key(key),
        JobUpdateStoreSupport.appendUpdateEvent(existing(key), event).newBuilder());
  }

  @Override
  public void saveJobInstanceUpdateEvent(IJobUpdateKey key, IJobInstanceUpdateEvent event) {
    database.requireWrite();
    records.put(
        key(key),
        JobUpdateStoreSupport.appendInstanceEvent(existing(key), event).newBuilder());
  }

  @Override
  public void removeJobUpdates(Set<IJobUpdateKey> keys) {
    database.requireWrite();
    requireNonNull(keys).forEach(key -> records.remove(key(key)));
  }

  @Override
  public void deleteAllUpdates() {
    database.requireWrite();
    records.clear();
  }

  private IJobUpdateDetails existing(IJobUpdateKey key) {
    return fetchJobUpdate(key).orElseThrow(() -> new StorageException("Update not found: " + key));
  }

  private static String key(IJobUpdateKey key) {
    return SqliteRecords.key(key.getJob().getRole(), key.getJob().getEnvironment(),
        key.getJob().getName(), key.getId());
  }
}
