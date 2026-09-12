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

import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;

import static java.util.Objects.requireNonNull;

/** SQLite-backed cron jobs. */
final class SqliteCronJobStore implements CronJobStore.Mutable {
  private final SqliteRecords<JobConfiguration> records;

  SqliteCronJobStore(SqliteDatabase db) {
    records = new SqliteRecords<>(requireNonNull(db), SqliteRecords.Table.CRON_JOBS,
        JobConfiguration::new);
  }

  @Override
  public void saveAcceptedJob(IJobConfiguration jobConfig) {
    IJobKey key = JobKeys.assertValid(jobConfig.getKey());
    records.put(SqliteRecords.key(key.getRole(), key.getEnvironment(), key.getName()),
        jobConfig.newBuilder());
  }

  @Override
  public void removeJob(IJobKey jobKey) {
    records.remove(SqliteRecords.key(jobKey.getRole(), jobKey.getEnvironment(), jobKey.getName()));
  }

  @Override
  public void deleteJobs() {
    records.clear();
  }

  @Override
  public Iterable<IJobConfiguration> fetchJobs() {
    return records.all().values().stream()
        .map(IJobConfiguration::build)
        .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public Optional<IJobConfiguration> fetchJob(IJobKey jobKey) {
    return records.get(
            SqliteRecords.key(jobKey.getRole(), jobKey.getEnvironment(), jobKey.getName()))
        .map(IJobConfiguration::build);
  }
}
