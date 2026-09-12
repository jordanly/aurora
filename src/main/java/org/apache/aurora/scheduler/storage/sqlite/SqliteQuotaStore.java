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

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;

import static java.util.Objects.requireNonNull;

/** SQLite-backed quotas. */
final class SqliteQuotaStore implements QuotaStore.Mutable {
  private final SqliteRecords<ResourceAggregate> records;

  SqliteQuotaStore(SqliteDatabase db) {
    records = new SqliteRecords<>(requireNonNull(db), SqliteRecords.Table.QUOTAS,
        ResourceAggregate::new);
  }

  @Override
  public void deleteQuotas() {
    records.clear();
  }

  @Override
  public void removeQuota(String role) {
    records.remove(role);
  }

  @Override
  public void saveQuota(String role, IResourceAggregate quota) {
    records.put(role, quota.newBuilder());
  }

  @Override
  public Optional<IResourceAggregate> fetchQuota(String role) {
    return records.get(role).map(IResourceAggregate::build);
  }

  @Override
  public Map<String, IResourceAggregate> fetchQuotas() {
    return records.all().entrySet().stream().collect(Collectors.toUnmodifiableMap(
        Map.Entry::getKey, entry -> IResourceAggregate.build(entry.getValue())));
  }
}
