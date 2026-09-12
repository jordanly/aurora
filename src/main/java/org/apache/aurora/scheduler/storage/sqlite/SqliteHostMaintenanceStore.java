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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.aurora.gen.HostMaintenanceRequest;
import org.apache.aurora.scheduler.storage.HostMaintenanceStore;
import org.apache.aurora.scheduler.storage.entities.IHostMaintenanceRequest;

import static java.util.Objects.requireNonNull;

/** SQLite-backed host maintenance requests. */
final class SqliteHostMaintenanceStore implements HostMaintenanceStore.Mutable {
  private final SqliteRecords<HostMaintenanceRequest> records;

  SqliteHostMaintenanceStore(SqliteDatabase db) {
    records = new SqliteRecords<>(requireNonNull(db), SqliteRecords.Table.HOST_MAINTENANCE,
        HostMaintenanceRequest::new);
  }

  @Override
  public Optional<IHostMaintenanceRequest> getHostMaintenanceRequest(String host) {
    return records.get(host).map(IHostMaintenanceRequest::build);
  }

  @Override
  public Set<IHostMaintenanceRequest> getHostMaintenanceRequests() {
    return records.all().values().stream()
        .map(IHostMaintenanceRequest::build)
        .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public void deleteHostMaintenanceRequests() {
    records.clear();
  }

  @Override
  public void saveHostMaintenanceRequest(IHostMaintenanceRequest request) {
    records.put(request.getHost(), request.newBuilder());
  }

  @Override
  public void removeHostMaintenanceRequest(String host) {
    records.remove(host);
  }
}
