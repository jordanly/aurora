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
package org.apache.aurora.scheduler.storage.mem;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.stats.StatsProvider;
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

public class MemJobUpdateStore implements JobUpdateStore.Mutable {
  @VisibleForTesting
  static final String UPDATE_STORE_SIZE = "mem_storage_update_size";

  private final Map<IJobUpdateKey, IJobUpdateDetails> updates = Maps.newConcurrentMap();

  @Inject
  MemJobUpdateStore(StatsProvider statsProvider) {
    statsProvider.makeGauge(UPDATE_STORE_SIZE, updates::size);
  }

  @Timed("job_update_store_fetch_details_query")
  @Override
  public synchronized List<IJobUpdateDetails> fetchJobUpdates(IJobUpdateQuery query) {
    return JobUpdateStoreSupport.query(updates.values().stream(), query);
  }

  @Timed("job_update_store_fetch_details")
  @Override
  public synchronized Optional<IJobUpdateDetails> fetchJobUpdate(IJobUpdateKey key) {
    return Optional.ofNullable(updates.get(key));
  }

  @Timed("job_update_store_save_update")
  @Override
  public synchronized void saveJobUpdate(IJobUpdate update) {
    IJobUpdateDetails details = JobUpdateStoreSupport.create(update);
    updates.put(update.getSummary().getKey(), details);
  }

  @Timed("job_update_store_save_event")
  @Override
  public synchronized void saveJobUpdateEvent(IJobUpdateKey key, IJobUpdateEvent event) {
    updates.put(key, JobUpdateStoreSupport.appendUpdateEvent(existing(key), event));
  }

  @Timed("job_update_store_save_instance_event")
  @Override
  public synchronized void saveJobInstanceUpdateEvent(
      IJobUpdateKey key,
      IJobInstanceUpdateEvent event) {

    updates.put(key, JobUpdateStoreSupport.appendInstanceEvent(existing(key), event));
  }

  @Timed("job_update_store_delete_updates")
  @Override
  public synchronized void removeJobUpdates(Set<IJobUpdateKey> key) {
    requireNonNull(key);
    updates.keySet().removeAll(key);
  }

  @Timed("job_update_store_delete_all")
  @Override
  public synchronized void deleteAllUpdates() {
    updates.clear();
  }

  private IJobUpdateDetails existing(IJobUpdateKey key) {
    IJobUpdateDetails update = updates.get(key);
    if (update == null) {
      throw new StorageException("Update not found: " + key);
    }
    return update;
  }
}
