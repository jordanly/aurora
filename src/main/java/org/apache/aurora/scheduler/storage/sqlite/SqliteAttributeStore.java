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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;

import static java.util.Objects.requireNonNull;

/** SQLite-backed host attributes. */
final class SqliteAttributeStore implements AttributeStore.Mutable {
  private final SqliteRecords<HostAttributes> records;

  SqliteAttributeStore(SqliteDatabase db) {
    records = new SqliteRecords<>(requireNonNull(db), SqliteRecords.Table.ATTRIBUTES,
        HostAttributes::new);
  }

  @Override
  public void deleteHostAttributes() {
    records.clear();
  }

  @Override
  public boolean saveHostAttributes(IHostAttributes input) {
    Preconditions.checkArgument(
        input.getAttributes().stream().allMatch(attribute -> !attribute.getValues().isEmpty()));
    Preconditions.checkArgument(input.isSetMode());
    String host = input.getHost();
    Optional<IHostAttributes> previous = getHostAttributes(host);
    IHostAttributes merged = merge(input, previous);
    records.put(host, merged.newBuilder());
    // Match MemAttributeStore: the changed result compares the caller's value with the
    // previously stored value, preserving the distinction between unset and defaulted fields.
    return !input.equals(previous.orElse(null));
  }

  private IHostAttributes merge(IHostAttributes input, Optional<IHostAttributes> previous) {
    HostAttributes attributes = input.newBuilder();
    if (!attributes.isSetMode()) {
      MaintenanceMode mode = previous.filter(IHostAttributes::isSetMode)
          .map(IHostAttributes::getMode).orElse(MaintenanceMode.NONE);
      attributes.setMode(mode);
    }
    if (!attributes.isSetAttributes()) {
      attributes.setAttributes(ImmutableSet.<Attribute>of());
    }
    return IHostAttributes.build(attributes);
  }

  @Override
  public Optional<IHostAttributes> getHostAttributes(String host) {
    return records.get(host).map(IHostAttributes::build);
  }

  @Override
  public Set<IHostAttributes> getHostAttributes() {
    return records.all().values().stream()
        .map(IHostAttributes::build)
        .collect(Collectors.toUnmodifiableSet());
  }
}
