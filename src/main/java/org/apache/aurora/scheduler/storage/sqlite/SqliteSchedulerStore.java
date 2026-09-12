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

import java.sql.SQLException;
import java.util.Optional;

import org.apache.aurora.scheduler.storage.SchedulerStore;

import static java.util.Objects.requireNonNull;

/** SQLite-backed scheduler metadata. */
final class SqliteSchedulerStore implements SchedulerStore.Mutable {
  private final SqliteDatabase db;

  SqliteSchedulerStore(SqliteDatabase db) {
    this.db = requireNonNull(db);
  }

  @Override
  public Optional<String> fetchFrameworkId() {
    try {
      try (var statement = db.connection().prepareStatement(
          "SELECT framework_id FROM scheduler_metadata WHERE singleton=1")) {
        try (var result = statement.executeQuery()) {
          return result.next() ? Optional.of(result.getString(1)) : Optional.empty();
        }
      }
    } catch (SQLException e) {
      throw db.failTransaction("Unable to read scheduler metadata", e);
    }
  }

  @Override
  public void saveFrameworkId(String frameworkId) {
    requireNonNull(frameworkId);
    try {
      db.requireWrite();
      try (var statement = db.connection().prepareStatement(
          "INSERT INTO scheduler_metadata(singleton, framework_id) VALUES (1, ?) "
              + "ON CONFLICT(singleton) DO UPDATE SET framework_id=excluded.framework_id")) {
        statement.setString(1, frameworkId);
        statement.executeUpdate();
      }
    } catch (SQLException e) {
      throw db.failTransaction("Unable to save scheduler metadata", e);
    }
  }
}
