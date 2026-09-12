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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import static java.util.Objects.requireNonNull;

/** Versioned Thrift records read and written only through the active SQLite transaction. */
final class SqliteRecords<T extends TBase<?, ?>> {
  enum Table {
    CRON_JOBS("cron_jobs"), QUOTAS("quotas"), ATTRIBUTES("attributes"),
    HOST_MAINTENANCE("host_maintenance"), TASKS("tasks"), JOB_UPDATES("job_updates");

    private final String sqlName;

    Table(String sqlName) {
      this.sqlName = sqlName;
    }
  }

  private static final int PAYLOAD_VERSION = 1;
  private final SqliteDatabase database;
  private final String table;
  private final Supplier<T> factory;

  SqliteRecords(SqliteDatabase database, Table table, Supplier<T> factory) {
    this.database = requireNonNull(database);
    this.table = requireNonNull(table).sqlName;
    this.factory = requireNonNull(factory);
  }

  static String key(String... components) {
    StringBuilder result = new StringBuilder();
    for (String component : components) {
      requireNonNull(component);
      result.append(component.length()).append(':').append(component);
    }
    return result.toString();
  }

  Optional<T> get(String key) {
    requireNonNull(key);
    try (PreparedStatement query = database.connection().prepareStatement(
        "SELECT payload_version,payload FROM " + table + " WHERE record_key=?")) {
      query.setString(1, key);
      try (ResultSet rows = query.executeQuery()) {
        return rows.next() ? Optional.of(decode(rows)) : Optional.empty();
      }
    } catch (SQLException e) {
      throw failure("read", e);
    }
  }

  Map<String, T> all() {
    try (PreparedStatement query = database.connection().prepareStatement(
        "SELECT payload_version,payload,record_key FROM " + table + " ORDER BY record_key");
         ResultSet rows = query.executeQuery()) {
      Map<String, T> result = new LinkedHashMap<>();
      while (rows.next()) {
        result.put(rows.getString(3), decode(rows));
      }
      return Collections.unmodifiableMap(result);
    } catch (SQLException e) {
      throw failure("read", e);
    }
  }

  void put(String key, T value) {
    database.requireWrite();
    requireNonNull(key);
    requireNonNull(value);
    try (PreparedStatement update = database.connection().prepareStatement(
        "INSERT INTO " + table + "(record_key,payload_version,payload) VALUES (?,?,?)"
            + " ON CONFLICT(record_key) DO UPDATE SET payload_version=excluded.payload_version,"
            + "payload=excluded.payload")) {
      update.setString(1, key);
      update.setInt(2, PAYLOAD_VERSION);
      // Thrift codecs hold mutable transports; each operation gets its own instance.
      update.setBytes(3, new TSerializer(new TBinaryProtocol.Factory()).serialize(value));
      update.executeUpdate();
    } catch (SQLException | TException e) {
      throw failure("write", e);
    }
  }

  void remove(String key) {
    database.requireWrite();
    requireNonNull(key);
    try (PreparedStatement delete = database.connection().prepareStatement(
        "DELETE FROM " + table + " WHERE record_key=?")) {
      delete.setString(1, key);
      delete.executeUpdate();
    } catch (SQLException e) {
      throw failure("delete", e);
    }
  }

  void clear() {
    database.requireWrite();
    try (PreparedStatement delete = database.connection()
        .prepareStatement("DELETE FROM " + table)) {
      delete.executeUpdate();
    } catch (SQLException e) {
      throw failure("delete", e);
    }
  }

  private T decode(ResultSet rows) throws SQLException {
    if (rows.getInt(1) != PAYLOAD_VERSION) {
      throw new StorageException("Unsupported " + table + " payload version: " + rows.getInt(1));
    }
    T value = factory.get();
    try {
      new TDeserializer(new TBinaryProtocol.Factory()).deserialize(value, rows.getBytes(2));
    } catch (TException e) {
      throw failure("decode", e);
    }
    return value;
  }

  private StorageException failure(String action, Exception cause) {
    return database.failTransaction("Unable to " + action + " SQLite " + table + " record", cause);
  }
}
