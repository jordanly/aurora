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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** Additive schema changes, executed inside the database owner's initialization transaction. */
final class SqliteSchema {
  private SqliteSchema() { }

  static void migrate(Connection connection, long version) throws SQLException {
    if (version == 0) {
      execute(connection, "CREATE TABLE storage_owner (singleton INTEGER PRIMARY KEY"
          + " CHECK(singleton=1), epoch INTEGER NOT NULL, session_id TEXT NOT NULL)");
      execute(connection, "INSERT INTO storage_owner VALUES (1, 0, '')");
      execute(connection, "CREATE TABLE storage_transactions (operation_id TEXT PRIMARY KEY"
          + " NOT NULL, owner_epoch INTEGER NOT NULL)");
    }
    if (version < 2) {
      execute(connection, "CREATE TABLE scheduler_metadata (singleton INTEGER PRIMARY KEY"
          + " CHECK(singleton=1), framework_id TEXT NOT NULL)");
      for (String table : new String[] {
          "cron_jobs", "quotas", "attributes", "host_maintenance", "tasks", "job_updates"}) {
        execute(connection, "CREATE TABLE " + table + " (record_key TEXT PRIMARY KEY NOT NULL,"
            + " payload_version INTEGER NOT NULL, payload BLOB NOT NULL)");
      }
      execute(connection, "PRAGMA user_version=2");
    }
    if (version < 3) {
      execute(connection,
          "CREATE TABLE command_outbox (sequence INTEGER PRIMARY KEY AUTOINCREMENT,"
          + " command_id TEXT NOT NULL UNIQUE, agent_id TEXT NOT NULL, task_id TEXT NOT NULL,"
          + " command_type TEXT NOT NULL,"
          + " payload_version INTEGER NOT NULL CHECK(payload_version>0),"
          + " payload BLOB NOT NULL, owner_epoch INTEGER NOT NULL,"
          + " acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0,1)))");
      execute(
          connection,
          "CREATE INDEX pending_commands ON command_outbox(acknowledged,sequence)");
      execute(connection, "CREATE TABLE observation_receipts (agent_id TEXT NOT NULL,"
          + " incarnation TEXT NOT NULL, sequence INTEGER NOT NULL CHECK(sequence>=0),"
          + " payload_version INTEGER NOT NULL CHECK(payload_version>0), payload BLOB NOT NULL,"
          + " PRIMARY KEY(agent_id,incarnation,sequence))");
      execute(connection, "PRAGMA user_version=3");
    }
    if (version < 4) {
      execute(connection, "CREATE TABLE automatic_outcome (singleton INTEGER PRIMARY KEY"
          + " CHECK(singleton=1), operation_id TEXT NOT NULL, owner_epoch INTEGER NOT NULL)");
      execute(connection, "CREATE TABLE agent_retention (agent_id TEXT PRIMARY KEY,"
          + " scope TEXT NOT NULL,"
          + " enabled INTEGER NOT NULL DEFAULT 0, next_ticket INTEGER NOT NULL DEFAULT 1,"
          + " retired TEXT NOT NULL DEFAULT '[]')");
      execute(connection, "CREATE TABLE attempt_retention (agent_id TEXT NOT NULL,"
          + " ticket INTEGER NOT NULL, task_id TEXT NOT NULL UNIQUE,"
          + " complete INTEGER NOT NULL DEFAULT 0, retiring INTEGER NOT NULL DEFAULT 0,"
          + " PRIMARY KEY(agent_id,ticket))");
      execute(connection, "CREATE TABLE receipt_watermarks (agent_id TEXT NOT NULL,"
          + " incarnation TEXT NOT NULL, sequence INTEGER NOT NULL,"
          + " PRIMARY KEY(agent_id,incarnation))");
      execute(connection, "PRAGMA user_version=4");
    }
    // An additive query index keeps version-3 backups compatible and is installed when an
    // existing database is opened, as well as when the outbox is first created.
    execute(connection, "CREATE INDEX IF NOT EXISTS pending_commands_by_agent"
        + " ON command_outbox(acknowledged,agent_id,sequence)");
    execute(connection, "CREATE INDEX IF NOT EXISTS pending_stops_by_agent"
        + " ON command_outbox(agent_id,sequence)"
        + " WHERE acknowledged=0 AND command_type='Stop'");
  }

  private static void execute(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }
}
