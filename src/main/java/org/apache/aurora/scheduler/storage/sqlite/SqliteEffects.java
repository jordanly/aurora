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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.aurora.scheduler.storage.Storage.StorageException;

import static java.util.Objects.requireNonNull;

/**
 * Durable command intentions and observation deduplication, sharing the enclosing store
 * transaction.
 * This class does not dispatch commands. Positive payload versions are opaque protocol metadata;
 * future consumers must reject unsupported versions before interpreting or executing a payload.
 * Acknowledged commands are retained so their identities cannot silently be reused.
 */
public final class SqliteEffects {
  public record Command(
      String id,
      String agentId,
      String taskId,
      String type,
      int payloadVersion,
      byte[] payload) {

    public Command {
      requireIdentifier(id);
      requireIdentifier(agentId);
      requireIdentifier(taskId);
      requireIdentifier(type);
      requireVersion(payloadVersion);
      payload = requireNonNull(payload).clone();
    }

    @Override
    public byte[] payload() {
      return payload.clone();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Command command
          && id.equals(command.id) && agentId.equals(command.agentId)
          && taskId.equals(command.taskId) && type.equals(command.type)
          && payloadVersion == command.payloadVersion && Arrays.equals(payload, command.payload);
    }

    @Override
    public int hashCode() {
      return 31 * Objects.hash(id, agentId, taskId, type, payloadVersion)
          + Arrays.hashCode(payload);
    }
  }

  public record PendingCommand(long sequence, long ownerEpoch, Command command) {
    public PendingCommand {
      if (sequence <= 0 || ownerEpoch <= 0) {
        throw new IllegalArgumentException("Command sequence and owner epoch must be positive");
      }
      requireNonNull(command);
    }
  }

  public record ReceiptKey(String agentId, String incarnation, long sequence) {
    public ReceiptKey {
      requireIdentifier(agentId);
      requireIdentifier(incarnation);
      if (sequence < 0) {
        throw new IllegalArgumentException("Observation sequence must not be negative");
      }
    }
  }

  private final SqliteDatabase database;

  SqliteEffects(SqliteDatabase database) {
    this.database = requireNonNull(database);
  }

  /** Returns false for an exact existing command, including an acknowledged command. */
  public boolean enqueue(Command command) {
    database.requireWrite();
    requireNonNull(command);
    try (PreparedStatement existing = database.connection().prepareStatement(
        "SELECT command_id,agent_id,task_id,command_type,payload_version,payload"
            + " FROM command_outbox WHERE command_id=?")) {
      existing.setString(1, command.id());
      try (ResultSet rows = existing.executeQuery()) {
        if (rows.next()) {
          if (!command.equals(readCommand(rows))) {
            throw new StorageException("Conflicting command body for ID: " + command.id());
          }
          return false;
        }
      }
      try (PreparedStatement insert = database.connection().prepareStatement(
          "INSERT INTO command_outbox(command_id,agent_id,task_id,command_type,payload_version,"
              + "payload,owner_epoch) VALUES (?,?,?,?,?,?,?)")) {
        insert.setString(1, command.id());
        insert.setString(2, command.agentId());
        insert.setString(3, command.taskId());
        insert.setString(4, command.type());
        insert.setInt(5, command.payloadVersion());
        insert.setBytes(6, command.payload());
        insert.setLong(7, database.currentOwnerEpoch());
        insert.executeUpdate();
        return true;
      }
    } catch (SQLException e) {
      throw database.failTransaction("Unable to persist command", e);
    }
  }

  /**
   * Returns at most limit unacknowledged commands in insertion order, including the current
   * transaction's writes.
   */
  public List<PendingCommand> pending(int limit) {
    return pending(null, limit, false);
  }

  /**
   * Returns at most limit unacknowledged commands for agentId in insertion order, including the
   * current transaction's writes.
   */
  public List<PendingCommand> pending(String agentId, int limit) {
    requireIdentifier(agentId);
    return pending(agentId, limit, false);
  }

  /** Returns pending Stops for one agent in FIFO order, independently of blocked Runs. */
  public List<PendingCommand> pendingStops(String agentId, int limit) {
    requireIdentifier(agentId);
    return pending(agentId, limit, true);
  }

  private List<PendingCommand> pending(String agentId, int limit, boolean stopsOnly) {
    database.connection();
    if (limit <= 0) {
      throw new IllegalArgumentException("Pending command limit must be positive");
    }
    try (PreparedStatement query = database.connection().prepareStatement(
        "SELECT command_id,agent_id,task_id,command_type,payload_version,payload,sequence,"
            + "owner_epoch"
            + " FROM command_outbox WHERE acknowledged=0"
            + (agentId != null ? " AND agent_id=?" : "")
            + (stopsOnly ? " AND command_type='Stop'" : "")
            + " ORDER BY sequence LIMIT ?")) {
      if (agentId != null) {
        query.setString(1, agentId);
      }
      query.setInt(agentId != null ? 2 : 1, limit);
      List<PendingCommand> pending = new ArrayList<>();
      try (ResultSet rows = query.executeQuery()) {
        while (rows.next()) {
          pending.add(new PendingCommand(rows.getLong(7), rows.getLong(8), readCommand(rows)));
        }
      }
      return List.copyOf(pending);
    } catch (SQLException e) {
      throw database.failTransaction("Unable to read pending commands", e);
    }
  }

  /** Whether this command still awaits a receipt, independent of queue position. */
  public boolean isPending(String commandId) {
    requireIdentifier(commandId);
    try (PreparedStatement query = database.connection().prepareStatement(
        "SELECT 1 FROM command_outbox WHERE command_id=? AND acknowledged=0")) {
      query.setString(1, commandId);
      try (ResultSet rows = query.executeQuery()) {
        return rows.next();
      }
    } catch (SQLException e) {
      throw database.failTransaction("Unable to read command receipt status", e);
    }
  }

  /** Marks a known command acknowledged; false means it was already acknowledged. */
  public boolean acknowledge(String commandId) {
    database.requireWrite();
    requireIdentifier(commandId);
    try (PreparedStatement update = database.connection().prepareStatement(
        "UPDATE command_outbox SET acknowledged=1 WHERE command_id=? AND acknowledged=0")) {
      update.setString(1, commandId);
      if (update.executeUpdate() == 1) {
        return true;
      }
      try (PreparedStatement query = database.connection().prepareStatement(
          "SELECT 1 FROM command_outbox WHERE command_id=?")) {
        query.setString(1, commandId);
        try (ResultSet rows = query.executeQuery()) {
          if (!rows.next()) {
            throw new StorageException("Unknown command ID: " + commandId);
          }
        }
      }
      return false;
    } catch (SQLException e) {
      throw database.failTransaction("Unable to acknowledge command", e);
    }
  }

  /** Returns false only for an exact receipt replay; conflicting content fails the write. */
  public boolean recordReceipt(ReceiptKey key, int payloadVersion, byte[] payload) {
    database.requireWrite();
    requireNonNull(key);
    requireVersion(payloadVersion);
    byte[] contents = requireNonNull(payload).clone();
    try (PreparedStatement query = database.connection().prepareStatement(
        "SELECT payload_version,payload FROM observation_receipts"
            + " WHERE agent_id=? AND incarnation=? AND sequence=?")) {
      bindKey(query, key);
      try (ResultSet rows = query.executeQuery()) {
        if (rows.next()) {
          if (rows.getInt(1) != payloadVersion || !Arrays.equals(rows.getBytes(2), contents)) {
            throw new StorageException("Conflicting observation receipt: " + key);
          }
          return false;
        }
      }
      try (PreparedStatement insert = database.connection().prepareStatement(
          "INSERT INTO observation_receipts(agent_id,incarnation,sequence,payload_version,payload)"
              + " VALUES (?,?,?,?,?)")) {
        bindKey(insert, key);
        insert.setInt(4, payloadVersion);
        insert.setBytes(5, contents);
        insert.executeUpdate();
        return true;
      }
    } catch (SQLException e) {
      throw database.failTransaction("Unable to persist observation receipt", e);
    }
  }

  /** Finds an immutable command, including retained acknowledged commands. */
  public java.util.Optional<Command> command(String commandId) {
    requireIdentifier(commandId);
    try (PreparedStatement query = database.connection().prepareStatement(
        "SELECT command_id,agent_id,task_id,command_type,payload_version,payload"
            + " FROM command_outbox WHERE command_id=?")) {
      query.setString(1, commandId);
      try (ResultSet rows = query.executeQuery()) {
        return rows.next() ? java.util.Optional.of(readCommand(rows)) : java.util.Optional.empty();
      }
    } catch (SQLException e) {
      throw database.failTransaction("Unable to read command", e);
    }
  }

  /** Highest durably processed cursor for an enrolled agent journal. */
  public long committedCursor(String agentId, String incarnation) {
    requireIdentifier(agentId);
    requireIdentifier(incarnation);
    try (PreparedStatement query = database.connection().prepareStatement(
        "SELECT COALESCE(MAX(sequence),0) FROM observation_receipts"
            + " WHERE agent_id=? AND incarnation=?")) {
      query.setString(1, agentId);
      query.setString(2, incarnation);
      try (ResultSet rows = query.executeQuery()) {
        return rows.next() ? rows.getLong(1) : 0;
      }
    } catch (SQLException e) {
      throw database.failTransaction("Unable to read observation cursor", e);
    }
  }

  public boolean hasReceipt(ReceiptKey key) {
    requireNonNull(key);
    try (PreparedStatement query = database.connection().prepareStatement(
        "SELECT 1 FROM observation_receipts WHERE agent_id=? AND incarnation=? AND sequence=?")) {
      bindKey(query, key);
      try (ResultSet rows = query.executeQuery()) {
        return rows.next();
      }
    } catch (SQLException e) {
      throw database.failTransaction("Unable to read observation receipt", e);
    }
  }

  private static void bindKey(PreparedStatement statement, ReceiptKey key) throws SQLException {
    statement.setString(1, key.agentId());
    statement.setString(2, key.incarnation());
    statement.setLong(3, key.sequence());
  }

  private static Command readCommand(ResultSet rows) throws SQLException {
    return new Command(rows.getString(1), rows.getString(2), rows.getString(3),
        rows.getString(4), rows.getInt(5), rows.getBytes(6));
  }

  private static void requireIdentifier(String value) {
    if (requireNonNull(value).isEmpty()) {
      throw new IllegalArgumentException("An identifier is required");
    }
  }

  private static void requireVersion(int version) {
    if (version <= 0) {
      throw new IllegalArgumentException("Payload version must be positive");
    }
  }
}
