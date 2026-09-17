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
 * Acknowledged commands remain immutable until a coordinated ordered-ticket retirement barrier.
 * Legacy command identities cannot be removed without the quiescent activation fence.
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

  /** Marks a known command acknowledged;
  false means it was already acknowledged. */
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

  /** Returns false only for an exact receipt replay;
  conflicting content fails the write. */
  public boolean recordReceipt(ReceiptKey key, int payloadVersion, byte[] payload) {
    database.requireWrite();
    requireNonNull(key);
    requireVersion(payloadVersion);
    byte[] contents = requireNonNull(payload).clone();
    try (var watermark = database.connection().prepareStatement(
        "SELECT sequence FROM receipt_watermarks WHERE agent_id=? AND incarnation=?")) {
      watermark.setString(1, key.agentId());
      watermark.setString(2, key.incarnation());
      try (var rows = watermark.executeQuery()) {
        if (rows.next() && key.sequence() <= rows.getLong(1)) {
          throw new StorageException("Receipt replay below durable retirement watermark");
        }
      }
    } catch (SQLException e) {
      throw database.failTransaction("Read receipt watermark", e);
    }
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
        "SELECT MAX(value) FROM (SELECT COALESCE(MAX(sequence),0) AS value"
            + " FROM observation_receipts WHERE agent_id=?1 AND incarnation=?2 UNION ALL"
            + " SELECT COALESCE(MAX(sequence),0) FROM receipt_watermarks"
            + " WHERE agent_id=?1 AND incarnation=?2)")) {
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

  public record Retention(boolean enabled, long nextTicket, String retired, String scope) { }
  public record Retiring(long ticket, String taskId) { }

  public java.util.Optional<Retention> retention(String agent) {
    try (var query = database.connection().prepareStatement(
        "SELECT enabled,next_ticket,retired,scope FROM agent_retention WHERE agent_id=?")) {
      query.setString(1, agent);
      try (var rows = query.executeQuery()) {
        return rows.next() ? java.util.Optional.of(new Retention(
            rows.getInt(1) == 1, rows.getLong(2), rows.getString(3), rows.getString(4)))
            : java.util.Optional.empty();
      }
    } catch (SQLException e) {
      throw database.failTransaction("Read retention", e);
    }
  }

  /** Durable launch fence spans the network activation barrier and restart. */
  public void beginRetention(String agent, String scope) {
    database.requireWrite();
    var prior = retention(agent);
    if (prior.isPresent()) {
      if (!prior.get().scope().equals(scope)) {
        throw new StorageException("Retention enrollment scope changed");
      }
      return;
    }
    try (var capacity = database.connection().prepareStatement(
        "SELECT COUNT(*) FROM agent_retention");
         var rows = capacity.executeQuery()) {
      if (rows.next() && rows.getInt(1) >= 128) {
        throw new StorageException("Retention enrollment capacity");
      }
    } catch (SQLException e) {
      throw database.failTransaction("Read enrollment capacity", e);
    }
    try (var query = database.connection().prepareStatement(
        "INSERT INTO agent_retention(agent_id,scope) VALUES (?,?)")) {
      query.setString(1, agent);
      query.setString(2, scope);
      query.executeUpdate();
    } catch (SQLException e) {
      throw database.failTransaction("Begin retention", e);
    }
  }

  public void finishLegacyRetention(String agent, String scope) {
    database.requireWrite();
    try (var commands = database.connection().prepareStatement(
        "DELETE FROM command_outbox WHERE agent_id=? AND acknowledged=1")) {
      commands.setString(1, agent);
      commands.executeUpdate();
      for (String table : List.of("observation_receipts", "receipt_watermarks")) {
        try (var query = database.connection().prepareStatement(
            "DELETE FROM " + table
                + " WHERE substr(agent_id,1,length(?)+1)=?||'/' AND incarnation=?")) {
          query.setString(1, agent);
          query.setString(2, agent);
          query.setString(3, scope);
          query.executeUpdate();
        }
      }
      pruneReceipts(agent, scope);
    } catch (SQLException e) {
      throw database.failTransaction("Retire legacy history", e);
    }
  }

  public void confirmRetention(String agent, String retired) {
    database.requireWrite();
    try (var query = database.connection().prepareStatement(
        "UPDATE agent_retention SET enabled=1,retired=? WHERE agent_id=?")) {
      query.setString(1, retired);
      query.setString(2, agent);
      if (query.executeUpdate() != 1) {
        throw new StorageException("Retention not prepared");
      }
    } catch (SQLException e) {
      throw database.failTransaction("Confirm retention", e);
    }
  }

  /** New launches stop while migration or the bounded retained window needs progress. */
  public boolean hasTicketCapacity(String agent) {
    var state = retention(agent);
    if (state.isPresent() && !state.get().enabled()) {
      return false;
    }
    try (var query = database.connection().prepareStatement(
        "SELECT COUNT(*) FROM attempt_retention WHERE agent_id=?")) {
      query.setString(1, agent);
      try (var rows = query.executeQuery()) {
        return rows.next() && rows.getInt(1) < 1024;
      }
    } catch (SQLException e) {
      throw database.failTransaction("Read ticket capacity", e);
    }
  }

  /** Allocation and immutable command insertion share the launch transaction. */
  public long allocateTicket(String agent, String taskId) {
    database.requireWrite();
    var state = retention(agent);
    if (state.isEmpty()) {
      return 0; // Legacy node before its quiescent barrier.
    }
    if (!state.get().enabled() || state.get().nextTicket() == Long.MAX_VALUE) {
      throw new StorageException("Retention barrier or ticket exhaustion");
    }
    try (var query = database.connection().prepareStatement(
        "SELECT COUNT(*) FROM attempt_retention WHERE agent_id=?")) {
      query.setString(1, agent);
      try (var rows = query.executeQuery()) {
        if (rows.next() && rows.getInt(1) >= 1024) {
          throw new StorageException("Agent retained attempt capacity");
        }
      }
      long ticket = state.get().nextTicket();
      try (var insert = database.connection().prepareStatement(
          "INSERT INTO attempt_retention(agent_id,ticket,task_id) VALUES (?,?,?)")) {
        insert.setString(1, agent);
        insert.setLong(2, ticket);
        insert.setString(3, taskId);
        insert.executeUpdate();
      }
      try (var update = database.connection().prepareStatement(
          "UPDATE agent_retention SET next_ticket=next_ticket+1 WHERE agent_id=?")) {
        update.setString(1, agent);
        update.executeUpdate();
      }
      return ticket;
    } catch (SQLException e) {
      throw database.failTransaction("Allocate attempt ticket", e);
    }
  }

  public void completeTicket(String agent, long ticket) {
    database.requireWrite();
    try (var query = database.connection().prepareStatement(
        "UPDATE attempt_retention SET complete=1 WHERE agent_id=? AND ticket=?")) {
      query.setString(1, agent);
      query.setLong(2, ticket);
      query.executeUpdate();
    } catch (SQLException e) {
      throw database.failTransaction("Complete attempt ticket", e);
    }
  }

  /** Freeze the oldest eligible completed tickets before sending any retirement request. */
  public List<Retiring> prepareRetirement(String agent, int keep) {
    database.requireWrite();
    if (keep < 0 || keep > 896) {
      throw new IllegalArgumentException("retained completed bound");
    }
    try (var query = database.connection().prepareStatement(
        "UPDATE attempt_retention SET retiring=1 WHERE agent_id=? AND ticket IN ("
            + "SELECT ticket FROM attempt_retention a WHERE a.agent_id=? AND complete=1"
            + " AND NOT EXISTS (SELECT 1 FROM command_outbox c WHERE c.task_id=a.task_id"
            + " AND acknowledged=0) ORDER BY ticket DESC LIMIT -1 OFFSET ?)")) {
      query.setString(1, agent);
      query.setString(2, agent);
      query.setInt(3, keep);
      query.executeUpdate();
      try (var select = database.connection().prepareStatement(
          "SELECT ticket,task_id FROM attempt_retention WHERE agent_id=? AND retiring=1"
              + " ORDER BY ticket LIMIT 128")) {
        select.setString(1, agent);
        List<Retiring> result = new ArrayList<>();
        try (var rows = select.executeQuery()) {
          while (rows.next()) {
            result.add(new Retiring(rows.getLong(1), rows.getString(2)));
          }
        }
        return List.copyOf(result);
      }
    } catch (SQLException e) {
      throw database.failTransaction("Prepare retirement", e);
    }
  }

  public boolean retiringTask(String task) {
    try (var query = database.connection().prepareStatement(
        "SELECT 1 FROM attempt_retention WHERE task_id=? AND retiring=1")) {
      query.setString(1, task);
      try (var rows = query.executeQuery()) {
        return rows.next();
      }
    } catch (SQLException e) {
      throw database.failTransaction("Read retirement fence", e);
    }
  }

  /** Called only after the agent has durably fenced replay and removed its artifacts. */
  public void finishRetirement(String agent, String scope, Retiring attempt, String attemptId) {
    database.requireWrite();
    try {
      for (String table : List.of("observation_receipts", "receipt_watermarks")) {
        try (var query = database.connection().prepareStatement(
            "DELETE FROM " + table + " WHERE agent_id=? AND incarnation=?")) {
          query.setString(1, agent + "/" + attemptId);
          query.setString(2, scope);
          query.executeUpdate();
        }
      }
      try (var query = database.connection().prepareStatement(
          "DELETE FROM command_outbox WHERE task_id=? AND acknowledged=1")) {
        query.setString(1, attempt.taskId());
        query.executeUpdate();
      }
      try (var query = database.connection().prepareStatement(
          "DELETE FROM attempt_retention WHERE agent_id=? AND ticket=? AND retiring=1")) {
        query.setString(1, agent);
        query.setLong(2, attempt.ticket());
        query.executeUpdate();
      }
    } catch (SQLException e) {
      throw database.failTransaction("Finish retirement", e);
    }
  }

  /** Retain a scalar receipt fence while dropping bulky committed receipt payloads. */
  public void pruneReceipts(String agent, String scope) {
    database.requireWrite();
    long cursor = committedCursor(agent, scope);
    try (var query = database.connection().prepareStatement(
        "INSERT INTO receipt_watermarks(agent_id,incarnation,sequence) VALUES (?,?,?)"
            + " ON CONFLICT(agent_id,incarnation) DO UPDATE SET"
            + " sequence=MAX(sequence,excluded.sequence)")) {
      query.setString(1, agent);
      query.setString(2, scope);
      query.setLong(3, cursor);
      query.executeUpdate();
      try (var prune = database.connection().prepareStatement(
          "DELETE FROM observation_receipts WHERE agent_id=? AND incarnation=? AND sequence<=?")) {
        prune.setString(1, agent);
        prune.setString(2, scope);
        prune.setLong(3, cursor);
        prune.executeUpdate();
      }
    } catch (SQLException e) {
      throw database.failTransaction("Prune receipts", e);
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
