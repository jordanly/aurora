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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.aurora.scheduler.storage.Storage.StorageException;

import static java.util.Objects.requireNonNull;

/**
 * Transaction foundation for the seven SQLite stores, not a scheduler storage implementation.
 * JDBC access stays package-local and must not escape callbacks or change transaction control.
 * The database must be a local file with no hard-link aliases and a stable ownership-lock path.
 */
final class SqliteDatabase implements AutoCloseable {
  static final int SCHEMA_VERSION = 3;
  private static final int BUSY_TIMEOUT_MS = 1000;
  private static final Set<Path> OWNED_PATHS = ConcurrentHashMap.newKeySet();

  @FunctionalInterface
  interface Work<T, E extends Exception> {
    T apply() throws E;
  }

  @FunctionalInterface
  interface ConnectionFactory {
    Connection open(String url) throws SQLException;
  }

  static final class CommitUncertainException extends StorageException {
    private final String operationId;

    CommitUncertainException(String operationId, Throwable cause) {
      super("Commit outcome requires reconciliation for operation " + operationId, cause);
      this.operationId = operationId;
    }

    String getOperationId() {
      return operationId;
    }
  }

  static final class AlreadyCommittedException extends StorageException {
    AlreadyCommittedException(String operationId) {
      super("Operation already committed; resolve its result from storage: " + operationId);
    }
  }

  private static final class Transaction {
    private final Connection connection;
    private final boolean writable;
    private final String operationId;
    private Throwable rollbackCause;
    private final List<Runnable> afterCommit = new ArrayList<>();

    Transaction(Connection connection, boolean writable, String operationId) {
      this.connection = connection;
      this.writable = writable;
      this.operationId = operationId;
    }
  }

  private final String url;
  private final Path path;
  private final ConnectionFactory connections;
  private final FileChannel lockChannel;
  private final FileLock ownershipLock;
  private final String sessionId = UUID.randomUUID().toString();
  private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock(true);
  private final ReentrantLock writer = new ReentrantLock(true);
  private final ThreadLocal<Transaction> current = new ThreadLocal<>();
  private final List<Connection> unclosedConnections = new ArrayList<>();
  private long epoch;
  private boolean closed;
  private boolean closing;
  private volatile String uncertainOperation;
  private List<Runnable> uncertainCallbacks = List.of();
  private final Deque<Publication> publications = new ArrayDeque<>();
  private boolean publishing;
  private volatile boolean failClosedOnWriteFailureEnabled;
  private Throwable writeFailure;
  private SqliteStorage.PostCommitException publicationFailure;

  private record Publication(String operationId, Runnable action) { }

  void failClosedOnWriteFailure() {
    failClosedOnWriteFailureEnabled = true;
  }

  /** Queues volatile notifications; durable external commands belong in SqliteEffects. */
  void afterCommit(Runnable action) {
    requireNonNull(action);
    if (current.get() == null) {
      action.run();
    } else {
      requireWrite();
      current.get().afterCommit.add(action);
    }
  }

  private void enqueue(String operationId, List<Runnable> callbacks) {
    callbacks.forEach(action -> publications.addLast(new Publication(operationId, action)));
  }

  private void publishCommitted() {
    if (publishing) {
      return;
    }
    publishing = true;
    try {
      while (!publications.isEmpty()) {
        Publication next = publications.removeFirst();
        try {
          next.action().run();
        } catch (RuntimeException | Error failure) {
          var failedPublication =
              new SqliteStorage.PostCommitException(next.operationId(), failure);
          publicationFailure = failedPublication;
          throw failedPublication;
        }
      }
    } finally {
      publishing = false;
    }
  }

  static SqliteDatabase open(Path path) {
    return open(path, DriverManager::getConnection);
  }

  static SqliteDatabase open(Path path, ConnectionFactory connections) {
    requireNonNull(path);
    requireNonNull(connections);
    FileChannel channel = null;
    boolean transferred = false;
    Path registeredPath = null;
    Throwable primary = null;
    try {
      Path absolute = path.toAbsolutePath().normalize();
      if (Files.isSymbolicLink(absolute) && !Files.exists(absolute)) {
        throw new StorageException(
            "SQLite database must not be a dangling symbolic link: " + absolute);
      }
      Path parent = absolute.getParent();
      if (parent == null) {
        throw new StorageException("SQLite database path must name a file: " + absolute);
      }
      Files.createDirectories(parent);
      Path canonical = Files.exists(absolute) ? absolute.toRealPath()
          : parent.toRealPath().resolve(absolute.getFileName());
      if (Files.exists(canonical)
          && ((Number) Files.getAttribute(canonical, "unix:nlink")).longValue() != 1) {
        throw new StorageException("SQLite database must not have hard-link aliases: " + canonical);
      }
      // Do not open and close another channel to an already owned lock file: some
      // platforms release process-level file locks when any such descriptor closes.
      if (!OWNED_PATHS.add(canonical)) {
        throw new StorageException("SQLite storage already has an owner: " + canonical);
      }
      registeredPath = canonical;
      channel = FileChannel.open(canonical.resolveSibling(canonical.getFileName() + ".owner"),
          StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
      FileLock lock = channel.tryLock();
      if (lock == null) {
        throw new StorageException("SQLite storage already has an owner: " + canonical);
      }
      SqliteDatabase database = new SqliteDatabase(canonical, connections, channel, lock);
      transferred = true;
      try {
        database.initialize();
        return database;
      } catch (RuntimeException | Error failure) {
        try {
          database.close();
        } catch (RuntimeException | Error cleanup) {
          suppress(failure, cleanup);
        }
        throw failure;
      }
    } catch (IOException | OverlappingFileLockException e) {
      primary = e;
      throw new StorageException("Unable to acquire SQLite storage ownership", e);
    } catch (RuntimeException | Error e) {
      primary = e;
      throw e;
    } finally {
      if (!transferred && channel != null) {
        try {
          channel.close();
        } catch (IOException cleanup) {
          if (primary != null) {
            suppress(primary, cleanup);
          } else {
            throw new StorageException("Unable to close ownership lock channel", cleanup);
          }
        }
      }
      if (!transferred && registeredPath != null && (channel == null || !channel.isOpen())) {
        OWNED_PATHS.remove(registeredPath);
      }
    }
  }

  private SqliteDatabase(
      Path path, ConnectionFactory connections, FileChannel channel, FileLock lock) {
    this.path = path;
    // A raw '?' in a JDBC filename introduces driver options and can alias a different file.
    this.url = "jdbc:sqlite:" + path.toUri().toASCIIString();
    this.connections = connections;
    this.lockChannel = channel;
    this.ownershipLock = lock;
  }

  private Connection openConnection() throws SQLException {
    Connection connection = connections.open(url);
    try {
      execute(connection, "PRAGMA busy_timeout=" + BUSY_TIMEOUT_MS);
      execute(connection, "PRAGMA synchronous=FULL");
      execute(connection, "PRAGMA foreign_keys=ON");
      if (scalar(connection, "PRAGMA synchronous") != 2
          || scalar(connection, "PRAGMA foreign_keys") != 1) {
        throw new SQLException("Required SQLite connection settings were not applied");
      }
      return connection;
    } catch (SQLException | RuntimeException | Error failure) {
      try {
        connection.close();
      } catch (SQLException | RuntimeException | Error cleanup) {
        suppress(failure, cleanup);
        synchronized (unclosedConnections) {
          unclosedConnections.add(connection);
        }
      }
      throw failure;
    }
  }

  private void initialize() {
    Connection connection = null;
    Throwable primary = null;
    try {
      connection = openConnection();
      long version = scalar(connection, "PRAGMA user_version");
      if (version < 0 || version > SCHEMA_VERSION) {
        throw new StorageException("Unsupported SQLite schema version: " + version);
      }
      if (version == 0 && scalar(connection,
          "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'") != 0) {
        throw new StorageException("Refusing to initialize an unversioned nonempty database");
      }
      try (Statement statement = connection.createStatement();
           ResultSet result = statement.executeQuery("PRAGMA journal_mode=WAL")) {
        if (!result.next() || !"wal".equalsIgnoreCase(result.getString(1))) {
          throw new StorageException("SQLite WAL mode is required");
        }
      }
      execute(connection, "BEGIN IMMEDIATE");
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
      try (PreparedStatement update = connection.prepareStatement(
          "UPDATE storage_owner SET epoch=epoch+1, session_id=? WHERE singleton=1")) {
        update.setString(1, sessionId);
        if (update.executeUpdate() != 1) {
          throw new StorageException("Missing SQLite ownership metadata");
        }
      }
      epoch = scalar(connection, "SELECT epoch FROM storage_owner WHERE singleton=1");
      execute(connection, "COMMIT");
    } catch (SQLException e) {
      primary = e;
      throw new StorageException("Failed to initialize SQLite storage", e);
    } catch (RuntimeException | Error e) {
      primary = e;
      throw e;
    } finally {
      if (connection != null) {
        if (primary != null) {
          try {
            execute(connection, "ROLLBACK");
          } catch (SQLException | RuntimeException | Error cleanup) {
            suppress(primary, cleanup);
          }
        }
        closeConnection(connection, primary, null);
      }
    }
  }

  <T, E extends Exception> T read(Work<T, E> work) throws E {
    return transact(false, null, work);
  }

  /**
   * Commits one caller-identified operation. Nested writes reuse the outer ID and transaction.
   * A repeated committed ID is rejected without running work; resolve its result from durable
   * state. Callbacks are never retried automatically, including after an uncertain commit.
   */
  <T, E extends Exception> T write(String operationId, Work<T, E> work) throws E {
    return transact(true, operationId, work);
  }

  Connection connection() {
    Transaction transaction = current.get();
    if (transaction == null) {
      throw new IllegalStateException("SQLite access requires an active transaction");
    }
    if (transaction.rollbackCause != null) {
      throw new StorageException("SQLite transaction is rollback-only", transaction.rollbackCause);
    }
    return transaction.connection;
  }

  StorageException failTransaction(String message, Exception failure) {
    Transaction transaction = current.get();
    if (failure instanceof SQLException && transaction != null
        && transaction.rollbackCause == null) {
      // SQLite may already have rolled back after FULL, IOERR, INTERRUPT or NOMEM.
      // Prevent further statements from silently committing in autocommit mode.
      transaction.rollbackCause = failure;
    }
    return new StorageException(message, failure);
  }

  long currentOwnerEpoch() {
    connection();
    return epoch;
  }

  String currentOperationId() {
    Transaction transaction = current.get();
    return transaction == null ? null : transaction.operationId;
  }

  void requireWrite() {
    connection();
    if (current.get() == null || !current.get().writable) {
      throw new IllegalStateException("SQLite mutation requires a write transaction");
    }
  }

  /** Queries on a fresh connection; an absent outcome permits an explicit caller retry. */
  boolean isCommitted(String operationId) {
    requireNonNull(operationId);
    if (current.get() != null) {
      throw new IllegalStateException("Reconciliation requires a fresh transaction");
    }
    lifecycle.readLock().lock();
    writer.lock();
    try {
      boolean committed = read(() -> hasOutcome(connection(), operationId));
      if (operationId.equals(uncertainOperation)) {
        uncertainOperation = null;
        if (committed) {
          enqueue(operationId, uncertainCallbacks);
        }
        uncertainCallbacks = List.of();
        publishCommitted();
      }
      return committed;
    } catch (SQLException e) {
      throw new StorageException("Unable to reconcile SQLite commit", e);
    } finally {
      writer.unlock();
      lifecycle.readLock().unlock();
    }
  }

  /**
   * Writes a consistent SQLite snapshot to a new local file, without copying live WAL files.
   * The destination's parent must exist. Publication never intentionally replaces a destination;
   * backup and restore paths must be managed by the same trusted local administrator as storage.
   */
  void backup(Path destination) {
    requireNonNull(destination);
    if (current.get() != null) {
      throw new IllegalStateException("Backup cannot run inside a transaction");
    }
    lifecycle.readLock().lock();
    writer.lock();
    Path temporaryDirectory = null;
    Throwable primary = null;
    try {
      checkOpen();
      if (uncertainOperation != null) {
        throw new StorageException("Reconcile uncertain operation before backup: "
            + uncertainOperation);
      }
      Path requested = destination.toAbsolutePath().normalize();
      Path requestedParent = requested.getParent();
      if (requestedParent == null) {
        throw new StorageException("SQLite backup path must name a file: " + requested);
      }
      Path targetParent = requestedParent.toRealPath();
      Path target = targetParent.resolve(requested.getFileName());
      if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)
          || target.equals(path)
          || target.equals(path.resolveSibling(path.getFileName() + "-wal"))
          || target.equals(path.resolveSibling(path.getFileName() + "-shm"))
          || target.equals(path.resolveSibling(path.getFileName() + "-journal"))
          || target.equals(path.resolveSibling(path.getFileName() + ".owner"))) {
        throw new StorageException("Backup destination must be a new independent file: " + target);
      }
      temporaryDirectory = Files.createTempDirectory(targetParent, ".aurora-backup-");
      Path snapshot = temporaryDirectory.resolve("snapshot.db");
      writeSnapshot(snapshot);
      try (Connection verification = DriverManager.getConnection(
          "jdbc:sqlite:" + snapshot.toUri().toASCIIString());
           Statement statement = verification.createStatement();
           ResultSet rows = statement.executeQuery("PRAGMA integrity_check")) {
        if (!rows.next() || !"ok".equals(rows.getString(1)) || rows.next()) {
          throw new StorageException("SQLite backup failed integrity verification");
        }
      }
      // Link publication is atomic and cannot overwrite a destination created concurrently.
      // Both paths are on the destination filesystem; the temporary link is removed afterward.
      Files.createLink(target, snapshot);
      Files.delete(snapshot);
      try (FileChannel directory = FileChannel.open(targetParent, StandardOpenOption.READ)) {
        directory.force(true);
      }
    } catch (IOException | SQLException e) {
      primary = e;
      throw new StorageException("Unable to create SQLite backup", e);
    } catch (RuntimeException | Error e) {
      primary = e;
      throw e;
    } finally {
      try {
        if (temporaryDirectory != null) {
          Files.deleteIfExists(temporaryDirectory.resolve("snapshot.db"));
          Files.deleteIfExists(temporaryDirectory.resolve("snapshot.db-wal"));
          Files.deleteIfExists(temporaryDirectory.resolve("snapshot.db-shm"));
          Files.deleteIfExists(temporaryDirectory);
        }
      } catch (IOException cleanup) {
        if (primary != null) {
          suppress(primary, cleanup);
        } else {
          throw new StorageException("Unable to clean up SQLite backup workspace", cleanup);
        }
      } finally {
        writer.unlock();
        lifecycle.readLock().unlock();
      }
    }
  }

  private void writeSnapshot(Path snapshot) throws SQLException {
    Connection connection = openConnection();
    Throwable primary = null;
    try {
      verifyOwner(connection);
      try (PreparedStatement statement = connection.prepareStatement("VACUUM INTO ?")) {
        statement.setString(1, snapshot.toString());
        statement.execute();
      }
    } catch (SQLException | RuntimeException | Error failure) {
      primary = failure;
      throw failure;
    } finally {
      closeConnection(connection, primary, null);
    }
  }

  private <T, E extends Exception> T transact(
      boolean writable, String operationId, Work<T, E> work) throws E {
    Transaction nested = current.get();
    if (nested != null) {
      try {
        validateWork(writable, operationId, work);
        if (writable && !nested.writable) {
          throw new StorageException("Cannot promote a read transaction to a write");
        }
        if (writable && !operationId.equals(nested.operationId)) {
          throw new StorageException("Nested writes must use the outer operation ID");
        }
        return work.apply();
      } catch (Exception | Error failure) {
        if (nested.rollbackCause == null) {
          nested.rollbackCause = failure;
        }
        throw failure;
      }
    }
    lifecycle.readLock().lock();
    try {
      if (!writable) {
        validateWork(false, operationId, work);
        checkOpen();
        return outerTransaction(false, operationId, work);
      }
      writer.lock();
      try {
        checkOpen();
        if (writeFailure != null) {
          throw new StorageException(
              "A previous write failed; scheduler restart required", writeFailure);
        }
        if (publicationFailure != null) {
          throw publicationFailure;
        }
        if (uncertainOperation != null) {
          throw new StorageException("Reconcile uncertain operation before writing: "
              + uncertainOperation);
        }
        validateWork(true, operationId, work);
        T result = outerTransaction(true, operationId, work);
        publishCommitted();
        return result;
      } catch (Exception | Error failure) {
        if (failClosedOnWriteFailureEnabled && writeFailure == null) {
          writeFailure = failure;
        }
        throw failure;
      } finally {
        writer.unlock();
      }
    } finally {
      lifecycle.readLock().unlock();
    }
  }

  private static void validateWork(boolean writable, String operationId, Work<?, ?> work) {
    requireNonNull(work);
    if (writable) {
      requireNonNull(operationId);
      if (operationId.isEmpty()) {
        throw new IllegalArgumentException("An operation ID is required");
      }
    }
  }

  private <T, E extends Exception> T outerTransaction(
      boolean writable, String operationId, Work<T, E> work) throws E {
    Connection connection;
    try {
      connection = openConnection();
    } catch (SQLException e) {
      throw new StorageException("Unable to open SQLite transaction", e);
    }
    boolean committed = false;
    Throwable primary = null;
    Transaction transaction = new Transaction(connection, writable, operationId);
    T result;
    try {
      try {
        if (!writable) {
          execute(connection, "PRAGMA query_only=ON");
        }
        execute(connection, writable ? "BEGIN IMMEDIATE" : "BEGIN");
        // Establish the snapshot before entering user work and validate the owner under
        // the same write lock as all subsequent mutations.
        verifyOwner(connection);
        if (writable && hasOutcome(connection, operationId)) {
          throw new AlreadyCommittedException(operationId);
        }
      } catch (SQLException e) {
        throw new StorageException("Unable to begin SQLite transaction", e);
      }
      current.set(transaction);
      result = work.apply();
      if (transaction.rollbackCause != null) {
        throw new StorageException("Nested failure marked transaction rollback-only",
            transaction.rollbackCause);
      }
      try {
        if (writable) {
          try (PreparedStatement insert = connection.prepareStatement(
              "INSERT INTO storage_transactions(operation_id, owner_epoch) VALUES (?, ?)")) {
            insert.setString(1, operationId);
            insert.setLong(2, epoch);
            insert.executeUpdate();
          }
        }
      } catch (SQLException e) {
        throw new StorageException("Unable to record transaction outcome", e);
      }
      try {
        execute(connection, "COMMIT");
      } catch (SQLException | RuntimeException | Error e) {
        if (writable) {
          uncertainOperation = operationId;
          throw new CommitUncertainException(operationId, e);
        }
        throw new StorageException("Unable to complete SQLite read", e);
      }
      committed = true;
    } catch (Exception | Error failure) {
      primary = failure;
      if (!committed) {
        try {
          execute(connection, "ROLLBACK");
        } catch (SQLException | RuntimeException | Error cleanup) {
          suppress(failure, cleanup);
        }
      }
      throw failure;
    } finally {
      current.remove();
      try {
        closeConnection(connection, primary, writable && committed ? operationId : null);
      } finally {
        if (writable && operationId.equals(uncertainOperation)) {
          uncertainCallbacks = List.copyOf(transaction.afterCommit);
        }
      }
    }
    if (writable) {
      enqueue(operationId, transaction.afterCommit);
    }
    return result;
  }

  private void closeConnection(
      Connection connection, Throwable primary, String committedOperation) {
    try {
      connection.close();
    } catch (SQLException | RuntimeException | Error cleanup) {
      synchronized (unclosedConnections) {
        unclosedConnections.add(connection);
      }
      if (primary != null) {
        suppress(primary, cleanup);
      } else if (committedOperation != null) {
        uncertainOperation = committedOperation;
        throw new CommitUncertainException(committedOperation, cleanup);
      } else {
        throw new StorageException("Unable to close SQLite connection", cleanup);
      }
    }
  }

  private void verifyOwner(Connection connection) throws SQLException {
    try (PreparedStatement query = connection.prepareStatement(
        "SELECT epoch, session_id FROM storage_owner WHERE singleton=1");
         ResultSet result = query.executeQuery()) {
      if (!result.next() || result.getLong(1) != epoch
          || !sessionId.equals(result.getString(2))) {
        throw new StorageException("SQLite storage ownership changed");
      }
    }
  }

  private static boolean hasOutcome(Connection connection, String operationId) throws SQLException {
    try (PreparedStatement query = connection.prepareStatement(
        "SELECT 1 FROM storage_transactions WHERE operation_id=?")) {
      query.setString(1, operationId);
      try (ResultSet result = query.executeQuery()) {
        return result.next();
      }
    }
  }

  private void checkOpen() {
    if (closed || closing) {
      throw new StorageException("SQLite storage is closing or closed");
    }
    synchronized (unclosedConnections) {
      if (!unclosedConnections.isEmpty()) {
        throw new StorageException("SQLite connection cleanup failed; close storage before reuse");
      }
    }
  }

  private static void execute(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static long scalar(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement();
         ResultSet result = statement.executeQuery(sql)) {
      if (!result.next()) {
        throw new SQLException("Missing scalar result for " + sql);
      }
      return result.getLong(1);
    }
  }

  // Throwable.addSuppressed forbids identity equality; value equality is irrelevant here.
  @SuppressWarnings("PMD.CompareObjectsWithEquals")
  private static void suppress(Throwable primary, Throwable cleanup) {
    if (primary != cleanup) {
      primary.addSuppressed(cleanup);
    }
  }

  // These connections are already quarantined after failed close attempts. A failed retry must
  // retain the entire set and ownership lock so no new owner can race a still-live connection.
  @SuppressWarnings("PMD.CloseResource")
  @Override
  public void close() {
    if (current.get() != null) {
      throw new IllegalStateException("Cannot close SQLite storage inside a transaction");
    }
    lifecycle.writeLock().lock();
    try {
      if (closed) {
        return;
      }
      // Even a failed shutdown may have released ownership. Only close retries remain legal.
      closing = true;
      synchronized (unclosedConnections) {
        for (Connection connection : unclosedConnections) {
          connection.close();
        }
        unclosedConnections.clear();
      }
      if (ownershipLock.isValid()) {
        ownershipLock.release();
      }
      lockChannel.close();
      closed = true;
      OWNED_PATHS.remove(path);
    } catch (SQLException | IOException e) {
      throw new StorageException("Unable to release SQLite storage ownership", e);
    } finally {
      lifecycle.writeLock().unlock();
    }
  }
}
