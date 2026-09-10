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
package org.apache.aurora.scheduler.storage.sql;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Single-owner native foundation, intentionally separate from legacy Storage and its stores. */
public final class NativeSqlStore implements AutoCloseable {
  private static final BigInteger MAX = new BigInteger("18446744073709551615");
  private static final int APPLICATION_ID = 1096110670;
  private final String url;
  private final String cluster;
  private final String incarnation;
  private final FileChannel lockChannel;
  private final FileLock ownerLock;
  private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();
  private final Object writer = new Object();
  private final ThreadLocal<Tx> current = new ThreadLocal<>();
  private boolean closed;

  @FunctionalInterface
  private interface Mutation { void apply() throws SQLException; }

  @FunctionalInterface
  public interface Work<T> {
    T apply(Tx transaction) throws Exception;
  }

  /** Unambiguous neutral job identity; components cannot contain the delimiter. */
  public static final class JobKey {
    private final String key;
    public JobKey(String role, String environment, String name) {
      key = token(role) + "/" + token(environment) + "/" + token(name);
    }
    @Override public String toString() { return key; }
    @Override public boolean equals(Object other) {
      return other instanceof JobKey && key.equals(((JobKey) other).key);
    }
    @Override public int hashCode() { return key.hashCode(); }
  }

  /** Cursor scope includes recovery and journal identity, never a daemon session. */
  public static final class Journal {
    private final String cluster;
    private final String incarnation;
    private final String node;
    private final String journal;
    public Journal(String cluster, String incarnation, String node, String journal) {
      this.cluster = token(cluster);
      this.incarnation = token(incarnation);
      this.node = token(node);
      this.journal = token(journal);
    }
    private String key() { return node + "/" + journal; }
  }

  public NativeSqlStore(Path directory, String cluster, String incarnation) throws Exception {
    this.cluster = token(cluster);
    this.incarnation = token(incarnation);
    Files.createDirectories(directory);
    if (!directory.toAbsolutePath().normalize().equals(directory.toRealPath())
        || Files.isSymbolicLink(directory.resolve("owner.lock"))
        || Files.isSymbolicLink(directory.resolve("scheduler.db"))) {
      throw new IOException("Symlinked scheduler state is unsupported");
    }
    boolean priorOwner = Files.exists(directory.resolve("owner.lock"));
    lockChannel = FileChannel.open(directory.resolve("owner.lock"),
        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    FileLock acquired;
    try {
      acquired = lockChannel.tryLock();
      if (acquired == null) { throw new IOException("Scheduler state already owned"); }
    } catch (Exception e) {
      lockChannel.close();
      throw e;
    }
    ownerLock = acquired;
    url = "jdbc:sqlite:" + directory.toRealPath().resolve("scheduler.db");
    boolean existing = Files.exists(directory.resolve("scheduler.db"));
    if (priorOwner && !existing) {
      ownerLock.release(); lockChannel.close();
      throw new IOException("Previously owned state is missing its database");
    }
    try (Connection c = connect(false)) {
      int version = Integer.parseInt(scalar(c, "PRAGMA user_version"));
      int application = Integer.parseInt(scalar(c, "PRAGMA application_id"));
      if (existing && ((version != 1 && version != 2) || application != APPLICATION_ID)) {
        throw new SQLException("Unsupported native schema/application");
      }
      if (version == 0 && (application != 0 || !"0".equals(scalar(c,
          "SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")))) {
        throw new SQLException("Refusing unidentified nonempty database");
      }
      c.setAutoCommit(false);
      if (!existing) {
        schema(c, 2);
        try (PreparedStatement p = c.prepareStatement("INSERT INTO metadata VALUES (1,?,?)")) {
          p.setString(1, cluster); p.setString(2, incarnation); p.executeUpdate();
        }
      }
      if (existing && version == 1) {
        verifySchema(c, 1);
        schedulerSchema(c);
      }
      verifySchema(c, 2);
      if (!"1".equals(scalar(c, "SELECT count(*) FROM metadata"))) {
        throw new SQLException("Missing or multiple metadata rows");
      }
      try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(
          "SELECT cluster,incarnation FROM metadata WHERE id=1")) {
        if (!r.next() || !cluster.equals(r.getString(1)) || !incarnation.equals(r.getString(2))) {
          throw new SQLException("Wrong cluster/recovery incarnation");
        }
      }
      c.commit();
      try (FileChannel parent = FileChannel.open(directory, StandardOpenOption.READ)) {
        parent.force(true);
      }
    } catch (Exception | Error e) {
      ownerLock.release(); lockChannel.close(); throw e;
    }
  }

  private Connection connect(boolean readOnly) throws SQLException {
    Connection c = DriverManager.getConnection(url);
    try {
      execute(c, "PRAGMA busy_timeout=5000");
      execute(c, "PRAGMA foreign_keys=ON");
      if (!"wal".equalsIgnoreCase(scalar(c, "PRAGMA journal_mode=WAL"))) {
        throw new SQLException("WAL unavailable");
      }
      execute(c, "PRAGMA synchronous=FULL");
      if (!"2".equals(scalar(c, "PRAGMA synchronous"))) {
        throw new SQLException("FULL synchronization unavailable");
      }
      execute(c, "PRAGMA read_uncommitted=OFF");
      if (readOnly) { execute(c, "PRAGMA query_only=ON"); }
      return c;
    } catch (SQLException e) { c.close(); throw e; }
  }

  private void schema(Connection c, int version) throws SQLException {
    execute(c, "CREATE TABLE metadata(id INTEGER PRIMARY KEY CHECK(id=1),"
        + "cluster TEXT NOT NULL,incarnation TEXT NOT NULL)");
    execute(c, "CREATE TABLE jobs(job TEXT PRIMARY KEY,revision TEXT NOT NULL CHECK("
        + "revision NOT GLOB '*[^0-9]*' AND length(revision) BETWEEN 1 AND 20 AND "
        + "(revision='0' OR substr(revision,1,1)!='0') AND "
        + "(length(revision)<20 OR revision<='18446744073709551615')),"
        + "kind TEXT NOT NULL CHECK(kind IN ('batch','service')),body TEXT NOT NULL)");
    execute(c, "CREATE TABLE instances(job TEXT NOT NULL REFERENCES jobs(job),"
        + "instance TEXT NOT NULL,desired INTEGER NOT NULL CHECK(desired IN (0,1)),"
        + "PRIMARY KEY(job,instance))");
    execute(c, "CREATE TABLE attempts(attempt TEXT PRIMARY KEY,job TEXT NOT NULL,"
        + "instance TEXT NOT NULL,state TEXT NOT NULL,"
        + "FOREIGN KEY(job,instance) REFERENCES instances(job,instance))");
    execute(c, "CREATE TABLE allocations(attempt TEXT PRIMARY KEY REFERENCES attempts(attempt),"
        + "node TEXT NOT NULL,body TEXT NOT NULL)");
    execute(c, "CREATE TABLE commands(command TEXT PRIMARY KEY,"
        + "attempt TEXT NOT NULL REFERENCES allocations(attempt),body TEXT NOT NULL,"
        + "pending INTEGER NOT NULL CHECK(pending IN (0,1)))");
    execute(c, "CREATE TABLE journals(scope TEXT PRIMARY KEY,cursor TEXT NOT NULL)");
    execute(c, "CREATE TABLE observations(scope TEXT NOT NULL REFERENCES journals(scope),"
        + "cursor TEXT NOT NULL,body TEXT NOT NULL,PRIMARY KEY(scope,cursor))");
    execute(c, "PRAGMA application_id=" + APPLICATION_ID);
    execute(c, "PRAGMA user_version=1");
    if (version == 2) { schedulerSchema(c); }
  }

  private void schedulerSchema(Connection c) throws SQLException {
    execute(c, "CREATE TABLE scheduler_state(id INTEGER PRIMARY KEY CHECK(id=1),"
        + "config TEXT NOT NULL,epoch TEXT NOT NULL)");
    execute(c, "CREATE TABLE attempt_observations(attempt TEXT PRIMARY KEY REFERENCES attempts(attempt),"
        + "sequence TEXT NOT NULL,state TEXT NOT NULL,cleanup TEXT NOT NULL,"
        + "ready INTEGER NOT NULL CHECK(ready IN (0,1)),updated_millis INTEGER NOT NULL)");
    execute(c, "PRAGMA user_version=2");
  }

  private void verifySchema(Connection c, int version) throws SQLException {
    String catalog = "SELECT group_concat(type||':'||name||':'||coalesce(sql,''),char(10)) "
        + "FROM (SELECT type,name,sql FROM sqlite_master ORDER BY name)";
    try (Connection expected = DriverManager.getConnection("jdbc:sqlite::memory:")) {
      schema(expected, version);
      if (!scalar(expected, catalog).equals(scalar(c, catalog))) {
        throw new SQLException("Native schema definition mismatch");
      }
    }
    if (!"ok".equals(scalar(c, "PRAGMA quick_check"))) {
      throw new SQLException("Database integrity check failed");
    }
    try (Statement statement = c.createStatement();
        ResultSet violations = statement.executeQuery("PRAGMA foreign_key_check")) {
      if (violations.next()) { throw new SQLException("Database foreign key violation"); }
    }
  }

  public <T> T write(Work<T> work) throws Exception {
    Tx nested = current.get();
    if (nested != null) {
      try {
        nested.check(true);
        return work.apply(nested);
      } catch (Exception | Error e) { nested.rollbackOnly = true; throw e; }
    }
    synchronized (writer) { return transact(false, work); }
  }

  public <T> T read(Work<T> work) throws Exception {
    // Reads inside writes observe that transaction; independent readers use their own snapshot.
    Tx nested = current.get();
    if (nested != null) {
      nested.readDepth++;
      try { return work.apply(nested); }
      catch (Exception | Error e) { nested.rollbackOnly = true; throw e; }
      finally { nested.readDepth--; }
    }
    return transact(true, work);
  }

  private <T> T transact(boolean readOnly, Work<T> work) throws Exception {
    lifecycle.readLock().lock();
    try {
      if (closed) { throw new IllegalStateException("Store closed"); }
      try (Connection c = connect(readOnly)) {
        c.setAutoCommit(false);
        // Establish snapshot before invoking caller, including an otherwise empty callback.
        scalar(c, "SELECT count(*) FROM metadata");
        Tx tx = new Tx(c, readOnly);
        current.set(tx);
        try {
          T result = work.apply(tx);
          if (tx.rollbackOnly) { throw new SQLException("Transaction is rollback-only"); }
          c.commit();
          return result;
        } catch (Exception | Error e) {
          try { c.rollback(); } catch (SQLException rollback) { e.addSuppressed(rollback); }
          throw e;
        } finally { tx.active = false; current.remove(); }
      }
    } finally { lifecycle.readLock().unlock(); }
  }

  /** Views cannot escape their callback or be used from a different thread. No mutable cache. */
  public final class Tx {
    private final Connection connection;
    private final boolean readOnly;
    private final Thread thread = Thread.currentThread();
    private boolean active = true;
    private boolean rollbackOnly;
    private int readDepth;
    private Tx(Connection connection, boolean readOnly) {
      this.connection = connection; this.readOnly = readOnly;
    }
    private void check(boolean mutation) {
      if (!active || Thread.currentThread() != thread) {
        throw new IllegalStateException("Transaction view escaped callback/thread");
      }
      if (mutation && (readOnly || readDepth > 0)) { throw new IllegalStateException("Read-only transaction"); }
    }
    private void mutate(Mutation mutation) throws SQLException {
      try { check(true); mutation.apply(); }
      catch (SQLException | RuntimeException | Error e) { rollbackOnly = true; throw e; }
    }
    private void update(String sql, String... values) throws SQLException {
      check(true);
      try (PreparedStatement p = connection.prepareStatement(sql)) {
        for (int i = 0; i < values.length; i++) { p.setString(i + 1, values[i]); }
        p.executeUpdate();
      } catch (SQLException | RuntimeException | Error e) { rollbackOnly = true; throw e; }
    }
    private String get(String sql, String... values) throws SQLException {
      check(false);
      try (PreparedStatement p = connection.prepareStatement(sql)) {
        for (int i = 0; i < values.length; i++) { p.setString(i + 1, values[i]); }
        try (ResultSet r = p.executeQuery()) { return r.next() ? r.getString(1) : null; }
      }
    }
    public void createJob(JobKey job, String revision, String kind, String body)
        throws SQLException {
      mutate(() -> {
        update("INSERT INTO jobs VALUES (?,?,?,?)", job.key, counter(revision), kind, body);
      });
    }
    public String jobBody(JobKey job) throws SQLException {
      return get("SELECT body FROM jobs WHERE job=?", job.key);
    }
    public void addInstance(JobKey job, String instance) throws SQLException {
      mutate(() -> {
        update("INSERT INTO instances VALUES (?,?,1)", job.key, token(instance));
      });
    }
    public boolean hasInstance(JobKey job, String instance) throws SQLException {
      return get("SELECT instance FROM instances WHERE job=? AND instance=? AND desired=1", job.key,
          token(instance)) != null;
    }
    public void removeInstance(JobKey job, String instance) throws SQLException {
      mutate(() -> {
        update("UPDATE instances SET desired=0 WHERE job=? AND instance=?", job.key, token(instance));
      });
    }
    public String jobRevision(JobKey job) throws SQLException {
      return get("SELECT revision FROM jobs WHERE job=?", job.key);
    }
    public void createAttempt(String attempt, JobKey job, String instance) throws SQLException {
      mutate(() -> {
        update("INSERT INTO attempts VALUES (?,?,?,'pending')", token(attempt), job.key,
            token(instance));
      });
    }
    public void completeAttempt(String attempt, String outcome) throws SQLException {
      mutate(() -> {
        if (!"succeeded".equals(outcome) && !"failed".equals(outcome)) {
          throw new IllegalArgumentException("Not a terminal outcome");
        }
        update("UPDATE attempts SET state=? WHERE attempt=? AND state='pending'", outcome,
            token(attempt));
        if (!outcome.equals(attemptState(attempt))) { throw new SQLException("Outcome conflict"); }
      });
    }
    public String attemptState(String attempt) throws SQLException {
      return get("SELECT state FROM attempts WHERE attempt=?", token(attempt));
    }
    public void allocate(String attempt, String node, String body) throws SQLException {
      mutate(() -> {
        update("INSERT INTO allocations VALUES (?,?,?)", token(attempt), token(node), body);
      });
    }
    public String allocation(String attempt) throws SQLException {
      return get("SELECT body FROM allocations WHERE attempt=?", token(attempt));
    }
    public void command(String command, String attempt, String canonicalBody) throws SQLException {
      mutate(() -> {
        check(true);
        String old = get("SELECT body FROM commands WHERE command=?", token(command));
        if (old != null) {
          if (!old.equals(canonicalBody) || !attempt.equals(get(
              "SELECT attempt FROM commands WHERE command=?", command))) {
            rollbackOnly = true; throw new SQLException("Conflicting immutable command identity");
          }
          return;
        }
        update("INSERT INTO commands VALUES (?,?,?,1)", command, token(attempt), canonicalBody);
      });
    }
    public List<String> pendingCommands() throws SQLException {
      check(false);
      List<String> result = new ArrayList<>();
      try (Statement s = connection.createStatement(); ResultSet r = s.executeQuery(
          "SELECT body FROM commands WHERE pending=1 ORDER BY command")) {
        while (r.next()) { result.add(r.getString(1)); }
      }
      return Collections.unmodifiableList(result);
    }
    public void acknowledgeCommand(String command) throws SQLException {
      mutate(() -> {
        update("UPDATE commands SET pending=0 WHERE command=?", token(command));
      });
    }
    public String schedulerConfig() throws SQLException {
      return get("SELECT config FROM scheduler_state WHERE id=1");
    }
    public String schedulerEpoch() throws SQLException {
      String epoch = get("SELECT epoch FROM scheduler_state WHERE id=1");
      return epoch == null ? "0" : counter(epoch);
    }
    public String startScheduler(String config) throws SQLException {
      mutate(() -> {
        String prior = schedulerConfig();
        if (prior == null) {
          update("INSERT INTO scheduler_state VALUES (1,?,'0')", config);
        } else if (!prior.equals(config)) {
          throw new SQLException("Scheduler enrollment/configuration changed");
        }
        String next = counter(new BigInteger(schedulerEpoch()).add(BigInteger.ONE).toString());
        update("UPDATE scheduler_state SET epoch=? WHERE id=1", next);
      });
      return schedulerEpoch();
    }
    public List<String> jobBodies() throws SQLException {
      return strings("SELECT body FROM jobs ORDER BY job");
    }
    public List<String> desiredInstances(JobKey job) throws SQLException {
      return strings("SELECT instance FROM instances WHERE job=? AND desired=1 ORDER BY instance",
          job.key);
    }
    public List<String> allInstances(JobKey job) throws SQLException {
      return strings("SELECT instance FROM instances WHERE job=? ORDER BY instance", job.key);
    }
    private List<String> strings(String sql, String... values) throws SQLException {
      check(false);
      List<String> result = new ArrayList<>();
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        for (int i = 0; i < values.length; i++) { statement.setString(i + 1, values[i]); }
        try (ResultSet rows = statement.executeQuery()) {
          while (rows.next()) { result.add(rows.getString(1)); }
        }
      }
      return Collections.unmodifiableList(result);
    }
    public List<AttemptRecord> attempts() throws SQLException {
      check(false);
      List<AttemptRecord> result = new ArrayList<>();
      try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(
          "SELECT a.attempt,a.job,a.instance,coalesce(o.state,a.state),"
              + "coalesce(o.sequence,'0'),coalesce(o.cleanup,'unknown'),coalesce(o.ready,0),"
              + "coalesce(o.updated_millis,0),l.node,l.body FROM attempts a "
              + "LEFT JOIN attempt_observations o ON a.attempt=o.attempt "
              + "LEFT JOIN allocations l ON a.attempt=l.attempt ORDER BY a.rowid")) {
        while (rows.next()) {
          result.add(new AttemptRecord(rows.getString(1), rows.getString(2), rows.getString(3),
              rows.getString(4), counter(rows.getString(5)), rows.getString(6), rows.getInt(7) != 0,
              rows.getLong(8), rows.getString(9), rows.getString(10)));
        }
      }
      return Collections.unmodifiableList(result);
    }
    public List<CommandRecord> commands() throws SQLException {
      check(false);
      List<CommandRecord> result = new ArrayList<>();
      try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(
          "SELECT command,attempt,body,pending FROM commands ORDER BY rowid")) {
        while (rows.next()) {
          result.add(new CommandRecord(rows.getString(1), rows.getString(2), rows.getString(3),
              rows.getInt(4) != 0));
        }
      }
      return Collections.unmodifiableList(result);
    }
    public void reduceAttempt(String attempt, String sequence, String state,
        String cleanup, boolean ready, long updatedMillis) throws SQLException {
      mutate(() -> {
        token(attempt); counter(sequence);
        if (!state.matches("pending|unknown|running|succeeded|failed|stopped|lost")
            || !cleanup.matches("unknown|pending|complete")
            || ("complete".equals(cleanup) && !terminal(state))
            || (ready && !"running".equals(state))) {
          throw new IllegalArgumentException("Invalid observation state");
        }
        String oldSequence = get("SELECT sequence FROM attempt_observations WHERE attempt=?", attempt);
        if (oldSequence != null && new BigInteger(sequence).compareTo(new BigInteger(oldSequence)) < 0) {
          return;
        }
        String oldState = get("SELECT state FROM attempt_observations WHERE attempt=?", attempt);
        String oldCleanup = get("SELECT cleanup FROM attempt_observations WHERE attempt=?", attempt);
        if (sequence.equals(oldSequence)) {
          String oldReady = get("SELECT ready FROM attempt_observations WHERE attempt=?", attempt);
          if (!state.equals(oldState) || !cleanup.equals(oldCleanup)
              || !(ready ? "1" : "0").equals(oldReady)) {
            throw new SQLException("Conflicting observation sequence");
          }
          return;
        }
        if ("complete".equals(oldCleanup) && !state.equals(oldState)) {
          throw new SQLException("Conflicting completed terminal outcome");
        }
        if (oldState != null && terminal(oldState) && !terminal(state)) { return; }
        if ("complete".equals(oldCleanup) && !"complete".equals(cleanup)) { return; }
        update("INSERT OR REPLACE INTO attempt_observations VALUES (?,?,?,?,?,?)", attempt,
            sequence, state, cleanup, ready ? "1" : "0", Long.toString(updatedMillis));
      });
    }
    private String scope(Journal journal) {
      if (!cluster.equals(journal.cluster) || !incarnation.equals(journal.incarnation)) {
        throw new IllegalArgumentException("Wrong journal cluster/recovery scope");
      }
      return journal.key();
    }
    /** Persist receipt in same transaction as its future reducer; returns no ACK until commit. */
    public void observe(Journal journal, String cursor, String canonicalBody) throws SQLException {
      mutate(() -> {
        String key = scope(journal);
        counter(cursor);
        if ("0".equals(cursor)) { throw new IllegalArgumentException("Zero is baseline only"); }
        update("INSERT OR IGNORE INTO journals VALUES (?,'0')", key);
        String old = get("SELECT body FROM observations WHERE scope=? AND cursor=?", key, cursor);
        if (old != null && !old.equals(canonicalBody)) {
          rollbackOnly = true; throw new SQLException("Conflicting observation cursor");
        }
        if (old == null) { update("INSERT INTO observations VALUES (?,?,?)", key, cursor,
            canonicalBody); }
        BigInteger contiguous = new BigInteger(committedCursor(journal));
        while (contiguous.compareTo(MAX) < 0) {
          String next = contiguous.add(BigInteger.ONE).toString();
          if (get("SELECT cursor FROM observations WHERE scope=? AND cursor=?", key, next) == null) {
            break;
          }
          contiguous = new BigInteger(next);
        }
        update("UPDATE journals SET cursor=? WHERE scope=?", contiguous.toString(), key);
      });
    }
    /** Only a completed outer read/write may supply this value to a transport ACK. */
    public String committedCursor(Journal journal) throws SQLException {
      String value = get("SELECT cursor FROM journals WHERE scope=?", scope(journal));
      return value == null ? "0" : counter(value);
    }
  }

  public static boolean terminal(String state) {
    return "succeeded".equals(state) || "failed".equals(state)
        || "stopped".equals(state) || "lost".equals(state);
  }

  public static final class AttemptRecord {
    public final String attempt;
    public final String job;
    public final String instance;
    public final String state;
    public final String sequence;
    public final String cleanup;
    public final boolean ready;
    public final long updatedMillis;
    public final String node;
    public final String runBody;
    private AttemptRecord(String attempt, String job, String instance, String state, String sequence,
        String cleanup, boolean ready, long updatedMillis, String node, String runBody) {
      this.attempt = attempt; this.job = job; this.instance = instance; this.state = state;
      this.sequence = sequence; this.cleanup = cleanup; this.ready = ready;
      this.updatedMillis = updatedMillis; this.node = node; this.runBody = runBody;
    }
    public boolean reserved() { return !terminal(state) || !"complete".equals(cleanup); }
  }

  public static final class CommandRecord {
    public final String command;
    public final String attempt;
    public final String body;
    public final boolean pending;
    private CommandRecord(String command, String attempt, String body, boolean pending) {
      this.command = command; this.attempt = attempt; this.body = body; this.pending = pending;
    }
  }

  /** Consistent online SQLite snapshot. Destination must be a new private directory. */
  public void snapshot(Path destination) throws Exception {
    synchronized (writer) {
      lifecycle.readLock().lock();
      try {
        if (closed || current.get() != null) { throw new IllegalStateException("Invalid snapshot scope"); }
        if (!destination.isAbsolute() || Files.exists(destination)) {
          throw new IOException("Snapshot destination must be new and absolute");
        }
        for (Path path = destination; path != null; path = path.getParent()) {
          if (Files.isSymbolicLink(path)) { throw new IOException("Snapshot symlink rejected"); }
        }
        Files.createDirectories(destination);
        Path output = destination.resolve("scheduler.db");
        try (Connection c = connect(false); PreparedStatement statement = c.prepareStatement(
            "VACUUM INTO ?")) {
          statement.setString(1, output.toString()); statement.execute();
        }
        try (FileChannel file = FileChannel.open(output, StandardOpenOption.WRITE)) { file.force(true); }
        try (FileChannel dir = FileChannel.open(destination, StandardOpenOption.READ)) { dir.force(true); }
        try (FileChannel parent = FileChannel.open(destination.getParent(), StandardOpenOption.READ)) {
          parent.force(true);
        }
      } finally { lifecycle.readLock().unlock(); }
    }
  }

  public static String counter(String value) {
    if (value == null || !value.matches("0|[1-9][0-9]{0,19}")
        || new BigInteger(value).compareTo(MAX) > 0) {
      throw new IllegalArgumentException("Invalid uint64 counter");
    }
    return value;
  }
  private static String token(String value) {
    if (value == null || !value.matches("[A-Za-z0-9_.-]+")) {
      throw new IllegalArgumentException("Invalid identity token");
    }
    return value;
  }
  private static String scalar(Connection c, String sql) throws SQLException {
    try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
      if (!r.next()) { throw new SQLException("Missing scalar result"); }
      return r.getString(1);
    }
  }
  private static void execute(Connection c, String sql) throws SQLException {
    try (Statement s = c.createStatement()) { s.execute(sql); }
  }
  @Override public void close() throws IOException {
    if (current.get() != null) { throw new IllegalStateException("Close within transaction"); }
    lifecycle.writeLock().lock();
    try {
      if (!closed) { closed = true; ownerLock.release(); lockChannel.close(); }
    } finally { lifecycle.writeLock().unlock(); }
  }
}
