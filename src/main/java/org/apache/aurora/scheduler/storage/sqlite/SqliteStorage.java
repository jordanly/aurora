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

import java.nio.file.Path;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.HostMaintenanceStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.TaskStore;

import static java.util.Objects.requireNonNull;

/**
 * Transactional implementation of the existing stores for a single local owner.
 * Independent reads see one committed snapshot; nested reads see the enclosing write.
 * This backend is not installed in the running scheduler until external effects become durable.
 */
public final class SqliteStorage implements Storage.NonVolatileStorage, AutoCloseable {
  /** The callback must not be retried until this operation's durable outcome is reconciled. */
  public static final class CommitUncertainException extends StorageException {
    private final String operationId;

    private CommitUncertainException(SqliteDatabase.CommitUncertainException cause) {
      super(cause.getMessage(), cause);
      operationId = cause.getOperationId();
    }

    public String getOperationId() {
      return operationId;
    }
  }

  /** The mutation committed, but volatile publication failed; restart to reconstruct subscribers.
   */
  public static final class PostCommitException extends StorageException {
    private final String operationId;

    PostCommitException(String operationId, Throwable cause) {
      super("Operation committed but event delivery failed; restart required: " + operationId,
          cause);
      this.operationId = operationId;
    }

    public String getOperationId() {
      return operationId;
    }
  }

  private final SqliteDatabase database;
  private final MutableStoreProvider stores;
  private final SqliteEffects durableEffects;
  private volatile Consumer<Throwable> writeFailureHandler = failure -> { };

  public static SqliteStorage open(Path path) {
    return open(path, event -> { });
  }

  /** Opens storage with the raw downstream event bus used for host-attribute notifications. */
  public static SqliteStorage open(Path path, EventSink downstream) {
    requireNonNull(downstream);
    SqliteDatabase database = SqliteDatabase.open(path);
    try {
      return new SqliteStorage(database, downstream);
    } catch (RuntimeException | Error failure) {
      try {
        database.close();
      } catch (RuntimeException | Error cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  private SqliteStorage(SqliteDatabase database, EventSink downstream) {
    this.database = database;
    durableEffects = new SqliteEffects(database);
    var scheduler = new SqliteSchedulerStore(database);
    var cron = new SqliteCronJobStore(database);
    var tasks = new SqliteTaskStore(database);
    var quotas = new SqliteQuotaStore(database);
    var attributes = new SqliteAttributeStore(database, transactionalEventSink(downstream));
    var updates = new SqliteJobUpdateStore(database);
    var maintenance = new SqliteHostMaintenanceStore(database);
    stores = new MutableStoreProvider() {
      @Override
      public SchedulerStore.Mutable getSchedulerStore() {
        return scheduler;
      }

      @Override
      public CronJobStore.Mutable getCronJobStore() {
        return cron;
      }

      @Override
      public TaskStore getTaskStore() {
        return tasks;
      }

      @Override
      public TaskStore.Mutable getUnsafeTaskStore() {
        return tasks;
      }

      @Override
      public QuotaStore.Mutable getQuotaStore() {
        return quotas;
      }

      @Override
      public AttributeStore.Mutable getAttributeStore() {
        return attributes;
      }

      @Override
      public JobUpdateStore.Mutable getJobUpdateStore() {
        return updates;
      }

      @Override
      public HostMaintenanceStore.Mutable getHostMaintenanceStore() {
        return maintenance;
      }
    };
  }

  @Override
  public <T, E extends Exception> T read(Work<T, E> work) throws E {
    return database.read(() -> work.apply(stores));
  }

  @Override
  public <T, E extends Exception> T write(MutateWork<T, E> work) throws E {
    String enclosing = database.currentOperationId();
    return write(enclosing == null ? UUID.randomUUID().toString() : enclosing, work);
  }

  /**
   * Executes a caller-identified operation exactly once per committed ID. A repeated committed
   * ID fails before invoking work; retrieve the result from durable state. Nested writes join
   * the enclosing transaction and must reuse its ID. No callback is automatically retried.
   */
  public <T, E extends Exception> T write(String operationId, MutateWork<T, E> work) throws E {
    boolean outermost = database.currentOperationId() == null;
    try {
      return database.write(operationId, () -> work.apply(stores));
    } catch (SqliteDatabase.CommitUncertainException e) {
      CommitUncertainException failure = new CommitUncertainException(e);
      notifyWriteFailure(outermost, failure);
      throw failure;
    } catch (Exception | Error failure) {
      notifyWriteFailure(outermost, failure);
      throw failure;
    }
  }

  /**
   * Enables scheduler fail-stop after an outer write failure, because policy caches may already
   * have changed. Configure before serving requests. The handler must request asynchronous shutdown
   * without waiting: synchronous subscribers may still hold an enclosing publication lock. Only a
   * fresh storage instance clears the latch.
   */
  public void setWriteFailureHandler(Consumer<Throwable> handler) {
    writeFailureHandler = requireNonNull(handler);
    database.failClosedOnWriteFailure();
  }

  private void notifyWriteFailure(boolean outermost, Throwable failure) {
    if (outermost) {
      try {
        writeFailureHandler.accept(failure);
      } catch (RuntimeException | Error notificationFailure) {
        if (!notificationFailure.equals(failure)) {
          failure.addSuppressed(notificationFailure);
        }
      }
    }
  }

  /**
   * Buffers events in the current write. Outside transactions, lifecycle events publish
   * immediately. Reads cannot publish. Ordering follows commit order and original post order,
   * including nested writes. These notifications are volatile: startup must reconstruct
   * subscribers from stored tasks. A delivery failure stops subsequent writes; do not replay the
   * committed callback.
   */
  public EventSink transactionalEventSink(EventSink downstream) {
    requireNonNull(downstream);
    return event -> {
      requireNonNull(event);
      database.afterCommit(() -> downstream.post(event));
    };
  }

  public boolean isCommitted(String operationId) {
    return database.isCommitted(operationId);
  }

  /** Access requires a storage callback; writes join its transaction. */
  /** Durable local ownership epoch for fencing the enrolled agent sessions. */
  public long ownerEpoch() {
    return read(provider -> database.currentOwnerEpoch());
  }

  public SqliteEffects effects() {
    return durableEffects;
  }

  /** Creates a consistent standalone backup at a new local path outside any storage callback. */
  public void backup(Path destination) {
    database.backup(destination);
  }

  @Override
  public void prepare() {
    database.read(() -> null);
  }

  /** Recovery notifications and readiness are supplied by CallOrderEnforcingStorage. */
  @Override
  public void start(MutateWork.NoResult.Quiet initializationLogic) {
    write(initializationLogic);
  }

  @Override
  public void stop() {
    close();
  }

  @Override
  public void close() {
    database.close();
  }
}
