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

import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.HostMaintenanceStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.TaskStore;

/**
 * Transactional implementation of the existing stores for a single local owner.
 * Independent reads see one committed snapshot; nested reads see the enclosing write.
 * This backend is not installed in the running scheduler until external effects become durable.
 */
public final class SqliteStorage implements Storage, AutoCloseable {
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

  private final SqliteDatabase database;
  private final MutableStoreProvider stores;
  private final SqliteEffects durableEffects;

  public static SqliteStorage open(Path path) {
    SqliteDatabase database = SqliteDatabase.open(path);
    try {
      return new SqliteStorage(database);
    } catch (RuntimeException | Error failure) {
      try {
        database.close();
      } catch (RuntimeException | Error cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  private SqliteStorage(SqliteDatabase database) {
    this.database = database;
    durableEffects = new SqliteEffects(database);
    var scheduler = new SqliteSchedulerStore(database);
    var cron = new SqliteCronJobStore(database);
    var tasks = new SqliteTaskStore(database);
    var quotas = new SqliteQuotaStore(database);
    var attributes = new SqliteAttributeStore(database);
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
    try {
      return database.write(operationId, () -> work.apply(stores));
    } catch (SqliteDatabase.CommitUncertainException e) {
      throw new CommitUncertainException(e);
    }
  }

  public boolean isCommitted(String operationId) {
    return database.isCommitted(operationId);
  }

  /** Access requires a storage callback; writes join its transaction. */
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

  @Override
  public void close() {
    database.close();
  }
}
