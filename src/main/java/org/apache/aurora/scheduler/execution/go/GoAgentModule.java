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
package org.apache.aurora.scheduler.execution.go;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.inject.Singleton;

import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;

import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.configuration.executor.ExecutorConfig;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.execution.ExecutionControl;
import org.apache.aurora.scheduler.execution.ExecutionDriver;
import org.apache.aurora.scheduler.execution.OfferTransport;
import org.apache.aurora.scheduler.execution.TaskConfigValidator;
import org.apache.aurora.scheduler.execution.TaskFactory;
import org.apache.aurora.scheduler.execution.TaskKiller;
import org.apache.aurora.scheduler.execution.TaskReconciliation;
import org.apache.aurora.scheduler.storage.CallOrderEnforcingStorage;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.sqlite.SqliteStorage;
import org.apache.mesos.v1.Protos;

/** Selects Go execution and SQLite inside the existing SchedulerMain application. */
public final class GoAgentModule extends AbstractModule {
  private final GoAgentConfig config;

  public GoAgentModule(CliOptions options) {
    try {
      config = GoAgentConfig.read(options.main.goAgentConfig.toPath(), options.main.clusterName);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to read Go agent enrollment", e);
    }
  }

  @Override
  protected void configure() {
    bind(GoAgentConfig.class).toInstance(config);
    install(CallOrderEnforcingStorage.wrappingModule(SqliteStorage.class));
    bind(GoAgentDriver.class).in(Singleton.class);
    bind(ExecutionDriver.class).to(GoAgentDriver.class);
    bind(ExecutionControl.class).to(GoAgentDriver.class);
    bind(TaskKiller.class).to(GoAgentDriver.class);
    bind(OfferTransport.class).to(GoAgentDriver.class);
    bind(TaskReconciliation.class).to(GoAgentDriver.class);
    bind(GoTaskFactory.class).in(Singleton.class);
    bind(TaskFactory.class).to(GoTaskFactory.class);
    bind(TaskConfigValidator.class).to(GoTaskFactory.class);
    // ExecutorSettings still serves the original resource-accounting contract. The placeholder
    // protobuf carries no executable payload or overhead, and is never sent to Mesos.
    bind(ExecutorSettings.class).toInstance(new ExecutorSettings(Map.of(GoTaskFactory.EXECUTOR,
        new ExecutorConfig(Protos.ExecutorInfo.newBuilder()
            .setExecutorId(Protos.ExecutorID.newBuilder().setValue("go-process"))
            .setCommand(Protos.CommandInfo.newBuilder().setValue("unused")).build(),
            List.of(), "go-")), false));
    bind(GoStorageBackup.class).in(Singleton.class);
    bind(StorageBackup.class).to(GoStorageBackup.class);
    bind(SnapshotStore.class).to(GoStorageBackup.class);
    bind(Recovery.class).to(GoStorageBackup.class);
  }

  @Provides
  @Singleton
  SqliteStorage provideSqlite(EventBus eventBus, Provider<ExecutionControl> execution) {
    SqliteStorage storage = SqliteStorage.open(config.database(), eventBus::post);
    storage.setWriteFailureHandler(failure ->
        Thread.startVirtualThread(() -> execution.get().abort()));
    return storage;
  }
}
