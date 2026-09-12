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
package org.apache.aurora.scheduler.app.local;

import java.io.File;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.util.Modules;

import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.TierModule;
import org.apache.aurora.scheduler.app.TestSchedulerLauncher;
import org.apache.aurora.scheduler.app.local.simulator.ClusterSimulatorModule;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.config.CommandLine;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.configuration.executor.TestExecutorSettings;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.backup.BackupModule;
import org.apache.aurora.scheduler.storage.log.SnapshotterImpl;
import org.apache.shiro.io.ResourceUtils;

/**
 * A main class that runs the scheduler in local mode, using fakes for external components.
 */
public final class LocalSchedulerMain {
  private LocalSchedulerMain() {
    // Utility class.
  }

  public static void main(String[] args) {
    File backupDir = Files.createTempDir();
    backupDir.deleteOnExit();
    List<String> arguments = ImmutableList.<String>builder()
        .add(args)
        .add("-cluster_name=local")
        .add("-serverset_path=/aurora/local/scheduler")
        .add("-zk_endpoints=localhost:2181")
        .add("-zk_in_proc=true")
        .add("-backup_dir=" + backupDir.getAbsolutePath())
        .add("-go_agent_config=unused-by-local-simulator")
        .add("-http_port=8081")
        .add("-http_authentication_mechanism=BASIC")
        .add("-shiro_ini_path="
            + ResourceUtils.CLASSPATH_PREFIX
            + "org/apache/aurora/scheduler/http/api/security/shiro-example.ini")
        .build();
    CliOptions options = CommandLine.parseOptions(arguments.toArray(new String[] {}));
    // The production CLI requires enrollment. This test launcher installs its fake boundary.
    options.main.goAgentConfig = null;

    Module persistentStorage = new AbstractModule() {
      @Override
      protected void configure() {
        bind(Storage.class).to(Key.get(Storage.class, Storage.Volatile.class));
        bind(NonVolatileStorage.class).to(FakeNonVolatileStorage.class);
        bind(SnapshotStore.class).toInstance(new SnapshotStore() {
          @Override
          public void snapshot() throws Storage.StorageException {
            // no-op
          }

          @Override
          public void snapshotWith(Snapshot snapshot) {
            // no-op
          }
        });
      }
    };

    Module fakeExecution = new AbstractModule() {
      @Override
      protected void configure() {
        install(new FakeExecutionModule());
        bind(ExecutorSettings.class).toInstance(TestExecutorSettings.THERMOS_EXECUTOR);
        install(new BackupModule(options.backup, SnapshotterImpl.class));
        install(new ClusterSimulatorModule());
      }
    };

    TestSchedulerLauncher.run(options,
        Modules.combine(fakeExecution, persistentStorage, new TierModule(options.tiers)));
  }
}
