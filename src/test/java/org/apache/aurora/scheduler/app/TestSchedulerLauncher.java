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
package org.apache.aurora.scheduler.app;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Module;

import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.discovery.FlaggedZooKeeperConfig;
import org.apache.aurora.scheduler.discovery.ServiceDiscoveryModule;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;

/** Allows the local test simulator to run the original scheduler lifecycle with a fake boundary. */
public final class TestSchedulerLauncher {
  private TestSchedulerLauncher() { }

  public static void run(CliOptions options, Module environment) {
    Module serverInfo = new AbstractModule() {
      @Override
      protected void configure() {
        bind(CliOptions.class).toInstance(options);
        bind(IServerInfo.class).toInstance(IServerInfo.build(new ServerInfo()
            .setClusterName(options.main.clusterName)
            .setStatsUrlPrefix(options.main.statsUrlPrefix)));
      }
    };
    var injector = Guice.createInjector(environment, SchedulerMain.getUniversalModule(options),
        new ServiceDiscoveryModule(FlaggedZooKeeperConfig.create(options.zk),
            options.main.serversetPath), serverInfo);
    Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
    try {
      SchedulerMain scheduler = new SchedulerMain();
      injector.injectMembers(scheduler);
      scheduler.run(options.main);
    } finally {
      lifecycle.shutdown();
    }
  }
}
