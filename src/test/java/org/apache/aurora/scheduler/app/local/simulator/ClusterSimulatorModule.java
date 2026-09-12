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
package org.apache.aurora.scheduler.app.local.simulator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.execution.TestOffer;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.resources.ResourceTestUtil;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;

/**
 * Module that sets up bindings to simulate fake cluster resources.
 */
public class ClusterSimulatorModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(FakeSlaves.class).in(Singleton.class);
    Multibinder<HostOffer> offers = Multibinder.newSetBinder(binder(), HostOffer.class);
    offers.addBinding()
        .toInstance(baseOffer("slave-1", "a", 16, 16 * 1024, 100 * 1024));
    offers.addBinding()
        .toInstance(baseOffer("slave-2", "a", 16, 16 * 1024, 100 * 1024));
    offers.addBinding()
        .toInstance(baseOffer("slave-3", "b", 16, 16 * 1024, 100 * 1024));
    offers.addBinding()
        .toInstance(baseOffer("slave-4", "b", 16, 16 * 1024, 100 * 1024));
    offers.addBinding()
        .toInstance(dedicated(baseOffer("slave-5", "c", 24, 128 * 1024, 1824 * 1024), "database"));
    offers.addBinding()
        .toInstance(dedicated(baseOffer("slave-6", "c", 24, 128 * 1024, 1824 * 1024), "database"));
    SchedulerServicesModule.addAppStartupServiceBinding(binder()).to(Register.class);
  }

  static class Register extends AbstractIdleService {
    private final EventBus eventBus;
    private final FakeSlaves slaves;

    @Inject
    Register(EventBus eventBus, FakeSlaves slaves) {
      this.eventBus = requireNonNull(eventBus);
      this.slaves = requireNonNull(slaves);
    }

    @Override
    protected void startUp() throws Exception {
      eventBus.register(slaves);
    }

    @Override
    protected void shutDown() {
      // No-op.
    }
  }

  private static HostOffer baseOffer(
      String slaveId,
      String rack,
      double cpu,
      double ramMb,
      double diskMb) {
    String host = slaveId + "-hostname";
    var offer = TestOffer.builder(UUID.randomUUID().toString()).agentId(slaveId).hostname(host)
        .resources(ResourceTestUtil.bag(Map.of(
            CPUS, cpu, RAM_MB, ramMb, DISK_MB, diskMb, PORTS, 1001.0)))
        .ports(java.util.stream.IntStream.rangeClosed(40000, 41000).boxed().toList()).build();
    return new HostOffer(offer, IHostAttributes.build(new HostAttributes()
        .setHost(host).setSlaveId(slaveId).setMode(MaintenanceMode.NONE)
        .setAttributes(Set.of(new Attribute("host", Set.of(host)),
            new Attribute("rack", Set.of(rack))))));
  }

  private static HostOffer dedicated(HostOffer base, String dedicatedTo) {
    HostAttributes attributes = base.getAttributes().newBuilder();
    attributes.setAttributes(new HashSet<>(attributes.getAttributes()));
    attributes.addToAttributes(new Attribute(DEDICATED_ATTRIBUTE, Set.of(dedicatedTo)));
    return new HostOffer(base.getOffer(), IHostAttributes.build(attributes));
  }
}
