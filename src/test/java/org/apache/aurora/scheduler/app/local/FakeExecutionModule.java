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

import jakarta.inject.Singleton;

import com.google.inject.AbstractModule;

import org.apache.aurora.scheduler.execution.ExecutionControl;
import org.apache.aurora.scheduler.execution.ExecutionDriver;
import org.apache.aurora.scheduler.execution.OfferTransport;
import org.apache.aurora.scheduler.execution.TaskConfigValidator;
import org.apache.aurora.scheduler.execution.TaskFactory;
import org.apache.aurora.scheduler.execution.TaskKiller;
import org.apache.aurora.scheduler.execution.TaskReconciliation;

/** Installs only the test execution boundary; all scheduling policy remains in AppModule. */
public final class FakeExecutionModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(FakeMaster.class).in(Singleton.class);
    bind(ExecutionDriver.class).to(FakeMaster.class);
    bind(ExecutionControl.class).to(FakeMaster.class);
    bind(TaskKiller.class).to(FakeMaster.class);
    bind(OfferTransport.class).to(FakeMaster.class);
    bind(TaskReconciliation.class).to(FakeMaster.class);
    bind(TaskFactory.class).toInstance((task, offer, revocable) -> new FakeMaster.Launch(task));
    bind(TaskConfigValidator.class).toInstance(task -> { });
  }
}
