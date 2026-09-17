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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Provider;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.app.local.simulator.events.OfferAccepted;
import org.apache.aurora.scheduler.app.local.simulator.events.Started;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverRegistered;
import org.apache.aurora.scheduler.events.PubsubEventModule.RegisteredEvents;
import org.apache.aurora.scheduler.execution.ExecutionDriver;
import org.apache.aurora.scheduler.execution.OfferTransport;
import org.apache.aurora.scheduler.execution.PreparedTask;
import org.apache.aurora.scheduler.execution.ReconciliationTarget;
import org.apache.aurora.scheduler.execution.TaskReconciliation;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;

import static com.google.common.base.Preconditions.checkState;

/** Test-only execution simulator driving the original offer and task state controllers. */
public class FakeMaster extends AbstractIdleService
    implements ExecutionDriver, OfferTransport, TaskReconciliation {
  private final Map<String, Task> activeTasks = new ConcurrentHashMap<>();
  private final Map<String, HostOffer> idleOffers = new ConcurrentHashMap<>();
  private final Map<String, HostOffer> sentOffers = new ConcurrentHashMap<>();
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
      Thread.ofPlatform().daemon().name("LocalExecution").factory());
  private final EventBus eventBus;
  private final EventSink registered;
  private final Provider<OfferManager> offers;
  private final Provider<StateManager> states;
  private final Storage storage;

  @Inject
  FakeMaster(EventBus eventBus, @RegisteredEvents EventSink registered,
             Provider<OfferManager> offers, Provider<StateManager> states, Storage storage) {
    this.eventBus = eventBus;
    this.registered = registered;
    this.offers = offers;
    this.states = states;
    this.storage = storage;
  }

  public void addResources(Iterable<HostOffer> resources) {
    for (HostOffer offer : resources) {
      checkState(idleOffers.putIfAbsent(offer.getOfferId(), offer) == null,
          "Duplicate offer id %s", offer.getOfferId());
    }
  }

  public void changeState(String taskId, ScheduleStatus status) {
    Task task = activeTasks.get(taskId);
    if (task == null) {
      return; // A delayed simulator event may follow cancellation.
    }
    storage.write(stores -> {
      states.get().changeState(stores, taskId, Optional.empty(), status,
          Optional.of("Local execution simulator"));
      return null;
    });
    if (org.apache.aurora.scheduler.base.Tasks.isTerminated(status)) {
      activeTasks.remove(taskId);
      idleOffers.put(task.offer().getOfferId(), task.offer());
    }
  }

  @Override
  protected void startUp() {
    registered.post(new DriverRegistered());
    eventBus.post(new Started());
    executor.scheduleWithFixedDelay(() -> {
      for (HostOffer offer : idleOffers.values()) {
        if (idleOffers.remove(offer.getOfferId(), offer)) {
          sentOffers.put(offer.getOfferId(), offer);
          storage.write(stores -> {
            stores.getAttributeStore().saveHostAttributes(offer.getAttributes());
            offers.get().add(offer);
            return null;
          });
        }
      }
    }, 1, 5, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() {
    executor.shutdownNow();
  }

  @Override
  public void launch(String offerId, PreparedTask prepared, double refuseSeconds) {
    HostOffer offer = sentOffers.remove(offerId);
    checkState(offer != null, "Offer %s is invalid", offerId);
    Launch launch = (Launch) prepared;
    String taskId = launch.assigned().getTaskId();
    checkState(activeTasks.putIfAbsent(taskId, new Task(offer, launch)) == null,
        "Task %s already exists", taskId);
    eventBus.post(new OfferAccepted(offerId, launch));
  }

  @Override
  public void killTask(String taskId) {
    // Status delivery is asynchronous, like an external execution agent.
    executor.execute(() -> changeState(taskId, ScheduleStatus.KILLED));
  }

  @Override
  public void decline(String offerId, double refuseSeconds) {
    HostOffer offer = sentOffers.remove(offerId);
    if (offer != null) {
      executor.schedule(() -> idleOffers.put(offerId, offer),
          Math.max(0, (long) refuseSeconds), TimeUnit.SECONDS);
    }
  }

  @Override
  public void reconcileTasks(Collection<ReconciliationTarget> targets) {
    // This in-process simulator has no independent journal to reconcile.
  }

  @Override public void abort() {
    stopAsync();
  }

  @Override public void blockUntilStopped() {
    awaitTerminated();
  }

  public record Launch(IAssignedTask assigned) implements PreparedTask { }
  private record Task(HostOffer offer, Launch launch) { }
}
