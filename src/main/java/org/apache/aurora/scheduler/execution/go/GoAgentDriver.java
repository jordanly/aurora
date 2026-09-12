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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverRegistered;
import org.apache.aurora.scheduler.events.PubsubEventModule.RegisteredEvents;
import org.apache.aurora.scheduler.execution.ExecutionDriver;
import org.apache.aurora.scheduler.execution.ExecutionOffer;
import org.apache.aurora.scheduler.execution.OfferTransport;
import org.apache.aurora.scheduler.execution.PreparedTask;
import org.apache.aurora.scheduler.execution.ReconciliationTarget;
import org.apache.aurora.scheduler.execution.TaskReconciliation;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IResource;
import org.apache.aurora.scheduler.storage.sqlite.SqliteEffects.Command;
import org.apache.aurora.scheduler.storage.sqlite.SqliteEffects.ReceiptKey;
import org.apache.aurora.scheduler.storage.sqlite.SqliteStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Durable execution adapter beneath Aurora's existing offers and state machine. */
final class GoAgentDriver extends AbstractIdleService
    implements ExecutionDriver, OfferTransport, TaskReconciliation {
  private static final Logger LOG = LoggerFactory.getLogger(GoAgentDriver.class);
  private final GoAgentConfig config;
  private final Storage storage;
  private final SqliteStorage sqlite;
  private final com.google.inject.Provider<OfferManager> offers;
  private final com.google.inject.Provider<StateManager> stateManager;
  private final EventSink registered;
  private final AgentTransport client;
  private final boolean autoPoll;
  private final String epoch;
  private final String session = "s-" + UUID.randomUUID();
  private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor(
      Thread.ofPlatform().daemon().name("GoAgentPoller").factory());

  @Inject
  GoAgentDriver(GoAgentConfig config, Storage storage, SqliteStorage sqlite,
                com.google.inject.Provider<OfferManager> offers,
                com.google.inject.Provider<StateManager> stateManager,
                @RegisteredEvents EventSink registered) throws Exception {
    this(config, storage, sqlite, offers, stateManager, registered, new GoAgentClient(config));
  }

  GoAgentDriver(GoAgentConfig config, Storage storage, SqliteStorage sqlite,
                com.google.inject.Provider<OfferManager> offers,
                com.google.inject.Provider<StateManager> stateManager,
                EventSink registered, AgentTransport client) {
    this(config, storage, sqlite, offers, stateManager, registered, client, true);
  }

  GoAgentDriver(GoAgentConfig config, Storage storage, SqliteStorage sqlite,
                com.google.inject.Provider<OfferManager> offers,
                com.google.inject.Provider<StateManager> stateManager,
                EventSink registered, AgentTransport client, boolean autoPoll) {
    this.autoPoll = autoPoll;
    this.config = config;
    this.storage = storage;
    this.sqlite = sqlite;
    this.offers = offers;
    this.stateManager = stateManager;
    this.registered = registered;
    this.client = client;
    epoch = Long.toString(sqlite.ownerEpoch());
  }

  @Override
  protected void startUp() throws Exception {
    // The existing lifecycle has recovered storage and published initialized task events.
    // Registration is withheld until every enrolled journal has reconciled successfully.
    try {
      for (GoAgentConfig.Node node : config.nodes()) {
        poll(node, false);
      }
      if (autoPoll) {
        worker.scheduleWithFixedDelay(this::tick, 1, 1, TimeUnit.SECONDS);
      }
      registered.post(new DriverRegistered());
    } catch (Exception | Error failure) {
      worker.shutdownNow();
      client.close();
      throw failure;
    }
  }

  void tick() {
    if (!isRunning()) {
      return;
    }
    for (GoAgentConfig.Node node : config.nodes()) {
      try {
        poll(node, true);
        dispatch(node);
      } catch (Exception e) {
        cancelOffer(node);
        LOG.warn("Agent {} exchange failed; its reservations are retained: {}",
            node.name(), e.toString());
      }
    }
  }

  private String journalScope(GoAgentConfig.Node node) {
    return config.incarnation() + "/" + node.journal();
  }

  private void poll(GoAgentConfig.Node node, boolean advertise) throws Exception {
    client.request(node, "/v1/session", WireJson.object().put("schedulerEpoch", epoch)
        .put("session", session), epoch, session);
    long after = storage.read(stores -> sqlite.effects().committedCursor(
        node.name(), journalScope(node)));
    for (int page = 0; page < 4; page++) {
      JsonNode response = client.request(node, "/v1/state?afterCursor=" + after + "&limit=128",
          null, epoch, session);
      validateState(node, response);
      long next = WireJson.counter(response, "nextCursor");
      List<JsonNode> observations = new java.util.ArrayList<>();
      long expected = after;
      for (JsonNode observation : response.path("state").path("observations")) {
        validateObservation(node, observation);
        WireJson.require(WireJson.counter(observation, "cursor") == Math.incrementExact(expected),
            "Observation cursor gap");
        expected++;
        observations.add(observation);
      }
      WireJson.require(expected == next
          && next <= WireJson.counter(response.path("state"), "cursor")
          && (!response.path("hasMore").asBoolean() || !observations.isEmpty()),
          "Invalid page cursor");
      long before = after;
      storage.write(stores -> {
        WireJson.require(
            sqlite.effects().committedCursor(node.name(), journalScope(node)) == before,
            "Concurrent observation consumer");
        for (JsonNode observation : observations) {
          observe(stores, node, observation);
        }
        return null;
      });
      ObjectNode ack = WireJson.base("ObservationAck").put("cluster", config.cluster())
          .put("incarnation", config.incarnation()).put("node", node.name())
          .put("journal", node.journal()).put("committedCursor", Long.toString(next));
      client.request(node, "/v1/ack", ack, epoch, session);
      after = next;
      if (!response.path("hasMore").asBoolean()) {
        if (advertise) {
          offer(node, response.path("state").path("attempts"));
        }
        return;
      }
    }
    throw new IOException("Observation backlog not drained");
  }

  private void validateState(GoAgentConfig.Node node, JsonNode response) {
    JsonNode actual = response.path("config");
    WireJson.require(config.cluster().equals(actual.path("cluster").asText())
        && config.incarnation().equals(actual.path("incarnation").asText())
        && epoch.equals(actual.path("schedulerEpoch").asText())
        && session.equals(actual.path("session").asText())
        && "scheduler".equals(actual.path("peer").asText())
        && node.cpuMillis() == actual.path("cpuMillis").asLong()
        && node.memoryBytes() == actual.path("memoryBytes").asLong(), "Agent enrollment differs");
    node.target().fields().forEachRemaining(entry -> WireJson.require(
        entry.getValue().equals(actual.path(entry.getKey())), "Agent target differs"));
    JsonNode state = response.path("state");
    WireJson.require(
        state.path("observations").isArray() && state.path("observations").size() <= 128
            && state.path("attempts").isObject() && state.path("attempts").size() <= 128
            && state.path("commands").isObject() && state.path("commands").size() <= 1024
            && response.path("hasMore").isBoolean(), "Invalid bounded agent inventory");
  }

  private void validateObservation(GoAgentConfig.Node node, JsonNode observation) {
    WireJson.fields(observation, "version", "kind", "identity", "source", "sequence", "cursor",
        "state", "ready", "cleanup");
    WireJson.require("native-v1alpha1".equals(observation.path("version").asText())
        && "Observation".equals(observation.path("kind").asText())
        && node.target().equals(observation.path("source"))
        && observation.path("ready").isBoolean()
        && Set.of("running", "succeeded", "failed", "lost", "stopped", "unknown")
            .contains(observation.path("state").asText())
        && Set.of("pending", "complete", "unknown").contains(observation.path("cleanup").asText()),
        "Invalid observation");
    WireJson.counter(observation, "sequence");
    WireJson.bytes(observation);
  }

  private Optional<Command> runFor(JsonNode identity) {
    String attempt = WireJson.text(identity, "attempt");
    WireJson.require(attempt.matches("a-[a-f0-9]{62}"), "Unknown attempt identity");
    Optional<Command> command = sqlite.effects().command("r-" + attempt.substring(2));
    if (command.isPresent()) {
      WireJson.require(identity.equals(parse(command.get()).path("identity")),
          "Observation identity differs from launch");
    }
    return command;
  }

  private void observe(MutableStoreProvider stores, GoAgentConfig.Node node, JsonNode observation) {
    long cursor = WireJson.counter(observation, "cursor");
    if (!sqlite.effects().recordReceipt(new ReceiptKey(node.name(), journalScope(node), cursor),
        1, WireJson.bytes(observation))) {
      return;
    }
    Command command = runFor(observation.path("identity"))
        .orElseThrow(() -> new IllegalArgumentException("Observation has no durable launch"));
    WireJson.require(node.name().equals(command.agentId()), "Observation agent differs");
    String attemptScope = node.name() + "/" + observation.path("identity").path("attempt").asText();
    long sequence = WireJson.counter(observation, "sequence");
    long previous = sqlite.effects().committedCursor(attemptScope, journalScope(node));
    WireJson.require(sequence == Math.incrementExact(previous), "Attempt sequence gap");
    sqlite.effects().recordReceipt(new ReceiptKey(attemptScope, journalScope(node), sequence),
        1, WireJson.bytes(observation));
    ScheduleStatus status = switch (observation.path("state").asText()) {
      case "running" -> observation.path("ready").asBoolean() ? ScheduleStatus.RUNNING : null;
      case "succeeded" -> ScheduleStatus.FINISHED;
      case "failed" -> ScheduleStatus.FAILED;
      case "lost" -> ScheduleStatus.LOST;
      case "stopped" -> ScheduleStatus.KILLED;
      default -> null;
    };
    if (status != null && (status == ScheduleStatus.RUNNING
        || "complete".equals(observation.path("cleanup").asText()))) {
      stateManager.get().changeState(stores, command.taskId(), Optional.empty(), status,
          Optional.of("Go agent: " + observation.path("state").asText()));
    }
  }

  private void offer(GoAgentConfig.Node node, JsonNode inventory) {
    storage.write(stores -> {
      cancelOffer(node);
      Set<String> reserved = new HashSet<>();
      for (JsonNode attempt : inventory) {
        WireJson.require(attempt.path("reserved").isBoolean(), "Incomplete reservation inventory");
        if (attempt.path("reserved").asBoolean()) {
          Optional<Command> run = runFor(attempt.path("identity"));
          if (run.isEmpty() || !node.name().equals(run.get().agentId())) {
            return null; // Unknown execution prevents advertising any free capacity.
          }
          reserved.add(run.get().taskId());
        }
      }
      ResourceBag available = resources(node.cpuMillis() / 1000.0,
          node.memoryBytes() / 1048576, node.diskMb());
      for (var task : stores.getTaskStore().fetchTasks(Query.unscoped())) {
        if (node.name().equals(task.getAssignedTask().getSlaveId())
            && (Tasks.isActive(task.getStatus())
                || reserved.contains(task.getAssignedTask().getTaskId()))) {
          available = available.subtract(ResourceManager.bagFromResources(
              task.getAssignedTask().getTask().getResources()));
          reserved.remove(task.getAssignedTask().getTaskId());
        }
      }
      if (!reserved.isEmpty()) {
        return null; // A pruned task with unfinished cleanup still reserves its agent.
      }
      var existing = stores.getAttributeStore().getHostAttributes(node.name());
      IHostAttributes attributes = existing.orElseGet(
          () -> IHostAttributes.build(new HostAttributes()
              .setHost(node.name()).setSlaveId(node.name()).setMode(MaintenanceMode.NONE)
              .setAttributes(Set.of(new Attribute().setName("host")
                  .setValues(Set.of(node.name()))))));
      if (existing.isEmpty()) {
        stores.getAttributeStore().saveHostAttributes(attributes);
      }
      offers.get().add(new HostOffer(
          new AgentOffer(node.name(), "offer-" + UUID.randomUUID(), available), attributes));
      return null;
    });
  }

  private void cancelOffer(GoAgentConfig.Node node) {
    offers.get().get(node.name()).ifPresent(offer -> offers.get().cancel(offer.getOfferId()));
  }

  private record AgentOffer(String agent, String id, ResourceBag resources)
      implements ExecutionOffer {
    @Override
    public String getOfferId() {
      return id;
    }
    @Override
    public String getAgentId() {
      return agent;
    }
    @Override
    public String getHostname() {
      return agent;
    }
    @Override
    public ResourceBag getTotalResources() {
      return resources;
    }
    @Override public ResourceBag getResources(boolean revocable) {
      return revocable ? ResourceBag.EMPTY : resources;
    }
    @Override
    public List<Integer> getAvailablePorts() {
      return List.of();
    }
    @Override
    public Optional<Instant> getUnavailabilityStart() {
      return Optional.empty();
    }
    @Override
    public boolean isDedicated() {
      return false;
    }
  }

  private static ResourceBag resources(double cpu, long memoryMb, long diskMb) {
    return ResourceManager.bagFromResources(List.of(IResource.build(Resource.numCpus(cpu)),
        IResource.build(Resource.ramMb(memoryMb)), IResource.build(Resource.diskMb(diskMb))));
  }

  @Override
  public void launch(String offerId, PreparedTask task, double refuseSeconds) {
    WireJson.require(isRunning() && task instanceof GoTaskFactory.Launch, "Go driver not running");
    GoTaskFactory.Launch launch = (GoTaskFactory.Launch) task;
    byte[] body = launch.body().getBytes(StandardCharsets.US_ASCII);
    sqlite.effects().enqueue(new Command(GoTaskFactory.identity("r-", launch.taskId()),
        launch.agentId(), launch.taskId(), "Run", 1, body));
  }

  @Override
  public void killTask(String taskId) {
    storage.write(stores -> {
      enqueueStop(taskId);
      return null;
    });
  }

  private void enqueueStop(String taskId) {
    // State transitions join their outer transaction; KillRetry also calls outside a write.
    Optional<Command> run = sqlite.effects().command(GoTaskFactory.identity("r-", taskId));
    if (run.isEmpty()) {
      return; // No committed or uncommitted launch exists for this task.
    }
    JsonNode body = parse(run.get());
    sqlite.effects().acknowledge(run.get().id());
    ObjectNode stop = WireJson.base("Stop").put("command", GoTaskFactory.identity("s-", taskId))
        .put("desiredRevision", "2").put("reason", "cancel")
        .put("graceMillis", body.path("assignment").path("stop").path("graceMillis").asInt());
    stop.set("identity", body.get("identity"));
    stop.set("target", body.get("target"));
    sqlite.effects().enqueue(new Command(GoTaskFactory.identity("s-", taskId), run.get().agentId(),
        taskId, "Stop", 1, WireJson.bytes(stop)));
  }

  private void dispatch(GoAgentConfig.Node node) {
    // Serialize the final cancellation check with policy writes. Only intents from an earlier
    // committed transaction are eligible; transport failures retain the exact identity/body.
    for (int count = 0; count < 16; count++) {
      boolean delivered = storage.write(stores -> {
        var next = sqlite.effects().pending(1024).stream()
            .filter(item -> item.command().agentId().equals(node.name())).findFirst();
        if (next.isEmpty() || !isRunning()) {
          return false;
        }
        Command command = next.get().command();
        if ("Run".equals(command.type())) {
          var task = stores.getTaskStore().fetchTask(command.taskId());
          if (task.isEmpty() || !Set.of(
                  ScheduleStatus.ASSIGNED, ScheduleStatus.STARTING, ScheduleStatus.RUNNING)
              .contains(task.get().getStatus())) {
            killTask(command.taskId());
            return true;
          }
        }
        ObjectNode delivery = WireJson.base("Delivery")
            .put("bodySha256", WireJson.hash(command.payload()));
        delivery.set("body", parse(command));
        delivery.set("authority", WireJson.object().put("cluster", config.cluster())
            .put("incarnation", config.incarnation()).put("schedulerEpoch", epoch)
            .put("session", session));
        try {
          JsonNode result = client.request(node, "/v1/deliver", delivery, epoch, session);
          WireJson.require(command.id().equals(result.path("command").asText())
              && WireJson.hash(command.payload()).equals(result.path("bodySha256").asText()),
              "Command receipt differs");
          String outcome = WireJson.text(result, "outcome");
          if (!"accepted".equals(outcome)) {
            WireJson.require("Run".equals(command.type()) && Set.of("rejected-capacity",
                "rejected-capability", "rejected-socket", "rejected-stopped").contains(outcome),
                "Command rejection has an ambiguous execution outcome");
            stateManager.get().changeState(stores, command.taskId(), Optional.empty(),
                ScheduleStatus.LOST, Optional.of("Go agent refused launch: " + outcome));
          }
          sqlite.effects().acknowledge(command.id());
          return true;
        } catch (IOException e) {
          LOG.warn("Command {} remains pending: {}", command.id(), e.toString());
          return false;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      });
      if (!delivered) {
        return;
      }
    }
  }

  private static JsonNode parse(Command command) {
    WireJson.require(command.payloadVersion() == 1, "Unsupported command payload version");
    try {
      return WireJson.parse(command.payload());
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid stored command", e);
    }
  }

  @Override
  public void decline(String offerId, double refuseSeconds) {
    // Offers are regenerated from the agent inventory on every poll.
  }
  @Override public void reconcileTasks(Collection<ReconciliationTarget> targets) {
    // Every poll reconciles full journal inventory; no task is lost solely on a missed heartbeat.
  }
  @Override
  public void blockUntilStopped() {
    awaitTerminated();
  }
  @Override
  public void abort() {
    stopAsync();
  }

  @Override
  protected void shutDown() {
    worker.shutdownNow();
    client.close();
    config.nodes().forEach(this::cancelOffer);
  }
}
