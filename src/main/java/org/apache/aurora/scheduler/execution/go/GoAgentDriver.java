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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverRegistered;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEventModule.RegisteredEvents;
import org.apache.aurora.scheduler.execution.ExecutionDriver;
import org.apache.aurora.scheduler.execution.ExecutionOffer;
import org.apache.aurora.scheduler.execution.OfferTransport;
import org.apache.aurora.scheduler.execution.PreparedTask;
import org.apache.aurora.scheduler.execution.ReconciliationTarget;
import org.apache.aurora.scheduler.execution.TaskLogReader;
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
    implements ExecutionDriver, OfferTransport, TaskReconciliation, EventSubscriber, TaskLogReader {
  private static final Logger LOG = LoggerFactory.getLogger(GoAgentDriver.class);
  private static final AtomicLong WATCH_CONNECTIONS =
      Stats.exportLong("go_agent_watch_connections");
  private static final AtomicLong WATCH_SNAPSHOTS = Stats.exportLong("go_agent_watch_snapshots");
  private static final AtomicLong WATCH_DELTAS = Stats.exportLong("go_agent_watch_deltas");
  private static final AtomicLong WATCH_HEARTBEATS = Stats.exportLong("go_agent_watch_heartbeats");
  private static final AtomicLong INVENTORY_REQUESTS =
      Stats.exportLong("go_agent_inventory_requests");
  private static final AtomicLong ACK_REQUESTS = Stats.exportLong("go_agent_ack_requests");
  private static final AtomicLong COMMAND_REQUESTS = Stats.exportLong("go_agent_command_requests");
  private final GoAgentConfig config;
  private final Storage storage;
  private final SqliteStorage sqlite;
  private final com.google.inject.Provider<OfferManager> offers;
  private final com.google.inject.Provider<StateManager> stateManager;
  private final EventSink registered;
  private final AgentTransport client;
  private final GoTaskLogReader logs;
  private final GoRetirementCoordinator retirement;
  private final boolean autoPoll;
  private final String epoch;
  private final String session = "s-" + UUID.randomUUID();
  private final ExecutorService consumers = Executors.newVirtualThreadPerTaskExecutor();
  private final java.util.Map<String, Inventory> inventories = new ConcurrentHashMap<>();
  private final java.util.Map<String, Long> offeredAt = new ConcurrentHashMap<>();
  private volatile boolean closing;
  private final int retainedCompleted = Integer.getInteger("aurora.go.retained-completed", 64);

  private static final class Inventory {
    private ObjectNode attempts = WireJson.object();
    private boolean ready;
    private boolean changed;
    private Set<?> tasks = Set.of();
    private final Semaphore wake = new Semaphore(0);
    private final CompletableFuture<Void> initial = new CompletableFuture<>();
  }

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
    WireJson.require(retainedCompleted >= 0 && retainedCompleted <= 896,
        "aurora.go.retained-completed must be between 0 and 896");
    this.autoPoll = autoPoll;
    this.config = config;
    this.storage = storage;
    this.sqlite = sqlite;
    this.offers = offers;
    this.stateManager = stateManager;
    this.registered = registered;
    this.client = client;
    epoch = Long.toString(sqlite.ownerEpoch());
    logs = new GoTaskLogReader(config, storage, sqlite, client, epoch, session);
    retirement = new GoRetirementCoordinator(
        config, storage, sqlite, client, epoch, session, retainedCompleted);
  }

  @Override
  public Optional<TaskLogReader.Page> readLog(String taskId, String stream, long offset, int limit)
      throws IOException, InterruptedException {
    return logs.readLog(taskId, stream, offset, limit);
  }

  @Override
  protected void startUp() throws Exception {
    // The existing lifecycle has recovered storage and published initialized task events.
    // Registration is withheld until every enrolled journal has reconciled successfully.
    try {
      if (autoPoll) {
        for (GoAgentConfig.Node node : config.nodes()) {
          Inventory inventory = new Inventory();
          inventories.put(node.name(), inventory);
          consumers.submit(() -> watch(node, inventory));
          consumers.submit(() -> localWork(node, inventory));
        }
        for (Inventory inventory : inventories.values()) {
          inventory.initial.get(90, TimeUnit.SECONDS);
        }
      } else {
        for (GoAgentConfig.Node node : config.nodes()) {
          poll(node, false);
        }
      }
      registered.post(new DriverRegistered());
      inventories.values().forEach(inventory -> inventory.wake.release());
    } catch (Exception | Error failure) {
      closing = true;
      consumers.shutdownNow();
      client.close();
      throw failure;
    }
  }

  private void refreshOffer(GoAgentConfig.Node node, Inventory inventory) {
    synchronized (inventory) {
      if (!inventory.ready) {
        return;
      }
      var tasks = storage.read(stores -> stores.getTaskStore().fetchTasks(Query.unscoped())
          .stream().filter(task -> node.name().equals(task.getAssignedTask().getSlaveId()))
          .collect(java.util.stream.Collectors.toSet()));
      if (inventory.changed || !inventory.tasks.equals(tasks)
          || offers.get().get(node.name()).isEmpty()
          || System.nanoTime() - offeredAt.getOrDefault(node.name(), 0L)
              >= TimeUnit.MINUTES.toNanos(1)) {
        offer(node, inventory.attempts);
        inventory.tasks = tasks;
        inventory.changed = false;
      }
    }
  }

  private void withdrawInventory(GoAgentConfig.Node node, Inventory inventory) {
    synchronized (inventory) {
      inventory.ready = false;
      cancelOffer(node);
    }
  }

  private void localWork(GoAgentConfig.Node node, Inventory inventory) {
    try {
      awaitRunning();
    } catch (IllegalStateException startupFailed) {
      return;
    }
    while (!closing && !Thread.currentThread().isInterrupted()) {
      try {
        boolean pending = false;
        if (isRunning()) {
          retirement.retainHistory(node);
          refreshOffer(node, inventory);
          dispatchIfPending(node);
          pending = hasPending(node);
        }
        inventory.wake.tryAcquire(pending ? 1 : 60, TimeUnit.SECONDS);
        inventory.wake.drainPermits();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        cancelOffer(node);
        dispatchStopsAfterFailure(node);
        LOG.warn("Agent {} local dispatch failed: {}", node.name(), e.toString());
        try {
          inventory.wake.tryAcquire(1, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private boolean hasPending(GoAgentConfig.Node node) {
    return storage.read(stores -> !sqlite.effects().pending(node.name(), 1).isEmpty());
  }

  private void dispatchIfPending(GoAgentConfig.Node node) {
    if (hasPending(node)) {
      dispatch(node);
    }
  }

  @Subscribe
  public void taskChanged(TaskStateChange event) {
    // The SQLite event sink publishes after commit. Never acquire inventory locks here.
    String agentId = event.getTask().getAssignedTask().getSlaveId();
    Inventory affected = agentId == null ? null : inventories.get(agentId);
    if (affected != null) {
      affected.wake.release();
    } else {
      // Unassigned tasks can consume capacity on any enrolled agent.
      inventories.values().forEach(inventory -> inventory.wake.release());
    }
  }

  private void watch(GoAgentConfig.Node node, Inventory inventory) {
    long retrySeconds = 1;
    while (!closing && !Thread.currentThread().isInterrupted()) {
      try {
        client.request(node, "/v1/session", WireJson.object().put("schedulerEpoch", epoch)
            .put("session", session), epoch, session);
        long after = storage.read(stores -> sqlite.effects().committedCursor(
            node.name(), journalScope(node)));
        try (AgentTransport.Watch stream = client.watch(node, after, epoch, session)) {
          WATCH_CONNECTIONS.incrementAndGet();
          boolean first = true;
          while (!closing) {
            JsonNode frame = stream.next();
            validateAuthority(node, frame);
            String kind = WireJson.text(frame, "kind");
            if ("heartbeat".equals(kind)) {
              WireJson.fields(frame, "kind", "config", "nextCursor");
              WireJson.require(!first && WireJson.counter(frame, "nextCursor") == after,
                  "Invalid watch heartbeat cursor");
              WATCH_HEARTBEATS.incrementAndGet();
              continue;
            }
            WireJson.require("snapshot".equals(kind) || !first && "delta".equals(kind),
                "Watch must begin with a snapshot");
            WireJson.fields(frame, "kind", "config", "state", "nextCursor", "hasMore");
            validateState(node, frame);
            long next = commitPage(node, frame, after);
            synchronized (inventory) {
              ObjectNode attempts = "snapshot".equals(kind)
                  ? WireJson.object() : inventory.attempts.deepCopy();
              frame.path("state").path("attempts").properties().forEach(
                  entry -> attempts.set(entry.getKey(), entry.getValue()));
              WireJson.require(attempts.size() <= 128, "Merged attempt inventory exceeds bound");
              for (JsonNode attempt : attempts) {
                WireJson.require(attempt.path("reserved").isBoolean(),
                    "Incomplete reservation inventory");
              }
              inventory.changed |= !inventory.attempts.equals(attempts);
              inventory.attempts = attempts;
              inventory.ready = !frame.path("hasMore").asBoolean();
              if (!inventory.ready) {
                cancelOffer(node);
              }
            }
            // Re-ACK the reconnect snapshot even when its cursor is unchanged: a prior
            // acknowledgement may have been lost after the scheduler committed its receipts.
            if (first || next != after) {
              acknowledge(node, next);
            }
            ("snapshot".equals(kind) ? WATCH_SNAPSHOTS : WATCH_DELTAS).incrementAndGet();
            after = next;
            first = false;
            retrySeconds = 1;
            if (!frame.path("hasMore").asBoolean()) {
              if (!inventory.initial.isDone()) {
                retirement.retainHistory(node);
              }
              inventory.initial.complete(null);
              inventory.wake.release();
            }
          }
        }
      } catch (InterruptedException e) {
        withdrawInventory(node, inventory);
        Thread.currentThread().interrupt();
        return;
      } catch (IOException | RuntimeException e) {
        withdrawInventory(node, inventory);
        LOG.warn("Agent {} watch failed; reservations retained: {}", node.name(), e.toString());
        try {
          TimeUnit.SECONDS.sleep(retrySeconds);
          retrySeconds = Math.min(30, retrySeconds * 2);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private long commitPage(GoAgentConfig.Node node, JsonNode response, long after) {
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
    WireJson.require(expected == next && next <= WireJson.counter(response.path("state"), "cursor")
        && (!response.path("hasMore").asBoolean() || !observations.isEmpty()),
        "Invalid page cursor");
    if (observations.isEmpty()) {
      return next;
    }
    storage.write(stores -> {
      WireJson.require(sqlite.effects().committedCursor(node.name(), journalScope(node)) == after,
          "Concurrent observation consumer");
      for (JsonNode observation : observations) {
        observe(stores, node, observation);
      }
      return null;
    });
    return next;
  }

  private void acknowledge(GoAgentConfig.Node node, long next)
      throws IOException, InterruptedException {
    ObjectNode ack = WireJson.base("ObservationAck").put("cluster", config.cluster())
        .put("incarnation", config.incarnation()).put("node", node.name())
        .put("journal", node.journal()).put("committedCursor", Long.toString(next));
    ACK_REQUESTS.incrementAndGet();
    client.request(node, "/v1/ack", ack, epoch, session);
    storage.write(stores -> {
      sqlite.effects().pruneReceipts(node.name(), journalScope(node));
      return null;
    });
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
        dispatchStopsAfterFailure(node);
        LOG.warn("Agent {} exchange failed; its reservations are retained: {}",
            node.name(), e.toString());
      }
    }
  }

  private String journalScope(GoAgentConfig.Node node) {
    return config.journalScope(node);
  }

  private void poll(GoAgentConfig.Node node, boolean advertise) throws Exception {
    client.request(node, "/v1/session", WireJson.object().put("schedulerEpoch", epoch)
        .put("session", session), epoch, session);
    long after = storage.read(stores -> sqlite.effects().committedCursor(
        node.name(), journalScope(node)));
    for (int page = 0; page < 4; page++) {
      INVENTORY_REQUESTS.incrementAndGet();
      JsonNode response = client.request(node, "/v1/state?afterCursor=" + after + "&limit=128",
          null, epoch, session);
      validateState(node, response);
      long next = commitPage(node, response, after);
      acknowledge(node, next);
      after = next;
      if (!response.path("hasMore").asBoolean()) {
        retirement.retainHistory(node);
        if (advertise) {
          offer(node, response.path("state").path("attempts"));
        }
        return;
      }
    }
    throw new IOException("Observation backlog not drained");
  }

  private void validateAuthority(GoAgentConfig.Node node, JsonNode response) {
    JsonNode actual = response.path("config");
    WireJson.require(config.cluster().equals(actual.path("cluster").asText())
        && config.incarnation().equals(actual.path("incarnation").asText())
        && epoch.equals(actual.path("schedulerEpoch").asText())
        && session.equals(actual.path("session").asText())
        && "scheduler".equals(actual.path("peer").asText())
        && node.cpuMillis() == actual.path("cpuMillis").asLong()
        && node.memoryBytes() == actual.path("memoryBytes").asLong(), "Agent enrollment differs");
    node.target().properties().forEach(entry -> WireJson.require(
        entry.getValue().equals(actual.path(entry.getKey())), "Agent target differs"));
  }

  private void validateState(GoAgentConfig.Node node, JsonNode response) {
    validateAuthority(node, response);
    JsonNode state = response.path("state");
    WireJson.require(
        state.path("observations").isArray() && state.path("observations").size() <= 128
            && state.path("attempts").isObject() && state.path("attempts").size() <= 128
            && state.path("commands").isObject() && state.path("commands").size() <= 1024
            && response.path("hasMore").isBoolean(), "Invalid bounded agent inventory");
  }

  private void validateObservation(GoAgentConfig.Node node, JsonNode observation) {
    WireJson.fields(observation, "version", "kind", "identity", "source", "sequence", "cursor",
        "state", "ready", "cleanup", "reason");
    WireJson.require("native-v1alpha1".equals(observation.path("version").asText())
        && "Observation".equals(observation.path("kind").asText())
        && node.target().equals(observation.path("source"))
        && observation.path("ready").isBoolean()
        && Set.of("running", "succeeded", "failed", "lost", "stopped", "unknown")
            .contains(observation.path("state").asText())
        && Set.of("pending", "complete", "unknown").contains(observation.path("cleanup").asText()),
        "Invalid observation");
    WireJson.require(!observation.has("reason")
        || Set.of("health-startup-timeout", "health-check-failed", "health-exited-before-ready")
            .contains(observation.path("reason").asText()), "Invalid health reason");
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
    if (observation.path("identity").has("ticket")) {
      if ("complete".equals(observation.path("cleanup").asText())
          && Set.of("succeeded", "failed", "lost", "stopped")
              .contains(observation.path("state").asText())) {
        sqlite.effects().completeTicket(node.name(),
            WireJson.counter(observation.path("identity"), "ticket"));
      }
      sqlite.effects().pruneReceipts(attemptScope, journalScope(node));
    }
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
          Optional.of("Go agent: " + observation.path("state").asText()
              + (observation.has("reason") ? ": " + observation.path("reason").asText() : "")));
    }
  }

  private void offer(GoAgentConfig.Node node, JsonNode inventory) {
    storage.write(stores -> {
      if (!sqlite.effects().hasTicketCapacity(node.name())) {
        cancelOffer(node);
        return null;
      }
      Set<String> reserved = new HashSet<>();
      Set<String> occupiedPorts = new HashSet<>();
      for (JsonNode attempt : inventory) {
        WireJson.require(attempt.path("reserved").isBoolean(), "Incomplete reservation inventory");
        if (attempt.path("reserved").asBoolean()) {
          Optional<Command> run = runFor(attempt.path("identity"));
          if (run.isEmpty() || !node.name().equals(run.get().agentId())) {
            cancelOffer(node);
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
          GoTaskFactory.healthSocket(task.getAssignedTask().getTask())
              .ifPresent(occupiedPorts::add);
          reserved.remove(task.getAssignedTask().getTaskId());
        }
      }
      if (!reserved.isEmpty()) {
        cancelOffer(node);
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
      var current = offers.get().get(node.name());
      if (current.isPresent() && current.get().getResourceBag(false).equals(available)
          && current.get().getOffer() instanceof AgentOffer prior
          && prior.occupiedPorts().equals(occupiedPorts)
          && System.nanoTime() - offeredAt.getOrDefault(node.name(), 0L)
              < TimeUnit.MINUTES.toNanos(1)) {
        return null;
      }
      cancelOffer(node);
      offers.get().add(new HostOffer(
          new AgentOffer(node.name(), "offer-" + UUID.randomUUID(), available,
              Set.copyOf(occupiedPorts)), attributes));
      offeredAt.put(node.name(), System.nanoTime());
      return null;
    });
  }

  private void cancelOffer(GoAgentConfig.Node node) {
    offers.get().get(node.name()).ifPresent(offer -> offers.get().cancel(offer.getOfferId()));
  }

  private record AgentOffer(String agent, String id, ResourceBag resources,
                            Set<String> occupiedPorts) implements ExecutionOffer.TaskAware {
    @Override
    public Optional<String> placementVeto(
        org.apache.aurora.scheduler.storage.entities.ITaskConfig task) {
      return GoTaskFactory.healthSocket(task).filter(occupiedPorts::contains)
          .map(socket -> "health TCP port " + socket + " is reserved");
    }

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
    long ticket = sqlite.effects().allocateTicket(launch.agentId(), launch.taskId());
    if (ticket != 0) {
      try {
        ObjectNode retained = (ObjectNode) WireJson.parse(body);
        ((ObjectNode) retained.path("identity")).put("ticket", Long.toString(ticket));
        body = WireJson.bytes(retained);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid launch body", e);
      }
    }
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
    if (sqlite.effects().retiringTask(taskId)) {
      return;
    }
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

  private Optional<Command> selectCommand(GoAgentConfig.Node node, boolean stopsOnly) {
    return storage.write(stores -> {
      Optional<Command> selected = Optional.empty();
      for (int count = 0; count < 16 && isRunning() && selected.isEmpty(); count++) {
        // A Run can wait for reservation capacity. Stops must still reach the agent to release
        // that capacity, even when their intent was committed after the blocked Run.
        var next = sqlite.effects().pendingStops(node.name(), 1).stream().findFirst();
        if (next.isEmpty() && !stopsOnly) {
          next = sqlite.effects().pending(node.name(), 1).stream().findFirst();
        }
        if (next.isEmpty()) {
          return Optional.empty();
        }
        Command command = next.get().command();
        var task = stores.getTaskStore().fetchTask(command.taskId());
        boolean canceledRun = "Run".equals(command.type()) && (task.isEmpty() || !Set.of(
                ScheduleStatus.ASSIGNED, ScheduleStatus.STARTING, ScheduleStatus.RUNNING)
            .contains(task.get().getStatus()));
        if (canceledRun) {
          enqueueStop(command.taskId());
        } else {
          selected = Optional.of(command);
        }
      }
      return selected;
    });
  }

  private void dispatchStopsAfterFailure(GoAgentConfig.Node node) {
    if (!isRunning() || Thread.currentThread().isInterrupted()) {
      return;
    }
    try {
      if (storage.read(stores -> sqlite.effects().pendingStops(node.name(), 1).isEmpty())) {
        return;
      }
      dispatch(node, true);
    } catch (RuntimeException failure) {
      LOG.warn("Agent {} Stop dispatch unavailable: {}", node.name(), failure.toString());
    }
  }

  private void dispatch(GoAgentConfig.Node node) {
    dispatch(node, false);
  }

  private void dispatch(GoAgentConfig.Node node, boolean stopsOnly) {
    for (int count = 0; count < 16; count++) {
      Optional<Command> selected = selectCommand(node, stopsOnly);
      if (selected.isEmpty() || !isRunning()) {
        return;
      }
      Command command = selected.get();
      ObjectNode delivery = WireJson.base("Delivery")
          .put("bodySha256", WireJson.hash(command.payload()));
      delivery.set("body", parse(command));
      delivery.set("authority", WireJson.object().put("cluster", config.cluster())
          .put("incarnation", config.incarnation()).put("schedulerEpoch", epoch)
          .put("session", session));
      try {
        // No SQLite lock spans network I/O. A concurrent Stop durably supersedes this Run.
        // The agent retains Stop tombstones: late Runs cannot resurrect stopped attempts,
        // and an already accepted Run remains reserved until cleanup is observed.
        COMMAND_REQUESTS.incrementAndGet();
        JsonNode result = client.request(node, "/v1/deliver", delivery, epoch, session);
        storage.write(stores -> {
          if (sqlite.effects().command(command.id()).isEmpty()
              && retirement.retiredCommand(command)) {
            return null; // A superseded in-flight reply arrived after durable retirement.
          }
          WireJson.require(command.id().equals(result.path("command").asText())
              && WireJson.hash(command.payload()).equals(result.path("bodySha256").asText()),
              "Command receipt differs");
          String outcome = WireJson.text(result, "outcome");
          if (!"accepted".equals(outcome)) {
            WireJson.require("Run".equals(command.type()) && Set.of("rejected-capacity",
                "rejected-capability", "rejected-socket", "rejected-stopped").contains(outcome),
                "Command rejection has an ambiguous execution outcome");
          }
          WireJson.require(sqlite.effects().command(command.id()).filter(command::equals)
              .isPresent(), "Durable command differs from delivery");
          if (!sqlite.effects().isPending(command.id())) {
            return null; // A committed Stop or another receipt already superseded this delivery.
          }
          if (!"accepted".equals(outcome)) {
            var task = stores.getTaskStore().fetchTask(command.taskId());
            if (task.isPresent() && Set.of(
                ScheduleStatus.ASSIGNED, ScheduleStatus.STARTING, ScheduleStatus.RUNNING)
                .contains(task.get().getStatus())) {
              stateManager.get().changeState(stores, command.taskId(), Optional.empty(),
                  ScheduleStatus.LOST, Optional.of("Go agent refused launch: " + outcome));
            }
            // A definite admission refusal creates no execution or terminal observation.
            // A stopped attempt can still be cleaning up; await its terminal observation.
            JsonNode identity = parse(command).path("identity");
            if (!"rejected-stopped".equals(outcome) && identity.has("ticket")) {
              sqlite.effects().completeTicket(node.name(), WireJson.counter(identity, "ticket"));
            }
          }
          sqlite.effects().acknowledge(command.id());
          return null;
        });
      } catch (IOException e) {
        LOG.warn("Command {} remains pending: {}", command.id(), e.toString());
        return;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
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
    // Placement can consume an offer without changing a task.
    inventories.values().forEach(inventory -> inventory.wake.release());
  }
  @Override
  public boolean reconcilesFromWatch() {
    return true;
  }

  @Override public void reconcileTasks(Collection<ReconciliationTarget> targets) {
    // Watch snapshots reconcile journals; a missed heartbeat does not imply task loss.
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
    closing = true;
    consumers.shutdownNow();
    client.close();
    config.nodes().forEach(this::cancelOffer);
  }
}
