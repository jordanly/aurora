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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverRegistered;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.sqlite.SqliteEffects.Command;
import org.apache.aurora.scheduler.storage.sqlite.SqliteStorage;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Real SQLite transactions with an enrolled deterministic transport and SQL-mutating policy
 * stub. */
public class GoAgentDriverTest {
  private static final String TASK = "task-1";
  private static final String SCOPE = "incarnation/journal";
  private static final GoAgentConfig.Node NODE = new GoAgentConfig.Node("agent-1",
      URI.create("https://agent-1"), "journal", "boot", "runtime", 2000, 2097152, 2);

  @Rule
  public TemporaryFolder temporary = new TemporaryFolder();

  private SqliteStorage sqlite;
  private GoAgentDriver driver;
  private FakeAgent agent;
  private volatile HostOffer currentOffer;
  private OfferManager offers;
  private StateManager states;
  private final List<PubsubEvent> registered = new java.util.concurrent.CopyOnWriteArrayList<>();
  private final List<ScheduleStatus> transitions =
      new java.util.concurrent.CopyOnWriteArrayList<>();
  private volatile boolean failTransition;
  private final java.util.concurrent.CountDownLatch transitionFailed =
      new java.util.concurrent.CountDownLatch(1);

  @Before
  public void setUp() {
    sqlite = SqliteStorage.open(temporary.getRoot().toPath().resolve("driver.db"));
    offers = EasyMock.createMock(OfferManager.class);
    expect(offers.get(anyString())).andAnswer(() -> Optional.ofNullable(currentOffer)).anyTimes();
    expect(offers.cancel(anyString())).andAnswer(() -> {
      currentOffer = null;
      return true;
    }).anyTimes();
    offers.add(anyObject());
    expectLastCall().andAnswer(() -> {
      currentOffer = EasyMock.getCurrentArgument(0);
      return null;
    }).anyTimes();
    states = EasyMock.createMock(StateManager.class);
    expect(states.changeState(anyObject(), anyString(), anyObject(), anyObject(), anyObject()))
        .andAnswer(() -> {
          MutableStoreProvider stores = EasyMock.getCurrentArgument(0);
          String task = EasyMock.getCurrentArgument(1);
          ScheduleStatus next = EasyMock.getCurrentArgument(3);
          boolean shouldFail = failTransition;
          transitions.add(next);
          stores.getUnsafeTaskStore().mutateTask(task,
              original -> IScheduledTask.build(original.newBuilder().setStatus(next)));
          if (shouldFail) {
            transitionFailed.countDown();
            throw new IllegalStateException("Policy failed after mutating task");
          }
          return StateChangeResult.SUCCESS;
        }).anyTimes();
    EasyMock.replay(offers, states);
    agent = new FakeAgent();
    GoAgentConfig config = new GoAgentConfig("cluster", "incarnation", null, null, "", null, "",
        List.of(NODE));
    driver = new GoAgentDriver(config, sqlite, sqlite, () -> offers, () -> states,
        event -> {
          assertTrue(agent.acknowledgments.contains(0L));
          registered.add(event);
        }, agent, false);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (driver.state() != com.google.common.util.concurrent.Service.State.FAILED) {
        driver.stopAsync().awaitTerminated(10, TimeUnit.SECONDS);
      }
    } finally {
      sqlite.close();
    }
  }

  private void start() throws Exception {
    driver.startAsync().awaitRunning(10, TimeUnit.SECONDS);
    assertEquals(1, registered.size());
    assertTrue(registered.get(0) instanceof DriverRegistered);
  }

  @Test
  public void testRolledBackLaunchNeverDispatches() throws Exception {
    start();
    try {
      sqlite.write("rolled-back-launch", stores -> {
        assign(stores);
        driver.launch("offer", launch(), 0);
        throw new IllegalArgumentException("rollback");
      });
      fail("Expected rollback");
    } catch (IllegalArgumentException expected) {
      assertEquals("rollback", expected.getMessage());
    }
    driver.tick();
    assertTrue(agent.deliveries.isEmpty());
    assertEquals(0, pending());
    assertFalse(sqlite.isCommitted("rolled-back-launch"));
    assertFalse(sqlite.read(stores -> stores.getTaskStore().fetchTask(TASK).isPresent()));
  }

  @Test
  public void testLostAckReplaysExactRunAfterRunningObservation() throws Exception {
    start();
    commitLaunch();
    agent.loseDeliveryAck = true;
    driver.tick();
    assertEquals(1, pending());
    JsonNode first = agent.deliveries.get(0);
    agent.observations.add(observation(1, "running", "pending", true));
    driver.tick();
    assertEquals(ScheduleStatus.RUNNING, status());
    assertEquals(0, pending());
    assertEquals(2, agent.deliveries.size());
    assertEquals(first, agent.deliveries.get(1));
    assertEquals("Run", first.path("body").path("kind").asText());
    assertEquals(List.of(ScheduleStatus.RUNNING), transitions);
    driver.tick();
    assertEquals(List.of(ScheduleStatus.RUNNING), transitions);
  }

  @Test
  public void testHealthPortReservedThroughDeliveryAndCleanup() throws Exception {
    start();
    commitLaunch();
    String profile = "{\"version\":\"aurora-process-v1\",\"argv\":[\"/bin/sleep\",\"60\"],"
        + "\"env\":{},\"graceMillis\":100,\"health\":{\"kind\":\"tcp\",\"port\":8080,"
        + "\"network\":\"agent-container\",\"intervalMillis\":100,\"timeoutMillis\":100,"
        + "\"startupTimeoutMillis\":1000,\"failureThreshold\":3}}";
    sqlite.write(stores -> {
      stores.getUnsafeTaskStore().mutateTask(TASK, original -> {
        var task = original.newBuilder();
        task.getAssignedTask().getTask().setExecutorConfig(
            new org.apache.aurora.gen.ExecutorConfig("go-process", profile));
        return IScheduledTask.build(task);
      });
      return null;
    });
    var task = sqlite.read(stores -> stores.getTaskStore().fetchTask(TASK).orElseThrow()
        .getAssignedTask().getTask());
    agent.loseDeliveryAck = true;
    driver.tick();
    assertEquals(1, pending());
    assertTrue(((org.apache.aurora.scheduler.execution.ExecutionOffer.TaskAware)
        currentOffer.getOffer()).placementVeto(task).orElseThrow().contains("8080"));
    // Agent cleanup alone cannot free a scheduler-active task's socket.
    agent.reserved = false;
    driver.tick();
    assertTrue(((org.apache.aurora.scheduler.execution.ExecutionOffer.TaskAware)
        currentOffer.getOffer()).placementVeto(task).isPresent());
    sqlite.write(stores -> {
      stores.getUnsafeTaskStore().mutateTask(TASK, original ->
          IScheduledTask.build(original.newBuilder().setStatus(ScheduleStatus.FAILED)));
      return null;
    });
    agent.reserved = true;
    driver.tick();
    assertTrue(((org.apache.aurora.scheduler.execution.ExecutionOffer.TaskAware)
        currentOffer.getOffer()).placementVeto(task).isPresent());
    agent.reserved = false;
    driver.tick();
    assertTrue(((org.apache.aurora.scheduler.execution.ExecutionOffer.TaskAware)
        currentOffer.getOffer()).placementVeto(task).isEmpty());
  }

  @Test
  public void testHealthReadinessAndFailureWaitForCleanup() throws Exception {
    start();
    commitLaunch();
    driver.tick();
    agent.observations.add(observation(1, "running", "pending", false));
    driver.tick();
    assertEquals(ScheduleStatus.ASSIGNED, status());
    agent.observations.add(observation(2, "running", "pending", true));
    driver.tick();
    assertEquals(ScheduleStatus.RUNNING, status());
    agent.observations.add(observation(3, "running", "pending", false));
    driver.tick();
    assertEquals(ScheduleStatus.RUNNING, status());
    ObjectNode failure = (ObjectNode) observation(4, "failed", "pending", false);
    failure.put("reason", "health-check-failed");
    agent.observations.add(failure);
    driver.tick();
    assertEquals(ScheduleStatus.RUNNING, status());
    ObjectNode cleaned = (ObjectNode) observation(5, "failed", "complete", false);
    cleaned.put("reason", "health-check-failed");
    agent.observations.add(cleaned);
    driver.tick();
    assertEquals(ScheduleStatus.FAILED, status());
    assertEquals(List.of(ScheduleStatus.RUNNING, ScheduleStatus.FAILED), transitions);
  }

  @Test
  public void testHealthStartupFailureNeverBecomesRunning() throws Exception {
    start();
    commitLaunch();
    driver.tick();
    agent.observations.add(observation(1, "running", "pending", false));
    ObjectNode failed = (ObjectNode) observation(2, "failed", "complete", false);
    failed.put("reason", "health-startup-timeout");
    agent.observations.add(failed);
    driver.tick();
    assertEquals(ScheduleStatus.FAILED, status());
    assertEquals(List.of(ScheduleStatus.FAILED), transitions);
  }

  @Test
  public void testCancellationBeforeDispatchSendsOnlyStop() throws Exception {
    start();
    commitLaunch();
    sqlite.write(stores -> {
      driver.killTask(TASK);
      stores.getUnsafeTaskStore().mutateTask(TASK,
          original -> IScheduledTask.build(
              original.newBuilder().setStatus(ScheduleStatus.KILLING)));
      return null;
    });
    driver.tick();
    assertEquals(1, agent.deliveries.size());
    assertEquals("Stop", agent.deliveries.get(0).path("body").path("kind").asText());
    assertEquals(0, pending());
  }

  @Test
  public void testHealthyAgentRunAndStopDispatchBehindLargeUnavailableBacklog() throws Exception {
    startWatch();
    sqlite.write((NoResult.Quiet) stores -> {
      for (int i = 0; i < 1025; i++) {
        assertTrue(sqlite.effects().enqueue(new Command("unavailable-" + i, "unavailable-agent",
            "backlog-task-" + i, "Run", 1, new byte[0])));
      }
    });

    commitLaunch();
    notifyAssignment();
    eventually(() -> agent.deliveries.size() == 1
        && sqlite.read(stores -> sqlite.effects().pending(NODE.name(), 1).isEmpty()));
    assertEquals(1, agent.deliveries.size());
    assertEquals("Run", agent.deliveries.get(0).path("body").path("kind").asText());

    sqlite.write(stores -> {
      driver.killTask(TASK);
      stores.getUnsafeTaskStore().mutateTask(TASK, original -> IScheduledTask.build(
          original.newBuilder().setStatus(ScheduleStatus.KILLING)));
      return null;
    });
    notifyAssignment();
    eventually(() -> agent.deliveries.size() == 2
        && sqlite.read(stores -> sqlite.effects().pending(NODE.name(), 1).isEmpty()));
    assertEquals(2, agent.deliveries.size());
    assertEquals("Stop", agent.deliveries.get(1).path("body").path("kind").asText());
    assertEquals(1025, (int) sqlite.read(stores -> sqlite.effects().pending(2000).size()));
  }

  @Test
  public void testStopBypassesBackpressuredRunForSameAgent() throws Exception {
    startWatch();
    commitLaunch();
    notifyAssignment();
    eventually(() -> agent.deliveries.size() == 1 && pending() == 0);

    String waitingTask = "task-waiting-for-inventory";
    String waitingRun = GoTaskFactory.identity("r-", waitingTask);
    agent.backpressureRuns = true;
    sqlite.write(stores -> {
      assign(stores, NODE, waitingTask);
      driver.launch("waiting-offer", launch(NODE, waitingTask), 0);
      return null;
    });
    notifyAssignment();
    eventually(() -> agent.deliveries.stream().anyMatch(delivery ->
        waitingRun.equals(delivery.path("body").path("command").asText())));
    assertEquals(1, pending());

    // Place the Stop beyond the old 1024-row receipt window as well as the blocked Run.
    sqlite.write(stores -> {
      for (int i = 0; i < 1025; i++) {
        assertTrue(sqlite.effects().enqueue(new Command("same-agent-backlog-" + i,
            NODE.name(), "backlog-task-" + i, "Run", 1, new byte[0])));
      }
      driver.killTask(TASK);
      stores.getUnsafeTaskStore().mutateTask(TASK, original -> IScheduledTask.build(
          original.newBuilder().setStatus(ScheduleStatus.KILLING)));
      return null;
    });
    notifyAssignment();
    String stop = GoTaskFactory.identity("s-", TASK);
    eventually(() -> agent.deliveries.stream().anyMatch(delivery ->
        stop.equals(delivery.path("body").path("command").asText()))
        && sqlite.read(stores -> {
          var pending = sqlite.effects().pending(NODE.name(), 2000);
          return pending.size() == 1026 && pending.get(0).command().id().equals(waitingRun)
              && pending.stream().noneMatch(command -> command.command().id().equals(stop));
        }));
    assertEquals(ScheduleStatus.ASSIGNED, sqlite.read(stores ->
        stores.getTaskStore().fetchTask(waitingTask).orElseThrow().getStatus()));
    assertTrue(sqlite.read(stores -> sqlite.effects()
        .command(GoTaskFactory.identity("r-", TASK)).isPresent()));
    assertTrue(sqlite.read(stores -> sqlite.effects().command(waitingRun).isPresent()));

    // Remove synthetic backlog fixtures before allowing the valid pending Run through.
    sqlite.write(stores -> {
      for (int i = 0; i < 1025; i++) {
        sqlite.effects().acknowledge("same-agent-backlog-" + i);
      }
      return null;
    });
    assertEquals(1, pending());
    agent.backpressureRuns = false;
    notifyAssignment();
    eventually(() -> pending() == 0);
    assertTrue(transitions.isEmpty());
  }

  @Test
  public void testKillRetryOutsideTransactionIsIdempotent() throws Exception {
    start();
    driver.killTask("unknown-task");
    assertEquals(0, pending());
    commitLaunch();
    driver.killTask(TASK);
    driver.killTask(TASK);
    assertEquals(1, pending());
    driver.tick();
    assertEquals(1, agent.deliveries.size());
    assertEquals("Stop", agent.deliveries.get(0).path("body").path("kind").asText());
    driver.killTask(TASK);
    driver.tick();
    assertEquals(1, agent.deliveries.size());
    assertEquals(0, pending());
  }

  @Test
  public void testFailedStartupClosesTransportWithoutRegistration() throws Exception {
    agent.failSession = true;
    try {
      driver.startAsync().awaitRunning(10, TimeUnit.SECONDS);
      fail("Enrollment failure must prevent driver readiness");
    } catch (IllegalStateException expected) {
      assertTrue(driver.failureCause() instanceof IOException);
      assertEquals("Enrollment unavailable", driver.failureCause().getMessage());
    }
    assertTrue(agent.closed);
    assertTrue(registered.isEmpty());
    assertTrue(agent.deliveries.isEmpty());
  }

  @Test
  public void testObservationRollbackRetainsReceiptAndAckForRetry() throws Exception {
    start();
    commitLaunch();
    agent.observations.add(observation(1, "running", "pending", true));
    failTransition = true;
    driver.tick();
    assertEquals(ScheduleStatus.ASSIGNED, status());
    assertEquals(0L, cursor());
    assertEquals(List.of(0L), agent.acknowledgments);
    failTransition = false;
    driver.tick();
    assertEquals(ScheduleStatus.RUNNING, status());
    assertEquals(1L, cursor());
    assertEquals(List.of(0L, 1L), agent.acknowledgments);
    driver.tick();
    assertEquals(2, transitions.size()); // Failed mutation retried once, committed receipt deduped.
  }

  @Test
  public void testTerminalPendingCleanupRetainsCapacityUntilComplete() throws Exception {
    start();
    commitLaunch();
    agent.reserved = true;
    agent.observations.add(observation(1, "succeeded", "pending", false));
    driver.tick();
    assertEquals(ScheduleStatus.ASSIGNED, status());
    assertEquals(1.0, currentOffer.getResourceBag(false).valueOf(ResourceType.CPUS), 0.0);
    agent.observations.add(observation(2, "succeeded", "complete", false));
    agent.reserved = false;
    driver.tick();
    assertEquals(ScheduleStatus.FINISHED, status());
    assertEquals(2.0, currentOffer.getResourceBag(false).valueOf(ResourceType.CPUS), 0.0);
  }

  @Test
  public void testDefiniteCapacityRejectionCommitsLostAndAcknowledgment() throws Exception {
    start();
    commitLaunch();
    agent.outcome = "rejected-capacity";
    driver.tick();
    assertEquals(ScheduleStatus.LOST, status());
    assertEquals(List.of(ScheduleStatus.LOST), transitions);
    assertEquals(0, pending());
  }

  @Test
  public void testAmbiguousRejectionRetainsIntentAndFailsClosed() throws Exception {
    start();
    commitLaunch();
    List<Throwable> failures = new ArrayList<>();
    sqlite.setWriteFailureHandler(failures::add);
    agent.outcome = "rejected-attempt-exists";
    driver.tick();
    assertEquals(ScheduleStatus.ASSIGNED, status());
    assertEquals(1, pending());
    assertEquals(1, failures.size());
    assertTrue(failures.get(0).getMessage().contains("ambiguous execution outcome"));
    assertTrue(transitions.isEmpty());
  }

  private void startWatch() throws Exception {
    startWatch(List.of(NODE), agent);
  }

  private void startWatch(List<GoAgentConfig.Node> nodes, AgentTransport transport)
      throws Exception {
    GoAgentConfig config = new GoAgentConfig("cluster", "incarnation", null, null, "", null, "",
        nodes);
    driver = new GoAgentDriver(config, sqlite, sqlite, () -> offers, () -> states,
        registered::add, transport, true);
    start();
  }

  private static void eventually(java.util.function.BooleanSupplier condition) throws Exception {
    eventually(10, condition);
  }

  private static void eventually(long seconds, java.util.function.BooleanSupplier condition)
      throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue(condition.getAsBoolean());
  }

  private void notifyAssignment() {
    driver.taskChanged(PubsubEvent.TaskStateChange.initialized(
        sqlite.read(stores -> stores.getTaskStore().fetchTask(TASK).orElseThrow())));
  }

  @Test
  public void testWatchIdleHeartbeatAndCommittedLocalAssignment() throws Exception {
    startWatch();
    eventually(() -> currentOffer != null);
    assertEquals(List.of("/v1/session", "/v1/ack"), agent.requests);
    ObjectNode heartbeat = WireJson.object().put("kind", "heartbeat").put("nextCursor", "0");
    heartbeat.set("config", agent.frame("snapshot", 0).path("config"));
    agent.frames.add(heartbeat);
    Thread.sleep(1200);
    assertEquals(List.of("/v1/session", "/v1/ack"), agent.requests);
    commitLaunch();
    notifyAssignment();
    eventually(() -> agent.deliveries.size() == 1 && pending() == 0 && currentOffer != null
        && currentOffer.getResourceBag(false).valueOf(ResourceType.CPUS) == 1.0);
    assertEquals(0, pending());
  }

  @Test
  public void testWatchReconnectUsesCommittedCursorWithoutDuplicateTransition() throws Exception {
    startWatch();
    commitLaunch();
    notifyAssignment();
    eventually(() -> pending() == 0);
    agent.observations.add(observation(1, "running", "pending", true));
    agent.frames.add(agent.frame("delta", 0));
    eventually(() -> agent.acknowledgments.contains(1L));
    agent.frames.add(new IOException("stream lost after committed receipt"));
    eventually(() -> agent.watchCursors.size() == 2);
    eventually(() -> agent.acknowledgments.size() == 3);
    assertEquals(List.of(0L, 1L), agent.watchCursors);
    assertEquals(List.of(ScheduleStatus.RUNNING), transitions);
  }

  @Test
  public void testWatchSnapshotReplacesCompletedHistoryBeforeNextReservation() throws Exception {
    startWatch();
    commitLaunch();
    notifyAssignment();
    eventually(() -> pending() == 0);

    JsonNode oldSnapshot = agent.frame("snapshot", 0);
    ObjectNode oldAttempts = (ObjectNode) oldSnapshot.path("state").path("attempts");
    for (int i = 0; i < 128; i++) {
      oldAttempts.set("completed-" + i, WireJson.object().put("reserved", false));
    }
    agent.frames.add(oldSnapshot);
    agent.reserved = true;
    agent.observations.add(observation(1, "running", "pending", true));
    agent.frames.add(agent.frame("snapshot", 0));

    eventually(() -> agent.acknowledgments.contains(1L) && currentOffer != null
        && currentOffer.getResourceBag(false).valueOf(ResourceType.CPUS) == 1.0);
    assertEquals(List.of(0L), agent.watchCursors);
    assertEquals(ScheduleStatus.RUNNING, status());
    assertEquals(List.of(ScheduleStatus.RUNNING), transitions);
  }

  @Test
  public void testWatchRollbackDoesNotAcknowledgeFailedObservation() throws Exception {
    startWatch();
    commitLaunch();
    notifyAssignment();
    eventually(() -> pending() == 0);
    failTransition = true;
    agent.observations.add(observation(1, "running", "pending", true));
    agent.frames.add(agent.frame("delta", 0));
    assertTrue(transitionFailed.await(2, TimeUnit.SECONDS));
    assertEquals(0L, cursor());
    assertEquals(List.of(0L), agent.acknowledgments);
    failTransition = false;
    eventually(() -> agent.acknowledgments.contains(1L));
    assertEquals(List.of(0L, 0L), agent.watchCursors);
    assertEquals(ScheduleStatus.RUNNING, status());
  }

  @Test
  public void testBlockedDeliveryDoesNotBlockOtherAgentDispatchOrObservation() throws Exception {
    GoAgentConfig.Node second = new GoAgentConfig.Node("agent-2", URI.create("https://agent-2"),
        "journal-2", "boot", "runtime", 2000, 2097152, 2);
    FakeAgent other = new FakeAgent(second, "task-2");
    AgentTransport router = new AgentTransport() {
      private FakeAgent target(GoAgentConfig.Node node) {
        return node.name().equals(NODE.name()) ? agent : other;
      }
      @Override
      public JsonNode request(GoAgentConfig.Node node, String path, JsonNode body,
                              String epoch, String session) throws IOException {
        return target(node).request(node, path, body, epoch, session);
      }
      @Override
      public Watch watch(GoAgentConfig.Node node, long after, String epoch, String session)
          throws IOException {
        return target(node).watch(node, after, epoch, session);
      }
      @Override public void close() {
        agent.close();
        other.close();
      }
    };
    startWatch(List.of(NODE, second), router);
    agent.blockRun = true;
    commitLaunch();
    notifyAssignment();
    assertTrue(agent.deliveryStarted.await(2, TimeUnit.SECONDS));
    try {
      java.util.concurrent.CompletableFuture.runAsync(() -> {
        sqlite.write(stores -> {
          assign(stores, second, "task-2");
          driver.launch("offer-2", launch(second, "task-2"), 0);
          return null;
        });
        notifyAssignment();
      }).get(2, TimeUnit.SECONDS);
      eventually(2, () -> other.deliveries.size() == 1);
      ObjectNode observation = (ObjectNode) observation(1, "running", "pending", true);
      observation.set("identity", identity("task-2"));
      observation.set("source", second.target());
      other.observations.add(observation);
      other.frames.add(other.frame("delta", 0));
      eventually(2, () -> other.acknowledgments.contains(1L));
      assertEquals(ScheduleStatus.RUNNING, sqlite.read(stores -> stores.getTaskStore()
          .fetchTask("task-2").orElseThrow().getStatus()));
      assertEquals(1L, agent.resumeDelivery.getCount());
      assertEquals(1, pending());
    } finally {
      agent.resumeDelivery.countDown();
    }
  }

  @Test
  public void testCancellationSupersedesLateAcceptedRunReceipt() throws Exception {
    cancellationDuringDelivery("accepted");
  }

  @Test
  public void testCancellationSupersedesLateStoppedRunRejection() throws Exception {
    cancellationDuringDelivery("rejected-stopped");
  }

  private void cancellationDuringDelivery(String lateOutcome) throws Exception {
    start();
    commitLaunch();
    agent.blockRun = true;
    var delivery = java.util.concurrent.CompletableFuture.runAsync(driver::tick);
    assertTrue(agent.deliveryStarted.await(2, TimeUnit.SECONDS));
    try {
      java.util.concurrent.CompletableFuture.runAsync(() -> sqlite.write(stores -> {
        driver.killTask(TASK);
        stores.getUnsafeTaskStore().mutateTask(TASK, original -> IScheduledTask.build(
            original.newBuilder().setStatus(ScheduleStatus.KILLING)));
        return null;
      })).get(2, TimeUnit.SECONDS);
      // The Stop can be delivered while the earlier Run exchange is still unresolved.
      driver.tick();
      assertEquals("Stop", agent.deliveries.get(1).path("body").path("kind").asText());
      assertEquals(0, pending());
      agent.outcome = lateOutcome;
      agent.resumeDelivery.countDown();
      delivery.get(2, TimeUnit.SECONDS);
      assertEquals(ScheduleStatus.KILLING, status());
      assertTrue(transitions.isEmpty());
      assertEquals(0, pending());
      assertEquals(2, agent.deliveries.size());
    } finally {
      agent.resumeDelivery.countDown();
      delivery.get(2, TimeUnit.SECONDS);
    }
  }

  private int pending() {
    return sqlite.read(stores -> sqlite.effects().pending(100).size());
  }

  private long cursor() {
    return sqlite.read(stores -> sqlite.effects().committedCursor(NODE.name(), SCOPE));
  }

  private ScheduleStatus status() {
    return sqlite.read(stores -> stores.getTaskStore().fetchTask(TASK).orElseThrow().getStatus());
  }

  private void commitLaunch() {
    sqlite.write("launch", stores -> {
      assign(stores);
      driver.launch("offer", launch(), 0);
      assertTrue(agent.deliveries.isEmpty());
      return null;
    });
    assertTrue(sqlite.isCommitted("launch"));
  }

  private static void assign(MutableStoreProvider stores) {
    assign(stores, NODE, TASK);
  }

  private static void assign(MutableStoreProvider stores, GoAgentConfig.Node node, String taskId) {
    var task = TaskTestUtil.makeTask(taskId, TaskTestUtil.JOB).newBuilder();
    task.setStatus(ScheduleStatus.ASSIGNED);
    task.getAssignedTask().setSlaveId(node.name()).setSlaveHost(node.name());
    task.getAssignedTask().getTask().setResources(
        Set.of(Resource.numCpus(1), Resource.ramMb(1), Resource.diskMb(1)));
    stores.getUnsafeTaskStore().saveTasks(Set.of(IScheduledTask.build(task)));
  }

  private static ObjectNode identity() {
    return identity(TASK);
  }

  private static ObjectNode identity(String taskId) {
    return WireJson.object().put("attempt", GoTaskFactory.identity("a-", taskId));
  }

  private static GoTaskFactory.Launch launch() {
    return launch(NODE, TASK);
  }

  private static GoTaskFactory.Launch launch(GoAgentConfig.Node node, String taskId) {
    ObjectNode body = WireJson.base("Run").put("command", GoTaskFactory.identity("r-", taskId));
    body.set("identity", identity(taskId));
    body.set("target", node.target());
    ObjectNode assignment = WireJson.object();
    assignment.set("stop", WireJson.object().put("graceMillis", 100));
    body.set("assignment", assignment);
    return new GoTaskFactory.Launch(taskId, node.name(), WireJson.string(body));
  }

  private static JsonNode observation(long sequence, String state, String cleanup, boolean ready) {
    ObjectNode body = WireJson.base("Observation").put("sequence", Long.toString(sequence))
        .put("cursor", Long.toString(sequence)).put("state", state).put("cleanup", cleanup)
        .put("ready", ready);
    body.set("identity", identity());
    body.set("source", NODE.target());
    return body;
  }

  private final class FakeAgent implements AgentTransport {
    private final GoAgentConfig.Node agentNode;
    private final String agentTask;
    private final java.util.concurrent.CountDownLatch deliveryStarted =
        new java.util.concurrent.CountDownLatch(1);
    private final java.util.concurrent.CountDownLatch resumeDelivery =
        new java.util.concurrent.CountDownLatch(1);
    private volatile boolean blockRun;
    private volatile boolean backpressureRuns;

    FakeAgent() {
      this(NODE, TASK);
    }

    FakeAgent(GoAgentConfig.Node agentNode, String agentTask) {
      this.agentNode = agentNode;
      this.agentTask = agentTask;
    }
    private final List<JsonNode> deliveries = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final List<JsonNode> observations = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final List<Long> acknowledgments = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final java.util.concurrent.BlockingQueue<Object> frames =
        new java.util.concurrent.LinkedBlockingQueue<>();
    private final List<Long> watchCursors = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final List<String> requests = new java.util.concurrent.CopyOnWriteArrayList<>();
    private volatile String watchEpoch;
    private volatile String watchSession;
    private boolean loseDeliveryAck;
    private boolean failSession;
    private boolean closed;
    private boolean reserved;
    private String outcome = "accepted";

    @Override
    public JsonNode request(GoAgentConfig.Node node, String path, JsonNode body,
                            String epoch, String session) throws IOException {
      if (!path.startsWith("/watch-state?")) {
        requests.add(path);
      }
      if ("/v1/session".equals(path)) {
        if (failSession) {
          throw new IOException("Enrollment unavailable");
        }
        return WireJson.object();
      }
      if (path.startsWith("/v1/state?") || path.startsWith("/watch-state?")) {
        long after = Long.parseLong(path.substring(path.indexOf('=') + 1, path.indexOf('&')));
        ObjectNode enrolled = agentNode.target().put("cluster", "cluster")
            .put("incarnation", "incarnation").put("schedulerEpoch", epoch)
            .put("session", session).put("peer", "scheduler")
            .put("cpuMillis", agentNode.cpuMillis()).put("memoryBytes", agentNode.memoryBytes());
        ObjectNode state = WireJson.object().put("cursor", Integer.toString(observations.size()));
        var page = state.putArray("observations");
        observations.stream().filter(value -> WireJson.counter(value, "cursor") > after)
            .forEach(page::add);
        ObjectNode inventory = state.putObject("attempts");
        if (reserved || !observations.isEmpty()) {
          ObjectNode attempt = WireJson.object().put("reserved", reserved);
          attempt.set("identity", identity(agentTask));
          inventory.set("attempt", attempt);
        }
        state.putObject("commands");
        ObjectNode response = WireJson.object().put("nextCursor",
                Integer.toString(observations.size()))
            .put("hasMore", false);
        response.set("state", state);
        response.set("config", enrolled);
        return response;
      }
      if ("/v1/ack".equals(path)) {
        // This API rejects active transaction contexts, proving ACK is outside the mutation.
        assertFalse(sqlite.isCommitted("unrelated-operation"));
        long acknowledged = WireJson.counter(body, "committedCursor");
        assertEquals(acknowledged, (long) sqlite.read(stores -> sqlite.effects().committedCursor(
            agentNode.name(), "incarnation/" + agentNode.journal())));
        acknowledgments.add(acknowledged);
        return WireJson.object();
      }
      if ("/v1/deliver".equals(path)) {
        assertFalse(sqlite.isCommitted("delivery-must-not-hold-a-write"));
        assertTrue(sqlite.read(stores -> sqlite.effects()
            .command(body.path("body").path("command").asText()).isPresent()));
        deliveries.add(body.deepCopy());
        if (backpressureRuns && "Run".equals(body.path("body").path("kind").asText())) {
          throw new IOException("HTTP 503: reservation inventory full");
        }
        if (blockRun && "Run".equals(body.path("body").path("kind").asText())) {
          blockDelivery();
        }
        if (loseDeliveryAck) {
          loseDeliveryAck = false;
          throw new IOException("Receipt lost after acceptance");
        }
        return WireJson.object().put("command", body.path("body").path("command").asText())
            .put("bodySha256", body.path("bodySha256").asText()).put("outcome",
                "Run".equals(body.path("body").path("kind").asText()) ? outcome : "accepted");
      }
      throw new AssertionError("Unexpected endpoint: " + path);
    }

    private void blockDelivery() throws IOException {
      deliveryStarted.countDown();
      try {
        if (!resumeDelivery.await(10, TimeUnit.SECONDS)) {
          throw new IOException("Test delivery was not released");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    private JsonNode frame(String kind, long after) throws IOException {
      JsonNode value = request(agentNode, "/watch-state?afterCursor=" + after + "&limit=128", null,
          watchEpoch, watchSession);
      ((ObjectNode) value).put("kind", kind);
      return value;
    }

    @Override
    public Watch watch(GoAgentConfig.Node node, long after, String epoch, String session)
        throws IOException {
      watchEpoch = epoch;
      watchSession = session;
      watchCursors.add(after);
      JsonNode initial = frame("snapshot", after);
      return new Watch() {
        private boolean first = true;
        @Override
        public JsonNode next() throws IOException, InterruptedException {
          if (first) {
            first = false;
            return initial;
          }
          Object next = frames.take();
          if (next instanceof IOException failure) {
            throw failure;
          }
          return (JsonNode) next;
        }
        @Override
        public void close() {
          // This in-memory stream owns no external resources.
        }
      };
    }

    @Override
    public void close() {
      resumeDelivery.countDown();
      closed = true;
    }
  }
}
