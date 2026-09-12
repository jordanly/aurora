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
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
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
  private HostOffer currentOffer;
  private final List<PubsubEvent> registered = new ArrayList<>();
  private final List<ScheduleStatus> transitions = new ArrayList<>();
  private boolean failTransition;

  @Before
  public void setUp() {
    sqlite = SqliteStorage.open(temporary.getRoot().toPath().resolve("driver.db"));
    OfferManager offers = EasyMock.createMock(OfferManager.class);
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
    StateManager states = EasyMock.createMock(StateManager.class);
    expect(states.changeState(anyObject(), anyString(), anyObject(), anyObject(), anyObject()))
        .andAnswer(() -> {
          MutableStoreProvider stores = EasyMock.getCurrentArgument(0);
          String task = EasyMock.getCurrentArgument(1);
          ScheduleStatus next = EasyMock.getCurrentArgument(3);
          transitions.add(next);
          stores.getUnsafeTaskStore().mutateTask(task,
              original -> IScheduledTask.build(original.newBuilder().setStatus(next)));
          if (failTransition) {
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
    var task = TaskTestUtil.makeTask(TASK, TaskTestUtil.JOB).newBuilder();
    task.setStatus(ScheduleStatus.ASSIGNED);
    task.getAssignedTask().setSlaveId(NODE.name()).setSlaveHost(NODE.name());
    task.getAssignedTask().getTask().setResources(
        Set.of(Resource.numCpus(1), Resource.ramMb(1), Resource.diskMb(1)));
    stores.getUnsafeTaskStore().saveTasks(Set.of(IScheduledTask.build(task)));
  }

  private static ObjectNode identity() {
    return WireJson.object().put("attempt", GoTaskFactory.identity("a-", TASK));
  }

  private static GoTaskFactory.Launch launch() {
    ObjectNode body = WireJson.base("Run").put("command", GoTaskFactory.identity("r-", TASK));
    body.set("identity", identity());
    body.set("target", NODE.target());
    ObjectNode assignment = WireJson.object();
    assignment.set("stop", WireJson.object().put("graceMillis", 100));
    body.set("assignment", assignment);
    return new GoTaskFactory.Launch(TASK, NODE.name(), WireJson.string(body));
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
    private final List<JsonNode> deliveries = new ArrayList<>();
    private final List<JsonNode> observations = new ArrayList<>();
    private final List<Long> acknowledgments = new ArrayList<>();
    private boolean loseDeliveryAck;
    private boolean failSession;
    private boolean closed;
    private boolean reserved;
    private String outcome = "accepted";

    @Override
    public JsonNode request(GoAgentConfig.Node node, String path, JsonNode body,
                            String epoch, String session) throws IOException {
      if ("/v1/session".equals(path)) {
        if (failSession) {
          throw new IOException("Enrollment unavailable");
        }
        return WireJson.object();
      }
      if (path.startsWith("/v1/state?")) {
        long after = Long.parseLong(path.substring(path.indexOf('=') + 1, path.indexOf('&')));
        ObjectNode enrolled = NODE.target().put("cluster", "cluster")
            .put("incarnation", "incarnation").put("schedulerEpoch", epoch)
            .put("session", session).put("peer", "scheduler")
            .put("cpuMillis", NODE.cpuMillis()).put("memoryBytes", NODE.memoryBytes());
        ObjectNode state = WireJson.object().put("cursor", Integer.toString(observations.size()));
        var page = state.putArray("observations");
        observations.stream().filter(value -> WireJson.counter(value, "cursor") > after)
            .forEach(page::add);
        ObjectNode inventory = state.putObject("attempts");
        if (reserved || !observations.isEmpty()) {
          ObjectNode attempt = WireJson.object().put("reserved", reserved);
          attempt.set("identity", identity());
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
        assertEquals(acknowledged, cursor());
        acknowledgments.add(acknowledged);
        return WireJson.object();
      }
      if ("/v1/deliver".equals(path)) {
        assertTrue(sqlite.effects().command(body.path("body").path("command").asText())
            .isPresent());
        deliveries.add(body.deepCopy());
        if (loseDeliveryAck) {
          loseDeliveryAck = false;
          throw new IOException("Receipt lost after acceptance");
        }
        return WireJson.object().put("command", body.path("body").path("command").asText())
            .put("bodySha256", body.path("bodySha256").asText()).put("outcome", outcome);
      }
      throw new AssertionError("Unexpected endpoint: " + path);
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
