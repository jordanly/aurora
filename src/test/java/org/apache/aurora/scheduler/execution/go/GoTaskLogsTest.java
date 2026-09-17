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
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.sqlite.SqliteEffects.Command;
import org.apache.aurora.scheduler.storage.sqlite.SqliteStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GoTaskLogsTest {
  private static final GoAgentConfig.Node NODE = new GoAgentConfig.Node("agent-a",
      URI.create("https://agent-a"), "journal", "boot", "runtime", 2000, 2097152, 2);
  @Rule
  public TemporaryFolder temporary = new TemporaryFolder();
  private SqliteStorage sqlite;
  private GoAgentDriver driver;
  private final LogTransport transport = new LogTransport();

  @Before
  public void setUp() {
    sqlite = SqliteStorage.open(temporary.getRoot().toPath().resolve("logs.db"));
    var config = new GoAgentConfig("cluster", "incarnation", null, null, "", null, "",
        List.of(NODE));
    driver = new GoAgentDriver(config, sqlite, sqlite, () -> null, () -> null,
        event -> { }, transport, false);
  }

  @After
  public void tearDown() {
    driver.stopAsync();
    sqlite.close();
  }

  private void saveRun(String agent, ObjectNode target) {
    ObjectNode identity = WireJson.object().put("cluster", "cluster")
        .put("incarnation", "incarnation").put("instance", "i-0")
        .put("attempt", "a-1").put("run", "run-1").put("process", "main");
    identity.set("jobKey", WireJson.object().put("role", "role").put("environment", "test")
        .put("name", "job"));
    ObjectNode body = WireJson.base("Run");
    body.set("identity", identity);
    body.set("target", target);
    sqlite.write((NoResult<RuntimeException>) stores -> sqlite.effects().enqueue(new Command(
        GoTaskFactory.identity("r-", "task-1"), agent, "task-1", "Run", 1,
        WireJson.bytes(body))));
    identity.remove(List.of("run", "process"));
    transport.attempt = WireJson.hash(WireJson.bytes(identity));
  }

  @Test
  public void testDurableRunRoutesBoundedUnicodeOutputOutsideTransaction() throws Exception {
    saveRun("agent-a", NODE.target());
    var page = driver.readLog("task-1", "stdout", 7, 12).orElseThrow();
    assertEquals("λ<script>", page.data());
    assertEquals(17, page.nextOffset());
    assertTrue(page.hasMore());
    assertEquals("task-1", page.taskId());
    assertTrue(transport.path.contains("attempt=" + transport.attempt));
    assertTrue(transport.path.endsWith("&stream=stdout&offset=7&limit=12"));
    assertEquals(1, transport.calls);
  }

  @Test
  public void testMissingRunNodeOrLog() throws Exception {
    assertEquals(Optional.empty(), driver.readLog("absent", "stdout", 0, 12));
    assertEquals(0, transport.calls);
    saveRun("agent-a", NODE.target());
    transport.status = 404;
    assertFalse(driver.readLog("task-1", "stderr", 7, 12).isPresent());
    transport.status = 503;
    try {
      driver.readLog("task-1", "stderr", 7, 12);
      fail("Expected unavailable");
    } catch (AgentTransport.ResponseException expected) {
      assertEquals(503, expected.status());
    }
  }

  @Test
  public void testNoLongerEnrolledNode() throws Exception {
    saveRun("removed", NODE.target());
    assertFalse(driver.readLog("task-1", "stdout", 7, 12).isPresent());
    assertEquals(0, transport.calls);
  }

  @Test
  public void testMalformedAgentPagesRejected() throws Exception {
    saveRun("agent-a", NODE.target());
    for (int corruption = 1; corruption <= 6; corruption++) {
      transport.corruption = corruption;
      try {
        driver.readLog("task-1", "stdout", 7, 12);
        fail("Expected malformed page rejection " + corruption);
      } catch (IOException expected) {
        assertEquals("Invalid agent log response", expected.getMessage());
      }
    }
  }

  private final class LogTransport implements AgentTransport {
    private String attempt;
    private String path;
    private int status;
    private int corruption;
    private int calls;

    @Override
    public JsonNode request(GoAgentConfig.Node node, String requestPath, JsonNode body,
                            String epoch, String session) throws IOException {
      calls++;
      path = requestPath;
      assertEquals(NODE, node);
      assertTrue(session.startsWith("s-"));
      // A write would be an illegal transaction upgrade if the log reader held a storage read.
      sqlite.write((NoResult<RuntimeException>) stores -> { });
      if (status != 0) {
        throw new AgentTransport.ResponseException(status);
      }
      String stream = URLDecoder.decode(URI.create(path).getQuery(), StandardCharsets.UTF_8)
          .contains("stream=stderr") ? "stderr" : "stdout";
      ObjectNode result = WireJson.object().put("attempt", attempt).put("stream", stream)
          .put("offset", 7).put("nextOffset", 17).put("hasMore", true)
          .put("truncated", false).put("complete", false).put("data", "λ<script>");
      switch (corruption) {
        case 1 -> result.put("attempt", "another-attempt");
        case 2 -> result.put("offset", 8);
        case 3 -> result.put("nextOffset", 99);
        case 4 -> result.put("data", "x".repeat(13));
        case 5 -> result.put("hasMore", "yes");
        case 6 -> result.put("extra", true);
        default -> { }
      }
      return result;
    }

    @Override
    public void close() {
      // This in-memory transport owns no resources.
    }
  }
}
