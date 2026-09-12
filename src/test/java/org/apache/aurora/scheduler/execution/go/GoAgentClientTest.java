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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;

import com.sun.net.httpserver.HttpServer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GoAgentClientTest {
  private final CountDownLatch release = new CountDownLatch(1);
  private HttpServer server;
  private ExecutorService worker;
  private GoAgentClient client;
  private GoAgentConfig.Node node;

  @Before
  public void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    worker = Executors.newSingleThreadExecutor();
    server.setExecutor(worker);
    client = new GoAgentClient(HttpClient.newHttpClient(), Duration.ofSeconds(5));
    node = new GoAgentConfig.Node("test", URI.create("http://"
        + server.getAddress().getAddress().getHostAddress() + ":"
        + server.getAddress().getPort()), "journal", "boot", "runtime", 1000, 1024, 1);
  }

  @After
  public void tearDown() throws InterruptedException {
    release.countDown();
    server.stop(0);
    client.close();
    worker.shutdownNow();
    assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testDeadlineIncludesStalledResponseBody() throws Exception {
    client.close();
    client = new GoAgentClient(HttpClient.newHttpClient(), Duration.ofSeconds(1));
    CountDownLatch headersSent = new CountDownLatch(1);
    server.createContext("/", exchange -> {
      try (exchange) {
        exchange.sendResponseHeaders(200, 100);
        exchange.getResponseBody().write('{');
        exchange.getResponseBody().flush();
        headersSent.countDown();
        try {
          release.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    server.start();
    try {
      client.request(node, "/v1/state", null, "1", "session");
      fail("A response that never finishes must time out");
    } catch (IOException expected) {
      Throwable cause = expected;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      assertTrue(cause instanceof java.util.concurrent.TimeoutException
          || cause instanceof java.net.http.HttpTimeoutException);
    }
    assertEquals(0L, headersSent.getCount());
  }

  @Test
  public void testOversizedResponseCancelledBeforeParsing() throws Exception {
    server.createContext("/", exchange -> {
      try (exchange) {
        exchange.sendResponseHeaders(200, WireJson.MAX_BYTES + 1L);
        try {
          exchange.getResponseBody().write(new byte[WireJson.MAX_BYTES + 1]);
        } catch (IOException expectedCancellation) {
          // The subscriber can cancel before the server finishes writing its last chunk.
        }
      }
    });
    server.start();
    try {
      client.request(node, "/v1/state", null, "1", "session");
      fail("Oversized response must be rejected");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("exceeds 1 MiB"));
    }
  }

  @Test
  public void testStructuredDeliveryRejectionReturnedForPolicyHandling() throws Exception {
    respond(409, "{\"command\":\"run\",\"bodySha256\":\"hash\","
        + "\"outcome\":\"rejected-capacity\"}");
    assertEquals("rejected-capacity", client.request(node, "/v1/deliver", WireJson.object(),
        "1", "session").path("outcome").asText());
  }

  @Test
  public void testUnstructuredConflictRemainsTransportFailure() throws Exception {
    respond(409, "{\"error\":\"session rejected\"}");
    try {
      client.request(node, "/v1/deliver", WireJson.object(), "1", "session");
      fail("A generic conflict is not a durable command receipt");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("409"));
    }
  }

  @Test
  public void testWatchReadsFramesBeforeResponseEnds() throws Exception {
    server.createContext("/v1/watch", exchange -> {
      try (exchange) {
        assertEquals("afterCursor=7&limit=128", exchange.getRequestURI().getQuery());
        assertEquals("session", exchange.getRequestHeaders().getFirst("X-Aurora-Session"));
        exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write(("{\"kind\":\"snapshot\"}\n"
            + "{\"kind\":\"heartbeat\"}\n").getBytes(StandardCharsets.US_ASCII));
        exchange.getResponseBody().flush();
        try {
          release.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    server.start();
    try (AgentTransport.Watch watch = client.watch(node, 7, "1", "session")) {
      assertEquals("snapshot", watch.next().path("kind").asText());
      assertEquals("heartbeat", watch.next().path("kind").asText());
    }
  }

  @Test
  public void testWatchPreservesSignalExitInDeltaAndReconnectSnapshot() throws Exception {
    AtomicInteger connections = new AtomicInteger();
    server.createContext("/v1/watch", exchange -> {
      try (exchange) {
        assertEquals("afterCursor=18&limit=128", exchange.getRequestURI().getQuery());
        String body = connections.getAndIncrement() == 0
            ? "{\"kind\":\"snapshot\"}\n" + terminalFrame("delta", "-1")
            : terminalFrame("snapshot", "-1");
        exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write((body + "\n").getBytes(StandardCharsets.US_ASCII));
      }
    });
    server.start();
    try (AgentTransport.Watch watch = client.watch(node, 18, "1", "session")) {
      assertEquals("snapshot", watch.next().path("kind").asText());
      JsonNode delta = watch.next();
      assertEquals("delta", delta.path("kind").asText());
      assertSignalExit(delta);
    }
    // The cursor may still be uncommitted when a stream disconnects. A fresh
    // snapshot containing the same terminal diagnostic must remain readable.
    try (AgentTransport.Watch watch = client.watch(node, 18, "1", "session")) {
      JsonNode snapshot = watch.next();
      assertEquals("snapshot", snapshot.path("kind").asText());
      assertSignalExit(snapshot);
    }
    assertEquals(2, connections.get());
  }

  @Test
  public void testWatchRejectsOtherNegativeAndInvalidNumbers() throws Exception {
    AtomicReference<String> frame = new AtomicReference<>();
    server.createContext("/v1/watch", exchange -> {
      try (exchange) {
        exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write((frame.get() + "\n")
            .getBytes(StandardCharsets.US_ASCII));
      }
    });
    server.start();
    for (String invalid : new String[] {terminalFrame("delta", "-2"),
        terminalFrame("delta", "-1.0"), terminalFrame("snapshot", "9007199254740992"),
        terminalFrame("delta", "-1").replace("\"signal\":15", "\"signal\":-1"),
        "{\"exitCode\":-1}", "{\"state\":{\"observations\":[{\"exitCode\":-1}]}}"}) {
      frame.set(invalid);
      try (AgentTransport.Watch watch = client.watch(node, 18, "1", "session")) {
        try {
          watch.next();
          fail("Invalid watch numeric value accepted: " + invalid);
        } catch (IOException expected) {
          assertTrue(expected.getMessage().contains("IllegalArgumentException"));
          assertTrue(expected.getCause().getCause().getMessage()
              .contains("safe nonnegative integers"));
        }
      }
    }
  }

  private static String terminalFrame(String kind, String exitCode) {
    return "{\"kind\":\"" + kind + "\",\"nextCursor\":\"19\",\"hasMore\":false,"
        + "\"state\":{\"attempts\":{\"attempt\":{\"reserved\":false,\"execution\":{"
        + "\"phase\":\"terminal\",\"outcome\":\"stopped\",\"cleanup\":\"complete\","
        + "\"exitCode\":" + exitCode + ",\"signal\":15}}},\"observations\":[]}}";
  }

  private static void assertSignalExit(JsonNode frame) {
    JsonNode execution = frame.path("state").path("attempts").path("attempt").path("execution");
    assertEquals(-1, execution.path("exitCode").asInt());
    assertEquals(15, execution.path("signal").asInt());
    assertEquals("complete", execution.path("cleanup").asText());
  }

  @Test
  public void testWatchRejectsOversizedUnterminatedFrame() throws Exception {
    server.createContext("/v1/watch", exchange -> {
      try (exchange) {
        exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write(new byte[WireJson.MAX_BYTES + 1]);
      }
    });
    server.start();
    try (AgentTransport.Watch watch = client.watch(node, 0, "1", "session")) {
      try {
        watch.next();
        fail("An unterminated oversized frame must fail");
      } catch (IOException expected) {
        assertTrue(expected.getCause().getCause().getMessage().contains("exceeds 1 MiB"));
      }
    }
  }

  @Test
  public void testWatchPartialFrameTimesOutAndCancelsRead() throws Exception {
    client.close();
    client = new GoAgentClient(HttpClient.newHttpClient(), Duration.ofSeconds(5),
        Duration.ofMillis(100));
    server.createContext("/v1/watch", exchange -> {
      try (exchange) {
        exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write('{');
        exchange.getResponseBody().flush();
        try {
          release.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    server.start();
    try (AgentTransport.Watch watch = client.watch(node, 0, "1", "session")) {
      try {
        watch.next();
        fail("Partial frame must meet watch deadline");
      } catch (IOException expected) {
        assertTrue(expected.getCause() instanceof java.util.concurrent.TimeoutException);
      }
    }
  }

  @Test
  public void testWatchRejectsWrongContentType() throws Exception {
    respond(200, "{}");
    try {
      client.watch(node, 0, "1", "session");
      fail("Watch requires NDJSON content type");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("Invalid agent watch response"));
    }
  }

  private void respond(int status, String body) {
    server.createContext("/", exchange -> {
      try (exchange) {
        byte[] bytes = body.getBytes(StandardCharsets.US_ASCII);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
      }
    });
    server.start();
  }
}
