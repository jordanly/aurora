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
