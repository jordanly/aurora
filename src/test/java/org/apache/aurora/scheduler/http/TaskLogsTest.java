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
package org.apache.aurora.scheduler.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.aurora.scheduler.execution.TaskLogReader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TaskLogsTest {
  @Test
  public void testLiteralOutputAndNoCache() throws Exception {
    var logs = new TaskLogs((task, stream, offset, limit) -> Optional.of(new TaskLogReader.Page(
        task, stream, offset, offset + 3, false, true, true, "<x>")));
    var response = logs.read("task-a", "stderr", 2, 3);
    assertEquals(200, response.getStatus());
    assertEquals("no-store", response.getHeaderString("Cache-Control"));
    assertEquals("nosniff", response.getHeaderString("X-Content-Type-Options"));
    var page = new ObjectMapper().readTree(response.getEntity().toString());
    assertEquals("<x>", page.path("data").asText());
    assertEquals(5, page.path("nextOffset").asInt());
    assertTrue(page.path("truncated").asBoolean());
  }

  @Test
  public void testBoundsAndAbsentOutput() throws Exception {
    var logs = new TaskLogs((task, stream, offset, limit) -> Optional.empty());
    assertEquals(404, logs.read("task", "stdout", 0, 1).getStatus());
    assertEquals(400, logs.read(null, "stdout", 0, 1).getStatus());
    assertEquals(400, logs.read("", "stdout", 0, 1).getStatus());
    assertEquals(400, logs.read("x".repeat(513), "stdout", 0, 1).getStatus());
    assertEquals(400, logs.read("task", "file", 0, 1).getStatus());
    assertEquals(400, logs.read("task", "stdout", -1, 1).getStatus());
    assertEquals(400, logs.read("task", "stdout", TaskLogReader.MAX_OFFSET + 1, 1).getStatus());
    assertEquals(400, logs.read("task", "stdout", 0, 0).getStatus());
    assertEquals(400, logs.read("task", "stdout", 0, 65537).getStatus());
  }

  @Test
  public void testFailureAndInterruption() throws Exception {
    var logs = new TaskLogs((task, stream, offset, limit) -> {
      throw new IOException("private diagnostic");
    });
    var response = logs.read("task", "stdout", 0, 1);
    assertEquals(503, response.getStatus());
    assertFalse(response.getEntity().toString().contains("private diagnostic"));
    logs = new TaskLogs((task, stream, offset, limit) -> {
      throw new InterruptedException();
    });
    try {
      assertEquals(503, logs.read("task", "stdout", 0, 1).getStatus());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void testConcurrentReadersBounded() throws Exception {
    CountDownLatch entered = new CountDownLatch(8);
    CountDownLatch release = new CountDownLatch(1);
    var logs = new TaskLogs((task, stream, offset, limit) -> {
      entered.countDown();
      assertTrue(release.await(10, TimeUnit.SECONDS));
      return Optional.empty();
    });
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var responses = new ArrayList<Future<Response>>();
      try {
        for (int i = 0; i < 8; i++) {
          responses.add(executor.submit(() -> logs.read("task", "stdout", 0, 1)));
        }
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        assertEquals(503, logs.read("task", "stdout", 0, 1).getStatus());
      } finally {
        release.countDown();
      }
      for (var response : responses) {
        assertEquals(404, response.get(10, TimeUnit.SECONDS).getStatus());
      }
    }
    assertEquals(404, logs.read("task", "stdout", 0, 1).getStatus());
  }
}
