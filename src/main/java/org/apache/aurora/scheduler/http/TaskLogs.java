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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;

import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.aurora.scheduler.execution.TaskLogReader;

/** Scheduler-local log proxy with the same access policy as the scheduler read APIs. */
@Path("/tasklogs")
@jakarta.inject.Singleton
public class TaskLogs {
  private final TaskLogReader logs;
  private final ObjectMapper mapper = new ObjectMapper();
  private final Semaphore readers = new Semaphore(8);

  @Inject
  TaskLogs(TaskLogReader logs) {
    this.logs = Objects.requireNonNull(logs);
  }

  @GET
  @Path("/{taskId}/{stream}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response read(
      @PathParam("taskId") String taskId,
      @PathParam("stream") String stream,
      @QueryParam("offset") @DefaultValue("0") long offset,
      @QueryParam("limit") @DefaultValue("65536") int limit) throws JsonProcessingException {
    if (taskId == null || taskId.isEmpty() || taskId.length() > 512
        || !("stdout".equals(stream) || "stderr".equals(stream))
        || offset < 0 || offset > TaskLogReader.MAX_OFFSET
        || limit < 1 || limit > TaskLogReader.MAX_PAGE_BYTES) {
      return response(400, Map.of("error", "Invalid log stream or page bounds"));
    }
    if (!readers.tryAcquire()) {
      return response(503, Map.of("error", "Log readers busy; retry shortly"));
    }
    try {
      var page = logs.readLog(taskId, stream, offset, limit);
      if (page.isEmpty()) {
        return response(404, Map.of("error", "Task logs not retained or not created"));
      }
      var value = page.get();
      return response(200, Map.of("taskId", value.taskId(), "stream", value.stream(),
          "offset", value.offset(), "nextOffset", value.nextOffset(), "hasMore", value.hasMore(),
          "truncated", value.truncated(), "complete", value.complete(), "data", value.data()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return response(503, Map.of("error", "Log read interrupted; retry shortly"));
    } catch (IOException e) {
      return response(503, Map.of("error", "Agent logs unavailable; retry shortly"));
    } finally {
      readers.release();
    }
  }

  private Response response(int status, Object body) throws JsonProcessingException {
    return Response.status(status).type(MediaType.APPLICATION_JSON)
        .header("Cache-Control", "no-store").header("X-Content-Type-Options", "nosniff")
        .entity(mapper.writeValueAsString(body)).build();
  }
}
