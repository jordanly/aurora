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
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.aurora.scheduler.execution.TaskLogReader;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.sqlite.SqliteEffects.Command;
import org.apache.aurora.scheduler.storage.sqlite.SqliteStorage;

/** Routes log reads using committed Run identities, without database locks during network I/O. */
final class GoTaskLogReader implements TaskLogReader {
  private final GoAgentConfig config;
  private final Storage storage;
  private final SqliteStorage sqlite;
  private final AgentTransport client;
  private final String epoch;
  private final String session;

  GoTaskLogReader(GoAgentConfig config, Storage storage, SqliteStorage sqlite,
      AgentTransport client, String epoch, String session) {
    this.config = config;
    this.storage = storage;
    this.sqlite = sqlite;
    this.client = client;
    this.epoch = epoch;
    this.session = session;
  }

  @Override
  public Optional<TaskLogReader.Page> readLog(String taskId, String stream, long offset, int limit)
      throws IOException, InterruptedException {
    WireJson.require(taskId != null && !taskId.isEmpty() && taskId.length() <= 512
        && ("stdout".equals(stream) || "stderr".equals(stream))
        && offset >= 0 && offset <= MAX_OFFSET && limit > 0 && limit <= MAX_PAGE_BYTES,
        "Invalid log request");
    // The immutable persisted Run binds task, enrolled node and attempt. No host/path from HTTP
    // participates in routing. Release the read transaction before contacting the agent.
    Optional<Command> command = storage.read(stores ->
        sqlite.effects().command(GoTaskFactory.identity("r-", taskId)));
    if (command.isEmpty()) {
      return Optional.empty();
    }
    Command run = command.get();
    WireJson.require(taskId.equals(run.taskId()) && "Run".equals(run.type())
        && run.payloadVersion() == 1, "Invalid retained Run");
    var node = config.nodes().stream().filter(item -> item.name().equals(run.agentId()))
        .findFirst();
    if (node.isEmpty()) {
      return Optional.empty();
    }
    JsonNode body = WireJson.parse(run.payload());
    WireJson.require(node.get().target().equals(body.path("target")), "Run enrollment mismatch");
    JsonNode identity = body.path("identity");
    ObjectNode key = WireJson.object();
    for (String field : List.of("cluster", "incarnation", "jobKey", "instance", "attempt")) {
      WireJson.require(identity.hasNonNull(field), "Missing retained task identity");
      key.set(field, identity.get(field));
    }
    String attempt = WireJson.hash(WireJson.bytes(key));
    JsonNode result;
    try {
      result = client.request(node.get(), "/v1/logs?attempt=" + attempt + "&stream=" + stream
          + "&offset=" + offset + "&limit=" + limit, null, epoch, session);
    } catch (AgentTransport.ResponseException e) {
      if (e.status() == 404) {
        return Optional.empty();
      }
      throw e;
    }
    try {
      WireJson.fields(result, "attempt", "stream", "offset", "nextOffset", "hasMore",
          "truncated", "complete", "data");
      WireJson.require(attempt.equals(WireJson.text(result, "attempt"))
          && stream.equals(WireJson.text(result, "stream"))
          && result.path("offset").isIntegralNumber()
          && result.path("offset").canConvertToLong() && result.path("offset").asLong() == offset
          && result.path("nextOffset").isIntegralNumber()
          && result.path("nextOffset").canConvertToLong(), "Log response identity or offset");
      long next = result.path("nextOffset").asLong();
      WireJson.require(next >= offset && next <= offset + limit && next <= MAX_OFFSET
          && result.path("data").isTextual()
          && result.path("data").asText().length() <= next - offset
          && result.path("hasMore").isBoolean() && result.path("truncated").isBoolean()
          && result.path("complete").isBoolean()
          && (next > offset || !result.path("hasMore").asBoolean()), "Invalid log page");
      return Optional.of(new TaskLogReader.Page(taskId, stream, offset, next,
          result.path("hasMore").asBoolean(), result.path("truncated").asBoolean(),
          result.path("complete").asBoolean(), result.path("data").asText()));
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid agent log response", e);
    }
  }

}
