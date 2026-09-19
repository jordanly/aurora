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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.sqlite.SqliteEffects.Command;
import org.apache.aurora.scheduler.storage.sqlite.SqliteEffects.Retiring;
import org.apache.aurora.scheduler.storage.sqlite.SqliteStorage;

/** Owns retirement authorization and the prepare / agent barrier / confirm transaction phases. */
final class GoRetirementCoordinator {
  private final GoAgentConfig config;
  private final Storage storage;
  private final SqliteStorage sqlite;
  private final AgentTransport client;
  private final String epoch;
  private final String session;
  private final int retainedCompleted;
  private final Map<String, Object> retentionLocks = new ConcurrentHashMap<>();
  private final Set<String> retentionValidated = ConcurrentHashMap.newKeySet();

  GoRetirementCoordinator(GoAgentConfig config, Storage storage, SqliteStorage sqlite,
      AgentTransport client, String epoch, String session, int retainedCompleted) {
    this.config = config;
    this.storage = storage;
    this.sqlite = sqlite;
    this.client = client;
    this.epoch = epoch;
    this.session = session;
    this.retainedCompleted = retainedCompleted;
  }

  boolean retiredCommand(Command command) {
    JsonNode identity = parse(command).path("identity");
    var retention = sqlite.effects().retention(command.agentId());
    if (retention.isEmpty() || !retention.get().enabled()) {
      return false;
    }
    // The quiescent activation barrier permanently retires every legacy identity.
    if (!identity.has("ticket")) {
      return true;
    }
    long ticket = WireJson.counter(identity, "ticket");
    try {
      JsonNode ranges = WireJson.parse(retention.get().retired()
          .getBytes(StandardCharsets.US_ASCII));
      for (JsonNode range : ranges) {
        if (WireJson.counter(range, "first") <= ticket
            && WireJson.counter(range, "last") >= ticket) {
          return true;
        }
      }
      return false;
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid durable retirement fence", e);
    }
  }

  void retainHistory(GoAgentConfig.Node node) throws IOException, InterruptedException {
    synchronized (retentionLocks.computeIfAbsent(node.name(), ignored -> new Object())) {
      var prepared = storage.write(stores -> {
        var current = sqlite.effects().retention(node.name());
        sqlite.effects().beginRetention(node.name(), config.journalScope(node));
        if (current.isEmpty() || !current.get().enabled()) {
          boolean active = stores.getTaskStore().fetchTasks(Query.unscoped()).stream().anyMatch(
              task -> node.name().equals(task.getAssignedTask().getSlaveId())
                  && Tasks.isActive(task.getStatus()));
          if (active || !sqlite.effects().pending(node.name(), 1).isEmpty()) {
            return false;
          }
        }
        return true;
      });
      if (!prepared) {
        return;
      }
      var before = storage.read(stores -> sqlite.effects().retention(node.name()).orElseThrow());
      var retiring = storage.write(stores -> before.enabled()
          ? sqlite.effects().prepareRetirement(node.name(), retainedCompleted)
          : List.<Retiring>of());
      if (before.enabled() && retiring.isEmpty() && retentionValidated.contains(node.name())) {
        return;
      }
      ObjectNode request = WireJson.object().put("journal", node.journal())
          .put("activate", !before.enabled());
      var tickets = request.putArray("tickets");
      retiring.forEach(attempt -> tickets.add(Long.toString(attempt.ticket())));
      JsonNode response = client.request(node, "/v1/retention", request, epoch, session);
      WireJson.fields(response, "version", "retired", "garbage");
      WireJson.require(response.path("version").asInt() == 1
          && response.path("garbage").isArray() && response.path("garbage").isEmpty()
          && response.path("retired").isArray() && response.path("retired").size() <= 1024,
          "Invalid agent retirement barrier");
      JsonNode ranges = response.path("retired");
      long previous = -1;
      for (JsonNode range : ranges) {
        WireJson.fields(range, "first", "last");
        long first = WireJson.counter(range, "first");
        long last = WireJson.counter(range, "last");
        WireJson.require(first > 0 && last >= first && first > previous + 1
            && last < before.nextTicket(), "Invalid or unknown retired ticket range");
        previous = last;
      }
      JsonNode prior = WireJson.parse(before.retired().getBytes(StandardCharsets.US_ASCII));
      List<TicketRange> authorized = new ArrayList<>(readRanges(prior));
      for (var attempt : retiring) {
        authorized.add(new TicketRange(attempt.ticket(), attempt.ticket()));
      }
      List<TicketRange> merged = merge(authorized);
      List<TicketRange> returned = readRanges(ranges);
      for (TicketRange range : returned) {
        WireJson.require(merged.stream().anyMatch(allowed -> allowed.contains(range)),
            "Agent retired a ticket outside the durable retirement request");
      }
      for (TicketRange range : readRanges(prior)) {
        WireJson.require(returned.stream().anyMatch(now -> now.contains(range)),
            "Agent retirement state rolled back");
      }
      var confirmed = new ArrayList<Retiring>();
      for (var attempt : retiring) {
        if (returned.stream().anyMatch(range -> range.contains(attempt.ticket()))) {
          confirmed.add(attempt);
        }
      }
      storage.write(stores -> {
        if (!before.enabled()) {
          sqlite.effects().finishLegacyRetention(node.name(), config.journalScope(node));
        }
        sqlite.effects().confirmRetention(node.name(), WireJson.string(ranges));
        for (var attempt : confirmed) {
          var run = sqlite.effects().command(GoTaskFactory.identity("r-", attempt.taskId()))
              .orElseThrow(() -> new IllegalStateException("Retiring launch missing"));
          sqlite.effects().finishRetirement(node.name(), config.journalScope(node), attempt,
              WireJson.text(parse(run).path("identity"), "attempt"));
        }
        return null;
      });
      retentionValidated.add(node.name());
    }
  }

  record TicketRange(long first, long last) {
    TicketRange {
      WireJson.require(first > 0 && last >= first, "Invalid retired ticket range");
    }

    boolean contains(long ticket) {
      return first <= ticket && ticket <= last;
    }

    boolean contains(TicketRange other) {
      return first <= other.first && last >= other.last;
    }
  }

  private static List<TicketRange> readRanges(JsonNode ranges) {
    List<TicketRange> result = new ArrayList<>();
    for (JsonNode range : ranges) {
      result.add(new TicketRange(
          WireJson.counter(range, "first"), WireJson.counter(range, "last")));
    }
    return List.copyOf(result);
  }

  static List<TicketRange> merge(List<TicketRange> ranges) {
    List<TicketRange> ordered = new ArrayList<>(ranges);
    ordered.sort(Comparator.comparingLong(TicketRange::first));
    List<TicketRange> merged = new ArrayList<>();
    for (TicketRange range : ordered) {
      if (!merged.isEmpty()) {
        TicketRange last = merged.getLast();
        if (last.last == Long.MAX_VALUE || range.first <= last.last + 1) {
          merged.set(merged.size() - 1,
              new TicketRange(last.first, Math.max(last.last, range.last)));
          continue;
        }
      }
      merged.add(range);
    }
    return List.copyOf(merged);
  }

  private static JsonNode parse(Command command) {
    WireJson.require(command.payloadVersion() == 1, "Unsupported command payload version");
    try {
      return WireJson.parse(command.payload());
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid stored command", e);
    }
  }

}
