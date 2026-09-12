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
import java.util.Set;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.execution.ExecutionOffer;
import org.apache.aurora.scheduler.execution.PreparedTask;
import org.apache.aurora.scheduler.execution.TaskConfigValidator;
import org.apache.aurora.scheduler.execution.TaskFactory;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

/** Explicit process-only executor cohort through the original TaskConfig contract. */
final class GoTaskFactory implements TaskFactory, TaskConfigValidator {
  static final String EXECUTOR = "go-process";
  private final GoAgentConfig config;
  private final TierManager tiers;

  @Inject
  GoTaskFactory(GoAgentConfig config, TierManager tiers) {
    this.config = config;
    this.tiers = tiers;
  }

  record Launch(String taskId, String agentId, String body) implements PreparedTask { }

  @Override
  public void validate(ITaskConfig task) throws TaskDescriptionException {
    try {
      WireJson.require(task.isSetExecutorConfig()
          && EXECUTOR.equals(task.getExecutorConfig().getName()),
          "Go agents require the go-process executor with aurora-process-v1 data");
      WireJson.require(!tiers.getTier(task).isRevocable(), "Go agents do not offer revocable CPU");
      WireJson.require(task.getContainer().isSetMesos()
          && !task.getContainer().getMesos().isSetImage()
          && task.getContainer().getMesos().getVolumes().isEmpty()
          && task.getMesosFetcherUris().isEmpty(),
          "Process profile has no images, volumes or fetcher");
      WireJson.require(task.getResources().stream().allMatch(resource ->
          Set.of(ResourceType.CPUS, ResourceType.RAM_MB, ResourceType.DISK_MB)
              .contains(ResourceType.fromResource(resource))),
          "Process profile has no ports or GPUs");
      WireJson.require(!task.isSetPartitionPolicy() || !task.getPartitionPolicy().isReschedule(),
          "Process profile preserves reservations through partitions; rescheduling is unsupported");
      var resources = ResourceManager.bagFromResources(task.getResources());
      double cpuMillis = resources.valueOf(ResourceType.CPUS) * 1000;
      WireJson.require(Double.isFinite(cpuMillis)
          && Double.compare(cpuMillis, Math.rint(cpuMillis)) == 0,
          "CPU reservations must be whole milliseconds of a core");
      WireJson.require(resources.valueOf(ResourceType.CPUS) * 1000 <= Integer.MAX_VALUE
          && resources.valueOf(ResourceType.RAM_MB) * 1048576 <= 9007199254740991L,
          "Resource value exceeds agent protocol range");
      var key = task.getJob();
      WireJson.require(java.util.stream.Stream.of(
              key.getRole(), key.getEnvironment(), key.getName())
          .allMatch(value -> value.matches("[a-z][a-z0-9-]{0,63}")),
          "Go process protocol currently requires lowercase job keys of at most 64 characters");
      process(task);
    } catch (IOException | IllegalArgumentException e) {
      throw new TaskDescriptionException(e.getMessage(), e);
    }
  }

  private static JsonNode process(ITaskConfig task) throws IOException {
    String data = task.getExecutorConfig().getData();
    WireJson.require(data != null, "Executor data is required");
    WireJson.require(data.getBytes(StandardCharsets.UTF_8).length <= WireJson.MAX_BYTES - 8192,
        "Executor data exceeds the command transport size, including its identity envelope");
    JsonNode spec = WireJson.parse(data.getBytes(StandardCharsets.UTF_8));
    WireJson.fields(spec, "version", "argv", "env", "graceMillis");
    WireJson.require("aurora-process-v1".equals(spec.path("version").asText()),
        "Unsupported process executor data version");
    JsonNode argv = spec.path("argv");
    WireJson.require(argv.isArray() && argv.size() > 0 && argv.size() <= 64,
        "argv must contain 1 through 64 arguments");
    for (JsonNode arg : argv) {
      WireJson.require(arg.isTextual() && arg.asText().length() <= 4096,
          "Arguments must be strings of at most 4096 characters");
    }
    WireJson.require(argv.get(0).asText().startsWith("/"), "Executable path must be absolute");
    JsonNode env = spec.path("env");
    WireJson.require(env.isObject(), "Explicit environment object required");
    env.fields().forEachRemaining(entry -> WireJson.require(
        entry.getKey().matches("[A-Z_][A-Z0-9_]*") && entry.getValue().isTextual()
            && entry.getValue().asText().length() <= 4096, "Invalid environment entry"));
    JsonNode grace = spec.path("graceMillis");
    WireJson.require(grace.isIntegralNumber() && grace.canConvertToInt()
        && grace.asInt() >= 0 && grace.asInt() <= 60000, "graceMillis must be 0 through 60000");
    WireJson.bytes(spec);
    return spec;
  }

  static String identity(String prefix, String taskId) {
    return prefix + WireJson.hash(taskId.getBytes(StandardCharsets.UTF_8)).substring(0, 62);
  }

  @Override
  public PreparedTask prepare(IAssignedTask task, ExecutionOffer offer, boolean revocable) {
    try {
      validate(task.getTask());
      WireJson.require(!revocable, "Revocable launch unsupported");
      GoAgentConfig.Node node = config.nodes().stream()
          .filter(item -> item.name().equals(offer.getAgentId())).findFirst().orElseThrow();
      JsonNode spec = process(task.getTask());
      var resources = ResourceManager.bagFromResources(task.getTask().getResources());
      ObjectNode assignment = WireJson.object().put("process", "main")
          .put("runtime", "trusted-host-process");
      assignment.set("argv", spec.get("argv"));
      assignment.set("env", spec.get("env"));
      assignment.set("resources", WireJson.object()
          .put("cpuMillis", (long) Math.ceil(resources.valueOf(ResourceType.CPUS) * 1000))
          .put("memoryBytes", (long) resources.valueOf(ResourceType.RAM_MB) * 1048576)
          .put("memoryEnforcement", "reservation"));
      assignment.putArray("ports");
      assignment.putArray("requiredCapabilities");
      assignment.set("readiness", WireJson.object().put("kind", "none"));
      assignment.set("retry", WireJson.object().put("maxRuns", 1));
      assignment.set("stop", WireJson.object().put("graceMillis", spec.get("graceMillis").asInt()));
      ObjectNode identity = WireJson.object().put("cluster", config.cluster())
          .put("incarnation", config.incarnation()).put("instance", "i-" + task.getInstanceId())
          .put("attempt", identity("a-", task.getTaskId())).put("process", "main")
          .put("run", identity("u-", task.getTaskId()));
      var key = task.getTask().getJob();
      identity.set("jobKey", WireJson.object().put("role", key.getRole())
          .put("environment", key.getEnvironment()).put("name", key.getName()));
      ObjectNode run = WireJson.base("Run").put("command", identity("r-", task.getTaskId()))
          .put("desiredRevision", "1")
          .put("templateSha256", WireJson.hash(WireJson.bytes(assignment)));
      run.set("identity", identity);
      run.set("target", node.target());
      run.set("assignment", assignment);
      return new Launch(task.getTaskId(), node.name(), WireJson.string(run));
    } catch (IOException | TaskDescriptionException e) {
      throw new IllegalArgumentException("Unable to prepare process task", e);
    }
  }
}
