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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Operator enrollment for a single SQLite owner; no dynamic identity or capacity assertions. */
record GoAgentConfig(String cluster, String incarnation, Path database, Path keyStore,
                     String keyStorePassword, Path trustStore, String trustStorePassword,
                     List<Node> nodes) {
  GoAgentConfig {
    nodes = List.copyOf(nodes);
  }

  record Node(String name, URI url, String journal, String boot, String runtime,
              long cpuMillis, long memoryBytes, long diskMb) {
    ObjectNode target() {
      return WireJson.object().put("node", name).put("journal", journal)
          .put("boot", boot).put("runtime", runtime);
    }
  }

  static GoAgentConfig read(Path path, String cluster) throws IOException {
    JsonNode root = WireJson.parse(Files.readAllBytes(path));
    WireJson.fields(root, "cluster", "incarnation", "database", "keyStore", "keyStorePassword",
        "trustStore", "trustStorePassword", "nodes");
    WireJson.require(cluster.equals(WireJson.text(root, "cluster")), "Cluster enrollment differs");
    WireJson.require(root.path("nodes").isArray() && root.path("nodes").size() > 0,
        "At least one enrolled agent is required");
    for (String field : List.of("cluster", "incarnation")) {
      WireJson.require(WireJson.text(root, field).matches("[a-z][a-z0-9-]{0,63}"),
          "Invalid protocol identity: " + field);
    }
    Set<String> names = new HashSet<>();
    List<Node> nodes = new ArrayList<>();
    for (JsonNode item : root.path("nodes")) {
      WireJson.fields(item, "name", "url", "journal", "boot", "runtime", "cpuMillis",
          "memoryBytes", "diskMb");
      String name = WireJson.text(item, "name");
      WireJson.require(name.matches("[a-z][a-z0-9-]{0,63}") && names.add(name),
          "Invalid or repeated agent name");
      URI url = URI.create(WireJson.text(item, "url"));
      WireJson.require("https".equals(url.getScheme()) && name.equals(url.getHost())
          && url.getUserInfo() == null && url.getQuery() == null && url.getFragment() == null
          && (url.getPath().isEmpty() || "/".equals(url.getPath())), "Invalid enrolled URL");
      for (String field : List.of("journal", "boot", "runtime")) {
        WireJson.require(WireJson.text(item, field).matches("[a-z][a-z0-9-]{0,63}"),
            "Invalid enrolled identity: " + field);
      }
      long cpu = positive(item, "cpuMillis");
      long memory = positive(item, "memoryBytes");
      long disk = positive(item, "diskMb");
      nodes.add(new Node(name, url, WireJson.text(item, "journal"), WireJson.text(item, "boot"),
          WireJson.text(item, "runtime"), cpu, memory, disk));
    }
    return new GoAgentConfig(cluster, WireJson.text(root, "incarnation"),
        Path.of(WireJson.text(root, "database")), Path.of(WireJson.text(root, "keyStore")),
        WireJson.text(root, "keyStorePassword"), Path.of(WireJson.text(root, "trustStore")),
        WireJson.text(root, "trustStorePassword"), nodes);
  }

  private static long positive(JsonNode item, String key) {
    JsonNode value = item.path(key);
    WireJson.require(value.isIntegralNumber() && value.canConvertToLong() && value.asLong() > 0
        && value.asLong() <= 9007199254740991L, "Invalid capacity: " + key);
    return value.asLong();
  }

  @Override
  public String toString() {
    return "GoAgentConfig[cluster=" + cluster + ", agents=" + nodes.size() + "]";
  }
}
