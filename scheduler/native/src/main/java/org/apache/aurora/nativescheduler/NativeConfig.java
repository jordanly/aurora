/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.nativescheduler;

import java.net.URI;
import java.util.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.aurora.scheduler.storage.sql.NativeSqlStore.Journal;

public final class NativeConfig {
  public final String cluster, incarnation, canonical;
  public final List<Node> nodes;
  public NativeConfig(byte[] data) throws Exception {
    JsonNode value = Json.parse(data);
    Json.fields(value, "cluster", "incarnation", "nodes");
    cluster = token(Json.string(value, "cluster")); incarnation = token(Json.string(value, "incarnation"));
    if (!value.get("nodes").isArray() || value.get("nodes").size() != 2) {
      throw new IllegalArgumentException("Exactly two static nodes required");
    }
    List<Node> list = new ArrayList<>(); Set<String> names = new HashSet<>();
    for (JsonNode n : value.get("nodes")) {
      Node node = new Node(n);
      if (!names.add(node.name)) { throw new IllegalArgumentException("Duplicate node"); }
      list.add(node);
    }
    nodes = Collections.unmodifiableList(list); canonical = Json.canonical(value);
  }
  static String token(String value) {
    if (!value.matches("[a-z0-9][a-z0-9._-]{0,127}")) {
      throw new IllegalArgumentException("Invalid identity token");
    }
    return value;
  }
  public final class Node {
    public final String name, journal, boot, runtime, url, network;
    public final long cpu, memory;
    public final int portStart;
    private Node(JsonNode n) throws Exception {
      Json.fields(n, "node", "journal", "boot", "runtime", "url", "cpuMillis", "memoryBytes", "network", "portStart");
      name = token(Json.string(n,"node")); journal = token(Json.string(n,"journal"));
      boot = token(Json.string(n,"boot")); runtime = token(Json.string(n,"runtime"));
      network = token(Json.string(n,"network")); url = Json.string(n,"url");
      URI uri = new URI(url);
      if (!"https".equals(uri.getScheme()) || !name.equals(uri.getHost()) || uri.getUserInfo()!=null
          || uri.getQuery()!=null || uri.getFragment()!=null || !"".equals(uri.getPath())
          || uri.getPort()<1) { throw new IllegalArgumentException("Node URL must be https://node:port"); }
      cpu = positive(n,"cpuMillis"); memory = positive(n,"memoryBytes");
      long port = positive(n,"portStart");
      if (port < 1024 || port > 65520) { throw new IllegalArgumentException("Invalid port range"); }
      portStart = (int) port;
    }
    public Journal scope() { return new Journal(cluster,incarnation,name,journal); }
    public ObjectNode target() {
      return Json.object().put("node",name).put("journal",journal).put("boot",boot).put("runtime",runtime);
    }
  }
  static long positive(JsonNode n, String key) {
    JsonNode v = n.path(key);
    if (!v.isIntegralNumber() || !v.canConvertToLong() || v.longValue()<1
        || v.longValue()>9007199254740991L) { throw new IllegalArgumentException("Invalid capacity"); }
    return v.longValue();
  }
}
