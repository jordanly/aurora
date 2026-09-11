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

import java.nio.charset.StandardCharsets;
import java.math.BigInteger;
import java.util.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import org.apache.aurora.nativeprotocol.ProtocolValidator;
import org.apache.aurora.nativeprotocol.ProtocolValidator.Message;
import org.apache.aurora.scheduler.storage.sql.NativeSqlStore;
import org.apache.aurora.scheduler.storage.sql.NativeSqlStore.*;

/** Single-owner conservative controller. Uncertain allocations remain reserved indefinitely. */
public final class NativeEngine {
  public interface Transport {
    JsonNode request(NativeConfig.Node node, String method, String path, JsonNode body,
        String epoch, String session) throws Exception;
  }
  public static final class Conflict extends Exception {
    Conflict(String message) { super(message); }
  }
  private final NativeSqlStore store;
  private final NativeConfig config;
  private final Transport transport;
  private final ProtocolValidator validator = new ProtocolValidator();
  private final Map<String,Boolean> reachable = new HashMap<>();
  private final Set<String> unknownReservations = new HashSet<>();
  private final Map<String,String> errors = new HashMap<>();
  public final String epoch, session;

  public NativeEngine(NativeSqlStore store, NativeConfig config, Transport transport,
      boolean inspectOnly) throws Exception {
    this.store=store; this.config=config; this.transport=transport;
    if (inspectOnly) {
      String prior=store.read(Tx::schedulerConfig);
      if (prior!=null && !prior.equals(config.canonical)) { throw new Conflict("Configuration differs"); }
      epoch=store.read(Tx::schedulerEpoch);
    } else { epoch=store.write(tx -> tx.startScheduler(config.canonical)); }
    session="scheduler-"+epoch;
  }
  private Message validated(JsonNode value) throws Exception {
    return validator.validate(Json.canonical(value).getBytes(StandardCharsets.UTF_8));
  }
  private static JobKey key(JsonNode value) {
    return new JobKey(Json.string(value,"role"),Json.string(value,"environment"),Json.string(value,"name"));
  }
  private void scope(JsonNode value) {
    if (!config.cluster.equals(value.path("cluster").asText())
        || !config.incarnation.equals(value.path("incarnation").asText())) {
      throw new IllegalArgumentException("Wrong cluster/incarnation");
    }
  }
  public synchronized boolean submit(byte[] bytes) throws Exception {
    if (bytes.length>4096) { throw new IllegalArgumentException("Job exceeds MVP 4 KiB limit"); }
    Message message=validator.validate(bytes); JsonNode job=message.json();
    if (!"Job".equals(job.path("kind").asText())) { throw new IllegalArgumentException("Expected Job"); }
    scope(job); ProtocolValidator.requireCapabilities(message,Collections.emptySet());
    String body=new String(message.canonicalBytes(),StandardCharsets.UTF_8); JobKey key=key(job.get("jobKey"));
    return store.write(tx -> {
      String prior=tx.jobBody(key);
      if (prior!=null) {
        if (!prior.equals(body)) { throw new Conflict("Job identity already has a different body"); }
        return false;
      }
      if (tx.jobBodies().size()>=64) { throw new Conflict("Job inventory limit"); }
      tx.createJob(key,job.get("revision").asText(),job.get("mode").asText(),body);
      for (int i=0;i<job.get("instances").asInt();i++) { tx.addInstance(key,"instance-"+i); }
      return true;
    });
  }
  public synchronized void stop(JsonNode request) throws Exception {
    Json.fields(request,"role","environment","name"); JobKey key=key(request);
    store.write(tx -> {
      if (tx.jobBody(key)==null) { throw new Conflict("Unknown Job"); }
      for (String instance:tx.desiredInstances(key)) { tx.removeInstance(key,instance); }
      for (AttemptRecord attempt:tx.attempts()) {
        if (!attempt.job.equals(key.toString()) || !attempt.reserved()) { continue; }
        boolean already=false;
        for (CommandRecord command:tx.commands()) {
          if (!command.attempt.equals(attempt.attempt)) { continue; }
          JsonNode body=Json.parse(command.body);
          if ("Stop".equals(body.path("kind").asText())) { already=true; }
          else if (command.pending) { tx.acknowledgeCommand(command.command); }
        }
        if (!already) {
          JsonNode run=Json.parse(attempt.runBody);
          ObjectNode command=base("Stop").put("command",id("stop"))
              .put("desiredRevision",run.path("desiredRevision").asText())
              .put("graceMillis",run.path("assignment").path("stop").path("graceMillis").asInt())
              .put("reason","cancel");
          command.set("identity",run.get("identity")); command.set("target",run.get("target"));
          Message valid=validated(command);
          tx.command(command.get("command").asText(),attempt.attempt,
              new String(valid.canonicalBytes(),StandardCharsets.UTF_8));
        }
      }
      return null;
    });
  }
  public synchronized void tick() throws Exception {
    for (NativeConfig.Node node:config.nodes) {
      reachable.put(node.name,false);
      try { poll(node); reachable.put(node.name,true); errors.remove(node.name); }
      catch (Exception e) { errors.put(node.name,"agent exchange failed: "+e.getClass().getSimpleName()); }
    }
    place();
    for (NativeConfig.Node node:config.nodes) {
      if (!Boolean.TRUE.equals(reachable.get(node.name))) { continue; }
      try { deliver(node); }
      catch (Exception e) { reachable.put(node.name,false); errors.put(node.name,"command exchange failed: "+e.getClass().getSimpleName()); }
    }
  }
  private record PollResult(String committedCursor, boolean unknownReservations) { }

  private void poll(NativeConfig.Node node) throws Exception {
    JsonNode authority = Json.object().put("schedulerEpoch", epoch).put("session", session);
    transport.request(node, "POST", "/v1/session", authority, epoch, session);
    String after = store.read(tx -> tx.committedCursor(node.scope()));
    // Bounded pages per tick; a large backlog never monopolizes operator access.
    for (int page = 0; page < 4; page++) {
      JsonNode response = transport.request(node, "GET",
          "/v1/state?afterCursor=" + after + "&limit=128", null, epoch, session);
      validateAgentState(node, response);
      JsonNode inventory = response.path("state").path("attempts");
      String next = NativeSqlStore.counter(response.path("nextCursor").asText());
      List<JsonNode> observations = validateObservations(node, response, after, next);
      PollResult result = store.write(tx ->
          reducePage(tx, node, observations, inventory));
      if (result.unknownReservations()) {
        unknownReservations.add(node.name);
      } else {
        unknownReservations.remove(node.name);
      }
      String committed = result.committedCursor();
      if (!committed.equals(next)) {
        throw new Conflict("Noncontiguous committed cursor");
      }
      ObjectNode ack = base("ObservationAck").put("cluster", config.cluster)
          .put("incarnation", config.incarnation).put("node", node.name)
          .put("journal", node.journal).put("committedCursor", committed);
      transport.request(node, "POST", "/v1/ack", validated(ack).json(), epoch, session);
      after = committed;
      if (!response.path("hasMore").asBoolean()) {
        return;
      }
    }
    throw new Conflict("Observation backlog not drained");
  }

  private void validateAgentState(NativeConfig.Node node, JsonNode response) throws Exception {
    JsonNode actual = response.path("config");
    scope(actual);
    for (String field : Arrays.asList("node", "journal", "boot", "runtime")) {
      if (!node.target().get(field).equals(actual.path(field))) {
        throw new Conflict("Agent identity differs");
      }
    }
    if (!epoch.equals(actual.path("schedulerEpoch").asText())
        || !session.equals(actual.path("session").asText())
        || !"scheduler".equals(actual.path("peer").asText())
        || actual.path("cpuMillis").asLong() != node.cpu
        || actual.path("memoryBytes").asLong() != node.memory) {
      throw new Conflict("Agent authority/capacity differs");
    }
    JsonNode state = response.path("state");
    JsonNode observations = state.path("observations"), inventory = state.path("attempts");
    if (!observations.isArray() || observations.size() > 128
        || !inventory.isObject() || inventory.size() > 128
        || !state.path("commands").isObject() || state.path("commands").size() > 1024
        || !response.path("hasMore").isBoolean()) {
      throw new Conflict("Unbounded/incomplete inventory");
    }
    NativeSqlStore.counter(state.path("cursor").asText());
    NativeSqlStore.counter(state.path("ack").asText());
  }

  private List<JsonNode> validateObservations(NativeConfig.Node node, JsonNode response,
      String after, String next) throws Exception {
    JsonNode state = response.path("state"), observations = state.path("observations");
    String expected = after;
    List<JsonNode> validated = new ArrayList<>();
    for (JsonNode observation : observations) {
      JsonNode value = validated(observation).json();
      if (!"Observation".equals(value.path("kind").asText())
          || !node.target().equals(value.path("source"))) {
        throw new Conflict("Observation scope differs");
      }
      scope(value.path("identity"));
      expected = new BigInteger(expected).add(BigInteger.ONE).toString();
      if (!expected.equals(value.path("cursor").asText())) {
        throw new Conflict("Observation cursor gap");
      }
      validated.add(value);
    }
    if (!expected.equals(next) || (response.path("hasMore").asBoolean() && observations.size() == 0)
        || new BigInteger(next).compareTo(new BigInteger(state.path("cursor").asText())) > 0) {
      throw new Conflict("Invalid page cursor");
    }
    return validated;
  }

  private PollResult reducePage(Tx tx, NativeConfig.Node node, List<JsonNode> observations,
      JsonNode inventory) throws Exception {
    boolean unknown = false;
    Map<String, AttemptRecord> attempts = new HashMap<>();
    for (AttemptRecord attempt : tx.attempts()) {
      attempts.put(attempt.attempt, attempt);
    }
    for (JsonNode observation : observations) {
      tx.observe(node.scope(), observation.path("cursor").asText(), Json.canonical(observation));
      AttemptRecord attempt = matching(attempts, observation.path("identity"), node);
      if (attempt != null) {
        tx.reduceAttempt(attempt.attempt, observation.path("sequence").asText(),
            observation.path("state").asText(), observation.path("cleanup").asText(),
            observation.path("ready").asBoolean(), System.currentTimeMillis());
      }
    }
    for (JsonNode entry : inventory) {
      AttemptRecord attempt = matching(attempts, entry.path("identity"), node);
      if (!entry.path("reserved").isBoolean()) {
        throw new Conflict("Missing reservation state");
      }
      if (attempt == null) {
        if (entry.path("reserved").asBoolean()) {
          unknown = true;
        }
        continue;
      }
      JsonNode execution = entry.path("execution");
      // Snapshot absence and stop-only tombstones do not establish cleanup.
      if (execution.isObject()) {
        String outcome = execution.path("outcome").asText();
        String phase = execution.path("phase").asText();
        if (!phase.matches("intent|spawned|released|terminal")
            || !("terminal".equals(phase)
                ? NativeSqlStore.terminal(outcome) : outcome.matches("unknown|running"))
            || !execution.path("ready").isBoolean()) {
          throw new Conflict("Invalid execution snapshot");
        }
        tx.reduceAttempt(attempt.attempt, NativeSqlStore.counter(entry.path("sequence").asText()),
            outcome, execution.path("cleanup").asText(), execution.path("ready").asBoolean(),
            System.currentTimeMillis());
      }
    }
    return new PollResult(tx.committedCursor(node.scope()), unknown);
  }
  private AttemptRecord matching(Map<String,AttemptRecord> attempts, JsonNode identity, NativeConfig.Node node) throws Exception {
    AttemptRecord attempt=attempts.get(identity.path("attempt").asText());
    if (attempt==null) { return null; }
    JsonNode run=Json.parse(attempt.runBody);
    if (!run.path("identity").equals(identity) || !node.target().equals(run.path("target"))) {
      throw new Conflict("Attempt identity conflicts");
    }
    return attempt;
  }
  private void place() throws Exception {
    store.write(tx -> {
      List<AttemptRecord> attempts=new ArrayList<>(tx.attempts());
      if (attempts.size()>=48 || tx.commands().size()>=96) { return null; }
      Map<String,Long> cpu=new HashMap<>(), memory=new HashMap<>();
      Map<String,Integer> jobCounts=new HashMap<>(), inventoryCounts=new HashMap<>();
      Map<String,Set<Integer>> ports=new HashMap<>();
      for (NativeConfig.Node node:config.nodes) { ports.put(node.name,new HashSet<>()); }
      for (AttemptRecord attempt:attempts) {
        inventoryCounts.put(attempt.node,inventoryCounts.getOrDefault(attempt.node,0)+1);
        if (!attempt.reserved()) { continue; }
        JsonNode assignment=Json.parse(attempt.runBody).path("assignment");
        cpu.put(attempt.node,cpu.getOrDefault(attempt.node,0L)+assignment.path("resources").path("cpuMillis").asLong());
        memory.put(attempt.node,memory.getOrDefault(attempt.node,0L)+assignment.path("resources").path("memoryBytes").asLong());
        String countKey=attempt.node+"/"+attempt.job;
        jobCounts.put(countKey,jobCounts.getOrDefault(countKey,0)+1);
        for (JsonNode port:assignment.path("ports")) { ports.get(attempt.node).add(port.path("number").asInt()); }
      }
      int created=0;
      for (String body:tx.jobBodies()) {
        JsonNode job=Json.parse(body); JobKey key=key(job.get("jobKey"));
        for (String instance:tx.desiredInstances(key)) {
          if (attempts.size()+created>=48) { return null; }
          boolean previous=false, reserved=false;
          long last=0;
          for (AttemptRecord attempt:attempts) {
            if (attempt.job.equals(key.toString()) && attempt.instance.equals(instance)) {
              previous=true; reserved|=attempt.reserved(); last=Math.max(last,attempt.updatedMillis);
            }
          }
          if (reserved || (previous && "batch".equals(job.get("mode").asText()))
              || (previous && System.currentTimeMillis()-last<1000)) { continue; }
          for (NativeConfig.Node node:config.nodes) {
            JsonNode resources=job.path("template").path("resources");
            String countKey=node.name+"/"+key.toString();
            long requestedCpu=resources.path("cpuMillis").asLong(),requestedMemory=resources.path("memoryBytes").asLong();
            if (unknownReservations.contains(node.name) || !Boolean.TRUE.equals(reachable.get(node.name))
                || requestedCpu>node.cpu-cpu.getOrDefault(node.name,0L)
                || requestedMemory>node.memory-memory.getOrDefault(node.name,0L)
                || jobCounts.getOrDefault(countKey,0)>=job.path("maxPerAgent").asInt()
                || inventoryCounts.getOrDefault(node.name,0)>=120
                || 16-ports.get(node.name).size()<job.path("template").path("ports").size()) { continue; }
            ObjectNode run=resolve(job,instance,node,ports.get(node.name)); Message message=validated(run);
            ProtocolValidator.requireResolution(validated(job),message);
            String canonical=new String(message.canonicalBytes(),StandardCharsets.UTF_8);
            String attempt=run.path("identity").path("attempt").asText();
            created++;
            tx.createAttempt(attempt,key,instance); tx.allocate(attempt,node.name,canonical);
            tx.command(run.get("command").asText(),attempt,canonical);
            cpu.put(node.name,cpu.getOrDefault(node.name,0L)+requestedCpu);
            memory.put(node.name,memory.getOrDefault(node.name,0L)+requestedMemory);
            jobCounts.put(countKey,jobCounts.getOrDefault(countKey,0)+1);
            inventoryCounts.put(node.name,inventoryCounts.getOrDefault(node.name,0)+1);
            for (JsonNode port:run.path("assignment").path("ports")) { ports.get(node.name).add(port.path("number").asInt()); }
            break;
          }
        }
      }
      return null;
    });
  }
  private ObjectNode resolve(JsonNode job,String instance,NativeConfig.Node node,Set<Integer> occupiedPorts) throws Exception {
    ObjectNode run=base("Run").put("command",id("command")).put("desiredRevision",job.path("revision").asText());
    ObjectNode identity=Json.object().put("cluster",config.cluster).put("incarnation",config.incarnation)
        .put("instance",instance).put("attempt",id("attempt")).put("process",job.path("template").path("process").asText()).put("run",id("run"));
    identity.set("jobKey",job.get("jobKey")); run.set("identity",identity); run.set("target",node.target());
    ObjectNode assignment=job.get("template").deepCopy(); Map<String,String> ports=new HashMap<>(); int index=0;
    for (JsonNode port:assignment.path("ports")) {
      while (occupiedPorts.contains(node.portStart+index)) { index++; }
      int number=node.portStart+index++; ((ObjectNode)port).put("number",number).put("network",node.network);
      ports.put(port.path("name").asText(),Integer.toString(number));
    }
    ArrayNode argv=Json.array();
    for (JsonNode arg:assignment.path("argv")) { if(arg.isObject()) { argv.add(ports.get(arg.path("portRef").asText())); } else { argv.add(arg); } }
    assignment.set("argv",argv); run.set("assignment",assignment);
    run.put("templateSha256",Json.sha(Json.canonical(job.get("template")))); return run;
  }
  private void deliver(NativeConfig.Node node) throws Exception {
    List<CommandRecord> commands=store.read(Tx::commands);
    for (CommandRecord command:commands) {
      if (!command.pending) { continue; }
      JsonNode body=Json.parse(command.body);
      if (!node.target().equals(body.path("target"))) { continue; }
      ObjectNode delivery=base("Delivery").put("bodySha256",Json.sha(command.body));
      delivery.set("body",body);
      delivery.set("authority",Json.object().put("cluster",config.cluster).put("incarnation",config.incarnation)
          .put("schedulerEpoch",epoch).put("session",session));
      JsonNode result=transport.request(node,"POST","/v1/deliver",validated(delivery).json(),epoch,session);
      if (!command.command.equals(result.path("command").asText())
          || !Json.sha(command.body).equals(result.path("bodySha256").asText())) { throw new Conflict("Command receipt differs"); }
      if (!"accepted".equals(result.path("outcome").asText())) { throw new Conflict("Command rejected; allocation retained"); }
      store.write(tx -> { tx.acknowledgeCommand(command.command); return null; });
    }
  }
  public synchronized JsonNode state() throws Exception {
    return store.read(tx -> {
      ObjectNode result=Json.object().put("cluster",config.cluster).put("incarnation",config.incarnation).put("epoch",epoch);
      result.set("limits",Json.object().put("jobs",64).put("jobBytes",4096).put("attempts",48)
          .put("attemptHistoryFull",tx.attempts().size()>=48));
      ArrayNode jobs=Json.array(),attempts=Json.array(),commands=Json.array(),nodes=Json.array();
      for(String body:tx.jobBodies()) {
        JsonNode job=Json.parse(body); ObjectNode item=Json.object().put("mode",job.path("mode").asText());
        item.set("jobKey",job.get("jobKey")); item.set("body",job);
        item.set("desiredInstances",Json.MAPPER.valueToTree(tx.desiredInstances(key(job.get("jobKey"))))); jobs.add(item);
      }
      for(AttemptRecord a:tx.attempts()) {
        ObjectNode item=Json.object().put("node",a.node).put("state",a.state).put("cleanup",a.cleanup)
            .put("ready",a.ready && Boolean.TRUE.equals(reachable.get(a.node))).put("reserved",a.reserved()).put("sequence",a.sequence);
        JsonNode run=Json.parse(a.runBody); item.set("identity",run.get("identity")); item.set("run",run); attempts.add(item);
      }
      for(CommandRecord c:tx.commands()) {
        ObjectNode item=Json.object().put("command",c.command).put("attempt",c.attempt).put("pending",c.pending);
        item.set("body",Json.parse(c.body)); commands.add(item);
      }
      for(NativeConfig.Node node:config.nodes) {
        nodes.add(Json.object().put("node",node.name).put("reachable",Boolean.TRUE.equals(reachable.get(node.name)))
            .put("lastError",errors.get(node.name)).put("cursor",tx.committedCursor(node.scope())));
      }
      result.set("jobs",jobs);result.set("attempts",attempts);result.set("commands",commands);result.set("nodes",nodes);return result;
    });
  }
  static ObjectNode base(String kind) { return Json.object().put("version","native-v1alpha1").put("kind",kind); }
  static String id(String prefix) { return prefix+"-"+UUID.randomUUID().toString(); }
}
