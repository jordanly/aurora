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

import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import org.apache.aurora.scheduler.storage.sql.NativeSqlStore;
import org.apache.aurora.scheduler.storage.sql.NativeSqlStore.*;

public class NativeEngineTest {
  @Rule public TemporaryFolder temporary=new TemporaryFolder();
  NativeConfig config; NativeSqlStore store; NativeEngine engine; Fake transport; Path state;
  @Before public void open() throws Exception {
    ObjectNode value=Json.object().put("cluster","lab").put("incarnation","recovery-a");
    ArrayNode nodes=Json.array();
    for(String suffix:Arrays.asList("a","b")) {
      nodes.add(Json.object().put("node","agent-"+suffix).put("journal","journal-"+suffix)
          .put("boot","boot-"+suffix).put("runtime","runtime-"+suffix).put("url","https://agent-"+suffix+":9443")
          .put("network","agent-container").put("portStart",18080).put("cpuMillis",1000).put("memoryBytes",536870912));
    }
    value.set("nodes",nodes); config=new NativeConfig(value.toString().getBytes(StandardCharsets.UTF_8));
    state=temporary.newFolder("state").toPath(); store=new NativeSqlStore(state,"lab","recovery-a");
    transport=new Fake(); engine=new NativeEngine(store,config,transport,false);
  }
  @After public void close() throws Exception { store.close(); }
  JsonNode job(String file) throws Exception { return Json.parse(Files.readAllBytes(Paths.get(System.getProperty("aurora.fixtures"),"valid",file))); }
  void submit(String file) throws Exception { engine.submit(Json.canonical(job(file)).getBytes(StandardCharsets.UTF_8)); }
  void restart() throws Exception { store.close(); store=new NativeSqlStore(state,"lab","recovery-a"); engine=new NativeEngine(store,config,transport,false); }
  @Test public void batchCompletesOnceAcrossRestartAndRetainsDesiredMembership() throws Exception {
    submit("batch.json"); engine.tick(); assertEquals(1,transport.runs.size());
    transport.observe(0,"succeeded","complete",false); engine.tick();
    assertFalse(engine.state().path("attempts").get(0).path("reserved").asBoolean());
    assertEquals(1,engine.state().path("jobs").get(0).path("desiredInstances").size());
    restart(); engine.tick(); assertEquals(1,transport.runs.size()); assertEquals("2",engine.epoch);
    assertEquals("1",transport.after.get("agent-a"));
  }
  @Test public void partitionAndUnknownCleanupNeverReplaceService() throws Exception {
    submit("service.json"); engine.tick(); assertEquals(2,transport.runs.size());
    transport.observe(0,"lost","unknown",false); engine.tick();
    transport.offline.add("agent-a"); engine.tick();
    assertEquals(2,transport.runs.size()); assertTrue(engine.state().path("attempts").get(0).path("reserved").asBoolean());
  }
  @Test public void serviceReplacesOnlyAfterTerminalCleanupAndNeverAfterCancellation() throws Exception {
    submit("service.json"); engine.tick();
    transport.observe(0,"failed","pending",false); engine.tick(); assertEquals(2,transport.runs.size());
    transport.observe(0,"failed","complete",false); engine.tick(); Thread.sleep(1050); engine.tick();
    assertEquals(3,transport.runs.size());
    engine.stop(job("service.json").get("jobKey")); engine.tick();
    assertEquals(2,transport.stops.size());
    restart(); engine.tick(); assertEquals(3,transport.runs.size());
    assertEquals(0,engine.state().path("jobs").get(0).path("desiredInstances").size());
  }
  @Test public void replayPendingBodyWithFreshEpochAfterLostReceipt() throws Exception {
    submit("batch.json"); transport.loseReceipt=true; engine.tick();
    String first=Json.canonical(transport.runs.get(0)); assertTrue(store.read(Tx::commands).get(0).pending);
    restart(); transport.loseReceipt=false; engine.tick();
    assertEquals(2,transport.runs.size()); assertEquals(first,Json.canonical(transport.runs.get(1)));
    assertEquals("2",transport.lastEpoch); assertFalse(store.read(Tx::commands).get(0).pending);
  }
  @Test public void cancelBeforeDeliveryCommitsStopAndSuppressesPendingRun() throws Exception {
    submit("batch.json"); transport.loseReceipt=true; engine.tick();
    engine.stop(job("batch.json").get("jobKey")); transport.loseReceipt=false; engine.tick();
    assertEquals(1,transport.runs.size()); assertEquals(1,transport.stops.size());
    assertEquals(0,engine.state().path("jobs").get(0).path("desiredInstances").size());
  }
  @Test public void cursorGapNeverCommitsOrAcknowledgesAndMatchingReceiptCommitsBeforeAck() throws Exception {
    submit("batch.json"); engine.tick(); transport.observe(0,"succeeded","complete",false);
    transport.observations.get("agent-a").get(0).put("cursor","2");
    transport.acks.clear(); engine.tick();
    assertEquals("0",store.read(tx -> tx.committedCursor(config.nodes.get(0).scope())));
    assertFalse(transport.acks.containsKey("agent-a"));
    transport.observations.get("agent-a").get(0).put("cursor","1"); engine.tick();
    assertEquals("1",transport.acks.get("agent-a"));
  }
  @Test public void conflictingObservationSequenceRollsBackCursorAndCannotFreeAllocation() throws Exception {
    submit("batch.json"); engine.tick(); transport.observe(0,"running","pending",false); engine.tick();
    transport.observe(0,"succeeded","complete",false);
    transport.observations.get("agent-a").get(1).put("sequence","1"); engine.tick();
    assertEquals("1",store.read(tx -> tx.committedCursor(config.nodes.get(0).scope())));
    assertTrue(engine.state().path("attempts").get(0).path("reserved").asBoolean());
  }
  @Test public void idempotentResubmitCannotResurrectCancelledMembership() throws Exception {
    submit("batch.json"); engine.stop(job("batch.json").get("jobKey"));
    assertFalse(engine.submit(Json.canonical(job("batch.json")).getBytes(StandardCharsets.UTF_8)));
    engine.tick(); assertEquals(0,transport.runs.size());
    ObjectNode different=job("batch.json").deepCopy(); different.put("revision","2");
    try { engine.submit(Json.canonical(different).getBytes(StandardCharsets.UTF_8)); fail(); }
    catch(NativeEngine.Conflict expected) { }
  }
  @Test public void wrongAgentScopeAndUnknownReservationBlockPlacement() throws Exception {
    submit("service.json"); transport.wrongScope=true; engine.tick(); assertEquals(0,transport.runs.size());
    transport.wrongScope=false; transport.unknown=true; engine.tick(); assertEquals(0,transport.runs.size());
  }
  @Test public void snapshotRestoresCompletedStateWithoutEpochAdvanceOrAgentContact() throws Exception {
    submit("batch.json"); engine.tick(); transport.observe(0,"succeeded","complete",false); engine.tick();
    Path snapshot=temporary.getRoot().toPath().resolve("backup/scheduler.db"); store.snapshot(snapshot.getParent());
    try(NativeSqlStore restored=new NativeSqlStore(snapshot.getParent(),"lab","recovery-a")) {
      NativeEngine read=new NativeEngine(restored,config,null,true);
      assertEquals("1",read.epoch); assertEquals("succeeded",read.state().path("attempts").get(0).path("state").asText());
    }
    try { store.snapshot(snapshot.getParent()); fail(); } catch(java.io.IOException expected) { }
  }
  @Test public void mixedJobsShareCapacityButPortsRemainExactAndDistinct() throws Exception {
    submit("service.json"); engine.tick(); submit("batch.json"); engine.tick();
    assertEquals(3,transport.runs.size());
    ObjectNode second=job("service.json").deepCopy();
    ((ObjectNode)second.get("jobKey")).put("name","second");
    engine.submit(Json.canonical(second).getBytes(StandardCharsets.UTF_8)); engine.tick();
    assertEquals(5,transport.runs.size());
    assertEquals(18080,transport.runs.get(0).path("assignment").path("ports").get(0).path("number").asInt());
    assertEquals(18081,transport.runs.get(3).path("assignment").path("ports").get(0).path("number").asInt());
  }
  @Test public void releasedSnapshotCanBeReadyAndAbsenceCannotReleaseReservation() throws Exception {
    submit("service.json"); engine.tick(); transport.released=true; engine.tick();
    assertTrue(engine.state().path("attempts").get(0).path("ready").asBoolean());
    transport.released=false;engine.tick();
    assertTrue(engine.state().path("attempts").get(0).path("reserved").asBoolean());
  }
  @Test public void sixtyFourJobBudgetRejectsFurtherIdentitiesWithoutPartialMembership() throws Exception {
    for (int i=0;i<64;i++) {
      ObjectNode value=job("service-zero.json").deepCopy();
      ((ObjectNode)value.get("jobKey")).put("name","job-"+i);
      engine.submit(Json.canonical(value).getBytes(StandardCharsets.UTF_8));
    }
    try { submit("batch.json"); fail(); } catch(NativeEngine.Conflict expected) { }
    assertEquals(64,engine.state().path("jobs").size());
    assertEquals(0,engine.state().path("attempts").size());
  }
  final class Fake implements NativeEngine.Transport {
    final List<JsonNode> runs=new ArrayList<>(),stops=new ArrayList<>();
    final Map<String,List<ObjectNode>> observations=new HashMap<>();
    final Map<String,String> after=new HashMap<>(),acks=new HashMap<>();
    final Set<String> offline=new HashSet<>();
    boolean loseReceipt,wrongScope,unknown,released; String lastEpoch;
    Fake() { for(NativeConfig.Node node:config.nodes) { observations.put(node.name,new ArrayList<>()); } }
    void observe(int run,String state,String cleanup,boolean ready) {
      JsonNode body=runs.get(run); String node=body.path("target").path("node").asText();
      List<ObjectNode> list=observations.get(node); long sequence=1;
      for(JsonNode old:list) { if(old.path("identity").equals(body.path("identity"))) { sequence++; } }
      ObjectNode value=NativeEngine.base("Observation").put("cursor",Integer.toString(list.size()+1))
          .put("sequence",Long.toString(sequence)).put("state",state).put("cleanup",cleanup).put("ready",ready);
      value.set("identity",body.get("identity"));value.set("source",body.get("target"));list.add(value);
    }
    @Override public JsonNode request(NativeConfig.Node node,String method,String path,JsonNode body,String epoch,String session) throws Exception {
      if(offline.contains(node.name)) { throw new java.io.IOException("partition"); }
      lastEpoch=epoch;
      if(path.equals("/v1/session")) { return body; }
      if(path.startsWith("/v1/state?")) {
        String cursor=path.substring(path.indexOf('=')+1,path.indexOf('&'));after.put(node.name,cursor);
        ObjectNode actual=node.target().put("cluster","lab").put("incarnation","recovery-a").put("peer","scheduler")
            .put("schedulerEpoch",epoch).put("session",session).put("cpuMillis",node.cpu).put("memoryBytes",node.memory);
        if(wrongScope) { actual.put("boot","different"); }
        ArrayNode page=Json.array();String next=cursor;
        for(ObjectNode observation:observations.get(node.name)) {
          if(Long.parseLong(observation.path("cursor").asText())>Long.parseLong(cursor)) {
            page.add(observation); next=observation.path("cursor").asText();
          }
        }
        ObjectNode inventory=Json.object();
        if (released) {
          for (JsonNode run:runs) {
            if (!node.name.equals(run.path("target").path("node").asText())) { continue; }
            ObjectNode entry=Json.object().put("sequence","1").put("reserved",true);
            entry.set("identity",run.get("identity"));
            entry.set("execution",Json.object().put("phase","released").put("outcome","running")
                .put("cleanup","pending").put("ready",true));
            inventory.set(run.path("identity").path("attempt").asText(),entry);
          }
        }
        if(unknown) { inventory.set("foreign",Json.object().put("reserved",true).set("identity",Json.object().put("attempt","foreign"))); }
        ObjectNode state=Json.object().put("cursor",next).put("ack",acks.getOrDefault(node.name,"0"));
        state.set("observations",page);state.set("commands",Json.object());state.set("attempts",inventory);
        ObjectNode response=Json.object().put("nextCursor",next).put("hasMore",false);response.set("config",actual);response.set("state",state);return response;
      }
      if(path.equals("/v1/ack")) {
        String committed=store.read(tx -> tx.committedCursor(node.scope()));
        assertEquals(committed,body.path("committedCursor").asText());acks.put(node.name,committed);return Json.object().put("ok",true);
      }
      if(path.equals("/v1/deliver")) {
        JsonNode command=body.get("body");
        if(command.path("kind").asText().equals("Run")) { runs.add(command); } else { stops.add(command); }
        if(loseReceipt) { throw new java.io.IOException("lost receipt"); }
        return Json.object().put("command",command.path("command").asText()).put("bodySha256",body.path("bodySha256").asText()).put("outcome","accepted");
      }
      throw new AssertionError(path);
    }
  }
}
