/* Licensed under the Apache License, Version 2.0. See LICENSE for details. */
package org.apache.aurora.nativescheduler;

import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import org.apache.aurora.scheduler.storage.sql.NativeSqlStore;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;

public class NativePolicyTest {
  @Rule public TemporaryFolder temporary=new TemporaryFolder();
  private NativeEngineTest f;
  private JsonNode settings;
  @Before public void open() throws Exception {
    f=new NativeEngineTest(); f.temporary=temporary; f.open(); f.store.close();
    settings=Json.parse("{\"attributes\":{\"agent-a\":{\"rack\":[\"a\"]},\"agent-b\":{\"rack\":[\"b\"]}}}");
    restart();
    call("/v1/quotas",Json.object().put("operation","quota-1").put("role","www-data")
        .put("expectedRevision","0").put("revision","1").put("cpuMillis",10000).put("memoryBytes",2147483648L));
  }
  @After public void close() throws Exception { if(f!=null) { f.store.close(); } }
  private void restart() throws Exception {
    f.store.close(); f.store=new NativeSqlStore(f.state,"lab","recovery-a",true);
    f.engine=new NativeEngine(f.store,f.config,f.transport,false,settings);
  }
  private JsonNode call(String path,JsonNode r) throws Exception { return f.engine.policyRequest(path,r); }
  private ObjectNode job(String name,int instances,int cpu) throws Exception {
    ObjectNode j=f.job("service.json").deepCopy(); ((ObjectNode)j.path("jobKey")).put("name",name).put("role","www-data");
    j.put("instances",instances); ((ObjectNode)j.path("template").path("resources")).put("cpuMillis",cpu); return j;
  }
  private ObjectNode policy(int priority,boolean preemptible,int minReady,String rack) {
    ObjectNode p=NativePolicy.defaults().put("priority",priority).put("preemptible",preemptible).put("minReady",minReady);
    if(rack!=null) {
      ObjectNode c=Json.object().put("attribute","rack").put("negated",false); c.set("values",Json.array().add(rack));
      ((ArrayNode)p.path("constraints")).add(c);
    }
    return p;
  }
  private JsonNode submit(String id,ObjectNode j,ObjectNode p) throws Exception {
    ObjectNode r=Json.object().put("operation",id); r.set("job",j);r.set("policy",p);return call("/v1/policy/jobs",r);
  }
  private ObjectNode update(String id,ObjectNode old,String revision) throws Exception {
    ObjectNode target=old.deepCopy(); target.put("revision",revision);
    ObjectNode r=Json.object().put("operation",id).put("expectedRevision",old.path("revision").asText());
    r.set("jobKey",old.path("jobKey"));r.set("job",target);call("/v1/jobs/update",r);return target;
  }
  private JsonNode operation(String id) throws Exception {
    for(JsonNode op:f.engine.state().path("policy").path("operations")) { if(id.equals(op.path("operation").asText())) { return op; } }
    throw new AssertionError(id);
  }
  private void cleanup(int run) throws Exception {
    f.transport.observe(run,"stopped","complete",false); f.engine.tick(); Thread.sleep(1050); f.engine.tick();
  }
  @Test public void quotaRejectsAtomicallyAndOperationReplayCannotMutatePolicy() throws Exception {
    ObjectNode j=job("too-large",2,750); ObjectNode q=Json.object().put("operation","quota-2").put("role","www-data")
        .put("expectedRevision","1").put("revision","2").put("cpuMillis",1000).put("memoryBytes",2147483648L);
    call("/v1/quotas",q); String before=Json.canonical(f.engine.state());
    try { submit("reject",j,policy(0,false,0,null)); fail(); } catch(NativeEngine.Conflict expected) { }
    assertEquals(before,Json.canonical(f.engine.state()));
    j.put("instances",1); JsonNode accepted=submit("accepted",j,policy(0,false,0,"a"));
    assertEquals(accepted,submit("accepted",j,policy(0,false,0,"a")));
    try { submit("accepted",j,policy(1,false,0,"a")); fail(); } catch(NativeEngine.Conflict expected) { }
    f.engine.tick(); assertEquals(1,f.transport.runs.size()); assertEquals("agent-a",f.transport.runs.get(0).path("target").path("node").asText());
    restart(); assertEquals("completed",operation("accepted").path("state").asText());
  }
  @Test public void updateRestartWaitsForCleanupAndRollbackRestoresTemplateWithNewRevision() throws Exception {
    ObjectNode original=job("rolling",2,100); submit("submit",original,policy(0,false,1,null)); f.engine.tick();
    f.transport.observe(0,"running","pending",true);f.transport.observe(1,"running","pending",true);f.engine.tick();
    ObjectNode target=update("update",original,"2"); f.engine.tick(); assertEquals(1,f.transport.stops.size());
    restart(); f.engine.tick(); assertEquals(2,f.transport.runs.size());
    f.transport.observe(0,"stopped","pending",false);f.engine.tick(); assertEquals(2,f.transport.runs.size());
    cleanup(0);assertEquals(3,f.transport.runs.size());assertEquals("2",f.transport.runs.get(2).path("desiredRevision").asText());
    assertEquals(1,f.transport.stops.size());
    java.nio.file.Path snapshot=temporary.getRoot().toPath().resolve("mid-update-snapshot");
    f.store.snapshot(snapshot);
    try(NativeSqlStore restored=new NativeSqlStore(snapshot,"lab","recovery-a",true)) {
      NativeEngine inspection=new NativeEngine(restored,f.config,null,true,settings);
      assertEquals(f.engine.epoch,inspection.epoch);
      assertEquals(f.engine.state().path("policy").path("operations"),inspection.state().path("policy").path("operations"));
    }
    f.transport.observe(2,"running","pending",true);f.engine.tick();
    ObjectNode rollback=Json.object().put("operation","rollback").put("updateOperation","update").put("expectedRevision","2").put("revision","3");
    call("/v1/jobs/rollback",rollback); f.engine.tick(); assertEquals(2,f.transport.stops.size());
    restart();cleanup(2);assertEquals("3",f.transport.runs.get(3).path("desiredRevision").asText());
    assertEquals("superseded",operation("update").path("state").asText());
    f.engine.stop(original.path("jobKey")); restart();f.engine.tick();
    assertEquals("cancelled",operation("rollback").path("state").asText());
    assertEquals(4,f.transport.runs.size());
  }
  @Test public void drainPersistsCordonAndDestinationUntilCleanupAndReady() throws Exception {
    ObjectNode j=job("evacuate",1,100);submit("submit",j,policy(0,false,0,null));f.engine.tick();
    f.transport.observe(0,"running","pending",true);f.engine.tick();
    call("/v1/nodes/drain",Json.object().put("operation","drain").put("node","agent-a").put("expectedRevision","0").put("revision","1"));
    f.engine.tick();assertEquals(1,f.transport.stops.size());restart();f.engine.tick();assertEquals(1,f.transport.runs.size());
    cleanup(0);assertEquals(2,f.transport.runs.size());assertEquals("agent-b",f.transport.runs.get(1).path("target").path("node").asText());
    assertNotEquals("completed",operation("drain").path("state").asText());
    f.transport.observe(1,"running","pending",true);f.engine.tick();f.engine.tick();
    assertEquals("completed",operation("drain").path("state").asText());
    assertEquals("drained",f.engine.state().path("policy").path("nodes").get(0).path("mode").asText());
  }
  @Test public void minReadyAndUnknownInventoryBlockDrainWithoutStopping() throws Exception {
    ObjectNode j=job("available",1,100);submit("submit",j,policy(0,false,1,null));f.engine.tick();
    f.transport.observe(0,"running","pending",true);f.engine.tick();
    call("/v1/nodes/drain",Json.object().put("operation","drain").put("node","agent-a").put("expectedRevision","0").put("revision","1"));
    f.engine.tick();assertEquals("min-ready",operation("drain").path("reason").asText());assertEquals(0,f.transport.stops.size());
    f.transport.unknown=true;f.engine.tick();assertEquals("untrusted-source-inventory",operation("drain").path("reason").asText());
    restart();f.engine.tick();assertEquals(0,f.transport.stops.size());
  }
  @Test public void singleVictimPreemptionCannotReuseCapacityBeforeCleanupOrLoseItToVictim() throws Exception {
    ObjectNode low=job("low",1,750),high=job("high",1,750);
    submit("low",low,policy(1,true,0,"a"));f.engine.tick();f.transport.observe(0,"running","pending",true);f.engine.tick();
    submit("high",high,policy(2,false,0,"a"));f.engine.tick();assertEquals(1,f.transport.runs.size());
    ObjectNode r=Json.object().put("operation","preempt").put("instance","instance-0").put("expectedRevision",high.path("revision").asText())
        .put("victimAttempt",f.transport.runs.get(0).path("identity").path("attempt").asText());r.set("jobKey",high.path("jobKey"));
    call("/v1/preempt",r);f.engine.tick();assertEquals(1,f.transport.stops.size());restart();f.engine.tick();
    f.transport.observe(0,"stopped","pending",false);f.engine.tick();assertEquals(1,f.transport.runs.size());
    cleanup(0);assertEquals(2,f.transport.runs.size());assertEquals("high",f.transport.runs.get(1).path("identity").path("jobKey").path("name").asText());
    assertEquals("completed",operation("preempt").path("state").asText());
    assertEquals(operation("preempt"),call("/v1/preempt",r));
    r.put("operation","stale");try { call("/v1/preempt",r);fail(); }catch(NativeEngine.Conflict expected) { }
  }
  @Test public void committedStopCannotBeCountedAsReadyForAnotherDisruption() throws Exception {
    ObjectNode j=job("ready-budget",2,100);submit("submit",j,policy(0,false,1,null));f.engine.tick();
    f.transport.observe(0,"running","pending",true);f.transport.observe(1,"running","pending",true);f.engine.tick();
    f.store.write(tx -> { f.engine.stopAttempt(tx,tx.attempts().get(1),"restart");return null; });
    update("update",j,"2");f.engine.tick();
    assertEquals("min-ready",operation("update").path("reason").asText());
    assertEquals(1,f.transport.stops.size());
    f.engine.stop(j.path("jobKey"));
    try {
      call("/v1/jobs/rollback",Json.object().put("operation","rollback-after-stop").put("updateOperation","update")
          .put("expectedRevision","2").put("revision","3"));fail();
    }catch(NativeEngine.Conflict expected) { }
    restart();f.engine.tick();assertEquals(2,f.transport.runs.size());
  }
  @Test public void missingAttributesRejectPositiveAndMatchNegation() throws Exception {
    ObjectNode positive=policy(0,false,0,"a");
    ((ObjectNode)positive.path("constraints").get(0)).put("attribute","missing");
    submit("positive",job("positive",1,100),positive);
    ObjectNode negative=positive.deepCopy();((ObjectNode)negative.path("constraints").get(0)).put("negated",true);
    submit("negative",job("negative",1,100),negative);f.engine.tick();
    assertEquals(1,f.transport.runs.size());
    assertEquals("negative",f.transport.runs.get(0).path("identity").path("jobKey").path("name").asText());
  }
  @Test public void existingJobsUpgradeWithoutChangingEpochOnEnrollmentFailure() throws Exception {
    java.nio.file.Path path=temporary.newFolder("existing-v2").toPath();
    ObjectNode j=job("existing",1,100);
    try(NativeSqlStore prior=new NativeSqlStore(path,"lab","recovery-a")) {
      NativeEngine e=new NativeEngine(prior,f.config,f.transport,false);
      e.submit(Json.canonical(j).getBytes(StandardCharsets.UTF_8));
    }
    try(NativeSqlStore upgraded=new NativeSqlStore(path,"lab","recovery-a",true)) {
      ObjectNode different=(ObjectNode)Json.parse(f.config.canonical);
      ((ObjectNode)different.path("nodes").get(0)).put("cpuMillis",999);
      NativeConfig wrong=new NativeConfig(Json.canonical(different).getBytes(StandardCharsets.UTF_8));
      try { new NativeEngine(upgraded,wrong,f.transport,false,settings);fail(); }catch(NativeEngine.Conflict expected) { }
      assertEquals("1",upgraded.read(NativeSqlStore.Tx::schedulerEpoch));
      assertNull(upgraded.read(tx -> tx.policy("config","settings")));
      NativeEngine current=new NativeEngine(upgraded,f.config,null,true,settings);
      // Inspection cannot initialize missing policy settings.
      fail("Uninitialized policy inspection unexpectedly succeeded");
    }catch(NativeEngine.Conflict expected) { }
    try(NativeSqlStore upgraded=new NativeSqlStore(path,"lab","recovery-a",true)) {
      NativeEngine current=new NativeEngine(upgraded,f.config,null,false,settings);
      assertEquals(1,current.state().path("jobs").size());assertEquals("2",current.epoch);
      try { new NativeEngine(upgraded,f.config,null,false);fail(); }catch(NativeEngine.Conflict expected) { }
    }
  }
  @Test public void configurationChangesAndUnsupportedPolicyFieldsAreRejected() throws Exception {
    ObjectNode p=policy(0,false,0,null).put("automaticRollback",true);
    try { submit("unsupported",job("unsupported",1,100),p);fail(); }catch(IllegalArgumentException expected) { }
    JsonNode changed=Json.parse("{\"attributes\":{\"agent-a\":{},\"agent-b\":{}}}");
    try { new NativeEngine(f.store,f.config,f.transport,false,changed);fail(); }catch(NativeEngine.Conflict expected) { }
    assertEquals(0,f.engine.state().path("jobs").size());
  }
}
