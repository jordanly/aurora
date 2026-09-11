/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.apache.aurora.nativescheduler;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import org.apache.aurora.nativeprotocol.ProtocolValidator;
import org.apache.aurora.scheduler.storage.sql.NativeSqlStore;
import org.apache.aurora.scheduler.storage.sql.NativeSqlStore.*;

/** Opt-in bounded policy. SQL owns every operation; NativeEngine remains the only allocator. */
final class NativePolicy {
  private final NativeEngine engine;
  private final NativeSqlStore store;
  private final NativeConfig config;
  private final JsonNode attributes;

  NativePolicy(NativeEngine engine,NativeSqlStore store,NativeConfig config,JsonNode settings,
      boolean inspect) throws Exception {
    this.engine=engine; this.store=store; this.config=config;
    Json.fields(settings,"attributes"); attributes=settings.path("attributes").deepCopy();
    if(!attributes.isObject() || attributes.size()!=config.nodes.size()) { invalid("Node attributes"); }
    for(NativeConfig.Node node:config.nodes) {
      JsonNode attrs=attributes.path(node.name);
      if(!attrs.isObject() || attrs.size()>8) { invalid("Node attributes"); }
      Iterator<String> names=attrs.fieldNames();
      while(names.hasNext()) {
        String name=names.next(); NativeConfig.token(name); values(attrs.get(name));
      }
    }
    String canonical=Json.canonical(settings);
    store.write(tx -> {
      if(!tx.policyEnabled()) { invalid("Policy schema not enabled"); }
      String old=tx.policy("config","settings");
      if(old!=null && !old.equals(canonical)) { conflict("Policy configuration differs"); }
      if(old==null && !inspect) { tx.putPolicy("config","settings",canonical); }
      if(old==null && inspect) { conflict("Policy not initialized"); }
      return null;
    });
  }
  static ObjectNode defaults() {
    ObjectNode p=Json.object().put("priority",0).put("preemptible",false).put("minReady",0);
    p.set("constraints",Json.array()); return p;
  }
  private static void invalid(String message) { throw new IllegalArgumentException(message); }
  private static void conflict(String message) throws NativeEngine.Conflict {
    throw new NativeEngine.Conflict(message);
  }
  private static String token(JsonNode n,String field) { return NativeConfig.token(Json.string(n,field)); }
  private static long number(JsonNode n,String field,long maximum) {
    JsonNode v=n.path(field);
    if(!v.isIntegralNumber() || !v.canConvertToLong() || v.asLong()<0 || v.asLong()>maximum) {
      invalid("Invalid "+field);
    }
    return v.asLong();
  }
  private static void values(JsonNode n) {
    if(!n.isArray() || n.size()<1 || n.size()>8) { invalid("Attribute value limit"); }
    Set<String> seen=new HashSet<>();
    for(JsonNode v:n) {
      if(!v.isTextual() || !seen.add(NativeConfig.token(v.asText()))) { invalid("Attribute values"); }
    }
  }
  private static void validatePolicy(JsonNode p,JsonNode job) {
    Json.fields(p,"constraints","priority","preemptible","minReady");
    number(p,"priority",1000); number(p,"minReady",job.path("instances").asInt());
    if(!p.path("preemptible").isBoolean()) { invalid("preemptible"); }
    JsonNode cs=p.path("constraints");
    if(!cs.isArray() || cs.size()>8) { invalid("Constraint limit"); }
    Set<String> names=new HashSet<>();
    for(JsonNode c:cs) {
      Json.fields(c,"attribute","values","negated");
      if(!names.add(token(c,"attribute")) || !c.path("negated").isBoolean()) { invalid("Constraint"); }
      values(c.path("values"));
    }
  }
  private static JobKey jobKey(JsonNode value) {
    Json.fields(value,"role","environment","name"); return NativeEngine.key(value);
  }
  private JsonNode validateJob(JsonNode value) throws Exception {
    byte[] bytes=Json.canonical(value).getBytes(StandardCharsets.UTF_8);
    if(bytes.length>4096) { invalid("Job exceeds 4 KiB"); }
    var message=engine.validated(value);
    if(!"Job".equals(value.path("kind").asText())) { invalid("Expected Job"); }
    engine.scope(value); ProtocolValidator.requireCapabilities(message,Collections.emptySet());
    return message.json();
  }
  private JsonNode doc(Tx tx,String kind,String id) throws Exception {
    String body=tx.policy(kind,id); return body==null?null:Json.parse(body);
  }
  private List<ObjectNode> operations(Tx tx) throws Exception {
    List<ObjectNode> result=new ArrayList<>();
    for(String body:tx.policies("operation")) { result.add((ObjectNode)Json.parse(body)); }
    return result;
  }
  private static boolean active(JsonNode op) {
    return !Set.of("completed","cancelled","superseded","aborted").contains(op.path("state").asText());
  }
  private void save(Tx tx,ObjectNode op) throws Exception {
    tx.putPolicy("operation",op.path("operation").asText(),Json.canonical(op));
  }
  private void block(Tx tx,ObjectNode op,String reason) throws Exception {
    op.put("state","blocked").put("reason",reason); save(tx,op);
  }
  private void running(ObjectNode op) { op.put("state","running").put("reason",""); }
  private static boolean isUpdate(JsonNode op) {
    return Set.of("update","rollback").contains(op.path("kind").asText());
  }
  private JsonNode policy(Tx tx,String job) throws Exception {
    JsonNode value=doc(tx,"job",job); return value==null?defaults():value;
  }
  private JsonNode node(Tx tx,String name) throws Exception {
    JsonNode n=doc(tx,"node",name);
    return n==null?Json.object().put("node",name).put("revision","0").put("mode","active"):n;
  }
  private NativeConfig.Node enrolled(String name) {
    for(NativeConfig.Node n:config.nodes) { if(n.name.equals(name)) { return n; } }
    invalid("Unknown node"); return null;
  }
  private static String revision(JsonNode request,String field) {
    return NativeSqlStore.counter(Json.string(request,field));
  }
  private static void nextRevision(String old,String expected,String next) throws Exception {
    if(!old.equals(expected) || new BigInteger(next).compareTo(new BigInteger(old))<=0) {
      conflict("Revision conflict");
    }
  }
  private void freeJob(Tx tx,String job,String ignoredOperation) throws Exception {
    for(ObjectNode op:operations(tx)) {
      if(!active(op) || op.path("operation").asText().equals(ignoredOperation)) { continue; }
      if(job.equals(op.path("job").asText()) || job.equals(op.path("current").path("job").asText())) {
        conflict("Job has active disruptive operation");
      }
      if("drain".equals(op.path("kind").asText())) {
        for(AttemptRecord a:tx.attempts()) {
          if(a.reserved() && a.job.equals(job) && a.node.equals(op.path("node").asText())) {
            conflict("Job belongs to active drain");
          }
        }
      }
      if("preempt".equals(op.path("kind").asText()) && job.equals(op.path("victimJob").asText())) {
        conflict("Job belongs to active preemption");
      }
    }
  }
  private void freeNode(Tx tx,String name) throws Exception {
    for(ObjectNode op:operations(tx)) {
      if(active(op) && (name.equals(op.path("node").asText())
          || name.equals(op.path("current").path("destination").asText()))) {
        conflict("Node has active disruptive operation");
      }
    }
  }
  JsonNode request(String path,JsonNode request) throws Exception {
    if(Json.canonical(request).length()>16384) { invalid("Policy request limit"); }
    String kind=switch(path) {
      case "/v1/policy/jobs" -> "submit";
      case "/v1/quotas" -> "quota";
      case "/v1/jobs/update" -> "update";
      case "/v1/jobs/rollback" -> "rollback";
      case "/v1/nodes/drain" -> "drain";
      case "/v1/nodes/cordon" -> "cordon";
      case "/v1/preempt" -> "preempt";
      default -> throw new IllegalArgumentException("Unknown policy operation");
    };
    String id=token(request,"operation"); String canonical=Json.canonical(request);
    return store.write(tx -> {
      JsonNode prior=doc(tx,"operation",id);
      if(prior!=null) {
        if(!kind.equals(prior.path("kind").asText())
            || !canonical.equals(Json.canonical(prior.path("request")))) { conflict("Operation identity conflict"); }
        return prior;
      }
      if(tx.policies("operation").size()>=64) { conflict("Operation history full"); }
      ObjectNode op=Json.object().put("operation",id).put("kind",kind).put("state","running").put("reason","");
      op.set("request",request);
      switch(kind) {
        case "submit" -> submit(tx,request,op);
        case "quota" -> quota(tx,request,op);
        case "update" -> update(tx,request,op);
        case "rollback" -> rollback(tx,request,op);
        case "drain","cordon" -> maintenance(tx,request,op,kind);
        case "preempt" -> preempt(tx,request,op);
        default -> throw new AssertionError();
      }
      save(tx,op); return op;
    });
  }
  void admitJob(Tx tx,JsonNode job,JsonNode p) throws Exception {
    validatePolicy(p,job); String key=NativeEngine.key(job.path("jobKey")).toString();
    checkQuota(tx,job,null);
    tx.putPolicy("job",key,Json.canonical(p));
  }
  private void submit(Tx tx,JsonNode r,ObjectNode op) throws Exception {
    Json.fields(r,"operation","job","policy"); JsonNode job=validateJob(r.path("job"));
    JobKey key=NativeEngine.key(job.path("jobKey"));
    if(tx.jobBody(key)!=null || tx.jobBodies().size()>=64) { conflict("Job exists or inventory full"); }
    admitJob(tx,job,r.path("policy"));
    tx.createJob(key,job.path("revision").asText(),job.path("mode").asText(),Json.canonical(job));
    for(int i=0;i<job.path("instances").asInt();i++) { tx.addInstance(key,"instance-"+i); }
    op.put("job",key.toString()).put("state","completed");
  }
  private void quota(Tx tx,JsonNode r,ObjectNode op) throws Exception {
    Json.fields(r,"operation","role","expectedRevision","revision","cpuMillis","memoryBytes");
    String role=token(r,"role"); JsonNode old=doc(tx,"quota",role);
    nextRevision(old==null?"0":old.path("revision").asText(),revision(r,"expectedRevision"),revision(r,"revision"));
    number(r,"cpuMillis",9007199254740991L); number(r,"memoryBytes",9007199254740991L);
    long[] usage=usage(tx,role,null,null);
    if(usage[0]>r.path("cpuMillis").asLong() || usage[1]>r.path("memoryBytes").asLong()) { conflict("Quota below admitted demand"); }
    ObjectNode quota=Json.object().put("role",role).put("revision",revision(r,"revision"))
        .put("cpuMillis",r.path("cpuMillis").asLong()).put("memoryBytes",r.path("memoryBytes").asLong());
    tx.putPolicy("quota",role,Json.canonical(quota)); op.put("state","completed");
  }
  private static long[] resources(JsonNode job) {
    JsonNode r=job.path("template").path("resources");
    return new long[]{r.path("cpuMillis").asLong(),r.path("memoryBytes").asLong()};
  }
  private static void max(long[] a,long[] b) { a[0]=Math.max(a[0],b[0]); a[1]=Math.max(a[1],b[1]); }
  private long[] usage(Tx tx,String role,JsonNode replacement,JsonNode envelope) throws Exception {
    Map<String,long[]> slots=new HashMap<>();
    String replacementKey=replacement==null?null:NativeEngine.key(replacement.path("jobKey")).toString();
    List<JsonNode> jobs=new ArrayList<>(); boolean found=false;
    for(String body:tx.jobBodies()) {
      JsonNode j=Json.parse(body);
      if(NativeEngine.key(j.path("jobKey")).toString().equals(replacementKey)) { j=replacement; found=true; }
      jobs.add(j);
    }
    if(replacement!=null && !found) { jobs.add(replacement); }
    for(JsonNode j:jobs) {
      if(!role.equals(j.path("jobKey").path("role").asText())) { continue; }
      JobKey key=NativeEngine.key(j.path("jobKey"));
      List<String> desired=tx.desiredInstances(key);
      if(j==replacement && !found) {
        desired=new ArrayList<>(); for(int i=0;i<j.path("instances").asInt();i++) { desired.add("instance-"+i); }
      }
      for(String instance:desired) {
        long[] amount=resources(j);
        if(key.toString().equals(replacementKey) && envelope!=null) { max(amount,resources(envelope)); }
        slots.put(key+"/"+instance,amount);
      }
    }
    for(ObjectNode op:operations(tx)) {
      if(!active(op) || !isUpdate(op) || !op.path("job").asText().startsWith(role+"/")) { continue; }
      Iterator<Map.Entry<String,JsonNode>> sources=op.path("sources").properties().iterator();
      while(sources.hasNext()) {
        var entry=sources.next(); long[] amount=slots.get(op.path("job").asText()+"/"+entry.getKey());
        if(amount!=null) { max(amount,resources(entry.getValue())); max(amount,resources(op.path("target"))); }
      }
    }
    Map<String,long[]> reserved=new HashMap<>();
    for(AttemptRecord a:tx.attempts()) {
      if(!a.reserved() || !a.job.startsWith(role+"/")) { continue; }
      JsonNode r=Json.parse(a.runBody).path("assignment").path("resources");
      long[] amount=reserved.computeIfAbsent(a.job+"/"+a.instance,k -> new long[2]);
      amount[0]=Math.addExact(amount[0],r.path("cpuMillis").asLong());
      amount[1]=Math.addExact(amount[1],r.path("memoryBytes").asLong());
    }
    for(var entry:reserved.entrySet()) { max(slots.computeIfAbsent(entry.getKey(),k -> new long[2]),entry.getValue()); }
    long[] result=new long[2];
    for(long[] amount:slots.values()) { result[0]=Math.addExact(result[0],amount[0]); result[1]=Math.addExact(result[1],amount[1]); }
    return result;
  }
  private void checkQuota(Tx tx,JsonNode replacement,JsonNode envelope) throws Exception {
    String role=replacement.path("jobKey").path("role").asText(); JsonNode quota=doc(tx,"quota",role);
    if(quota==null) { conflict("Role quota required"); }
    long[] demand=usage(tx,role,replacement,envelope);
    if(demand[0]>quota.path("cpuMillis").asLong() || demand[1]>quota.path("memoryBytes").asLong()) {
      conflict("Role quota exceeded");
    }
  }
  private JsonNode requireJob(Tx tx,JobKey key) throws Exception {
    String body=tx.jobBody(key); if(body==null) { conflict("Unknown job"); }
    return Json.parse(body);
  }
  private void update(Tx tx,JsonNode r,ObjectNode op) throws Exception {
    Json.fields(r,"operation","jobKey","expectedRevision","job"); JobKey key=jobKey(r.path("jobKey"));
    JsonNode old=requireJob(tx,key),target=validateJob(r.path("job"));
    if(!key.equals(NativeEngine.key(target.path("jobKey")))) { invalid("Update job key differs"); }
    nextRevision(old.path("revision").asText(),revision(r,"expectedRevision"),target.path("revision").asText());
    if(!"service".equals(old.path("mode").asText()) || !"service".equals(target.path("mode").asText())
        || old.path("instances").asInt()!=target.path("instances").asInt()
        || tx.desiredInstances(key).size()!=old.path("instances").asInt()) { invalid("Only unchanged-size active service updates supported"); }
    freeJob(tx,key.toString(),null); checkQuota(tx,target,old);
    ObjectNode sources=Json.object(); for(String instance:tx.desiredInstances(key)) { sources.set(instance,old); }
    initializeUpdate(op,key,sources,target,old);
    tx.replaceJob(key,old.path("revision").asText(),target.path("revision").asText(),Json.canonical(target));
  }
  private static void initializeUpdate(ObjectNode op,JobKey key,ObjectNode sources,JsonNode target,JsonNode before) {
    op.put("job",key.toString()).put("index",0).put("phase","select");
    op.set("sources",sources); op.set("target",target); op.set("before",before); op.set("selected",Json.array());
  }
  private void rollback(Tx tx,JsonNode r,ObjectNode op) throws Exception {
    Json.fields(r,"operation","updateOperation","expectedRevision","revision");
    JsonNode original=doc(tx,"operation",token(r,"updateOperation"));
    if(original==null || !"update".equals(original.path("kind").asText())
        || Set.of("superseded","cancelled","aborted").contains(original.path("state").asText())) { conflict("Update cannot roll back"); }
    JobKey key=NativeEngine.key(original.path("target").path("jobKey")); JsonNode old=requireJob(tx,key);
    if(!old.path("revision").equals(original.path("target").path("revision"))) { conflict("Update no longer current"); }
    nextRevision(old.path("revision").asText(),revision(r,"expectedRevision"),revision(r,"revision"));
    if(tx.desiredInstances(key).size()!=old.path("instances").asInt()) { conflict("Job was stopped"); }
    freeJob(tx,key.toString(),original.path("operation").asText());
    ObjectNode target=original.path("before").deepCopy(); target.put("revision",revision(r,"revision"));
    validateJob(target); checkQuota(tx,target,old);
    ObjectNode sources=Json.object();
    for(String instance:tx.desiredInstances(key)) { sources.set(instance,effectiveJob(tx,old,instance)); }
    initializeUpdate(op,key,sources,target,old);
    ObjectNode superseded=original.deepCopy(); superseded.put("state","superseded"); save(tx,superseded);
    tx.replaceJob(key,old.path("revision").asText(),target.path("revision").asText(),Json.canonical(target));
  }
  private void maintenance(Tx tx,JsonNode r,ObjectNode op,String kind) throws Exception {
    if("drain".equals(kind)) { Json.fields(r,"operation","node","expectedRevision","revision"); }
    else { Json.fields(r,"operation","node","expectedRevision","revision","cordoned");
      if(!r.path("cordoned").isBoolean()) { invalid("cordoned"); } }
    String name=token(r,"node"); enrolled(name); JsonNode old=node(tx,name);
    nextRevision(old.path("revision").asText(),revision(r,"expectedRevision"),revision(r,"revision"));
    freeNode(tx,name);
    if("drain".equals(kind)) {
      for(AttemptRecord a:tx.attempts()) { if(a.reserved() && a.node.equals(name)) { freeJob(tx,a.job,null); } }
    }
    String mode="drain".equals(kind)?"draining":r.path("cordoned").asBoolean()?"cordoned":"active";
    tx.putPolicy("node",name,Json.canonical(Json.object().put("node",name).put("revision",revision(r,"revision")).put("mode",mode)));
    op.put("node",name); if("cordon".equals(kind)) { op.put("state","completed"); }
  }
  private AttemptRecord attempt(Tx tx,String id) throws Exception {
    for(AttemptRecord a:tx.attempts()) { if(a.attempt.equals(id)) { return a; } } return null;
  }
  private AttemptRecord reserved(Tx tx,String job,String instance) throws Exception {
    for(AttemptRecord a:tx.attempts()) { if(a.job.equals(job) && a.instance.equals(instance) && a.reserved()) { return a; } }
    return null;
  }
  private boolean stopping(Tx tx,String id) throws Exception {
    for(CommandRecord c:tx.commands()) { if(c.attempt.equals(id) && "Stop".equals(Json.parse(c.body).path("kind").asText())) { return true; } }
    return false;
  }
  private boolean ready(Tx tx,AttemptRecord a) throws Exception {
    return a!=null && a.reserved() && a.ready && engine.trusted(a.node) && !stopping(tx,a.attempt);
  }
  private boolean canDisrupt(Tx tx,AttemptRecord victim) throws Exception {
    int ready=0;
    for(AttemptRecord a:tx.attempts()) {
      if(a.job.equals(victim.job) && !a.attempt.equals(victim.attempt) && ready(tx,a)) { ready++; }
    }
    return ready>=policy(tx,victim.job).path("minReady").asInt();
  }
  private void preempt(Tx tx,JsonNode r,ObjectNode op) throws Exception {
    Json.fields(r,"operation","jobKey","instance","victimAttempt","expectedRevision"); JobKey key=jobKey(r.path("jobKey"));
    String instance=token(r,"instance"); JsonNode job=requireJob(tx,key);
    if(!revision(r,"expectedRevision").equals(job.path("revision").asText())) { conflict("Candidate revision conflict"); }
    if(!tx.hasInstance(key,instance) || reserved(tx,key.toString(),instance)!=null) { conflict("Candidate not pending"); }
    if(!"service".equals(job.path("mode").asText())) { invalid("Service preemption only"); }
    AttemptRecord victim=attempt(tx,token(r,"victimAttempt"));
    if(victim==null || !ready(tx,victim)) { conflict("Victim not ready/current"); }
    JsonNode victimJob=requireJob(tx,NativeEngine.key(Json.parse(victim.runBody).path("identity").path("jobKey")));
    JsonNode vp=policy(tx,victim.job),cp=policy(tx,key.toString());
    if(!"service".equals(victimJob.path("mode").asText()) || !vp.path("preemptible").asBoolean()
        || !victim.job.startsWith(job.path("jobKey").path("role").asText()+"/")
        || cp.path("priority").asInt()<=vp.path("priority").asInt() || victim.job.equals(key.toString())) {
      conflict("Victim requires lower priority same-role preemptible service");
    }
    freeJob(tx,key.toString(),null); freeJob(tx,victim.job,null); freeNode(tx,victim.node);
    if(tx.attempts().size()>=48 || tx.commands().size()>=96) { conflict("History full"); }
    NativeConfig.Node node=enrolled(victim.node);
    if(!"active".equals(node(tx,node.name).path("mode").asText()) || !matches(tx,job,node)
        || !engine.fits(tx,job,node,victim.attempt) || !canDisrupt(tx,victim)) { conflict("Preemption fit or availability rejected"); }
    for(AttemptRecord a:tx.attempts()) { /* Batch history cannot accidentally regenerate candidate. */
      if(a.job.equals(key.toString()) && a.instance.equals(instance) && a.reserved()) { conflict("Candidate changed"); }
    }
    op.put("job",key.toString()).put("instance",instance).put("revision",job.path("revision").asText())
        .put("victim",victim.attempt).put("victimJob",victim.job).put("node",victim.node).put("phase","cleanup");
    op.set("jobKey",job.path("jobKey")); engine.stopAttempt(tx,victim,"restart");
  }
  JsonNode effectiveJob(Tx tx,JsonNode stored,String instance) throws Exception {
    String key=NativeEngine.key(stored.path("jobKey")).toString();
    for(ObjectNode op:operations(tx)) {
      if(active(op) && isUpdate(op) && key.equals(op.path("job").asText())) {
        return selected(op,instance)?op.path("target"):op.path("sources").path(instance);
      }
    }
    return stored;
  }
  private static boolean selected(JsonNode op,String instance) {
    for(JsonNode value:op.path("selected")) { if(instance.equals(value.asText())) { return true; } } return false;
  }
  private boolean matches(Tx tx,JsonNode job,NativeConfig.Node n) throws Exception {
    JsonNode p=policy(tx,NativeEngine.key(job.path("jobKey")).toString());
    for(JsonNode c:p.path("constraints")) {
      boolean intersection=false;
      for(JsonNode v:attributes.path(n.name).path(c.path("attribute").asText())) {
        for(JsonNode wanted:c.path("values")) { if(v.equals(wanted)) { intersection=true; } }
      }
      if(intersection==c.path("negated").asBoolean()) { return false; }
    }
    return true;
  }
  boolean allowed(Tx tx,JsonNode job,String instance,NativeConfig.Node n) throws Exception {
    if(!"active".equals(node(tx,n.name).path("mode").asText()) || !matches(tx,job,n)) { return false; }
    String key=NativeEngine.key(job.path("jobKey")).toString();
    for(ObjectNode op:operations(tx)) {
      if(!active(op)) { continue; }
      if("preempt".equals(op.path("kind").asText())) {
        boolean candidate=key.equals(op.path("job").asText()) && instance.equals(op.path("instance").asText());
        if(candidate && (!n.name.equals(op.path("node").asText()) || !"place".equals(op.path("phase").asText()))) { return false; }
        if(n.name.equals(op.path("node").asText()) && !candidate) { return false; }
        if(key.equals(op.path("victimJob").asText())) { return false; }
      }
      if("drain".equals(op.path("kind").asText()) && op.path("current").isObject()) {
        JsonNode c=op.path("current");
        boolean replacement=key.equals(c.path("job").asText()) && instance.equals(c.path("instance").asText());
        if(replacement && (!n.name.equals(c.path("destination").asText()) || !"place".equals(c.path("phase").asText()))) { return false; }
        if(n.name.equals(c.path("destination").asText()) && !replacement) { return false; }
      }
    }
    return true;
  }
  void allocated(Tx tx,JsonNode job,String instance,NativeConfig.Node node,String attempt) throws Exception {
    String key=NativeEngine.key(job.path("jobKey")).toString();
    for(ObjectNode op:operations(tx)) {
      if(!active(op)) { continue; }
      if("preempt".equals(op.path("kind").asText()) && key.equals(op.path("job").asText())
          && instance.equals(op.path("instance").asText())) {
        op.put("candidateAttempt",attempt).put("state","completed").put("reason",""); save(tx,op);
      }
      if("drain".equals(op.path("kind").asText()) && key.equals(op.path("current").path("job").asText())
          && instance.equals(op.path("current").path("instance").asText())) {
        ((ObjectNode)op.path("current")).put("replacement",attempt); save(tx,op);
      }
    }
  }
  void cancelJob(Tx tx,JobKey key) throws Exception {
    for(ObjectNode op:operations(tx)) {
      if(!active(op)) { continue; }
      if(key.toString().equals(op.path("job").asText())) {
        op.put("state","cancelled").put("reason","Job stopped"); save(tx,op);
      }
      // Drain remains cordoned and waits for the stop's exact cleanup proof.
      if(key.toString().equals(op.path("current").path("job").asText())) {
        ((ObjectNode)op.path("current")).put("cancelled",true); save(tx,op);
      }
    }
  }
  void advance(Tx tx) throws Exception {
    for(ObjectNode op:operations(tx)) {
      if(!active(op)) { continue; }
      switch(op.path("kind").asText()) {
        case "update","rollback" -> advanceUpdate(tx,op);
        case "drain" -> advanceDrain(tx,op);
        case "preempt" -> advancePreempt(tx,op);
        default -> throw new IllegalStateException("Invalid durable operation kind");
      }
    }
  }
  private void advanceUpdate(Tx tx,ObjectNode op) throws Exception {
    JsonNode target=op.path("target"); JobKey key=NativeEngine.key(target.path("jobKey"));
    if(!target.path("revision").asText().equals(tx.jobRevision(key))) { block(tx,op,"revision-conflict"); return; }
    List<String> instances=tx.desiredInstances(key);
    if(instances.size()!=target.path("instances").asInt()) {
      op.put("state","cancelled").put("reason","Desired membership changed"); save(tx,op); return;
    }
    int index=op.path("index").asInt();
    if(index>=instances.size()) { op.put("state","completed").put("reason",""); save(tx,op); return; }
    String instance=instances.get(index); AttemptRecord a=reserved(tx,key.toString(),instance);
    if(selected(op,instance)) {
      if(a!=null && target.path("revision").asText().equals(Json.parse(a.runBody).path("desiredRevision").asText()) && ready(tx,a)) {
        running(op); op.put("index",index+1).put("phase","select"); save(tx,op);
      } else { block(tx,op,a!=null && stopping(tx,a.attempt)?"awaiting-cleanup":"awaiting-target-ready"); }
      return;
    }
    if(tx.attempts().size()>=48 || tx.commands().size()>=96) { block(tx,op,"history-full"); return; }
    boolean fits=false;
    for(NativeConfig.Node candidate:config.nodes) {
      if(allowed(tx,target,instance,candidate) && engine.fits(tx,target,candidate,a==null?null:a.attempt)) {
        fits=true; break;
      }
    }
    if(!fits) { block(tx,op,"no-update-capacity"); return; }
    if(a!=null) {
      if(!canDisrupt(tx,a)) { block(tx,op,"min-ready"); return; }
      if(!engine.trusted(a.node)) { block(tx,op,"untrusted-agent"); return; }
      engine.stopAttempt(tx,a,"restart");
    }
    ((ArrayNode)op.path("selected")).add(instance); running(op); op.put("phase","replace"); save(tx,op);
  }
  private void advanceDrain(Tx tx,ObjectNode op) throws Exception {
    String name=op.path("node").asText();
    if(!engine.trusted(name)) { block(tx,op,"untrusted-source-inventory"); return; }
    if(op.path("current").isObject()) {
      ObjectNode current=(ObjectNode)op.path("current"); AttemptRecord old=attempt(tx,current.path("victim").asText());
      if(old==null || old.reserved()) { block(tx,op,"awaiting-cleanup"); return; }
      JobKey key=NativeEngine.key(current.path("jobKey")); String instance=current.path("instance").asText();
      if(!tx.hasInstance(key,instance) || current.path("cancelled").asBoolean()) {
        op.remove("current"); running(op); save(tx,op); return;
      }
      AttemptRecord replacement=reserved(tx,key.toString(),instance);
      if(ready(tx,replacement) && !name.equals(replacement.node)) {
        op.remove("current"); running(op); save(tx,op); return;
      }
      current.put("phase","place"); block(tx,op,"awaiting-evacuated-ready"); return;
    }
    AttemptRecord victim=null;
    for(AttemptRecord a:tx.attempts()) { if(a.reserved() && a.node.equals(name)) { victim=a; break; } }
    if(victim==null) {
      ObjectNode state=node(tx,name).deepCopy(); state.put("mode","drained");
      tx.putPolicy("node",name,Json.canonical(state)); op.put("state","completed").put("reason",""); save(tx,op); return;
    }
    if(tx.attempts().size()>=48 || tx.commands().size()>=96) { block(tx,op,"history-full"); return; }
    JsonNode identity=Json.parse(victim.runBody).path("identity"); JobKey key=NativeEngine.key(identity.path("jobKey"));
    JsonNode job=requireJob(tx,key);
    if(!tx.hasInstance(key,victim.instance)) { block(tx,op,"awaiting-cancelled-cleanup"); return; }
    if(!"service".equals(job.path("mode").asText())) { block(tx,op,"batch-evacuation-unsupported"); return; }
    if(stopping(tx,victim.attempt)) { block(tx,op,"awaiting-existing-stop"); return; }
    if(!canDisrupt(tx,victim)) { block(tx,op,"min-ready"); return; }
    NativeConfig.Node destination=null;
    for(NativeConfig.Node candidate:config.nodes) {
      if(!candidate.name.equals(name) && allowed(tx,job,victim.instance,candidate) && engine.fits(tx,job,candidate,null)) {
        destination=candidate; break;
      }
    }
    if(destination==null) { block(tx,op,"no-evacuation-capacity"); return; }
    ObjectNode current=Json.object().put("victim",victim.attempt).put("job",victim.job)
        .put("instance",victim.instance).put("destination",destination.name).put("phase","cleanup");
    current.set("jobKey",identity.path("jobKey")); op.set("current",current);
    engine.stopAttempt(tx,victim,"drain"); running(op); save(tx,op);
  }
  private void advancePreempt(Tx tx,ObjectNode op) throws Exception {
    JobKey key=NativeEngine.key(op.path("jobKey")); String instance=op.path("instance").asText();
    if(!tx.hasInstance(key,instance) || !op.path("revision").asText().equals(tx.jobRevision(key))) {
      op.put("state","aborted").put("reason","Candidate changed"); save(tx,op); return;
    }
    AttemptRecord victim=attempt(tx,op.path("victim").asText());
    if(victim==null || victim.reserved()) { block(tx,op,"awaiting-cleanup"); return; }
    JsonNode job=requireJob(tx,key); NativeConfig.Node n=enrolled(op.path("node").asText());
    if(!engine.trusted(n.name) || !"active".equals(node(tx,n.name).path("mode").asText())
        || !matches(tx,job,n) || !engine.fits(tx,job,n,null)) { block(tx,op,"candidate-fit-unavailable"); return; }
    op.put("phase","place"); running(op); save(tx,op);
  }
  JsonNode state(Tx tx) throws Exception {
    ObjectNode result=Json.object().put("version","policy-01").put("operationLimit",64)
        .put("quotaModel","role desired CPU/memory; update envelope; cleanup reservation floor")
        .put("drainModel","scheduler cordon; service evacuation; integer minReady; no force timeout")
        .put("updateModel","service fixed-size; sequential stop-first; explicit rollback")
        .put("preemptionModel","operator selected single same-role lower-priority preemptible service victim");
    result.set("attributes",attributes); ArrayNode ops=Json.array(),quotas=Json.array(),nodes=Json.array(),jobs=Json.array(),pending=Json.array();
    for(ObjectNode op:operations(tx)) {
      ObjectNode summary=op.deepCopy(); summary.remove(List.of("sources","target","before"));
      if(isUpdate(op)) { summary.put("targetRevision",op.path("target").path("revision").asText()); }
      ops.add(summary);
    }
    for(String body:tx.policies("quota")) {
      ObjectNode quota=(ObjectNode)Json.parse(body); long[] amount=usage(tx,quota.path("role").asText(),null,null);
      quota.put("admittedCpuMillis",amount[0]).put("admittedMemoryBytes",amount[1]); quotas.add(quota);
    }
    for(NativeConfig.Node n:config.nodes) { nodes.add(node(tx,n.name)); }
    for(String body:tx.jobBodies()) {
      JsonNode job=Json.parse(body); JobKey key=NativeEngine.key(job.path("jobKey"));
      ObjectNode entry=Json.object(); entry.set("jobKey",job.path("jobKey")); entry.set("policy",policy(tx,key.toString())); jobs.add(entry);
      for(String instance:tx.desiredInstances(key)) {
        if(reserved(tx,key.toString(),instance)!=null) { continue; }
        boolean completedBatch=false;
        if("batch".equals(job.path("mode").asText())) {
          for(AttemptRecord a:tx.attempts()) { completedBatch|=a.job.equals(key.toString()) && a.instance.equals(instance); }
        }
        if(completedBatch) { continue; }
        ObjectNode p=Json.object().put("job",key.toString()).put("instance",instance); ArrayNode reasons=Json.array();
        JsonNode effective=effectiveJob(tx,job,instance);
        for(NativeConfig.Node n:config.nodes) {
          String reason=!engine.trusted(n.name)?"untrusted-agent":!"active".equals(node(tx,n.name).path("mode").asText())?"cordoned"
              :!matches(tx,effective,n)?"constraint":!allowed(tx,effective,instance,n)?"operation-reservation"
              :!engine.fits(tx,effective,n,null)?"resources-ports-or-max-per-agent":"eligible";
          reasons.add(Json.object().put("node",n.name).put("reason",reason));
        }
        p.set("nodes",reasons); pending.add(p);
      }
    }
    result.set("operations",ops); result.set("quotas",quotas); result.set("nodes",nodes); result.set("jobs",jobs); result.set("pending",pending);
    return result;
  }
}
