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

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.KeyStore;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.*;
import com.sun.net.httpserver.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.aurora.scheduler.storage.sql.NativeSqlStore;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;

/** Actual TLS sockets, with private disposable certificates and no external services. */
public class NativeHttpTest {
  @ClassRule public static TemporaryFolder certificates = new TemporaryFolder();
  @Rule public TemporaryFolder temporary = new TemporaryFolder();
  private static final Map<String,SSLContext> contexts = new HashMap<>();
  private static SSLContext anonymous;
  private final List<HttpsServer> servers = new ArrayList<>();
  private final List<ExecutorService> executors = new ArrayList<>();
  private NativeSqlStore store;
  private NativeEngine engine;
  private HttpsServer api;
  private Path state;

  @BeforeClass public static void certificates() throws Exception {
    Path root=certificates.getRoot().toPath();
    Files.write(root.resolve("password"),"test-password".getBytes(StandardCharsets.UTF_8));
    // Separate self-signed leaves are explicitly trusted; the untrusted leaf is never imported.
    for(String name:Arrays.asList("localhost","operator","scheduler","untrusted")) {
      keytool(root,"-genkeypair","-alias",name,"-keyalg","EC","-groupname","secp256r1",
          "-dname","CN="+name,"-ext","SAN=dns:"+name,"-validity","2",
          "-keystore",root.resolve(name+".p12").toString(),"-storetype","PKCS12");
      keytool(root,"-exportcert","-alias",name,"-keystore",root.resolve(name+".p12").toString(),
          "-file",root.resolve(name+".cer").toString());
      if(!name.equals("untrusted")) {
        keytool(root,"-importcert","-noprompt","-alias",name,"-file",root.resolve(name+".cer").toString(),
            "-keystore",root.resolve("trust.p12").toString(),"-storetype","PKCS12");
      }
    }
    for(String name:Arrays.asList("localhost","operator","scheduler","untrusted")) {
      Map<String,String> options=new HashMap<>();
      options.put("--tls-keystore",root.resolve(name+".p12").toString());
      options.put("--tls-truststore",root.resolve("trust.p12").toString());
      options.put("--tls-password-file",root.resolve("password").toString());
      contexts.put(name,NativeSchedulerMain.tls(options));
    }
    KeyStore trust=KeyStore.getInstance("PKCS12");
    try(InputStream in=Files.newInputStream(root.resolve("trust.p12"))) { trust.load(in,"test-password".toCharArray()); }
    TrustManagerFactory tm=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tm.init(trust); anonymous=SSLContext.getInstance("TLS"); anonymous.init(new KeyManager[0],tm.getTrustManagers(),null);
  }
  private static void keytool(Path root,String... arguments) throws Exception {
    List<String> command=new ArrayList<>();
    command.add(Paths.get(System.getProperty("java.home"),"bin","keytool").toString());
    command.addAll(Arrays.asList("-J-Xmx64m","-J-XX:ActiveProcessorCount=2"));
    command.addAll(Arrays.asList(arguments));
    command.addAll(Arrays.asList("-storepass","test-password"));
    Path log=root.resolve("keytool.log");
    Process process=new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue("keytool timed out",process.waitFor(20,TimeUnit.SECONDS));
      assertEquals(new String(Files.readAllBytes(log),StandardCharsets.UTF_8),0,process.exitValue());
    } finally { if(process.isAlive()) { process.destroyForcibly(); process.waitFor(5,TimeUnit.SECONDS); } }
  }
  @Before public void open() throws Exception {
    state=temporary.newFolder("state").toPath();
    store=new NativeSqlStore(state,"lab","recovery-a");
    engine=new NativeEngine(store,config(1),null,true);
    api=server(contexts.get("localhost"),exchange ->
        NativeSchedulerMain.handle((HttpsExchange)exchange,engine,store,state));
  }
  @After public void close() throws Exception {
    for(HttpsServer server:servers) { server.stop(0); }
    for(ExecutorService executor:executors) { executor.shutdownNow(); assertTrue(executor.awaitTermination(5,TimeUnit.SECONDS)); }
    if(store!=null) { store.close(); }
  }
  private HttpsServer server(SSLContext context,HttpHandler handler) throws Exception {
    HttpsServer server=HttpsServer.create(new InetSocketAddress("127.0.0.1",0),8); servers.add(server);
    NativeSchedulerMain.configureTls(server,context);
    ExecutorService executor=Executors.newFixedThreadPool(2); executors.add(executor);
    server.setExecutor(executor); server.createContext("/",handler); server.start(); return server;
  }
  private NativeConfig config(int port) throws Exception {
    ObjectNode value=Json.object().put("cluster","lab").put("incarnation","recovery-a");
    com.fasterxml.jackson.databind.node.ArrayNode nodes=Json.array();
    for(String name:Arrays.asList("localhost","unused")) {
      nodes.add(Json.object().put("node",name).put("journal","journal").put("boot","boot").put("runtime","runtime")
          .put("url","https://"+name+":"+port).put("network","agent-container").put("portStart",18080)
          .put("cpuMillis",1000).put("memoryBytes",536870912));
    }
    value.set("nodes",nodes); return new NativeConfig(Json.canonical(value).getBytes(StandardCharsets.UTF_8));
  }
  private HttpsURLConnection connection(HttpsServer server,SSLContext context,String path) throws Exception {
    HttpsURLConnection connection=(HttpsURLConnection)URI.create("https://localhost:"+server.getAddress().getPort()+path)
        .toURL().openConnection(Proxy.NO_PROXY);
    connection.setSSLSocketFactory(context.getSocketFactory()); connection.setConnectTimeout(1500);
    connection.setReadTimeout(2500); connection.setInstanceFollowRedirects(false); return connection;
  }
  private JsonNode request(String method,String path,String body,int expected) throws Exception {
    HttpsURLConnection connection=connection(api,contexts.get("operator"),path);
    try {
      connection.setRequestMethod(method);
      if(body!=null) {
        connection.setDoOutput(true); connection.setRequestProperty("Content-Type","application/json");
        try(OutputStream out=connection.getOutputStream()) { out.write(body.getBytes(StandardCharsets.UTF_8)); }
      }
      assertEquals(expected,connection.getResponseCode());
      assertEquals("application/json",connection.getHeaderField("Content-Type"));
      assertEquals("no-store",connection.getHeaderField("Cache-Control"));
      try(InputStream in=expected>=400?connection.getErrorStream():connection.getInputStream()) { return Json.parse(Json.read(in)); }
    } finally { connection.disconnect(); }
  }
  @Test(timeout=15000) public void mutualTlsRejectsMissingAndUntrustedClientsAndWrongRole() throws Exception {
    assertEquals("lab",request("GET","/v1/state",null,200).path("cluster").asText());
    for(SSLContext context:Arrays.asList(anonymous,contexts.get("untrusted"))) {
      HttpsURLConnection connection=connection(api,context,"/v1/state");
      try { connection.getResponseCode(); fail("TLS accepted untrusted or absent identity"); }
      catch(IOException expected) { /* Handshake or fatal TLS alert, before an HTTP response. */ }
      finally { connection.disconnect(); }
    }
    HttpsURLConnection wrong=connection(api,contexts.get("scheduler"),"/v1/state");
    try { assertEquals(403,wrong.getResponseCode()); } finally { wrong.disconnect(); }
    assertEquals(0,engine.state().path("jobs").size());
  }
  @Test(timeout=15000) public void operatorRoutesValidateRequestsAndPersistReplayStopAndBackup() throws Exception {
    request("GET","/missing",null,404); request("PUT","/v1/state",null,404);
    request("GET","/v1/state?unexpected=1",null,400);
    request("POST","/v1/jobs","{} {}",400); request("POST","/v1/jobs","{\"kind\":1,\"kind\":2}",400);
    HttpsURLConnection wrong=connection(api,contexts.get("operator"),"/v1/jobs");
    try { wrong.setRequestMethod("POST"); assertEquals(400,wrong.getResponseCode()); } finally { wrong.disconnect(); }
    JsonNode job=Json.parse(Files.readAllBytes(Paths.get(System.getProperty("aurora.fixtures"),"valid","batch.json")));
    assertTrue(request("POST","/v1/jobs",Json.canonical(job),201).path("created").asBoolean());
    assertFalse(request("POST","/v1/jobs",Json.canonical(job),200).path("created").asBoolean());
    ObjectNode changed=job.deepCopy(); changed.put("revision","2");
    request("POST","/v1/jobs",Json.canonical(changed),409);
    request("POST","/v1/jobs/stop",Json.canonical(job.get("jobKey")),200);
    JsonNode stopped=request("GET","/v1/state",null,200);
    request("POST","/v1/jobs/stop",Json.canonical(job.get("jobKey")),200);
    assertEquals(stopped,request("GET","/v1/state",null,200));
    assertEquals(0,stopped.path("jobs").get(0).path("desiredInstances").size());
    request("POST","/v1/backup","{\"name\":\"../escape\"}",400);
    request("POST","/v1/backup","{\"name\":\"copy\"}",200);
    try(NativeSqlStore restored=new NativeSqlStore(state.resolve("backups/copy"),"lab","recovery-a")) {
      assertEquals(stopped,new NativeEngine(restored,config(1),null,true).state());
    }
  }
  @Test(timeout=10000) public void operatorBodyAndReplyLimitsFailClosed() throws Exception {
    String oversized=" ".repeat(Json.LIMIT+1);
    request("POST","/v1/jobs",oversized,400);
    HttpsURLConnection wrong=connection(api,contexts.get("operator"),"/v1/jobs");
    try {
      wrong.setRequestMethod("POST"); wrong.setDoOutput(true);
      wrong.setRequestProperty("Content-Type","application/json; charset=utf-8");
      try(OutputStream out=wrong.getOutputStream()) { out.write("{}".getBytes(StandardCharsets.UTF_8)); }
      assertEquals(400,wrong.getResponseCode());
    } finally { wrong.disconnect(); }
    HttpsServer huge=server(contexts.get("localhost"),exchange -> NativeSchedulerMain.reply(
        (HttpsExchange)exchange,200,Json.object().put("oversized",oversized)));
    HttpsURLConnection response=connection(huge,contexts.get("operator"),"/");
    try {
      assertEquals(503,response.getResponseCode());
      try(InputStream in=response.getErrorStream()) {
        assertEquals("state inventory limit",Json.parse(Json.read(in)).path("error").asText());
      }
    } finally { response.disconnect(); }
    assertEquals(0,engine.state().path("jobs").size());
  }
  @Test(timeout=10000) public void rawGetBodyIsRejectedWithoutChangingState() throws Exception {
    try(SSLSocket socket=(SSLSocket)contexts.get("operator").getSocketFactory().createSocket("localhost",api.getAddress().getPort())) {
      socket.setSoTimeout(2500); socket.startHandshake();
      socket.getOutputStream().write("GET /v1/state HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}".getBytes(StandardCharsets.US_ASCII));
      BufferedReader reader=new BufferedReader(new InputStreamReader(socket.getInputStream(),StandardCharsets.US_ASCII));
      assertEquals("HTTP/1.1 400 Bad Request",reader.readLine());
    }
  }
  @Test(timeout=10000) public void transportSendsAuthorityAndCanonicalJsonOverVerifiedTls() throws Exception {
    HttpsServer peer=server(contexts.get("localhost"),exchange -> {
      assertEquals("9",exchange.getRequestHeaders().getFirst("X-Aurora-Epoch"));
      assertEquals("session-nine",exchange.getRequestHeaders().getFirst("X-Aurora-Session"));
      assertEquals("application/json",exchange.getRequestHeaders().getFirst("Content-Type"));
      assertEquals("{\"a\":2,\"z\":1}",new String(Json.read(exchange.getRequestBody()),StandardCharsets.UTF_8));
      NativeSchedulerMain.reply((HttpsExchange)exchange,200,Json.object().put("ok",true));
    });
    JsonNode response=new NativeSchedulerMain.HttpsTransport(contexts.get("scheduler")).request(
        config(peer.getAddress().getPort()).nodes.get(0),"POST","/v1/deliver",Json.object().put("z",1).put("a",2),"9","session-nine");
    assertTrue(response.path("ok").asBoolean());
  }
  @Test(timeout=10000) public void transportRejectsTrustedWrongHostnameAndRedirect() throws Exception {
    AtomicInteger visited=new AtomicInteger();
    HttpsServer wrong=server(contexts.get("operator"),exchange -> { visited.incrementAndGet(); exchange.close(); });
    NativeSchedulerMain.HttpsTransport transport=new NativeSchedulerMain.HttpsTransport(contexts.get("scheduler"));
    try { transport.request(config(wrong.getAddress().getPort()).nodes.get(0),"GET","/",null,"1","s"); fail("Wrong hostname accepted"); }
    catch(SSLException expected) { }
    assertEquals(0,visited.get());
    HttpsServer redirect=server(contexts.get("localhost"),exchange -> {
      visited.incrementAndGet(); exchange.getResponseHeaders().set("Location","/followed");
      exchange.sendResponseHeaders(302,-1); exchange.close();
    });
    try { transport.request(config(redirect.getAddress().getPort()).nodes.get(0),"GET","/",null,"1","s"); fail("Redirect accepted"); }
    catch(IOException expected) { assertTrue(expected.getMessage().contains("302")); }
    assertEquals(1,visited.get());
  }
  @Test(timeout=15000) public void transportBoundsResponsesAndReadWait() throws Exception {
    HttpsServer peer=server(contexts.get("localhost"),exchange -> {
      try {
        if(exchange.getRequestURI().getPath().equals("/large")) {
          byte[] bytes=new byte[Json.LIMIT+1]; Arrays.fill(bytes,(byte)' ');
          exchange.sendResponseHeaders(200,bytes.length); exchange.getResponseBody().write(bytes);
        } else { exchange.sendResponseHeaders(200,8); Thread.sleep(3500); }
      } catch(InterruptedException expected) { Thread.currentThread().interrupt(); }
        catch(IOException expected) { /* Client rejects the bounded response and closes. */ }
      finally { exchange.close(); }
    });
    NativeSchedulerMain.HttpsTransport transport=new NativeSchedulerMain.HttpsTransport(contexts.get("scheduler"));
    NativeConfig.Node node=config(peer.getAddress().getPort()).nodes.get(0);
    try { transport.request(node,"GET","/large",null,"1","s"); fail("Oversized response accepted"); }
    catch(IOException expected) { assertEquals("Body too large",expected.getMessage()); }
    long start=System.nanoTime();
    try { transport.request(node,"GET","/stall",null,"1","s"); fail("Stalled response accepted"); }
    catch(SocketTimeoutException expected) { assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-start)<5000); }
  }
}
