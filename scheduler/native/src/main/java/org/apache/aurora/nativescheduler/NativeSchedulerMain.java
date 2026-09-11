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
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.*;
import javax.net.ssl.*;
import com.sun.net.httpserver.*;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.aurora.scheduler.storage.sql.NativeSqlStore;

/** Small static-enrollment HTTPS daemon; no legacy scheduler or Mesos classpath. */
public final class NativeSchedulerMain {
  private NativeSchedulerMain() { }
  public static void main(String[] args) throws Exception {
    Map<String,String> options=new HashMap<>(); boolean inspect=false;
    for(int i=0;i<args.length;i++) {
      if ("--inspect-only".equals(args[i])) { inspect=true; continue; }
      if (!Arrays.asList("--config","--state","--listen","--tls-keystore","--tls-truststore","--tls-password-file").contains(args[i])
          || i+1==args.length || options.put(args[i],args[++i])!=null) { throw new IllegalArgumentException("Invalid CLI arguments"); }
    }
    NativeConfig config=new NativeConfig(Files.readAllBytes(Paths.get(required(options,"--config"))));
    Path directory=Paths.get(required(options,"--state"));
    if(!directory.isAbsolute()) { throw new IllegalArgumentException("Absolute state directory required"); }
    try(NativeSqlStore store=new NativeSqlStore(directory,config.cluster,config.incarnation)) {
      if(inspect) {
        System.out.println(new NativeEngine(store,config,null,true).state()); return;
      }
      SSLContext tls=tls(options);
      NativeEngine engine=new NativeEngine(store,config,new HttpsTransport(tls),false);
      String[] listen=required(options,"--listen").split(":",-1);
      if(listen.length!=2) { throw new IllegalArgumentException("Use IPv4 address:port listen"); }
      System.setProperty("sun.net.httpserver.maxReqTime","5");
      System.setProperty("sun.net.httpserver.maxRspTime","5");
      System.setProperty("sun.net.httpserver.maxReqHeaders","32");
      HttpsServer server=HttpsServer.create(new InetSocketAddress(listen[0],Integer.parseInt(listen[1])),16);
      configureTls(server,tls);
      ThreadPoolExecutor requests=new ThreadPoolExecutor(2,2,0,TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<Runnable>(16),new ThreadPoolExecutor.AbortPolicy());
      server.setExecutor(requests);
      server.createContext("/",exchange -> handle((HttpsExchange)exchange,engine,store,directory));
      ScheduledExecutorService controller=Executors.newSingleThreadScheduledExecutor();
      CountDownLatch stopped=new CountDownLatch(1);
      Thread hook=new Thread(() -> {
        server.stop(1); controller.shutdownNow(); requests.shutdownNow();
        try { controller.awaitTermination(15,TimeUnit.SECONDS); } catch(InterruptedException e) { Thread.currentThread().interrupt(); }
        stopped.countDown();
      },"native-scheduler-shutdown");
      Runtime.getRuntime().addShutdownHook(hook);
      server.start();
      controller.scheduleWithFixedDelay(() -> {
        try { engine.tick(); }
        catch(Exception e) { System.err.println("Scheduler tick failed: "+e.getClass().getSimpleName()); }
      },0,250,TimeUnit.MILLISECONDS);
      System.out.println("NATIVE_SCHEDULER_READY epoch="+engine.epoch);
      stopped.await();
    }
  }
  static void configureTls(HttpsServer server,SSLContext tls) {
    server.setHttpsConfigurator(new HttpsConfigurator(tls) {
      @Override public void configure(HttpsParameters parameters) {
        SSLParameters settings=tls.getDefaultSSLParameters(); settings.setNeedClientAuth(true);
        parameters.setSSLParameters(settings);
      }
    });
  }
  static String required(Map<String,String> options,String name) {
    String value=options.get(name); if(value==null) { throw new IllegalArgumentException("Missing "+name); } return value;
  }
  static SSLContext tls(Map<String,String> options) throws Exception {
    Path passwordPath=Paths.get(required(options,"--tls-password-file"));
    byte[] passwordBytes=Files.readAllBytes(passwordPath);
    if(passwordBytes.length>1024) { throw new IllegalArgumentException("Invalid password file"); }
    char[] password=new String(passwordBytes,StandardCharsets.UTF_8).trim().toCharArray();
    Arrays.fill(passwordBytes,(byte)0);
    try {
      KeyStore keys=KeyStore.getInstance("PKCS12"), roots=KeyStore.getInstance("PKCS12");
      try(InputStream in=Files.newInputStream(Paths.get(required(options,"--tls-keystore")))) { keys.load(in,password); }
      try(InputStream in=Files.newInputStream(Paths.get(required(options,"--tls-truststore")))) { roots.load(in,password); }
      KeyManagerFactory km=KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()); km.init(keys,password);
      TrustManagerFactory tm=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); tm.init(roots);
      SSLContext context=SSLContext.getInstance("TLS"); context.init(km.getKeyManagers(),tm.getTrustManagers(),null); return context;
    } finally { Arrays.fill(password,' '); }
  }
  static boolean operator(HttpsExchange exchange) {
    try {
      X509Certificate leaf=(X509Certificate)exchange.getSSLSession().getPeerCertificates()[0];
      Collection<List<?>> names=leaf.getSubjectAlternativeNames();
      if(names!=null) { for(List<?> name:names) { if(Integer.valueOf(2).equals(name.get(0)) && "operator".equals(name.get(1))) { return true; } } }
    } catch(Exception ignored) { }
    return false;
  }
  static void handle(HttpsExchange exchange,NativeEngine engine,NativeSqlStore store,Path directory) throws IOException {
    int code=200; JsonNode response;
    try {
      if(!operator(exchange)) { reply(exchange,403,Json.object().put("error","operator certificate required")); return; }
      String method=exchange.getRequestMethod(), path=exchange.getRequestURI().getPath();
      if ("POST".equals(method) && !"application/json".equals(exchange.getRequestHeaders().getFirst("Content-Type"))) {
        throw new IllegalArgumentException("Expected application/json");
      }
      if ("GET".equals(method) && (exchange.getRequestHeaders().containsKey("Transfer-Encoding")
          || (exchange.getRequestHeaders().containsKey("Content-Length")
              && !"0".equals(exchange.getRequestHeaders().getFirst("Content-Length"))))) {
        throw new IllegalArgumentException("GET body unsupported");
      }
      if(exchange.getRequestURI().getRawQuery()!=null) { throw new IllegalArgumentException("Unexpected query"); }
      if("GET".equals(method) && "/v1/state".equals(path)) { response=engine.state(); }
      else if("POST".equals(method) && "/v1/jobs".equals(path)) {
        boolean created=engine.submit(Json.read(exchange.getRequestBody())); code=created?201:200;
        response=Json.object().put("ok",true).put("created",created);
      } else if("POST".equals(method) && "/v1/jobs/stop".equals(path)) {
        engine.stop(Json.parse(Json.read(exchange.getRequestBody()))); response=Json.object().put("ok",true);
      } else if("POST".equals(method) && "/v1/backup".equals(path)) {
        JsonNode request=Json.parse(Json.read(exchange.getRequestBody())); Json.fields(request,"name");
        String name=NativeConfig.token(Json.string(request,"name"));
        if(name.equals(".") || name.equals("..")) { throw new IllegalArgumentException("Invalid snapshot name"); }
        store.snapshot(directory.resolve("backups").resolve(name));
        response=Json.object().put("ok",true).put("name",name);
      } else { code=404; response=Json.object().put("error","unknown route"); }
    } catch(NativeEngine.Conflict e) { code=409; response=Json.object().put("error",e.getMessage()); }
      catch(IllegalArgumentException|IOException e) { code=400; response=Json.object().put("error","invalid request"); }
      catch(Exception e) { code=503; response=Json.object().put("error","durable operation unavailable"); }
    reply(exchange,code,response);
  }
  static void reply(HttpsExchange exchange,int code,JsonNode response) throws IOException {
    byte[] bytes=response.toString().getBytes(StandardCharsets.UTF_8);
    if(bytes.length>Json.LIMIT) { code=503; bytes="{\"error\":\"state inventory limit\"}".getBytes(StandardCharsets.UTF_8); }
    try { exchange.getResponseHeaders().set("Content-Type","application/json");
      exchange.getResponseHeaders().set("Cache-Control","no-store");
      exchange.sendResponseHeaders(code,bytes.length); exchange.getResponseBody().write(bytes);
    } finally { exchange.close(); }
  }
  public static final class HttpsTransport implements NativeEngine.Transport {
    private final SSLSocketFactory sockets;
    public HttpsTransport(SSLContext context) { sockets=context.getSocketFactory(); }
    @Override public JsonNode request(NativeConfig.Node node,String method,String path,JsonNode body,
        String epoch,String session) throws Exception {
      HttpsURLConnection connection=(HttpsURLConnection)URI.create(node.url+path).toURL().openConnection(Proxy.NO_PROXY);
      connection.setSSLSocketFactory(sockets); // Platform hostname verification remains enabled.
      connection.setConnectTimeout(1000); connection.setReadTimeout(1500);
      connection.setInstanceFollowRedirects(false); connection.setRequestMethod(method);
      connection.setRequestProperty("X-Aurora-Epoch",epoch); connection.setRequestProperty("X-Aurora-Session",session);
      try {
        if(body!=null) {
          byte[] bytes=Json.canonical(body).getBytes(StandardCharsets.UTF_8);
          connection.setDoOutput(true); connection.setFixedLengthStreamingMode(bytes.length);
          connection.setRequestProperty("Content-Type","application/json");
          try(OutputStream out=connection.getOutputStream()) { out.write(bytes); }
        }
        int status=connection.getResponseCode();
        if(status!=200) { throw new IOException("Agent HTTP status "+status); }
        try(InputStream input=connection.getInputStream()) { return Json.parse(Json.read(input)); }
      } finally { connection.disconnect(); }
    }
  }
}
