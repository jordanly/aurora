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

import java.net.InetSocketAddress;
import java.util.concurrent.*;
import javax.net.ssl.SSLContext;
import com.sun.net.httpserver.*;

/** Owns partial startup and keeps durable ownership until all workers have stopped. */
final class NativeDaemon implements AutoCloseable {
  static class Hooks {
    void add(Thread hook) { Runtime.getRuntime().addShutdownHook(hook); }
    void remove(Thread hook) { Runtime.getRuntime().removeShutdownHook(hook); }
  }

  private final AutoCloseable store;
  private final Hooks hooks;
  private final long timeoutNanos;
  private final CountDownLatch stopped = new CountDownLatch(1);
  private HttpsServer server;
  private boolean serverStarted;
  private CountDownLatch serverStopped;
  private volatile Throwable serverStopFailure;
  private ExecutorService requests;
  private ScheduledExecutorService controller;
  private Thread hook;
  private boolean started;
  private boolean closed;

  NativeDaemon(AutoCloseable store) { this(store, new Hooks(), 15, TimeUnit.SECONDS); }

  NativeDaemon(AutoCloseable store, Hooks hooks, long timeout, TimeUnit unit) {
    this.store = store;
    this.hooks = hooks;
    timeoutNanos = unit.toNanos(timeout);
  }

  synchronized void start(InetSocketAddress address, SSLContext tls, HttpHandler handler,
      Runnable tick) throws Exception {
    if (started || closed) { throw new IllegalStateException("Daemon already started or closed"); }
    started = true;
    System.setProperty("sun.net.httpserver.maxReqTime", "5");
    System.setProperty("sun.net.httpserver.maxRspTime", "5");
    System.setProperty("sun.net.httpserver.maxReqHeaders", "32");
    server = HttpsServer.create();
    NativeSchedulerMain.configureTls(server, tls);
    requests = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<Runnable>(16), new ThreadPoolExecutor.AbortPolicy());
    server.setExecutor(requests);
    server.createContext("/", handler);
    controller = Executors.newSingleThreadScheduledExecutor();
    Thread shutdown = new Thread(() -> {
      try { close(); }
      catch (Exception | Error e) {
        System.err.println("Scheduler shutdown failed: " + e.getClass().getSimpleName());
      } finally { stopped.countDown(); }
    }, "native-scheduler-shutdown");
    // Start the dispatcher immediately after binding: JDK shutdown needs it to release
    // the selector's registered listener before a failed hook registration can unwind.
    server.bind(address, 16);
    server.start();
    serverStarted = true;
    hooks.add(shutdown);
    hook = shutdown;
    controller.scheduleWithFixedDelay(tick, 0, 250, TimeUnit.MILLISECONDS);
  }

  void awaitShutdown() throws InterruptedException {
    try { stopped.await(); }
    catch (InterruptedException e) { Thread.currentThread().interrupt(); throw e; }
  }

  private static Throwable append(Throwable primary, Throwable next) {
    if (primary == null) { return next; }
    if (primary != next) { primary.addSuppressed(next); }
    return primary;
  }

  private void stopServer() {
    if (server == null || serverStopped != null) { return; }
    serverStopped = new CountDownLatch(1);
    try {
      Thread stopper = new Thread(() -> {
        Throwable failure = null;
        try {
          if (!serverStarted) {
            // JDK stop alone leaves a registered listener in an unstarted selector. A
            // cleanup-only loopback dispatcher drains cancellation and closes that selector.
            if (server.getAddress() == null) {
              server.bind(new InetSocketAddress("127.0.0.1", 0), 1);
            }
            server.start();
          }
        } catch (Exception | Error e) { failure = e; }
        finally {
          try { server.stop(serverStarted ? 1 : 0); }
          catch (Exception | Error e) { failure = append(failure, e); }
          serverStopFailure = failure;
          serverStopped.countDown();
        }
      }, "native-scheduler-server-stop");
      stopper.setDaemon(true);
      stopper.start();
    } catch (Exception | Error e) {
      serverStopFailure = e;
      serverStopped.countDown();
    }
  }

  /**
   * A server/worker timeout retains the store and hook; a later close may finish after quiescence.
   * Once workers terminate, store cleanup is attempted once, matching NativeSqlStore's contract;
   * its failure is reported without making subsequent closes retry a released resource.
   */
  @Override public synchronized void close() throws Exception {
    if (closed) { return; }
    boolean interrupted = Thread.interrupted();
    Throwable failure = null;
    try {
      long deadline = System.nanoTime() + timeoutNanos;
      stopServer();
      ExecutorService[] pools = {controller, requests};
      for (ExecutorService pool : pools) {
        if (pool != null) {
          try { pool.shutdownNow(); }
          catch (Exception | Error e) { failure = append(failure, e); }
        }
      }
      if (serverStopped != null) {
        while (serverStopped.getCount() != 0) {
          long remaining = deadline - System.nanoTime();
          if (remaining <= 0) { break; }
          try { serverStopped.await(remaining, TimeUnit.NANOSECONDS); }
          catch (InterruptedException e) { interrupted = true; }
        }
      }
      boolean terminated = serverStopped == null || serverStopped.getCount() == 0;
      if (terminated && serverStopFailure != null) {
        failure = append(failure, serverStopFailure);
      }
      for (ExecutorService pool : pools) {
        if (pool == null) { continue; }
        while (!pool.isTerminated()) {
          long remaining = deadline - System.nanoTime();
          if (remaining <= 0) { break; }
          try { pool.awaitTermination(remaining, TimeUnit.NANOSECONDS); }
          catch (InterruptedException e) { interrupted = true; }
          catch (Exception | Error e) { failure = append(failure, e); break; }
        }
        terminated &= pool.isTerminated();
      }
      if (!terminated) {
        failure = append(failure, new TimeoutException("Scheduler server or workers did not terminate"));
      } else if (serverStopFailure == null) {
        closed = true;
        try { store.close(); }
        catch (Exception | Error e) { failure = append(failure, e); }
        if (hook != null) {
          try { hooks.remove(hook); hook = null; }
          catch (IllegalStateException shutdownInProgress) { /* JVM owns the running hook. */ }
          catch (Exception | Error e) { failure = append(failure, e); }
        }
      }
    } finally {
      if (interrupted) { Thread.currentThread().interrupt(); }
      stopped.countDown();
    }
    if (failure instanceof Error error) { throw error; }
    if (failure instanceof Exception exception) { throw exception; }
  }
}
