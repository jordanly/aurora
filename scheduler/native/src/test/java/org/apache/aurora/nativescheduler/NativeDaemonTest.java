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
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.net.ssl.SSLContext;
import org.apache.aurora.scheduler.storage.sql.NativeSqlStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;

public class NativeDaemonTest {
  @Rule public TemporaryFolder temporary = new TemporaryFolder();

  private static class TestHooks extends NativeDaemon.Hooks {
    Thread registered;
    int removals;
    RuntimeException addFailure;
    @Override void add(Thread hook) {
      if (addFailure != null) { throw addFailure; }
      registered = hook;
    }
    @Override void remove(Thread hook) {
      assertSame(registered, hook);
      removals++;
    }
  }

  private NativeSqlStore open(Path path) throws Exception { return new NativeSqlStore(path, "c", "i"); }
  private static void start(NativeDaemon daemon, Runnable tick) throws Exception {
    daemon.start(new InetSocketAddress("127.0.0.1", 0), SSLContext.getDefault(),
        exchange -> exchange.close(), tick);
  }
  private static void join(Thread thread) throws InterruptedException {
    thread.join(5000);
    assertFalse("Worker remained alive", thread.isAlive());
  }

  @Test(timeout = 15000) public void occupiedPortClosesStoreAfterPartialStartup() throws Exception {
    Path path = temporary.newFolder().toPath();
    TestHooks hooks = new TestHooks();
    try (ServerSocket occupied = new ServerSocket(0)) {
      try (NativeDaemon daemon = new NativeDaemon(open(path), hooks, 1, TimeUnit.SECONDS)) {
        daemon.start(new InetSocketAddress("127.0.0.1", occupied.getLocalPort()),
            SSLContext.getDefault(), exchange -> exchange.close(), () -> { });
        fail("Expected occupied port");
      } catch (java.net.BindException expected) { }
    }
    assertNull(hooks.registered);
    try (NativeSqlStore reopened = open(path)) {
      assertEquals("0", reopened.read(NativeSqlStore.Tx::schedulerEpoch));
    }
  }

  @Test(timeout = 15000) public void failedHookRegistrationReleasesBoundPortAndPreservesPrimary() throws Exception {
    Path path = temporary.newFolder().toPath();
    int port;
    try (ServerSocket reserve = new ServerSocket(0)) { port = reserve.getLocalPort(); }
    TestHooks hooks = new TestHooks();
    hooks.addFailure = new IllegalStateException("hook registration");
    Exception cleanup = new Exception("store close");
    NativeSqlStore store = open(path);
    try (NativeDaemon daemon = new NativeDaemon(() -> { store.close(); throw cleanup; },
        hooks, 1, TimeUnit.SECONDS)) {
      daemon.start(new InetSocketAddress("127.0.0.1", port), SSLContext.getDefault(),
          exchange -> exchange.close(), () -> { });
      fail("Expected registration failure");
    } catch (IllegalStateException expected) {
      assertSame(hooks.addFailure, expected);
      assertArrayEquals(new Throwable[] {cleanup}, expected.getSuppressed());
    }
    try (ServerSocket rebound = new ServerSocket(port); NativeSqlStore reopened = open(path)) {
      assertEquals(port, rebound.getLocalPort());
      assertEquals("0", reopened.read(NativeSqlStore.Tx::schedulerEpoch));
    }
    assertEquals(0, hooks.removals);
  }

  @Test(timeout = 15000) public void cooperativeControllerStopsBeforeStoreAndRepeatedCloseIsNoOp() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    AtomicBoolean exited = new AtomicBoolean();
    AtomicInteger closes = new AtomicInteger();
    TestHooks hooks = new TestHooks();
    NativeDaemon daemon = new NativeDaemon(() -> {
      assertTrue("Controller still owns work", exited.get());
      closes.incrementAndGet();
    }, hooks, 1, TimeUnit.SECONDS);
    try {
      start(daemon, () -> {
        entered.countDown();
        try { new CountDownLatch(1).await(); }
        catch (InterruptedException expected) { Thread.currentThread().interrupt(); }
        finally { exited.set(true); }
      });
      assertTrue(entered.await(5, TimeUnit.SECONDS));
    } finally { daemon.close(); }
    daemon.close();
    assertEquals(1, closes.get());
    assertEquals(1, hooks.removals);
  }

  @Test(timeout = 15000) public void interruptedMainWaitClosesResourcesAndPreservesInterrupt() throws Exception {
    AtomicInteger closes = new AtomicInteger();
    AtomicBoolean interrupted = new AtomicBoolean();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    CountDownLatch waiting = new CountDownLatch(1);
    TestHooks hooks = new TestHooks();
    Thread main = new Thread(() -> {
      try (NativeDaemon daemon = new NativeDaemon(closes::incrementAndGet,
          hooks, 1, TimeUnit.SECONDS)) {
        start(daemon, () -> { });
        waiting.countDown();
        daemon.awaitShutdown();
        failure.set(new AssertionError("Wait returned without interrupt"));
      } catch (InterruptedException expected) {
        interrupted.set(Thread.currentThread().isInterrupted());
      } catch (Throwable unexpected) { failure.set(unexpected); }
    });
    main.start();
    try {
      assertTrue(waiting.await(5, TimeUnit.SECONDS));
      main.interrupt();
      join(main);
    } finally { main.interrupt(); join(main); }
    assertNull(failure.get());
    assertTrue(interrupted.get());
    assertEquals(1, closes.get());
    assertEquals(1, hooks.removals);
  }

  @Test(timeout = 15000) public void uncooperativeControllerRetainsStoreUntilSuccessfulCloseRetry() throws Exception {
    Path path = temporary.newFolder().toPath();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch exited = new CountDownLatch(1);
    TestHooks hooks = new TestHooks();
    NativeDaemon daemon = new NativeDaemon(open(path), hooks, 30, TimeUnit.MILLISECONDS);
    try {
      start(daemon, () -> {
        entered.countDown();
        try {
          while (release.getCount() != 0) {
            try { release.await(); } catch (InterruptedException ignored) { }
          }
        } finally { exited.countDown(); }
      });
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      long closeStarted = System.nanoTime();
      try { daemon.close(); fail("Expected bounded shutdown timeout"); }
      catch (TimeoutException expected) { }
      assertTrue("Shutdown exceeded bounded wait",
          System.nanoTime() - closeStarted < TimeUnit.SECONDS.toNanos(3));
      assertEquals(0, hooks.removals);
      try (NativeSqlStore unexpected = open(path)) {
        fail("Store ownership released while controller remained active");
      } catch (java.nio.channels.OverlappingFileLockException expected) { }
    } finally {
      release.countDown();
      assertTrue(exited.await(5, TimeUnit.SECONDS));
      daemon.close();
    }
    assertEquals(1, hooks.removals);
    try (NativeSqlStore reopened = open(path)) {
      assertEquals("0", reopened.read(NativeSqlStore.Tx::schedulerEpoch));
    }
  }

  @Test(timeout = 15000) public void shutdownHookAlwaysReleasesMainWaitEvenWhenCloseFails() throws Exception {
    TestHooks hooks = new TestHooks();
    NativeDaemon daemon = new NativeDaemon(() -> { throw new Exception("private detail"); },
        hooks, 1, TimeUnit.SECONDS);
    start(daemon, () -> { });
    hooks.registered.start();
    join(hooks.registered);
    daemon.awaitShutdown();
    daemon.close();
    assertEquals(1, hooks.removals);
  }
}
