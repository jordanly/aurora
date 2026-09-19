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
package org.apache.aurora.common.application;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class LifecycleTest {
  @Test
  public void testCallbacksRunOutsideLocksAndOnlyOnce() throws Exception {
    var registry = new ShutdownRegistry.ShutdownRegistryImpl();
    var lifecycle = new Lifecycle(registry);
    List<Integer> order = new ArrayList<>();
    registry.addAction(() -> order.add(1));
    registry.addAction(() -> {
      CompletableFuture.runAsync(() -> {
        assertTrue(lifecycle.isAlive());
        lifecycle.shutdown();
        assertThrows(IllegalStateException.class, () -> registry.addAction(() -> { }));
      }).get(10, TimeUnit.SECONDS);
      order.add(2);
    });
    lifecycle.shutdown();
    lifecycle.shutdown();
    registry.execute();
    assertEquals(List.of(2, 1), order);
    assertFalse(lifecycle.isAlive());
  }

  @Test
  public void testAwaitInterruptShutsDownAndPreservesInterrupt() throws Exception {
    AtomicInteger stopped = new AtomicInteger();
    var lifecycle = new Lifecycle(stopped::incrementAndGet);
    AtomicBoolean interrupted = new AtomicBoolean();
    CountDownLatch ready = new CountDownLatch(1);
    Thread waiter = Thread.ofPlatform().start(() -> {
      ready.countDown();
      lifecycle.awaitShutdown();
      interrupted.set(Thread.currentThread().isInterrupted());
    });
    try {
      assertTrue(ready.await(10, TimeUnit.SECONDS));
      waiter.interrupt();
      waiter.join(10000);
      assertFalse(waiter.isAlive());
      assertTrue(interrupted.get());
      assertEquals(1, stopped.get());
      assertFalse(lifecycle.isAlive());
    } finally {
      lifecycle.shutdown();
      waiter.interrupt();
      waiter.join(10000);
    }
  }

  @Test
  public void testFailingHookStillNotifiesWaiters() {
    var lifecycle = new Lifecycle(() -> {
      throw new IllegalStateException("hook failed");
    });
    assertThrows(IllegalStateException.class, lifecycle::shutdown);
    assertFalse(lifecycle.isAlive());
    lifecycle.awaitShutdown();
  }
}
