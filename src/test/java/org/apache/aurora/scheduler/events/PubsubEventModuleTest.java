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
package org.apache.aurora.scheduler.events;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.AppStartup;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.app.LifecycleModule;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;
import org.apache.aurora.scheduler.execution.ExecutionControl;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PubsubEventModuleTest extends EasyMockTest {

  private FakeStatsProvider statsProvider;
  private Logger logger;
  private UncaughtExceptionHandler exceptionHandler;

  @Before
  public void setUp() {
    statsProvider = new FakeStatsProvider();
    logger = createMock(Logger.class);
    exceptionHandler = createMock(UncaughtExceptionHandler.class);
  }

  @Test
  public void testHandlesDeadEvent() {
    logger.warn(String.format(PubsubEventModule.DEAD_EVENT_MESSAGE, "hello"));
    logger.warn(String.format(PubsubEventModule.DEAD_EVENT_MESSAGE, "hello2"));

    control.replay();

    Injector injector = getInjector();
    injector.getInstance(EventBus.class).post("hello");
    assertEquals(1, statsProvider.getLongValue(PubsubEventModule.EVENT_BUS_DEAD_EVENTS));
    injector.getInstance(Key.get(EventBus.class, PubsubEventModule.RegisteredEvents.class))
        .post("hello2");
    assertEquals(2, statsProvider.getLongValue(PubsubEventModule.EVENT_BUS_DEAD_EVENTS));
  }

  @Test
  public void testPubsubExceptionTracking() throws Exception {
    logger.error(anyString(), EasyMock.<Throwable>anyObject());
    expectLastCall().times(2);

    control.replay();

    Injector injector = getInjector(
        new AbstractModule() {
          @Override
          protected void configure() {
            PubsubEventModule.bindSubscriber(binder(), ThrowingSubscriber.class);
            PubsubEventModule.bindRegisteredSubscriber(binder(), ThrowingSubscriber.class);
          }
        });
    injector.getInstance(Key.get(GuavaUtils.ServiceManagerIface.class, AppStartup.class))
        .startAsync().awaitHealthy();
    assertEquals(0, statsProvider.getLongValue(PubsubEventModule.EXCEPTIONS_STAT));
    injector.getInstance(EventBus.class).post("hello");
    assertEquals(1, statsProvider.getLongValue(PubsubEventModule.EXCEPTIONS_STAT));
    injector.getInstance(Key.get(EventBus.class, PubsubEventModule.RegisteredEvents.class))
        .post("hello2");
    assertEquals(2, statsProvider.getLongValue(PubsubEventModule.EXCEPTIONS_STAT));
    assertEquals(0, statsProvider.getLongValue(PubsubEventModule.EVENT_BUS_DEAD_EVENTS));
  }

  @Test
  public void testSynchronousRecoveryAndAsynchronousRegistration() {
    control.replay();
    List<Runnable> registered = new ArrayList<>();
    Injector injector = getInjector(new PubsubEventModule(logger, registered::add, true),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(ExecutionControl.class).toInstance(() -> { });
          }
        });
    AtomicInteger received = new AtomicInteger();
    var subscriber = new CountingSubscriber(received);
    EventBus ordinary = injector.getInstance(EventBus.class);
    ordinary.register(subscriber);
    ordinary.post("recovered");
    assertEquals(1, received.get());
    EventBus registration = injector.getInstance(
        Key.get(EventBus.class, PubsubEventModule.RegisteredEvents.class));
    registration.register(subscriber);
    registration.post("registered");
    assertEquals(1, received.get());
    registered.forEach(Runnable::run);
    assertEquals(2, received.get());
  }

  @Test
  public void testSubscriberFailureAbortsLazilyAndPreventsReadiness() throws Exception {
    logger.error(anyString(), EasyMock.<Throwable>anyObject());
    control.replay();
    CountDownLatch aborted = new CountDownLatch(1);
    AtomicInteger resolved = new AtomicInteger();
    Injector injector = getInjector(
        new PubsubEventModule(logger, MoreExecutors.directExecutor(), true),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(ExecutionControl.class).toProvider(() -> {
              resolved.incrementAndGet();
              return aborted::countDown;
            });
          }
        });
    EventBus bus = injector.getInstance(EventBus.class);
    bus.register(new ThrowingSubscriber());
    assertEquals(0, resolved.get());
    try {
      bus.post("recovery");
      fail("Subscriber failure must prevent recovery from reporting success");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getCause() instanceof UnsupportedOperationException);
    }
    assertTrue(aborted.await(10, TimeUnit.SECONDS));
    assertEquals(1, resolved.get());
    assertEquals(1, statsProvider.getLongValue(PubsubEventModule.EXCEPTIONS_STAT));
    try {
      bus.post("later");
      fail("A failed event bus must reject later publication");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getCause() instanceof UnsupportedOperationException);
    }
    assertEquals(1, resolved.get());
  }

  public static final class CountingSubscriber {
    private final AtomicInteger received;

    CountingSubscriber(AtomicInteger received) {
      this.received = received;
    }

    @Subscribe
    public void receive(String value) {
      received.incrementAndGet();
    }
  }

  static class ThrowingSubscriber implements PubsubEvent.EventSubscriber {
    @Subscribe
    public void receiveString(String value) {
      throw new UnsupportedOperationException();
    }
  }

  public Injector getInjector(Module... additionalModules) {
    return getInjector(new PubsubEventModule(logger, MoreExecutors.directExecutor()),
        additionalModules);
  }

  private Injector getInjector(PubsubEventModule eventModule, Module... additionalModules) {
    return Guice.createInjector(
        new LifecycleModule(),
        eventModule,
        new SchedulerServicesModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(Executor.class).annotatedWith(AsyncExecutor.class)
                .toInstance(MoreExecutors.directExecutor());

            bind(UncaughtExceptionHandler.class).toInstance(exceptionHandler);

            bind(StatsProvider.class).toInstance(statsProvider);
            for (Module module : additionalModules) {
              install(module);
            }
          }
        });
  }
}
