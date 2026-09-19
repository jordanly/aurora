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
package org.apache.aurora.scheduler.http.api.security;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import jakarta.servlet.ServletContextListener;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.TypeLiteral;
import com.google.inject.spi.Elements;

import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.application.ShutdownRegistry.ShutdownRegistryImpl;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.config.CliOptions;
import org.easymock.EasyMock;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class Kerberos5ShiroRealmModuleTest extends EasyMockTest {
  private static final KerberosPrincipal SERVER_PRINCIPAL =
      new KerberosPrincipal("HTTP/aurora.example.com@EXAMPLE.COM");

  private File serverKeytab;
  private GSSManager gssManager;

  private GSSCredential gssCredential;

  private Kerberos5ShiroRealmModule module;

  @Before
  public void setUp() {
    serverKeytab = createMock(File.class);
    gssManager = createMock(GSSManager.class);
    gssCredential = createMock(GSSCredential.class);

    module = new Kerberos5ShiroRealmModule(serverKeytab, SERVER_PRINCIPAL, gssManager);
  }

  @Test
  public void testConfigure() throws Exception {
    expect(serverKeytab.getAbsolutePath()).andReturn("path.keytab");
    expect(
        gssManager.createCredential(
            EasyMock.isNull(),
            eq(GSSCredential.INDEFINITE_LIFETIME),
            anyObject(Oid[].class),
            eq(GSSCredential.ACCEPT_ONLY)))
        .andAnswer(() -> {
          assertTrue(Subject.current().getPrincipals().contains(SERVER_PRINCIPAL));
          return gssCredential;
        });

    Service http = runningHttp();
    gssCredential.dispose();
    expectLastCall().andAnswer(() -> {
      assertEquals(Service.State.TERMINATED, http.state());
      return null;
    });
    control.replay();

    ShutdownRegistryImpl shutdown = new ShutdownRegistryImpl();
    LoginContext login = new LoginContext(Kerberos5ShiroRealmModule.class.getName(),
        null, null, module.createJaasConfiguration());
    assertSame(gssCredential, module.acquireServerCredential(login, shutdown, http));
    shutdown.execute();
    shutdown.execute();
  }

  private Service runningHttp() {
    Service http = new AbstractIdleService() {
      @Override
      protected void startUp() {
        // This fake HTTP service owns no resources.
      }

      @Override
      protected void shutDown() {
        // This fake HTTP service owns no resources.
      }
    };
    http.startAsync().awaitRunning();
    return http;
  }

  @Test
  public void testProgrammaticConfigurationPreservesOptionValues() {
    String path = "a\\directory/quoted\"keytab";
    expect(serverKeytab.getAbsolutePath()).andReturn(path);
    control.replay();
    var configuration = module.createJaasConfiguration();
    AppConfigurationEntry[] entries = configuration.getAppConfigurationEntry(
        Kerberos5ShiroRealmModule.class.getName());
    assertEquals(1, entries.length);
    assertEquals("com.sun.security.auth.module.Krb5LoginModule", entries[0].getLoginModuleName());
    assertEquals(
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, entries[0].getControlFlag());
    assertEquals(Map.of("useKeyTab", "true", "storeKey", "true", "doNotPrompt", "true",
        "isInitiator", "false", "keyTab", path, "principal", SERVER_PRINCIPAL.getName(),
        "debug", "true"), entries[0].getOptions());
    assertNull(configuration.getAppConfigurationEntry("unrelated"));
  }

  @Test
  public void testConfigurationDoesNotAcquireCredentials() {
    control.replay();
    Elements.getElements(module);
  }

  @Test
  public void testLoginFailureDoesNotAcquireCredential() throws Exception {
    LoginContext login = createMock(LoginContext.class);
    LoginException failure = new LoginException("login failed");
    login.login();
    expectLastCall().andThrow(failure);
    control.replay();
    assertSame(failure, assertThrows(LoginException.class, () -> module.acquireServerCredential(
        login, new ShutdownRegistryImpl(), null)));
  }

  @Test
  public void testAcquisitionFailureLogsOutAndPreservesCleanupFailure() throws Exception {
    LoginContext login = createMock(LoginContext.class);
    login.login();
    expect(login.getSubject()).andReturn(new Subject());
    RuntimeException failure = new RuntimeException("GSS acquisition failed");
    expect(gssManager.createCredential(EasyMock.isNull(), eq(GSSCredential.INDEFINITE_LIFETIME),
        anyObject(Oid[].class), eq(GSSCredential.ACCEPT_ONLY))).andThrow(failure);
    LoginException cleanupFailure = new LoginException("logout failed");
    login.logout();
    expectLastCall().andThrow(cleanupFailure);
    control.replay();
    assertSame(failure, assertThrows(RuntimeException.class, () -> module.acquireServerCredential(
        login, new ShutdownRegistryImpl(), null)));
    assertEquals(1, failure.getSuppressed().length);
    assertSame(cleanupFailure, failure.getSuppressed()[0]);
  }

  @Test
  public void testRegistrationFailureDisposesCredentialAndLogsOut() throws Exception {
    LoginContext login = createMock(LoginContext.class);
    login.login();
    expect(login.getSubject()).andReturn(new Subject());
    expect(gssManager.createCredential(EasyMock.isNull(), eq(GSSCredential.INDEFINITE_LIFETIME),
        anyObject(Oid[].class), eq(GSSCredential.ACCEPT_ONLY))).andReturn(gssCredential);
    gssCredential.dispose();
    login.logout();
    control.replay();
    ShutdownRegistryImpl shutdown = new ShutdownRegistryImpl();
    shutdown.execute();
    Service http = runningHttp();
    assertThrows(IllegalStateException.class,
        () -> module.acquireServerCredential(login, shutdown, http));
    http.stopAsync().awaitTerminated();
  }

  @Test
  public void testFailedHttpStartupStillReleasesCredential() throws Exception {
    LoginContext login = createMock(LoginContext.class);
    login.login();
    expect(login.getSubject()).andReturn(new Subject());
    expect(gssManager.createCredential(EasyMock.isNull(), eq(GSSCredential.INDEFINITE_LIFETIME),
        anyObject(Oid[].class), eq(GSSCredential.ACCEPT_ONLY))).andReturn(gssCredential);
    gssCredential.dispose();
    login.logout();
    control.replay();
    Service http = new AbstractIdleService() {
      @Override
      protected void startUp() {
        throw new IllegalStateException("HTTP failed");
      }

      @Override
      protected void shutDown() {
        // This fake HTTP service owns no resources.
      }
    };
    assertThrows(IllegalStateException.class, () -> http.startAsync().awaitRunning());
    ShutdownRegistryImpl shutdown = new ShutdownRegistryImpl();
    module.acquireServerCredential(login, shutdown, http);
    shutdown.execute();
  }

  @Test
  public void testShutdownTimeoutRetainsCredentialUntilHttpTerminates() throws Exception {
    AtomicBoolean stopped = new AtomicBoolean();
    AtomicBoolean disposed = new AtomicBoolean();
    AtomicReference<Runnable> finishStop = new AtomicReference<>();
    Service http = new AbstractService() {
      @Override
      protected void doStart() {
        notifyStarted();
      }

      @Override
      protected void doStop() {
        finishStop.set(() -> {
          stopped.set(true);
          notifyStopped();
        });
      }
    };
    http.startAsync().awaitRunning();
    LoginContext login = createMock(LoginContext.class);
    login.login();
    expect(login.getSubject()).andReturn(new Subject());
    expect(gssManager.createCredential(EasyMock.isNull(), eq(GSSCredential.INDEFINITE_LIFETIME),
        anyObject(Oid[].class), eq(GSSCredential.ACCEPT_ONLY))).andReturn(gssCredential);
    gssCredential.dispose();
    expectLastCall().andAnswer(() -> {
      assertTrue(stopped.get());
      disposed.set(true);
      return null;
    });
    login.logout();
    control.replay();
    ShutdownRegistryImpl shutdown = new ShutdownRegistryImpl();
    module.acquireServerCredential(login, shutdown, http, Duration.ZERO);
    shutdown.execute();
    assertEquals(Service.State.STOPPING, http.state());
    assertFalse(disposed.get());
    finishStop.get().run();
    assertTrue(disposed.get());
    shutdown.execute();
  }

  @Test
  public void testCredentialProviderBindingGraph() throws Exception {
    expect(serverKeytab.getAbsolutePath()).andReturn("path.keytab");
    expect(gssManager.createCredential(EasyMock.isNull(), eq(GSSCredential.INDEFINITE_LIFETIME),
        anyObject(Oid[].class), eq(GSSCredential.ACCEPT_ONLY))).andReturn(gssCredential);
    gssCredential.dispose();
    control.replay();
    ShutdownRegistryImpl shutdown = new ShutdownRegistryImpl();
    var injector = Guice.createInjector(module, new AbstractModule() {
      @Override
      protected void configure() {
        bind(ShutdownRegistry.class).toInstance(shutdown);
        bind(CliOptions.class).toInstance(new CliOptions());
        bind(ServletContextListener.class).toInstance(new ServletContextListener() { });
        bind(new TypeLiteral<Optional<String>>() { }).toInstance(Optional.empty());
      }
    });
    org.junit.Assert.assertNotNull(injector.getInstance(Kerberos5Realm.class));
    shutdown.execute();
  }

  @Test
  public void testCredentialSubjectIsScopedToAction() throws Exception {
    Subject subject = new Subject();
    Subject previous = Subject.current();
    expect(gssManager.createCredential(
        EasyMock.isNull(),
        eq(GSSCredential.INDEFINITE_LIFETIME),
        anyObject(Oid[].class),
        eq(GSSCredential.ACCEPT_ONLY))).andAnswer(() -> {
          assertSame(subject, Subject.current());
          return gssCredential;
        });
    control.replay();

    assertSame(gssCredential, module.createServerCredential(subject));
    assertSame(previous, Subject.current());
  }

  @Test
  public void testGssFailureRetainsRuntimeWrapper() throws Exception {
    GSSException failure = new GSSException(GSSException.NO_CRED);
    expect(gssManager.createCredential(
        EasyMock.isNull(),
        eq(GSSCredential.INDEFINITE_LIFETIME),
        anyObject(Oid[].class),
        eq(GSSCredential.ACCEPT_ONLY))).andThrow(failure);
    control.replay();

    Subject previous = Subject.current();
    try {
      module.createServerCredential(new Subject());
      fail("Expected credential failure");
    } catch (RuntimeException e) {
      assertEquals(RuntimeException.class, e.getClass());
      assertSame(failure, e.getCause());
    }
    assertSame(previous, Subject.current());
  }

  @Test
  public void testActionCompletionExceptionIsNotUnwrapped() throws Exception {
    CompletionException failure = new CompletionException(new IllegalStateException("failure"));
    expect(gssManager.createCredential(
        EasyMock.isNull(),
        eq(GSSCredential.INDEFINITE_LIFETIME),
        anyObject(Oid[].class),
        eq(GSSCredential.ACCEPT_ONLY))).andThrow(failure);
    control.replay();

    try {
      module.createServerCredential(new Subject());
      fail("Expected credential failure");
    } catch (CompletionException e) {
      assertSame(failure, e);
    }
  }
}
