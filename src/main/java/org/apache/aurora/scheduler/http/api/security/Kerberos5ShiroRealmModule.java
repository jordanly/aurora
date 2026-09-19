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
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import jakarta.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.sun.security.auth.module.Krb5LoginModule;

import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.http.JettyServerModule.HttpServerLauncher;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures and provides a Shiro {@link org.apache.shiro.realm.Realm}.
 *
 * @see org.apache.aurora.scheduler.http.api.security.Kerberos5Realm
 */
public class Kerberos5ShiroRealmModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(Kerberos5ShiroRealmModule.class);

  /**
   * Standard Object Identifier for the Kerberos 5 GSS-API mechanism.
   */
  private static final String GSS_KRB5_MECH_OID = "1.2.840.113554.1.2.2";

  /**
   * Standard Object Identifier for the SPNEGO GSS-API mechanism.
   */
  private static final String GSS_SPNEGO_MECH_OID = "1.3.6.1.5.5.2";

  @Parameters(separators = "=")
  public static class Options {
    private static final String SERVER_KEYTAB_ARGNAME = "-kerberos_server_keytab";
    private static final String SERVER_PRINCIPAL_ARGNAME = "-kerberos_server_principal";

    @Parameter(names = SERVER_KEYTAB_ARGNAME, description = "Path to the server keytab.")
    public File serverKeytab;

    @Parameter(names = SERVER_PRINCIPAL_ARGNAME,
        description = "Kerberos server principal to use, usually of the form "
            + "HTTP/aurora.example.com@EXAMPLE.COM")
    public KerberosPrincipal serverPrincipal;

    @Parameter(names = "-kerberos_debug",
        description = "Produce additional Kerberos debugging output.",
        arity = 1)
    public boolean kerberosDebug = false;
  }

  private final Optional<File> serverKeyTab;
  private final Optional<KerberosPrincipal> serverPrincipal;
  private final GSSManager gssManager;
  private final boolean kerberosDebugEnabled;

  public Kerberos5ShiroRealmModule(CliOptions options) {
    this(
        Optional.ofNullable(options.kerberos.serverKeytab),
        Optional.ofNullable(options.kerberos.serverPrincipal),
        GSSManager.getInstance(),
        options.kerberos.kerberosDebug);
  }

  @VisibleForTesting
  Kerberos5ShiroRealmModule(
      File serverKeyTab,
      KerberosPrincipal serverPrincipal,
      GSSManager gssManager) {

    this(
        Optional.of(serverKeyTab),
        Optional.of(serverPrincipal),
        gssManager,
        true);
  }

  private Kerberos5ShiroRealmModule(
      Optional<File> serverKeyTab,
      Optional<KerberosPrincipal> serverPrincipal,
      GSSManager gssManager,
      boolean kerberosDebugEnabled) {

    this.serverKeyTab = serverKeyTab;
    this.serverPrincipal = serverPrincipal;
    this.gssManager = gssManager;
    this.kerberosDebugEnabled = kerberosDebugEnabled;
  }

  @Override
  protected void configure() {
    if (!serverKeyTab.isPresent()) {
      addError("No -" + Options.SERVER_KEYTAB_ARGNAME + " specified.");
      return;
    }

    if (!serverPrincipal.isPresent()) {
      addError("No -" + Options.SERVER_PRINCIPAL_ARGNAME + " specified.");
      return;
    }

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(GSSManager.class).toInstance(gssManager);

        bind(Kerberos5Realm.class).in(Singleton.class);
        expose(Kerberos5Realm.class);
      }

      @Provides
      @Singleton
      GSSCredential provideServerCredential(ShutdownRegistry shutdown, HttpServerLauncher http) {
        try {
          LoginContext login = new LoginContext(
              Kerberos5ShiroRealmModule.class.getName(), null, null, createJaasConfiguration());
          return acquireServerCredential(login, shutdown, http);
        } catch (LoginException e) {
          throw new RuntimeException(e);
        }
      }
    });
    ShiroUtils.addRealmBinding(binder()).to(Kerberos5Realm.class);
  }

  @VisibleForTesting
  Configuration createJaasConfiguration() {
    Map<String, String> options = Map.of(
        "useKeyTab", "true",
        "storeKey", "true",
        "doNotPrompt", "true",
        "isInitiator", "false",
        "keyTab", serverKeyTab.orElseThrow().getAbsolutePath(),
        "principal", serverPrincipal.orElseThrow().getName(),
        "debug", Boolean.toString(kerberosDebugEnabled));
    return new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        if (!Kerberos5ShiroRealmModule.class.getName().equals(name)) {
          return null;
        }
        return new AppConfigurationEntry[] {new AppConfigurationEntry(
            Krb5LoginModule.class.getName(),
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options)};
      }
    };
  }

  @VisibleForTesting
  GSSCredential acquireServerCredential(LoginContext login, ShutdownRegistry shutdown, Service http)
      throws LoginException {
    return acquireServerCredential(login, shutdown, http, Duration.ofSeconds(5));
  }

  @VisibleForTesting
  GSSCredential acquireServerCredential(
      LoginContext login, ShutdownRegistry shutdown, Service http, Duration shutdownTimeout)
      throws LoginException {
    login.login();
    OwnedCredential owned = null;
    try {
      owned = new OwnedCredential(login, createServerCredential(login.getSubject()));
      OwnedCredential resource = owned;
      http.addListener(new Service.Listener() {
        @Override
        public void terminated(Service.State from) {
          resource.closeAfterHttp();
        }

        @Override
        public void failed(Service.State from, Throwable failure) {
          resource.closeAfterHttp();
        }
      }, MoreExecutors.directExecutor());
      shutdown.addAction(() -> {
        // Startup services stop concurrently. Wait for HTTP to finish handling requests before
        // disposing the credential shared by their authentication contexts.
        try {
          http.stopAsync().awaitTerminated(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (IllegalStateException e) {
          if (http.state() != Service.State.FAILED) {
            throw e;
          }
          // Failed HTTP startup has already attempted server rollback.
        }
        resource.close();
      });
      // A listener added after termination receives no past events. Close that registration race.
      if (http.state() == Service.State.TERMINATED || http.state() == Service.State.FAILED) {
        resource.closeAfterHttp();
      }
      return owned.credential;
    } catch (RuntimeException | Error failure) {
      try {
        if (owned == null) {
          login.logout();
        } else {
          owned.close();
        }
      } catch (Exception | Error cleanup) {
        if (failure != cleanup) { // NOPMD - Throwable forbids self-suppression by identity.
          failure.addSuppressed(cleanup);
        }
      }
      throw failure;
    }
  }

  private static final class OwnedCredential implements AutoCloseable {
    private final LoginContext login;
    private final GSSCredential credential;
    private boolean closed;

    OwnedCredential(LoginContext login, GSSCredential credential) {
      this.login = login;
      this.credential = credential;
    }

    void closeAfterHttp() {
      try {
        close();
      } catch (GSSException | LoginException | RuntimeException e) {
        LOG.warn("Failed to release Kerberos server credentials.", e);
      }
    }

    @Override
    public synchronized void close() throws GSSException, LoginException {
      if (closed) {
        return;
      }
      closed = true;
      try {
        credential.dispose();
      } catch (GSSException | RuntimeException | Error failure) {
        try {
          login.logout();
        } catch (LoginException | RuntimeException | Error cleanup) {
          if (failure != cleanup) { // NOPMD - Throwable forbids self-suppression by identity.
            failure.addSuppressed(cleanup);
          }
        }
        throw failure;
      }
      login.logout();
    }
  }

  @VisibleForTesting
  // Preserve the original unchecked cause object when unwrapping Subject.callAs.
  @SuppressWarnings("PMD.PreserveStackTrace")
  GSSCredential createServerCredential(Subject subject) {
    try {
      return Subject.callAs(subject, () -> {
        try {
          return gssManager.createCredential(
              null /* Use the service principal name defined in the JAAS configuration */,
              GSSCredential.INDEFINITE_LIFETIME,
              new Oid[] {new Oid(GSS_SPNEGO_MECH_OID), new Oid(GSS_KRB5_MECH_OID)},
              GSSCredential.ACCEPT_ONLY);
        } catch (GSSException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (CompletionException e) {
      // doAs propagated the action's unchecked failure directly. Remove only callAs's wrapper.
      if (e.getCause() instanceof RuntimeException cause) {
        throw cause;
      }
      throw e;
    }
  }
}
