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
package org.apache.aurora.scheduler.http;

import java.util.List;

import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.ws.rs.ext.Provider;

import com.google.inject.Injector;
import com.google.inject.Module;

import org.jboss.resteasy.plugins.server.servlet.ResteasyBootstrap;
import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.HttpResponse;
import org.jboss.resteasy.spi.PropertyInjector;
import org.jboss.resteasy.spi.ResourceFactory;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.jboss.resteasy.util.GetRestful;

import static java.util.Objects.requireNonNull;

/** Binds the existing Guice resource graph to Jakarta REST without the retired Guice extension. */
abstract class GuiceRestContextListener extends ResteasyBootstrap {
  private final Injector parent;

  GuiceRestContextListener(Injector parent) {
    this.parent = requireNonNull(parent);
  }

  protected abstract List<? extends Module> getModules(ServletContext context);

  @Override
  public void contextInitialized(ServletContextEvent event) {
    super.contextInitialized(event);
    try {
      for (Injector injector = parent.createChildInjector(getModules(event.getServletContext()));
          injector != null; injector = injector.getParent()) {
        // Providers precede resources so property injection sees all custom codecs.
        for (var binding : injector.getBindings().values()) {
          Class<?> type = binding.getKey().getTypeLiteral().getRawType();
          if (type.isAnnotationPresent(Provider.class)) {
            deployment.getProviderFactory().registerProviderInstance(binding.getProvider().get());
          }
        }
        for (var binding : injector.getBindings().values()) {
          Class<?> type = binding.getKey().getTypeLiteral().getRawType();
          if (GetRestful.isRootResource(type)) {
            deployment.getRegistry().addResourceFactory(
                new InjectedResource(binding.getProvider(), type));
          }
        }
      }
    } catch (RuntimeException | Error failure) {
      try {
        super.contextDestroyed(event);
      } catch (RuntimeException | Error cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  private static final class InjectedResource implements ResourceFactory {
    private final com.google.inject.Provider<?> provider;
    private final Class<?> type;
    private PropertyInjector properties;

    InjectedResource(com.google.inject.Provider<?> provider, Class<?> type) {
      this.provider = provider;
      this.type = type;
    }

    @Override
    public Class<?> getScannableClass() {
      return type;
    }

    @Override
    public void registered(ResteasyProviderFactory factory) {
      properties = factory.getInjectorFactory().createPropertyInjector(type, factory);
    }

    @Override
    public Object createResource(
        HttpRequest request, HttpResponse response, ResteasyProviderFactory factory) {
      Object resource = provider.get();
      var injected = properties.inject(request, response, resource, true);
      return injected == null ? resource : injected.thenApply(unused -> resource);
    }

    @Override
    public void requestFinished(HttpRequest request, HttpResponse response, Object resource) {
      // Guice owns the resource scope.
    }

    @Override
    public void unregistered() {
      // Guice owns the resource scope.
    }
  }
}
