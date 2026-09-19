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

import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class JettyServerModuleTest {
  @Test
  public void testStartupFailureReleasesOpenedConnector() throws Exception {
    AtomicInteger port = new AtomicInteger();
    IllegalStateException failure = new IllegalStateException("startup failed");
    Server server = new Server() {
      @Override
      protected void doStart() throws Exception {
        port.set(((ServerConnector) getConnectors()[0]).getLocalPort());
        throw failure;
      }
    };
    ServerConnector connector = new ServerConnector(server);
    connector.setPort(0);
    server.addConnector(connector);

    RuntimeException thrown = assertThrows(RuntimeException.class,
        () -> JettyServerModule.HttpServerLauncher.startServer(
            server, connector, () -> "localhost"));
    assertSame(failure, thrown.getCause());
    assertReleased(server, connector, port.get());
  }

  @Test
  public void testAddressFailureStopsRunningServer() throws Exception {
    Server server = new Server();
    ServerConnector connector = new ServerConnector(server);
    connector.setPort(0);
    server.addConnector(connector);
    AtomicInteger port = new AtomicInteger();
    UnknownHostException failure = new UnknownHostException("resolution failed");

    RuntimeException thrown = assertThrows(RuntimeException.class,
        () -> JettyServerModule.HttpServerLauncher.startServer(server, connector, () -> {
          assertTrue(server.isStarted());
          port.set(connector.getLocalPort());
          throw failure;
        }));
    assertSame(failure, thrown.getCause());
    assertReleased(server, connector, port.get());
  }

  private static void assertReleased(Server server, ServerConnector connector, int port)
      throws Exception {
    assertTrue(port > 0);
    assertFalse(connector.isOpen());
    assertTrue(server.isStopped());
    try (ServerSocket rebound = new ServerSocket(port)) {
      assertEquals(port, rebound.getLocalPort());
    }
  }
}
