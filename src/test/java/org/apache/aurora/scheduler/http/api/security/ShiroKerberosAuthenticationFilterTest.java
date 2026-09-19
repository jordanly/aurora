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

import java.io.IOException;
import java.util.function.Function;

import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.core.HttpHeaders;

import com.google.inject.Module;
import com.google.inject.servlet.ServletModule;
import com.google.inject.util.Providers;

import org.apache.aurora.scheduler.http.AbstractJettyTest;
import org.apache.aurora.scheduler.testing.LogCapture;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ShiroKerberosAuthenticationFilterTest extends AbstractJettyTest {
  private static final String PATH = "/test";

  private Subject subject;
  private HttpServlet mockServlet;

  private ShiroKerberosAuthenticationFilter filter;

  @Before
  public void setUp() {
    subject = createMock(Subject.class);
    mockServlet = createMock(HttpServlet.class);

    filter = new ShiroKerberosAuthenticationFilter(Providers.of(subject));
  }

  private HttpServlet getMockServlet() {
    return mockServlet;
  }

  @Override
  public Function<ServletContext, Module> getChildServletModule() {
    return servletContext -> new ServletModule() {
      @Override
      protected void configureServlets() {
        filter(PATH).through(filter);
        serve(PATH).with(new HttpServlet() {
          @Override
          protected void service(HttpServletRequest req, HttpServletResponse resp)
              throws ServletException, IOException {

            getMockServlet().service(req, resp);
            resp.setStatus(HttpServletResponse.SC_OK);
          }
        });
      }
    };
  }

  @Test
  public void testDoesNotPermitUnauthenticated() throws ServletException, IOException {
    replayAndStart();

    jakarta.ws.rs.core.Response clientResponse = getRequestBuilder(PATH).get();
    assertEquals(HttpServletResponse.SC_UNAUTHORIZED, clientResponse.getStatus());
    assertEquals(
        ShiroKerberosAuthenticationFilter.NEGOTIATE,
        clientResponse.getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE));
  }

  @Test
  public void testRejectsMalformedMechanism() {
    replayAndStart();

    try (LogCapture logs = new LogCapture(ShiroKerberosAuthenticationFilter.class)) {
      jakarta.ws.rs.core.Response clientResponse = getRequestBuilder(PATH)
          .header(HttpHeaders.AUTHORIZATION, "Basic synthetic-header-secret")
          .get();
      assertEquals(HttpServletResponse.SC_BAD_REQUEST, clientResponse.getStatus());
      assertTrue(logs.messages().contains("Malformed Authorize header"));
      assertFalse(logs.messages().contains("synthetic-header-secret"));
    }
  }

  @Test
  public void testLoginFailure401() {
    subject.login(isA(AuthenticationToken.class));
    expectLastCall().andThrow(new AuthenticationException("synthetic-auth-secret"));

    replayAndStart();

    try (LogCapture logs = new LogCapture(ShiroKerberosAuthenticationFilter.class)) {
      jakarta.ws.rs.core.Response clientResponse = getRequestBuilder(PATH)
          .header(HttpHeaders.AUTHORIZATION, ShiroKerberosAuthenticationFilter.NEGOTIATE + " asdf")
          .get();

      assertEquals(HttpServletResponse.SC_UNAUTHORIZED, clientResponse.getStatus());
      assertEquals(
          ShiroKerberosAuthenticationFilter.NEGOTIATE,
          clientResponse.getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE));
      assertTrue(logs.messages().contains("Kerberos login failed"));
      assertFalse(logs.messages().contains("synthetic-auth-secret"));
    }
  }

  @Test
  public void testLoginSuccess200() throws ServletException, IOException {
    subject.login(isA(AuthenticationToken.class));
    mockServlet.service(anyObject(HttpServletRequest.class), anyObject(HttpServletResponse.class));

    replayAndStart();

    jakarta.ws.rs.core.Response clientResponse = getRequestBuilder(PATH)
        .header(HttpHeaders.AUTHORIZATION, ShiroKerberosAuthenticationFilter.NEGOTIATE + " asdf")
        .get();

    assertEquals(HttpServletResponse.SC_OK, clientResponse.getStatus());
  }
}
