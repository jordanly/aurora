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

import java.io.IOException;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class HttpStatsFilterTest extends EasyMockTest {
  private HttpServletRequest request;
  private HttpServletResponse response;

  @Before
  public void setUp() {
    Stats.flush();
    request = createMock(HttpServletRequest.class);
    response = createMock(HttpServletResponse.class);
  }

  @After
  public void clearStats() {
    Stats.flush();
  }

  private void assertRecorded(int status, FilterChain action) throws Exception {
    expect(response.getStatus()).andReturn(status);
    control.replay();
    new HttpStatsFilter().doFilter(request, response, (req, resp) -> {
      assertSame(response, resp);
      action.doFilter(req, resp);
    });
    assertEquals(1L, Stats.getVariable("http_" + status + "_responses_events").read());
    if (status != 200) {
      assertNull(Stats.getVariable("http_200_responses_events"));
    }
  }

  @Test
  public void testDefaultSuccess() throws Exception {
    assertRecorded(200, (req, resp) -> { });
  }

  @Test
  public void testExplicitStatus() throws Exception {
    response.setStatus(201);
    assertRecorded(201, (req, resp) -> ((HttpServletResponse) resp).setStatus(201));
  }

  @Test
  public void testSendError() throws Exception {
    response.sendError(401);
    assertRecorded(401, (req, resp) -> ((HttpServletResponse) resp).sendError(401));
  }

  @Test
  public void testSendRedirect() throws Exception {
    response.sendRedirect("/elsewhere");
    assertRecorded(302, (req, resp) -> ((HttpServletResponse) resp).sendRedirect("/elsewhere"));
  }

  @Test
  public void testThrowingRequestIsNotRecorded() {
    control.replay();
    assertThrows(IOException.class, () -> new HttpStatsFilter().doFilter(request, response,
        (req, resp) -> {
          throw new IOException("failed");
        }));
    assertNull(Stats.getVariable("http_200_responses_events"));
  }
}
