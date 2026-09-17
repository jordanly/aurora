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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response.Status;

import com.google.common.io.Resources;

import org.junit.Test;

import static org.apache.aurora.gen.apiConstants.BYPASS_LEADER_REDIRECT_HEADER_NAME;
import static org.junit.Assert.assertEquals;

public class ServletFilterTest extends AbstractJettyTest {

  protected jakarta.ws.rs.core.Response get(String path) {
    return getRequestBuilder(path)
        .header(HttpHeaders.ACCEPT_ENCODING, "gzip")
        .get();
  }

  protected jakarta.ws.rs.core.Response post(String path, String body) {
    return getRequestBuilder(path)
        .header(HttpHeaders.ACCEPT_ENCODING, "gzip")
        .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN_TYPE.toString())
        .post(Entity.text(body));
  }

  private void assertContentEncoding(
      jakarta.ws.rs.core.Response response, Optional<String> encoding) {
    assertEquals(
        encoding.orElse(null),
        response.getHeaders().getFirst(HttpHeaders.CONTENT_ENCODING));
  }

  private void assertGzipEncodedGet(String path) {
    assertContentEncoding(get(path), Optional.of("gzip"));
  }

  @Test
  public void testGzipEncoding() throws Exception {
    replayAndStart();

    assertGzipEncodedGet("/");
    assertGzipEncodedGet("/scheduler");
    assertGzipEncodedGet("/scheduler/");
    assertGzipEncodedGet("/scheduler/role");
    assertGzipEncodedGet("/scheduler/role/");
    assertGzipEncodedGet("/scheduler/role/env/");
    assertGzipEncodedGet("/scheduler/role/env/job");
    assertGzipEncodedGet("/scheduler/role/env/job/");

    assertGzipEncodedGet("/updates");
    assertGzipEncodedGet("/updates/");
  }

  private void assertResponseStatus(
      String path,
      Status expectedStatus,
      Optional<URL> responseResource) throws IOException {

    jakarta.ws.rs.core.Response response = get(path);
    assertEquals(expectedStatus.getStatusCode(), response.getStatus());

    if (responseResource.isPresent()) {
      assertEquals(
          Resources.toString(responseResource.get(), StandardCharsets.UTF_8),
          response.readEntity(String.class));
    }
  }

  private void leaderRedirectSmokeTest(Status expectedStatus, Optional<URL> responseResource)
      throws IOException {

    assertResponseStatus("/scheduler", expectedStatus, responseResource);
    assertResponseStatus("/scheduler/", expectedStatus, responseResource);
    assertResponseStatus("/scheduler/role", expectedStatus, responseResource);
    assertResponseStatus("/scheduler/role/env", expectedStatus, responseResource);
    assertResponseStatus("/scheduler/role/env/job", expectedStatus, responseResource);

    assertResponseStatus("/updates", expectedStatus, responseResource);
    assertResponseStatus("/updates/", expectedStatus, responseResource);
  }

  @Test
  public void testLeaderRedirect() throws Exception {
    replayAndStart();

    assertResponseStatus("/", Status.OK, Optional.empty());

    // If there's no leader, we should send service unavailable and an error page.
    unsetLeadingSchduler();
    leaderRedirectSmokeTest(
        Status.SERVICE_UNAVAILABLE,
        Optional.of(
            Resources.getResource(
                LeaderRedirectFilter.class,
                LeaderRedirectFilter.NO_LEADER_PAGE)));

    // This process is leading
    setLeadingScheduler(httpServer.getHost(), httpServer.getPort());
    leaderRedirectSmokeTest(Status.OK, Optional.empty());

    setLeadingScheduler("otherHost", 1234);
    leaderRedirectSmokeTest(Status.TEMPORARY_REDIRECT, Optional.empty());
    assertResponseStatus("/", Status.OK, Optional.empty());
  }

  @Test
  public void testHeaderOverridesLeaderRedirect() throws Exception {
    replayAndStart();

    unsetLeadingSchduler();

    jakarta.ws.rs.core.Response response = getRequestBuilder("/scheduler")
        .header(BYPASS_LEADER_REDIRECT_HEADER_NAME, "true")
        .get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }
}
