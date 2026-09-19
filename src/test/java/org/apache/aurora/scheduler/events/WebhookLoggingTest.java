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

import java.util.Map;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.aurora.scheduler.testing.LogCapture;
import org.asynchttpclient.AsyncHttpClient;
import org.junit.Test;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class WebhookLoggingTest {
  @Test
  public void testMalformedConfigurationDoesNotExposeInput() {
    String secret = "synthetic-invalid-webhook-secret";
    try (LogCapture logs = new LogCapture(WebhookModule.class)) {
      IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
          () -> WebhookModule.parseWebhookConfig("{\"targetURL\":\"" + secret));
      assertFalse(error.toString().contains(secret));
      assertNull(error.getCause());
      assertTrue(logs.messages().contains("Error parsing Webhook configuration"));
      assertFalse(logs.messages().contains(secret));
    }
  }

  @Test
  public void testConfigurationAndRequestFailuresDoNotLogSecrets() throws Exception {
    String secret = "synthetic-webhook-secret";
    String target = "https://user:" + secret + "@example.com/" + secret
        + "?token=" + secret + "#" + secret;
    WebhookInfo info = new WebhookInfo(Map.of("Authorization", secret), target, 1000, null);
    AsyncHttpClient client = createMock(AsyncHttpClient.class);
    expect(client.preparePost(target)).andThrow(new IllegalArgumentException(target));
    client.close();
    replay(client);
    try (LogCapture logs = new LogCapture(Webhook.class)) {
      Webhook webhook = new Webhook(() -> client, info, new FakeStatsProvider());
      webhook.startAsync().awaitRunning();
      try {
        webhook.taskChangedState(TaskStateChange.transition(
            TaskTestUtil.makeTask("id", TaskTestUtil.JOB), ScheduleStatus.FAILED));
        assertTrue(logs.messages().contains("headerNames=[Authorization]"));
        assertTrue(logs.messages().contains("targetHost=example.com"));
        assertTrue(logs.messages().contains("IllegalArgumentException"));
        assertFalse(logs.messages().contains(secret));
        assertFalse(info.toString().contains(secret));
      } finally {
        webhook.stopAsync().awaitTerminated();
      }
    }
    verify(client);
  }
}
