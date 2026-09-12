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
package org.apache.aurora.scheduler.execution.go;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GoAgentConfigTest {
  @Test
  public void readsEnrollmentAndRejectsUnknownOrRepeatedFields() throws Exception {
    String valid = "{\"cluster\":\"c\",\"incarnation\":\"i\","
        + "\"database\":\"db.sqlite\",\"keyStore\":\"key.p12\","
        + "\"keyStorePassword\":\"secret\",\"trustStore\":\"trust.p12\","
        + "\"trustStorePassword\":\"secret\",\"nodes\":[{"
        + "\"name\":\"agent-1\",\"url\":\"https://agent-1/\","
        + "\"journal\":\"j\",\"boot\":\"b\",\"runtime\":\"r\","
        + "\"cpuMillis\":1000,\"memoryBytes\":1024,\"diskMb\":1}]}";
    Path path = Files.createTempFile("go-agent", ".json");
    Files.writeString(path, valid);

    GoAgentConfig config = GoAgentConfig.read(path, "c");

    assertEquals("c", config.cluster());
    assertEquals("agent-1", config.nodes().get(0).name());
    assertEquals("j", config.nodes().get(0).target().get("journal").asText());

    Files.writeString(path, valid.replace("\"nodes\":", "\"extra\":1,\"nodes\":"));
    assertThrowsIllegalArgument(() -> GoAgentConfig.read(path, "c"));
  }

  @Test
  public void rejectsWrongClusterDuplicateAgentAndNonHttpsUrl() throws Exception {
    String base = "{\"cluster\":\"c\",\"incarnation\":\"i\","
        + "\"database\":\"d\",\"keyStore\":\"k\",\"keyStorePassword\":\"p\","
        + "\"trustStore\":\"t\",\"trustStorePassword\":\"p\",\"nodes\":[%s]}";
    String node = "{\"name\":\"a\",\"url\":\"https://a\",\"journal\":\"j\","
        + "\"boot\":\"b\",\"runtime\":\"r\",\"cpuMillis\":1,"
        + "\"memoryBytes\":1,\"diskMb\":1}";
    Path path = Files.createTempFile("go-agent", ".json");
    Files.writeString(path, base.formatted(node + "," + node));
    assertThrowsIllegalArgument(() -> GoAgentConfig.read(path, "c"));
    Files.writeString(path, base.formatted(node.replace("https://a", "http://a")));
    assertThrowsIllegalArgument(() -> GoAgentConfig.read(path, "c"));
    Files.writeString(path, base.formatted(node));
    assertThrowsIllegalArgument(() -> GoAgentConfig.read(path, "wrong"));
  }

  private static void assertThrowsIllegalArgument(ThrowingAction action) {
    try {
      action.run();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected.
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }

  @FunctionalInterface
  private interface ThrowingAction {
    void run() throws Exception;
  }
}
