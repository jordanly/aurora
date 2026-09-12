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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class WireJsonTest {
  @Test
  public void rejectsDuplicateAndTrailingJson() {
    assertThrowsIOException(() -> WireJson.parse(
        "{\"a\":1,\"a\":2}".getBytes(StandardCharsets.US_ASCII)));
    assertThrowsIllegalArgument(() -> WireJson.parse(
        "{\"a\":1} false".getBytes(StandardCharsets.US_ASCII)));
  }

  @Test
  public void rejectsNonAsciiCanonicalWireValues() throws Exception {
    JsonNode value = WireJson.parse("{\"name\":\"café\"}".getBytes(StandardCharsets.UTF_8));
    assertThrowsIllegalArgument(() -> WireJson.bytes(value));
  }

  @Test
  public void canonicalEncodingAndHashMatchFixture() throws Exception {
    JsonNode value = WireJson.parse(
        "{\"z\":[3,2],\"a\":{\"z\":2,\"y\":1}}"
            .getBytes(StandardCharsets.US_ASCII));
    byte[] expected = "{\"a\":{\"y\":1,\"z\":2},\"z\":[3,2]}"
        .getBytes(StandardCharsets.US_ASCII);

    assertArrayEquals(expected, WireJson.bytes(value));
    assertEquals(
        "010a912e081fc2bd94303700e32464fe5b27a8101b78271808a1d39f459601e3",
        WireJson.hash(expected));
  }

  private static void assertThrowsIOException(ThrowingAction action) {
    try {
      action.run();
      fail("Expected IOException");
    } catch (IOException expected) {
      // Expected.
    } catch (Exception e) {
      throw new AssertionError("Unexpected exception", e);
    }
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
