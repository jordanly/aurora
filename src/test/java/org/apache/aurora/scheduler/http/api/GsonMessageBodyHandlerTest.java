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
package org.apache.aurora.scheduler.http.api;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;

import javax.ws.rs.core.MediaType;

import com.google.gson.JsonParseException;

import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.TaskConstraint;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GsonMessageBodyHandlerTest {
  @Test
  public void testReadClosesInput() throws IOException {
    TrackingInputStream input = new TrackingInputStream("{\"numCpus\":1}", null);
    assertEquals(Resource.numCpus(1), read(input));
    assertTrue(input.closed);
  }

  @Test
  public void testParseFailureSuppressesCloseFailure() throws IOException {
    IOException closeFailure = new IOException("close failed");
    TrackingInputStream input = new TrackingInputStream("{", closeFailure);
    try {
      read(input);
      fail("Expected invalid JSON");
    } catch (JsonParseException e) {
      assertTrue(input.closed);
      assertEquals(1, e.getSuppressed().length);
      assertSame(closeFailure, e.getSuppressed()[0]);
    }
  }

  @Test
  public void testSuccessfulReadPropagatesCloseFailure() {
    IOException closeFailure = new IOException("close failed");
    TrackingInputStream input = new TrackingInputStream("{\"numCpus\":1}", closeFailure);
    try {
      read(input);
      fail("Expected close failure");
    } catch (IOException e) {
      assertTrue(input.closed);
      assertSame(closeFailure, e);
    }
  }

  private static Object read(TrackingInputStream input) throws IOException {
    return new GsonMessageBodyHandler().readFrom(Object.class, Resource.class,
        new Annotation[0], MediaType.APPLICATION_JSON_TYPE, null, input);
  }

  private static class TrackingInputStream extends ByteArrayInputStream {
    private final IOException closeFailure;
    private boolean closed;

    TrackingInputStream(String json, IOException closeFailure) {
      super(json.getBytes(StandardCharsets.UTF_8));
      this.closeFailure = closeFailure;
    }

    @Override
    public void close() throws IOException {
      closed = true;
      if (closeFailure != null) {
        throw closeFailure;
      }
    }
  }

  @Test
  public void testUnionRoundTrips() {
    assertRoundTrip(Resource.numCpus(1.5), Resource.class);
    assertRoundTrip(Resource.ramMb(9007199254740993L), Resource.class);
    assertRoundTrip(Resource.namedPort("http"), Resource.class);
    assertRoundTrip(TaskConstraint.limit(new LimitConstraint(2)), TaskConstraint.class);
  }

  private static <T> void assertRoundTrip(T value, Class<T> type) {
    assertEquals(value, GsonMessageBodyHandler.GSON.fromJson(
        GsonMessageBodyHandler.GSON.toJson(value), type));
  }

  @Test(expected = JsonParseException.class)
  public void testEmptyUnionRejected() {
    GsonMessageBodyHandler.GSON.fromJson("{}", Resource.class);
  }

  @Test(expected = JsonParseException.class)
  public void testMultipleFieldsRejected() {
    GsonMessageBodyHandler.GSON.fromJson("{\"numCpus\":1,\"ramMb\":2}", Resource.class);
  }

  @Test
  public void testInaccessibleConstructor() {
    assertConstructorFailure(InaccessibleResource.class, IllegalAccessException.class);
  }

  @Test
  public void testMissingConstructor() {
    assertConstructorFailure(MissingConstructorResource.class, InstantiationException.class);
  }

  private static void assertConstructorFailure(Class<?> type, Class<?> causeType) {
    try {
      GsonMessageBodyHandler.GSON.fromJson("{\"numCpus\":1}", type);
      fail("Expected constructor failure");
    } catch (RuntimeException e) {
      assertTrue(causeType.isInstance(e.getCause()));
    }
  }

  @Test
  public void testConstructorFailurePropagatesDirectly() {
    try {
      GsonMessageBodyHandler.GSON.fromJson("{\"numCpus\":1}", ThrowingResource.class);
      fail("Expected constructor failure");
    } catch (Exception e) {
      assertSame(ThrowingResource.FAILURE, e);
    }
  }

  public static class InaccessibleResource extends Resource {
    private InaccessibleResource() { }
  }

  public static class MissingConstructorResource extends Resource {
    public MissingConstructorResource(String ignored) { }
  }

  public static class ThrowingResource extends Resource {
    static final UnsupportedOperationException FAILURE =
        new UnsupportedOperationException("constructor failed");

    public ThrowingResource() {
      throw FAILURE;
    }
  }
}
