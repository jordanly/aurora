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
package org.apache.aurora.scheduler.storage.durability;

import org.apache.aurora.gen.Resource;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class GeneratorTest {
  @Test
  public void testRuntimeConstructorFailurePropagatesDirectly() {
    try {
      Generator.newStruct(ThrowingResource.class);
      fail("Expected constructor failure");
    } catch (UnsupportedOperationException e) {
      assertSame(ThrowingResource.FAILURE, e);
    }
  }

  @Test
  public void testReflectiveConstructorFailureKeepsOriginalWrapper() {
    try {
      Generator.newStruct(ReflectiveFailureResource.class);
      fail("Expected constructor failure");
    } catch (RuntimeException e) {
      assertSame(ReflectiveFailureResource.FAILURE, e.getCause());
    }
  }

  public static class ThrowingResource extends Resource {
    static final UnsupportedOperationException FAILURE =
        new UnsupportedOperationException("constructor failed");

    public ThrowingResource() {
      throw FAILURE;
    }
  }

  public static class ReflectiveFailureResource extends Resource {
    static final ReflectiveOperationException FAILURE =
        new ReflectiveOperationException("constructor failed");

    public ReflectiveFailureResource() throws ReflectiveOperationException {
      throw FAILURE;
    }
  }
}
