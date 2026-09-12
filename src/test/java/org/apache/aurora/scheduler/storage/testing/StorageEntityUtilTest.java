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
package org.apache.aurora.scheduler.storage.testing;

import java.util.Arrays;
import java.util.Collections;

import org.apache.aurora.gen.Resource;
import org.junit.Test;

import static org.junit.Assert.assertSame;

public class StorageEntityUtilTest {
  @Test
  public void scalarValuesAreLeaves() {
    Long value = 1L;
    assertSame(value, StorageEntityUtil.assertFullyPopulated(value));
    assertSame(Character.valueOf('x'), StorageEntityUtil.assertFullyPopulated('x'));
  }

  @Test
  public void populatedBoxedValuesInContainersAreLeaves() {
    StorageEntityUtil.assertFullyPopulated(Arrays.asList(1L));
    StorageEntityUtil.assertFullyPopulated(Collections.singletonMap("key", true));
    StorageEntityUtil.assertFullyPopulated(Resource.ramMb(1L));
  }

  @Test(expected = AssertionError.class)
  public void defaultBoxedValuesInContainersAreRejected() {
    StorageEntityUtil.assertFullyPopulated(Arrays.asList(0L));
  }

  @Test(expected = AssertionError.class)
  public void defaultBooleanInMapIsRejected() {
    StorageEntityUtil.assertFullyPopulated(Collections.singletonMap("key", false));
  }

  @Test(expected = AssertionError.class)
  public void defaultCharacterIsRejected() {
    StorageEntityUtil.assertFullyPopulated('\0');
  }

  @Test(expected = AssertionError.class)
  public void defaultUnionValueIsRejected() {
    StorageEntityUtil.assertFullyPopulated(Resource.ramMb(0L));
  }

  private static class RequiredFields {
    // Read reflectively by the existing serialization/field-inspection contract.
    @SuppressWarnings("PMD.UnusedPrivateField")
    private final String name;
    // Read reflectively by the existing serialization/field-inspection contract.
    @SuppressWarnings("PMD.UnusedPrivateField")
    private final long count;

    RequiredFields(String name, long count) {
      this.name = name;
      this.count = count;
    }
  }

  @Test(expected = AssertionError.class)
  public void nestedNullFieldIsRejected() {
    StorageEntityUtil.assertFullyPopulated(Arrays.asList(new RequiredFields(null, 1L)));
  }

  @Test(expected = AssertionError.class)
  public void nestedDefaultFieldIsRejected() {
    StorageEntityUtil.assertFullyPopulated(Arrays.asList(new RequiredFields("set", 0L)));
  }

  @Test
  public void ignoredFieldRemainsOptional() {
    StorageEntityUtil.assertFullyPopulated(new RequiredFields(null, 1L),
        StorageEntityUtil.getField(RequiredFields.class, "name"));
  }
}
