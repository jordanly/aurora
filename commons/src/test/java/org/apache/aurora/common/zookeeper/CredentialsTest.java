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
package org.apache.aurora.common.zookeeper;

import java.util.HashSet;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class CredentialsTest {
  @Test
  public void testValueEqualityAndDefensiveOwnership() {
    byte[] source = {1, 2, 3};
    Credentials credentials = new Credentials("digest", source);
    Credentials equal = new Credentials("digest", source.clone());
    assertEquals(credentials, equal);
    assertEquals(credentials.hashCode(), equal.hashCode());
    assertEquals(1, new HashSet<>(List.of(credentials, equal)).size());
    source[0] = 99;
    credentials.authToken()[1] = 99;
    assertArrayEquals(new byte[] {1, 2, 3}, credentials.authToken());
    assertEquals(credentials, equal);
    assertNotEquals(credentials, new Credentials("other", new byte[] {1, 2, 3}));
    assertNotEquals(credentials, new Credentials("digest", new byte[] {1, 2, 4}));
  }
}
