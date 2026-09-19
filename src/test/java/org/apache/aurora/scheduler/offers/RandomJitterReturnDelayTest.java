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
package org.apache.aurora.scheduler.offers;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.Random;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RandomJitterReturnDelayTest extends EasyMockTest {
  private void assertRandomJitterReturnDelay(
      int minHoldTimeMs,
      int jitterWindowMs,
      boolean shouldThrow) {

    int randomValue = 123;

    Random mockRandom = control.createMock(Random.class);

    if (!shouldThrow) {
      expect(mockRandom.nextLong(jitterWindowMs)).andReturn((long) randomValue);
    }

    control.replay();

    assertEquals(
        minHoldTimeMs + randomValue,
        new RandomJitterReturnDelay(
            minHoldTimeMs,
            jitterWindowMs,
            mockRandom).get().getValue().intValue());
  }

  @Test
  public void testRandomJitterReturnDelay() throws Exception {
    assertRandomJitterReturnDelay(100, 200, false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeHoldTimeThrowsIllegalArgumentException() throws Exception {
    assertRandomJitterReturnDelay(-1, 200, true);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeWindowThrowsIllegalArgumentException() throws Exception {
    assertRandomJitterReturnDelay(100, -1, true);
  }

  @Test
  public void testZeroHoldTime() throws Exception {
    assertRandomJitterReturnDelay(0, 200, false);
  }

  @Test
  public void testZeroWindow() throws Exception {
    control.replay();
    assertEquals(100L, new RandomJitterReturnDelay(100, 0,
        Random.Util.newDefaultRandom()).get().getValue().longValue());
  }

  @Test
  public void testZeroHoldTimeZeroWindow() throws Exception {
    control.replay();
    assertEquals(0L, new RandomJitterReturnDelay(0, 0,
        Random.Util.newDefaultRandom()).get().getValue().longValue());
  }

  @Test
  public void testLargeWindowRange() {
    control.replay();
    long window = (long) Integer.MAX_VALUE + 1000;
    RandomJitterReturnDelay delay = new RandomJitterReturnDelay(100, window,
        new Random.SystemRandom(new java.util.Random(0)));
    for (int i = 0; i < 100; i++) {
      long value = delay.get().getValue();
      assertTrue(value >= 100);
      assertTrue(value < 100 + window);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOverflowingDelayRejected() {
    control.replay();
    new RandomJitterReturnDelay(Long.MAX_VALUE, 2, Random.Util.newDefaultRandom());
  }

  @Test
  public void testMaximumRepresentableDelay() {
    control.replay();
    assertEquals(Long.MAX_VALUE, new RandomJitterReturnDelay(Long.MAX_VALUE, 1,
        Random.Util.newDefaultRandom()).get().getValue().longValue());
  }

}
