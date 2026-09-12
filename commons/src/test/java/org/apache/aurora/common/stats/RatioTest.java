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
package org.apache.aurora.common.stats;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RatioTest {
  @Test
  public void testFiniteValues() {
    assertEquals(1.5, sample(6, 4), 0);
    assertEquals(-1.5, sample(-6, 4), 0);
  }

  @Test
  public void testZeroDenominator() {
    assertEquals(0, sample(6, 0), 0);
    assertEquals(0, sample(6, -0.0), 0);
  }

  @Test
  public void testNaNSamplesUseZeroFallback() {
    assertEquals(0, sample(Double.NaN, 1), 0);
    assertEquals(0, sample(1, Double.NaN), 0);
    assertEquals(0, sample(Double.NaN, Double.NaN), 0);
  }

  @Test
  public void testInfinitySemanticsUnchanged() {
    assertEquals(Double.POSITIVE_INFINITY, sample(Double.POSITIVE_INFINITY, 1), 0);
    assertEquals(0, sample(1, Double.POSITIVE_INFINITY), 0);
    assertTrue(Double.isNaN(sample(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY)));
  }

  private static double sample(double numerator, double denominator) {
    return Ratio.of("ratio-test", numerator, denominator).doSample();
  }
}
