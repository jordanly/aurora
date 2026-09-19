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
package org.apache.aurora.common.quantity;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;

/**
 * @author John Sirois
 */
public class AmountTest {

  @Test
  public void testEquals() {
    assertEquals("expected value equality semantics",
        Amount.of(1L, Time.DAYS), Amount.of(1L, Time.DAYS));

    assertEquals("expected equality to be calculated from amounts converted to a common unit",
        Amount.of(1L, Time.DAYS), Amount.of(24L, Time.HOURS));

    assertNotEquals("expected unit conversions for equality tests to not lose precision",
        Amount.of(25L, Time.HOURS), Amount.of(1L, Time.DAYS));
    assertNotEquals("expected unit conversions for equality tests to not lose precision",
        Amount.of(1L, Time.DAYS), Amount.of(25L, Time.HOURS));

    assertFalse("expected value equality to work only for the same Number types",
        Amount.of(1L, Time.DAYS).equals(Amount.of(1.0, Time.DAYS)));
    assertFalse("expected value equality to work only for the same Number types",
        Amount.of(1L, Time.DAYS).equals(Amount.of(1, Time.DAYS)));

    assertFalse("amounts with incompatible units should never be equal even if their values are",
        Amount.of(1L, Time.NANOSECONDS).equals(Amount.of(1L, Data.BITS)));
  }

  @Test
  public void testComparisonMixedUnits() {
    assertTrue(Amount.of(1, Time.MINUTES).compareTo(Amount.of(59, Time.SECONDS)) > 0);
    assertTrue(Amount.of(1, Time.MINUTES).compareTo(Amount.of(60, Time.SECONDS)) == 0);
    assertTrue(Amount.of(1, Time.MINUTES).compareTo(Amount.of(61, Time.SECONDS)) < 0);

    assertTrue(Amount.of(59, Time.SECONDS).compareTo(Amount.of(1, Time.MINUTES)) < 0);
    assertTrue(Amount.of(60, Time.SECONDS).compareTo(Amount.of(1, Time.MINUTES)) == 0);
    assertTrue(Amount.of(61, Time.SECONDS).compareTo(Amount.of(1, Time.MINUTES)) > 0);
  }

  @Test
  @SuppressWarnings("unchecked") // Needed because type information lost in vargs.
  public void testOrderingMixedUnits() {
    assertEquals(
        Lists.newArrayList(
            Amount.of(1, Data.BITS),
            Amount.of(1, Data.KB),
            Amount.of(1, Data.MB),
            Amount.of(1, Data.MB)),
        Ordering.natural().sortedCopy(Lists.newArrayList(
            Amount.of(1, Data.KB),
            Amount.of(1024, Data.KB),
            Amount.of(1, Data.BITS),
            Amount.of(1, Data.MB))));
  }

  @Test
  @SuppressWarnings("unchecked") // Needed because type information lost in vargs.
  public void testOrderingSameUnits() {
    assertEquals(
        Lists.newArrayList(
            Amount.of(1, Time.MILLISECONDS),
            Amount.of(2, Time.MILLISECONDS),
            Amount.of(3, Time.MILLISECONDS),
            Amount.of(4, Time.MILLISECONDS)),
        Ordering.natural().sortedCopy(Lists.newArrayList(
            Amount.of(3, Time.MILLISECONDS),
            Amount.of(2, Time.MILLISECONDS),
            Amount.of(1, Time.MILLISECONDS),
            Amount.of(4, Time.MILLISECONDS))));
  }

  @Test
  public void testConvert() {
    Amount<Long, Time> integralDuration = Amount.of(15L, Time.MINUTES);
    assertEquals(Long.valueOf(15 * 60 * 1000), integralDuration.as(Time.MILLISECONDS));

    assertEquals("expected conversion losing precision to use truncation",
        Long.valueOf(0), integralDuration.as(Time.HOURS));

    assertEquals("expected conversion losing precision to use truncation",
        Long.valueOf(0), Amount.of(45L, Time.MINUTES).as(Time.HOURS));

    Amount<Double, Time> decimalDuration = Amount.of(15.0, Time.MINUTES);
    assertEquals(Double.valueOf(15 * 60 * 1000), decimalDuration.as(Time.MILLISECONDS));
    assertEquals(Double.valueOf(0.25), decimalDuration.as(Time.HOURS));
  }

  @Test(expected = Amount.TypeOverflowException.class)
  public void testAmountThrowsTypeOverflowException() {
    Amount.of(1000, Time.DAYS).asChecked(Time.MILLISECONDS);
  }
  @Test
  public void testCrossUnitHashCollectionsAndNumericTypes() {
    Set<Amount<Long, Time>> amounts = new HashSet<>();
    amounts.add(Amount.of(1L, Time.DAYS));
    amounts.add(Amount.of(24L, Time.HOURS));
    assertEquals(1, amounts.size());
    assertEquals(Amount.of(1L, Time.DAYS).hashCode(), Amount.of(24L, Time.HOURS).hashCode());
    assertEquals(Amount.of(-1, Data.KB), Amount.of(-1024, Data.BYTES));
    assertEquals(Amount.of(-1, Data.KB).hashCode(), Amount.of(-1024, Data.BYTES).hashCode());
    assertNotEquals(Amount.of(1.0f, Time.SECONDS), Amount.of(1.0, Time.SECONDS));
  }

  @Test
  public void testLargeIntegersRemainExact() {
    long beyondDoublePrecision = 9_007_199_254_740_993L;
    assertEquals(Long.valueOf(beyondDoublePrecision),
        Amount.of(beyondDoublePrecision * 1000, Time.NANOSECONDS).as(Time.MICROSECONDS));
    assertEquals(Amount.of(beyondDoublePrecision, Time.MICROSECONDS),
        Amount.of(beyondDoublePrecision * 1000, Time.NANOSECONDS));
    assertNotEquals(Amount.of(beyondDoublePrecision, Time.NANOSECONDS),
        Amount.of(beyondDoublePrecision - 1, Time.NANOSECONDS));
    assertTrue(Amount.of(Long.MAX_VALUE, Time.DAYS)
        .compareTo(Amount.of(Long.MAX_VALUE, Time.HOURS)) > 0);
    assertNotEquals(Amount.of(Long.MAX_VALUE, Time.DAYS), Amount.of(Long.MAX_VALUE, Time.HOURS));
  }

  @Test
  public void testIntegralBoundsAndTruncation() {
    assertEquals(Long.valueOf(Long.MAX_VALUE),
        Amount.of(Long.MAX_VALUE, Time.NANOSECONDS).asChecked(Time.NANOSECONDS));
    assertEquals(Long.valueOf(Long.MIN_VALUE),
        Amount.of(Long.MIN_VALUE, Time.NANOSECONDS).asChecked(Time.NANOSECONDS));
    assertEquals(Integer.valueOf(Integer.MAX_VALUE),
        Amount.of(Integer.MAX_VALUE, Data.BITS).asChecked(Data.BITS));
    assertEquals(Integer.valueOf(Integer.MIN_VALUE),
        Amount.of(Integer.MIN_VALUE, Data.BITS).asChecked(Data.BITS));
    assertEquals(Long.valueOf(-1), Amount.of(-1999L, Time.NANOSECONDS).as(Time.MICROSECONDS));
    assertEquals(Integer.valueOf(-1), Amount.of(-1999, Time.NANOSECONDS).as(Time.MICROSECONDS));
    assertEquals(Long.valueOf(Long.MAX_VALUE),
        Amount.of(Long.MAX_VALUE, Time.MICROSECONDS).as(Time.NANOSECONDS));
    assertEquals(Long.valueOf(Long.MIN_VALUE),
        Amount.of(Long.MIN_VALUE, Time.MICROSECONDS).as(Time.NANOSECONDS));
    assertThrows(Amount.TypeOverflowException.class,
        () -> Amount.of(Long.MAX_VALUE, Time.MICROSECONDS).asChecked(Time.NANOSECONDS));
    assertThrows(Amount.TypeOverflowException.class,
        () -> Amount.of(Long.MIN_VALUE, Time.MICROSECONDS).asChecked(Time.NANOSECONDS));
    assertThrows(Amount.TypeOverflowException.class,
        () -> Amount.of(Integer.MIN_VALUE, Data.BYTES).asChecked(Data.BITS));
  }

  @Test
  public void testFloatingPointCanonicalValuesAndSpecialValues() {
    assertEquals(Amount.of(0.1, Time.SECONDS), Amount.of(100.0, Time.MILLISECONDS));
    assertEquals(Amount.of(0.1f, Time.SECONDS), Amount.of(100.0f, Time.MILLISECONDS));
    assertEquals(Amount.of(0.1f, Time.SECONDS).hashCode(),
        Amount.of(100.0f, Time.MILLISECONDS).hashCode());
    assertEquals(Amount.of(Double.NaN, Time.SECONDS), Amount.of(Double.NaN, Time.DAYS));
    assertEquals(Amount.of(Double.NaN, Time.SECONDS).hashCode(),
        Amount.of(Double.NaN, Time.DAYS).hashCode());
    assertEquals(Amount.of(Float.NaN, Time.SECONDS), Amount.of(Float.NaN, Time.DAYS));
    assertEquals(Amount.of(Float.NEGATIVE_INFINITY, Time.SECONDS),
        Amount.of(Float.NEGATIVE_INFINITY, Time.DAYS));
    assertNotEquals(Amount.of(-0.0f, Time.SECONDS), Amount.of(0.0f, Time.DAYS));
    assertEquals(Amount.of(Double.POSITIVE_INFINITY, Time.SECONDS),
        Amount.of(Double.POSITIVE_INFINITY, Time.DAYS));
    assertEquals(Amount.of(-0.0, Time.SECONDS), Amount.of(-0.0, Time.DAYS));
    assertNotEquals(Amount.of(-0.0, Time.SECONDS), Amount.of(0.0, Time.DAYS));
    assertTrue(Amount.of(-0.0, Time.SECONDS).compareTo(Amount.of(0.0, Time.DAYS)) < 0);
    assertTrue(Amount.of(Double.MAX_VALUE, Time.DAYS)
        .compareTo(Amount.of(Double.POSITIVE_INFINITY, Time.SECONDS)) < 0);
    assertEquals(Double.valueOf(Double.MAX_VALUE),
        Amount.of(Double.MAX_VALUE, Time.SECONDS).asChecked(Time.SECONDS));
    assertEquals(Double.valueOf(Double.POSITIVE_INFINITY),
        Amount.of(Double.POSITIVE_INFINITY, Time.SECONDS).asChecked(Time.NANOSECONDS));
    assertThrows(Amount.TypeOverflowException.class,
        () -> Amount.of(Double.MAX_VALUE, Time.SECONDS).asChecked(Time.NANOSECONDS));
    assertThrows(Amount.TypeOverflowException.class,
        () -> Amount.of(-Float.MAX_VALUE, Time.SECONDS).asChecked(Time.NANOSECONDS));
  }

}
