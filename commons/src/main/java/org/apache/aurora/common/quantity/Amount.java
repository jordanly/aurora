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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.apache.aurora.common.collections.Pair;

/**
 * Represents a value in a unit system and facilitates unambiguous communication of amounts.
 * Instances are created via static factory {@code of(...)} methods.
 * Equality and ordering compare exact decimal magnitudes in a common base unit. Equality also
 * requires the same numeric type and unit family. Floating-point NaN and signed zero follow the
 * corresponding boxed number's equality semantics.
 *
 * @param <T> the type of number the amount value is expressed in
 * @param <U> the type of unit that this amount quantifies
 *
 * @author John Sirois
 */
public abstract class Amount<T extends Number & Comparable<T>, U extends Unit<U>>
    implements Comparable<Amount<T, U>> {

  /**
   * Thrown when a checked operation on an amount would overflow.
   */

  public static class TypeOverflowException extends RuntimeException {
    public TypeOverflowException() {
      super();
    }
  }

  private final Pair<T, U> amount;
  private final T maxValue;

  protected Amount(T value, U unit, T maxValue) {
    Preconditions.checkNotNull(value);
    Preconditions.checkNotNull(unit);
    this.maxValue = maxValue;
    this.amount = Pair.of(value, unit);
  }

  public T getValue() {
    return amount.getFirst();
  }

  public U getUnit() {
    return amount.getSecond();
  }

  public T as(U unit) {
    return asUnit(unit, false);
  }

  /**
   * Throws TypeOverflowException if the converted value exceeds the numeric type's range.
   * Integral conversions truncate toward zero before checking the range. Existing non-finite
   * floating-point inputs are preserved; finite inputs that scale to infinity are overflow.
   */
  public T asChecked(U unit) {
    return asUnit(unit, true);
  }

  private T asUnit(U unit, boolean checked) {
    if (getUnit().equals(unit)) {
      return getValue();
    }
    if (getValue() instanceof Long || getValue() instanceof Integer) {
      BigDecimal converted = magnitude().divide(
          BigDecimal.valueOf(unit.multiplier()), 0, RoundingMode.DOWN);
      BigDecimal maximum = new BigDecimal(maxValue.toString());
      BigDecimal minimum = maximum.negate().subtract(BigDecimal.ONE);
      if (checked && (converted.compareTo(maximum) > 0 || converted.compareTo(minimum) < 0)) {
        throw new TypeOverflowException();
      }
      // Preserve the saturation of the original narrowing primitive casts for unchecked calls.
      return integralValue(converted.max(minimum).min(maximum));
    }
    T converted = scale(getUnit().multiplier() / unit.multiplier());
    if (checked && Double.isFinite(getValue().doubleValue())
        && !Double.isFinite(converted.doubleValue())) {
      throw new TypeOverflowException();
    }
    return converted;
  }

  @SuppressWarnings("unchecked")
  private T integralValue(BigDecimal value) {
    if (getValue() instanceof Long) {
      return (T) Long.valueOf(value.longValueExact());
    }
    return (T) Integer.valueOf(value.intValueExact());
  }

  // Canonical finite magnitude in the unit family's base unit. Decimal numeric spellings are
  // compared exactly, with no intermediate primitive conversion, rounding or saturation.
  private BigDecimal magnitude() {
    return new BigDecimal(getValue().toString())
        .multiply(BigDecimal.valueOf(getUnit().multiplier()));
  }

  private Object canonicalValue() {
    double value = getValue().doubleValue();
    if (!Double.isFinite(value) || Double.doubleToLongBits(value) == Long.MIN_VALUE) {
      // Keep Number's NaN/infinity and negative-zero value semantics across units.
      return getValue();
    }
    return magnitude().stripTrailingZeros();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getValue().getClass(), getUnit().getClass(), canonicalValue());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    return obj instanceof Amount<?, ?> other
        && getValue().getClass().equals(other.getValue().getClass())
        && getUnit().getClass().equals(other.getUnit().getClass())
        && canonicalValue().equals(other.canonicalValue());
  }

  @Override
  public String toString() {
    return amount.toString();
  }

  @Override
  public int compareTo(Amount<T, U> other) {
    double value = getValue().doubleValue();
    double otherValue = other.getValue().doubleValue();
    if (!Double.isFinite(value) || !Double.isFinite(otherValue)
        || (value == 0.0 && otherValue == 0.0)) {
      return Double.compare(value, otherValue);
    }
    return magnitude().compareTo(other.magnitude());
  }

  protected abstract T scale(double multiplier);

  /**
   * Creates an amount that uses a {@code double} value.
   *
   * @param number the number of units the returned amount should quantify
   * @param unit the unit the returned amount is expressed in terms of
   * @param <U> the type of unit that the returned amount quantifies
   * @return an amount quantifying the given {@code number} of {@code unit}s
   */
  public static <U extends Unit<U>> Amount<Double, U> of(double number, U unit) {
    return new Amount<Double, U>(number, unit, Double.MAX_VALUE) {
      @Override protected Double scale(double multiplier) {
        return getValue() * multiplier;
      }
    };
  }

  /**
   * Creates an amount that uses a {@code float} value.
   *
   * @param number the number of units the returned amount should quantify
   * @param unit the unit the returned amount is expressed in terms of
   * @param <U> the type of unit that the returned amount quantifies
   * @return an amount quantifying the given {@code number} of {@code unit}s
   */
  public static <U extends Unit<U>> Amount<Float, U> of(float number, U unit) {
    return new Amount<Float, U>(number, unit, Float.MAX_VALUE) {
      @Override protected Float scale(double multiplier) {
        return (float) (getValue() * multiplier);
      }
    };
  }

  /**
   * Creates an amount that uses a {@code long} value.
   *
   * @param number the number of units the returned amount should quantify
   * @param unit the unit the returned amount is expressed in terms of
   * @param <U> the type of unit that the returned amount quantifies
   * @return an amount quantifying the given {@code number} of {@code unit}s
   */
  public static <U extends Unit<U>> Amount<Long, U> of(long number, U unit) {
    return new Amount<Long, U>(number, unit, Long.MAX_VALUE) {
      @Override protected Long scale(double multiplier) {
        return (long) (getValue() * multiplier);
      }
    };
  }

  /**
   * Creates an amount that uses an {@code int} value.
   *
   * @param number the number of units the returned amount should quantify
   * @param unit the unit the returned amount is expressed in terms of
   * @param <U> the type of unit that the returned amount quantifies
   * @return an amount quantifying the given {@code number} of {@code unit}s
   */
  public static <U extends Unit<U>> Amount<Integer, U> of(int number, U unit) {
    return new Amount<Integer, U>(number, unit, Integer.MAX_VALUE) {
      @Override protected Integer scale(double multiplier) {
        return (int) (getValue() * multiplier);
      }
    };
  }
}
