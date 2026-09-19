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

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.junit.After;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertThat;

/**
 * @author William Farner
 */
public class StatsTest {

  @After
  public void tearDown() {
    Stats.flush();
  }

  @Test
  public void testSimpleExport() {
    AtomicLong var = Stats.exportLong("test_long");
    assertCounter("test_long", 0);
    var.incrementAndGet();
    assertCounter("test_long", 1);
    var.addAndGet(100);
    assertCounter("test_long", 101);
  }

  @Test
  public void testSupportedSpecialChars() {
    AtomicLong hyphen = Stats.exportLong("c-d");
    AtomicLong slash = Stats.exportLong("d/f");
    AtomicLong dot = Stats.exportLong("f.g");

    hyphen.incrementAndGet();
    slash.incrementAndGet();
    dot.incrementAndGet();

    assertCounter("c-d", 1);
    assertCounter("d/f", 1);
    assertCounter("f.g", 1);
  }

  @Test
  public void testDuplicateCounterSharesRegisteredValue() {
    AtomicLong firstExport = Stats.exportLong("somevar");
    firstExport.incrementAndGet();
    firstExport.incrementAndGet();
    assertCounter("somevar", 2L);
    AtomicLong secondExport = Stats.exportLong("somevar");
    assertSame(firstExport, secondExport);
    secondExport.incrementAndGet();
    assertCounter("somevar", 3L);
  }

  @Test
  public void testNormalizesSpace() {
    AtomicLong leading = Stats.exportLong("  leading space");
    AtomicLong trailing = Stats.exportLong("trailing space   ");
    AtomicLong surround = Stats.exportLong("   surround space   ");

    leading.incrementAndGet();
    trailing.incrementAndGet();
    surround.incrementAndGet();
    assertCounter("__leading_space", 1);
    assertCounter("trailing_space___", 1);
    assertCounter("___surround_space___", 1);
  }

  @Test
  public void testNormalizesIllegalChars() {
    AtomicLong colon = Stats.exportLong("a:b");
    AtomicLong plus = Stats.exportLong("b+c");

    colon.incrementAndGet();
    plus.incrementAndGet();
    assertCounter("a_b", 1);
    assertCounter("b_c", 1);
  }

  @Test
  public void testOwnedRegistrationCleanupAndReplacement() {
    var previous = ImmutableList.copyOf(Stats.getNumericVariables());
    StatsProvider.Registration first = Stats.STATS_PROVIDER.registerGauge("owned", () -> 1L);
    assertCounter("owned", 1L);
    assertEquals(previous.size() + 1, Iterables.size(Stats.getNumericVariables()));
    assertThrows(IllegalArgumentException.class,
        () -> Stats.STATS_PROVIDER.registerGauge("owned", () -> 2L));
    assertThrows(IllegalArgumentException.class, () -> Stats.exportLong("owned"));
    first.close();
    assertNull(Stats.getVariable("owned"));
    assertEquals(previous, ImmutableList.copyOf(Stats.getNumericVariables()));
    StatsProvider.Registration second = Stats.STATS_PROVIDER.registerGauge("owned", () -> 3L);
    first.close();
    assertCounter("owned", 3L);
    assertEquals(previous.size() + 1, Iterables.size(Stats.getNumericVariables()));
    second.close();
    assertNull(Stats.getVariable("owned"));
    assertEquals(previous, ImmutableList.copyOf(Stats.getNumericVariables()));
  }

  @Test
  public void testUntrackedOwnershipAndCounterCollision() {
    var previous = ImmutableList.copyOf(Stats.getNumericVariables());
    StatsProvider provider = Stats.STATS_PROVIDER.untracked();
    StatsProvider.Registration registration = provider.registerGauge("instant", () -> 4L);
    assertCounter("instant", 4L);
    assertEquals(previous, ImmutableList.copyOf(Stats.getNumericVariables()));
    registration.close();
    assertNull(Stats.getVariable("instant"));
    AtomicLong counter = provider.makeCounter("instant");
    assertSame(counter, provider.makeCounter("instant"));
    assertThrows(IllegalArgumentException.class, () -> Stats.exportLong("instant"));
  }

  private void assertCounter(String name, long value) {
    assertThat(Stats.<Long>getVariable(name).read(), is(value));
  }

  private void assertCounter(String name, double value) {
    Double var = (Double) Stats.getVariable(name).read();
    assertEquals(var, value, 1e-6);
  }
}

