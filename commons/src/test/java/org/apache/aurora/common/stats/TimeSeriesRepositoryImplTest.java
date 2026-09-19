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

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.common.util.testing.FakeBuildInfo.generateBuildInfo;
import static org.easymock.EasyMock.createStrictControl;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * @author William Farner
 */
public class TimeSeriesRepositoryImplTest extends EasyMockTest {

  private static final Amount<Long, Time> RETENTION_PERIOD = Amount.of(10L, Time.MINUTES);
  private static final Amount<Long, Time> SAMPLE_PERIOD = Amount.of(1L, Time.SECONDS);

  private StatRegistry statRegistry;
  private TimeSeriesRepositoryImpl repo;
  private FakeClock clock;

  @Before
  public void setUp() {
    control = createStrictControl();
    statRegistry = control.createMock(StatRegistry.class);
    repo = new TimeSeriesRepositoryImpl(
        statRegistry,
        SAMPLE_PERIOD,
        RETENTION_PERIOD,
        generateBuildInfo());
    clock = new FakeClock();
  }

  @After
  public void after() {
    control.verify();
  }

  @Test
  public void testSamplesInOrder() {
    RecordingStat<Integer> statA = mockedStat();
    RecordingStat<Integer> statB = mockedStat();
    RecordingStat<Integer> statC = mockedStat();
    RecordingStat<Integer> statD = mockedStat();

    expect(statRegistry.getStats())
        .andReturn(ImmutableList.<RecordingStat<? extends Number>>of(statB, statA, statC, statD));

    expect(statB.getName()).andReturn("statB");
    expect(statB.sample()).andReturn(1);

    expect(statA.getName()).andReturn("statA");
    expect(statA.sample()).andReturn(2);

    expect(statC.getName()).andReturn("statC");
    expect(statC.sample()).andReturn(3);

    expect(statD.getName()).andReturn("statD");
    expect(statD.sample()).andReturn(4);

    control.replay();
    repo.runSampler(clock);
  }

  @Test
  public void testDelayedExport() throws InterruptedException {
    RecordingStat<Integer> earlyExport = mockedStat();

    for (int i = 1; i <= 4; i++) {
      expect(statRegistry.getStats())
        .andReturn(ImmutableList.<RecordingStat<? extends Number>>of(earlyExport));
      expect(earlyExport.getName()).andReturn("early");
      expect(earlyExport.sample()).andReturn(i * 2);
    }

    RecordingStat<Integer> delayedExport = mockedStat();
    expect(statRegistry.getStats())
        .andReturn(ImmutableList.<RecordingStat<? extends Number>>of(earlyExport, delayedExport));
    expect(earlyExport.getName()).andReturn("early");
    expect(earlyExport.sample()).andReturn(10);
    expect(delayedExport.getName()).andReturn("delayed");
    expect(delayedExport.sample()).andReturn(100);

    control.replay();

    clock.setNowMillis(1000);

    for (int i = 0; i < 4; i++) {
      repo.runSampler(clock);
      clock.waitFor(1000);
    }

    expectTimestamps(1000L, 2000L, 3000L, 4000L);
    expectSeriesData("early", 2, 4, 6, 8);

    repo.runSampler(clock);

    expectTimestamps(1000L, 2000L, 3000L, 4000L, 5000L);
    expectSeriesData("early", 2, 4, 6, 8, 10);
    expectSeriesData("delayed", 0L, 0L, 0L, 0L, 100);
  }

  @Test
  public void testSubsecondRetentionAndDetachedSnapshots() throws Exception {
    repo = new TimeSeriesRepositoryImpl(statRegistry, Amount.of(500L, Time.MILLISECONDS),
        Amount.of(1L, Time.SECONDS), generateBuildInfo());
    RecordingStat<Integer> stat = mockedStat();
    for (int value = 1; value <= 3; value++) {
      expect(statRegistry.getStats()).andReturn(ImmutableList.of(stat));
      expect(stat.getName()).andReturn("value");
      expect(stat.sample()).andReturn(value);
    }
    control.replay();
    repo.runSampler(clock);
    clock.waitFor(500);
    repo.runSampler(clock);
    var snapshot = repo.snapshot(List.of("value", "unknown"));
    var timestamps = repo.getTimestamps();
    var samples = repo.get("value").getSamples();
    clock.waitFor(500);
    repo.runSampler(clock);
    assertEquals(List.of(0L, 500L), snapshot.timestamps());
    assertEquals(List.of(1, 2), snapshot.series().get("value"));
    assertEquals(List.of(0L, 500L), timestamps);
    assertEquals(List.of(1, 2), samples);
    assertEquals(List.of(500L, 1000L), repo.getTimestamps());
    assertEquals(List.of(2, 3), repo.get("value").getSamples());
    assertEquals(null, snapshot.series().get("unknown"));
  }

  @Test
  public void testRetiredAndReplacedRegistrationsReleaseHistory() {
    RecordingStat<Integer> first = mockedStat();
    RecordingStat<Integer> replacement = mockedStat();
    expect(statRegistry.getStats()).andReturn(ImmutableList.of(first));
    expect(first.getName()).andReturn("owned");
    expect(first.sample()).andReturn(7);
    expect(statRegistry.getStats()).andReturn(ImmutableList.of(replacement));
    expect(replacement.getName()).andReturn("owned");
    expect(replacement.sample()).andReturn(9);
    expect(statRegistry.getStats()).andReturn(ImmutableList.of());
    control.replay();

    repo.runSampler(clock);
    var previous = repo.snapshot(List.of("owned"));
    repo.runSampler(clock);
    assertEquals(List.of(0L, 9), ImmutableList.copyOf(repo.get("owned").getSamples()));
    assertEquals(List.of(7), previous.series().get("owned"));
    repo.runSampler(clock);
    assertEquals(java.util.Set.of(), repo.getAvailableSeries());
    assertEquals(null, repo.get("owned"));
  }

  @Test
  public void testRejectsUnrepresentableSampleCount() {
    control.replay();
    assertThrows(IllegalArgumentException.class, () -> new TimeSeriesRepositoryImpl(
        statRegistry, Amount.of(1L, Time.NANOSECONDS),
        Amount.of(Long.MAX_VALUE, Time.DAYS), generateBuildInfo()));
  }

  private RecordingStat<Integer> mockedStat() {
    return createMock(new Clazz<RecordingStat<Integer>>() { });
  }

  private void expectTimestamps(Number... timestamps) {
    assertEquals(ImmutableList.copyOf(timestamps), ImmutableList.copyOf(repo.getTimestamps()));
  }

  private void expectSeriesData(String series, Number... values) {
    assertEquals(ImmutableList.copyOf(values), ImmutableList.copyOf(repo.get(series).getSamples()));
  }
}
