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
package org.apache.aurora.codec;

import java.util.HexFormat;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class ThriftBinaryCodecTest {

  @Test
  public void testRoundTrip() throws CodingException {
    ScheduledTask original = TaskTestUtil.makeTask("id", TaskTestUtil.JOB).newBuilder();
    assertEquals(original,
        ThriftBinaryCodec.decode(ScheduledTask.class, ThriftBinaryCodec.encode(original)));
  }

  @Test
  public void testRoundTripNull() throws CodingException {
    assertNull(ThriftBinaryCodec.decode(ScheduledTask.class, ThriftBinaryCodec.encode(null)));
  }

  @Test
  public void testRoundTripNonNull() throws CodingException {
    ScheduledTask original = TaskTestUtil.makeTask("id", TaskTestUtil.JOB).newBuilder();
    assertEquals(original,
        ThriftBinaryCodec.decodeNonNull(
            ScheduledTask.class,
            ThriftBinaryCodec.encodeNonNull(original)));
  }

  @Test(expected = NullPointerException.class)
  public void testEncodeNonNull() throws CodingException {
    ThriftBinaryCodec.encodeNonNull(null);
  }

  @Test(expected = NullPointerException.class)
  public void testDecodeNonNull() throws CodingException {
    ThriftBinaryCodec.decodeNonNull(ScheduledTask.class, null);
  }

  @Test
  public void testInflateDeflateRoundTrip() throws CodingException {
    ScheduledTask original = TaskTestUtil.makeTask("id", TaskTestUtil.JOB).newBuilder();

    byte[] deflated = ThriftBinaryCodec.deflateNonNull(original);

    ScheduledTask inflated = ThriftBinaryCodec.inflateNonNull(ScheduledTask.class, deflated);

    assertEquals(original, inflated);
  }

  @Test
  public void testHistoricalCompressedFixture() {
    // Captured from the original codec at 6cf7f0ea0 with the pinned Java 25 toolchain.
    byte[] compressed = HexFormat.of().parseHex(
        "785ee366606460606029cacf49e5666002329953f3cab8199841acacfc240600402c0468");
    JobKey key = new JobKey("role", "env", "job");

    assertArrayEquals(compressed, ThriftBinaryCodec.deflateNonNull(key));
    assertEquals(key, ThriftBinaryCodec.inflateNonNull(JobKey.class, compressed));
  }

  @Test
  public void testCompressionPreservesWriteFailure() {
    TException failure = new TException("failed after writing the fields");
    JobKey key = new JobKey("role", "env", "job") {
      @Override
      public void write(TProtocol protocol) throws TException {
        super.write(protocol);
        throw failure;
      }
    };

    try {
      ThriftBinaryCodec.deflateNonNull(key);
      fail("Expected failed serialization");
    } catch (CodingException e) {
      assertSame(failure, e.getCause());
    }
  }

  @Test(expected = CodingException.class)
  public void testMalformedCompressedInput() {
    ThriftBinaryCodec.inflateNonNull(JobKey.class, new byte[] {3, 4, 5});
  }
}
