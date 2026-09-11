/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.nativescheduler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class JsonTest {
  @Test public void exactLimitAllowsPartialBulkReadsAndKeepsInputOpen() throws Exception {
    byte[] expected = new byte[Json.LIMIT];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = (byte) i;
    }
    PartialInput input = new PartialInput(expected, 17, null, -1);

    assertArrayEquals(expected, Json.read(input));
    assertFalse(input.closed);
  }

  @Test public void limitPlusOneIsRejectedAndKeepsInputOpen() throws Exception {
    PartialInput input = new PartialInput(new byte[Json.LIMIT + 1], 31, null, -1);

    try {
      Json.read(input);
      fail("expected an oversized body to be rejected");
    } catch (IOException e) {
      assertEquals("Body too large", e.getMessage());
    }
    assertFalse(input.closed);
  }

  @Test public void readPropagatesUnderlyingExceptionAndKeepsInputOpen() throws Exception {
    IOException expected = new IOException("transport failed");
    PartialInput input = new PartialInput(new byte[256], 19, expected, 100);

    try {
      Json.read(input);
      fail("expected the source exception to propagate");
    } catch (IOException actual) {
      assertSame(expected, actual);
    }
    assertFalse(input.closed);
  }

  @Test public void shaUsesUtf8AndReturnsKnownDigest() throws Exception {
    assertEquals(
        "850f7dc43910ff890f8879c0ed26fe697c93a067ad93a7d50f466a7028a9bf4e",
        Json.sha("café"));
  }

  @Test public void shaPreservesLeadingZeroDigestBytes() throws Exception {
    assertEquals(
        "00db57de56c23d7616da1961591ce0878cd14fe1102db36dcd54f389e6356ed6",
        Json.sha("leading-zero-182"));
  }

  private static final class PartialInput extends InputStream {
    private final byte[] data;
    private final int chunkSize;
    private final IOException failure;
    private final int failureOffset;
    private int offset;
    private boolean closed;

    PartialInput(byte[] data, int chunkSize, IOException failure, int failureOffset) {
      this.data = Arrays.copyOf(data, data.length);
      this.chunkSize = chunkSize;
      this.failure = failure;
      this.failureOffset = failureOffset;
    }

    @Override public int read() throws IOException {
      if (offset == data.length) {
        return -1;
      }
      if (failure != null && offset >= failureOffset) {
        throw failure;
      }
      return data[offset++] & 0xff;
    }

    @Override public int read(byte[] buffer, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      if (offset == data.length) {
        return -1;
      }
      if (failure != null && offset >= failureOffset) {
        throw failure;
      }
      int count = Math.min(Math.min(len, chunkSize), data.length - offset);
      if (failure != null) {
        count = Math.min(count, failureOffset - offset);
      }
      System.arraycopy(data, offset, buffer, off, count);
      offset += count;
      return count;
    }

    @Override public void close() {
      closed = true;
    }
  }
}
