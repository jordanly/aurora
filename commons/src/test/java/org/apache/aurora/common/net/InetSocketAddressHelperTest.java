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
package org.apache.aurora.common.net;

import java.net.InetSocketAddress;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertThrows;

/**
 * @author John Sirois
 */
public class InetSocketAddressHelperTest {

  @Test
  public void testParseValueInvalid() {
    try {
      InetSocketAddressHelper.parse(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }

    try {
      InetSocketAddressHelper.parse("");
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      InetSocketAddressHelper.parse(":");
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      InetSocketAddressHelper.parse("*:");
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      InetSocketAddressHelper.parse(":jake");
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      InetSocketAddressHelper.parse(":70000");
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      InetSocketAddressHelper.parse("localhost:");
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testParseArgValuePort() {
    assertEquals(new InetSocketAddress(6666), InetSocketAddressHelper.parse(":6666"));
    assertEquals(new InetSocketAddress(0), InetSocketAddressHelper.parse(":*"));
  }

  @Test
  public void testParseArgValueHostPort() {
    assertEquals(InetSocketAddress.createUnresolved("localhost", 5555),
        InetSocketAddressHelper.parse("localhost:5555"));

    assertEquals(InetSocketAddress.createUnresolved("127.0.0.1", 4444),
        InetSocketAddressHelper.parse("127.0.0.1:4444"));
  }

  @Test
  public void testInetSocketAddressToServerString() {
    assertEquals("localhost:8000",
        InetSocketAddressHelper.toString(InetSocketAddress.createUnresolved("localhost", 8000)));

    assertEquals("foo.bar.baz:8000",
        InetSocketAddressHelper.toString(InetSocketAddress.createUnresolved("foo.bar.baz", 8000)));

    assertEquals("127.0.0.1:8000",
        InetSocketAddressHelper.toString(InetSocketAddress.createUnresolved("127.0.0.1", 8000)));

    assertEquals("10.0.0.1:8000",
        InetSocketAddressHelper.toString(InetSocketAddress.createUnresolved("10.0.0.1", 8000)));

    assertEquals("0.0.0.0:80", InetSocketAddressHelper.toString(new InetSocketAddress(80)));
  }
  @Test
  public void testIpv6AndUnresolvedRoundTrips() {
    for (String address : new String[] {"[::1]:8081", "[fe80::1%eth0]:12", "missing.invalid:5"}) {
      InetSocketAddress socket = InetSocketAddressHelper.parse(address);
      assertEquals(true, socket.isUnresolved());
      assertEquals(address, InetSocketAddressHelper.toString(socket));
    }
    assertEquals("[::1]:0", InetSocketAddressHelper.toString(
        InetSocketAddressHelper.parse("[::1]:*")));
    for (String invalid : new String[] {"::1:80", "[::1]", "[::1]:", "[::1", "[::1]:-1"}) {
      assertThrows(IllegalArgumentException.class, () -> InetSocketAddressHelper.parse(invalid));
    }
  }

}
