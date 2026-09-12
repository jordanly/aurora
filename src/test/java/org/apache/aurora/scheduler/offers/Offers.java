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

import org.apache.aurora.scheduler.execution.TestOffer;

import static org.apache.aurora.scheduler.resources.ResourceTestUtil.bag;

/** Resource offers for the original scheduling policy tests. */
public final class Offers {
  public static final String DEFAULT_HOST = "hostname";

  private Offers() { }

  public static TestOffer makeOffer(String offerId) {
    return makeOffer(offerId, DEFAULT_HOST);
  }

  public static TestOffer makeOffer(String offerId, String hostname) {
    return TestOffer.builder(offerId).agentId("slave_id-" + offerId).hostname(hostname)
        .resources(bag(10, 1024, 0)).build();
  }
}
