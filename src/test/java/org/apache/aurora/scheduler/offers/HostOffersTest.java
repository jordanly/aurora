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

import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.testing.FakeTicker;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.MaintenanceMode.SCHEDULED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HostOffersTest {
  @Test
  public void testEqualResourcesKeepDistinctOffersAndIndexes() {
    for (List<OfferOrder> order : List.of(
        List.<OfferOrder>of(), List.of(OfferOrder.CPU), List.of(OfferOrder.MEMORY))) {
      for (boolean removeFirst : List.of(false, true)) {
        checkEqualResources(order, removeFirst);
      }
    }
  }

  private void checkEqualResources(List<OfferOrder> order, boolean removeFirst) {
    OfferSet offerSet = new OfferSetImpl(OfferOrderBuilder.create(order));
    HostOffers offers = new HostOffers(new FakeStatsProvider(), new OfferSettings(
        Amount.of(0L, Time.SECONDS), offerSet, Amount.of(1L, Time.HOURS),
        100L, new FakeTicker()), (resource, request) -> ImmutableSet.of());
    HostOffer first = hostOffer("offer-a", "host-a");
    HostOffer second = hostOffer("offer-b", "host-b");
    assertEquals(Optional.empty(), offers.addAndPreventAgentCollision(second));
    assertEquals(Optional.empty(), offers.addAndPreventAgentCollision(first));
    assertEquals(2, offerSet.size());
    assertEquals(ImmutableList.of(first, second), ImmutableList.copyOf(offerSet.values()));
    assertEquals(Optional.of(first), offers.get(first.getAgentId()));
    assertEquals(Optional.of(second), offers.get(second.getAgentId()));

    // Reindexing maintenance attributes must neither lose nor replace an equal-resource peer.
    IHostAttributes scheduled = IHostAttributes.build(
        first.getAttributes().newBuilder().setMode(SCHEDULED));
    offers.updateHostAttributes(scheduled);
    assertEquals(2, offerSet.size());
    assertEquals(second, ImmutableList.copyOf(offerSet.values()).get(0));
    offers.updateHostAttributes(first.getAttributes());
    assertEquals(ImmutableList.of(first, second), ImmutableList.copyOf(offerSet.values()));

    HostOffer removed = removeFirst ? first : second;
    HostOffer retained = removeFirst ? second : first;
    assertTrue(offers.remove(removed.getOfferId()));
    assertEquals(1, offerSet.size());
    assertEquals(ImmutableList.of(retained), ImmutableList.copyOf(offers.getOffers()));
    assertEquals(Optional.empty(), offers.get(removed.getAgentId()));
    assertEquals(Optional.of(retained), offers.get(retained.getAgentId()));
    assertTrue(offers.remove(retained.getOfferId()));
    assertEquals(0, offerSet.size());
  }

  private HostOffer hostOffer(String id, String host) {
    return new HostOffer(Offers.makeOffer(id, host), IHostAttributes.build(
        new HostAttributes().setHost(host).setMode(NONE)));
  }
}
