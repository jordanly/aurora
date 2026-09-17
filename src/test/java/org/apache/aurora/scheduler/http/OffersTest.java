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
package org.apache.aurora.scheduler.http;

import java.time.Instant;
import java.util.List;

import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.config.CommandLine;
import org.apache.aurora.scheduler.execution.TestOffer;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.resources.ResourceTestUtil.bag;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OffersTest extends EasyMockTest {
  private Offers offers;
  private OfferManager offerManager;

  @Before
  public void setUp() {
    CommandLine.initializeForTest();
    offerManager = createMock(OfferManager.class);
    offers = new Offers(offerManager);
  }

  @Test
  public void testNoOffers() throws Exception {
    expect(offerManager.getAll()).andReturn(List.of());
    control.replay();
    Response response = offers.getOffers();
    assertEquals(200, response.getStatus());
    assertEquals("[]", response.getEntity());
  }

  @Test
  public void testNeutralOfferWithResourcesAndMaintenance() throws Exception {
    var offer = TestOffer.builder("offer-1").agentId("agent-a").hostname("host-a")
        .resources(bag(1.0, 128, 256)).revocable(bag(0.5, 0, 0))
        .ports(31000, 31001).dedicated(true).unavailableAt(Instant.EPOCH).build();
    expect(offerManager.getAll()).andReturn(List.of(new HostOffer(offer,
        IHostAttributes.build(new HostAttributes().setHost("host-a")
            .setMode(MaintenanceMode.DRAINING)))));
    control.replay();
    Response response = offers.getOffers();
    assertEquals(200, response.getStatus());
    JsonNode result = new ObjectMapper().readTree(response.getEntity().toString());
    assertTrue(result.isArray());
    assertEquals(1, result.size());
    JsonNode actual = result.get(0);
    assertEquals("offer-1", actual.path("offerId").asText());
    assertEquals("agent-a", actual.path("agentId").asText());
    assertEquals("host-a", actual.path("hostname").asText());
    assertEquals(1.0, actual.path("resources").path("CPUS").asDouble(), 0.0);
    assertEquals(128, actual.path("resources").path("RAM_MB").asInt());
    assertEquals(256, actual.path("resources").path("DISK_MB").asInt());
    assertEquals(0.5, actual.path("revocableResources").path("CPUS").asDouble(), 0.0);
    assertEquals("[31000,31001]", actual.path("ports").toString());
    assertEquals(Instant.EPOCH.toString(), actual.path("unavailableAt").asText());
    assertTrue(actual.path("dedicated").asBoolean());
    assertEquals("DRAINING", actual.path("maintenanceMode").asText());
  }
}
