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

import java.util.Objects;
import java.util.stream.StreamSupport;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.OfferManager;

/** Exposes the resource offers retained by the original scheduler, independently of transport. */
@Path("/offers")
public class Offers {
  private final OfferManager offerManager;
  private final ObjectMapper mapper = new ObjectMapper();

  @Inject
  Offers(OfferManager offerManager) {
    this.offerManager = Objects.requireNonNull(offerManager);
  }

  /** Resource units follow their explicit Aurora type names: CPU cores, MB and counts. */
  private ObjectNode view(HostOffer hostOffer) {
    var offer = hostOffer.getOffer();
    ObjectNode result = mapper.createObjectNode()
        .put("offerId", offer.getOfferId()).put("agentId", offer.getAgentId())
        .put("hostname", offer.getHostname()).put("dedicated", offer.isDedicated())
        .put("maintenanceMode", hostOffer.getAttributes().getMode().name())
        .put("unavailableAt", offer.getUnavailabilityStart().map(Object::toString).orElse(null));
    result.set("resources", mapper.valueToTree(offer.getResources(false).getResourceVectors()));
    result.set("revocableResources", mapper.valueToTree(
        offer.getResources(true).getResourceVectors()));
    result.set("ports", mapper.valueToTree(offer.getAvailablePorts()));
    return result;
  }

  /** Returns retained offers with current maintenance mode and resource availability. */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getOffers() throws JsonProcessingException {
    var views = StreamSupport.stream(offerManager.getAll().spliterator(), false)
        .map(this::view).toList();
    return Response.ok(mapper.writeValueAsString(views)).build();
  }
}
