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
package org.apache.aurora.scheduler.mesos;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.Range;

import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.execution.ExecutionOffer;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.mesos.v1.Protos.Offer;

import static java.util.Objects.requireNonNull;

import static com.google.common.collect.DiscreteDomain.integers;

/** Retains the complete native offer while exposing only scheduling resource views. */
public final class MesosOffer implements ExecutionOffer {
  private final Offer offer;

  public MesosOffer(Offer offer) {
    this.offer = requireNonNull(offer);
  }

  /** Native access is confined to adapters and native diagnostic endpoints. */
  public static Offer toMesos(ExecutionOffer offer) {
    return ((MesosOffer) offer).offer;
  }

  @Override
  public String getOfferId() {
    return offer.getId().getValue();
  }

  @Override
  public String getAgentId() {
    return offer.getAgentId().getValue();
  }

  @Override
  public String getHostname() {
    return offer.getHostname();
  }

  @Override
  public ResourceBag getTotalResources() {
    return MesosResourceManager.bagFromMesosResources(offer.getResourcesList());
  }

  @Override
  public ResourceBag getResources(boolean revocable) {
    return MesosResourceManager.bagFromMesosResources(
        MesosResourceManager.getOfferResources(offer, revocable));
  }

  @Override
  public List<Integer> getAvailablePorts() {
    return StreamSupport.stream(
        MesosResourceManager.getOfferResources(offer, ResourceType.PORTS).spliterator(), false)
        .flatMap(resource -> resource.getRanges().getRangeList().stream())
        .flatMap(range -> ContiguousSet.create(
            Range.closed((int) range.getBegin(), (int) range.getEnd()), integers()).stream())
        .toList();
  }

  @Override
  public Optional<Instant> getUnavailabilityStart() {
    return offer.hasUnavailability()
        ? Optional.of(Conversions.getStart(offer.getUnavailability())) : Optional.empty();
  }

  @Override
  public boolean isDedicated() {
    return Conversions.isDedicated(offer);
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof MesosOffer mesosOffer && offer.equals(mesosOffer.offer);
  }

  @Override
  public int hashCode() {
    return offer.hashCode();
  }

  @Override
  public String toString() {
    return offer.toString();
  }
}
