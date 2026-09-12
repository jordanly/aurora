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
package org.apache.aurora.scheduler.execution;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.aurora.scheduler.resources.ResourceBag;

/** Small readable offer fixture implementing the transport-neutral contract. */
public final class TestOffer implements ExecutionOffer {
  private final String offerId;
  private final String agentId;
  private final String hostname;
  private final ResourceBag total;
  private final ResourceBag revocable;
  private final ResourceBag nonRevocable;
  private final List<Integer> ports;
  private final Optional<Instant> unavailable;
  private final boolean dedicated;

  private TestOffer(Builder builder) {
    offerId = Objects.requireNonNull(builder.configuredOfferId);
    agentId = Objects.requireNonNull(builder.configuredAgentId);
    hostname = Objects.requireNonNull(builder.configuredHostname);
    total = Objects.requireNonNull(builder.configuredTotal);
    revocable = Objects.requireNonNull(builder.configuredRevocable);
    nonRevocable = Objects.requireNonNull(builder.configuredNonRevocable);
    ports = List.copyOf(builder.configuredPorts);
    unavailable = Objects.requireNonNull(builder.unavailable);
    dedicated = builder.configuredDedicated;
  }

  public static Builder builder(String offerId) {
    return new Builder(offerId);
  }

  public static Builder copyOf(ExecutionOffer offer) {
    Builder copy = builder(offer.getOfferId())
        .agentId(offer.getAgentId()).hostname(offer.getHostname())
        .total(offer.getTotalResources())
        .revocable(offer instanceof TestOffer test ? test.revocable : offer.getResources(true))
        .nonRevocable(offer.getResources(false)).ports(offer.getAvailablePorts())
        .dedicated(offer.isDedicated());
    offer.getUnavailabilityStart().ifPresent(copy::unavailableAt);
    return copy;
  }

  public static final class Builder {
    private String configuredOfferId;
    private String configuredAgentId;
    private String configuredHostname = "hostname";
    private ResourceBag configuredTotal = ResourceBag.EMPTY;
    private ResourceBag configuredRevocable = ResourceBag.EMPTY;
    private ResourceBag configuredNonRevocable = ResourceBag.EMPTY;
    private List<Integer> configuredPorts = List.of();
    private Optional<Instant> unavailable = Optional.empty();
    private boolean configuredDedicated;

    private Builder(String offerId) {
      configuredOfferId = Objects.requireNonNull(offerId);
      configuredAgentId = "agent-" + offerId;
    }

    public Builder offerId(String value) {
      configuredOfferId = value;
      return this;
    }

    public Builder agentId(String value) {
      configuredAgentId = value;
      return this;
    }

    public Builder hostname(String value) {
      configuredHostname = value;
      return this;
    }

    public Builder total(ResourceBag value) {
      configuredTotal = value;
      return this;
    }

    public Builder revocable(ResourceBag value) {
      configuredRevocable = value;
      return this;
    }

    public Builder nonRevocable(ResourceBag value) {
      configuredNonRevocable = value;
      return this;
    }

    public Builder ports(List<Integer> value) {
      configuredPorts = value;
      return this;
    }

    public Builder dedicated(boolean value) {
      configuredDedicated = value;
      return this;
    }

    /** Sets a normal (non-revocable) offer with no special resource split. */
    public Builder resources(ResourceBag value) {
      configuredTotal = value;
      configuredNonRevocable = value;
      return this;
    }

    public Builder ports(int... values) {
      configuredPorts = Arrays.stream(values).boxed().toList();
      return this;
    }

    public Builder unavailableAt(Instant value) {
      unavailable = Optional.of(value);
      return this;
    }

    public TestOffer build() {
      return new TestOffer(this);
    }
  }

  @Override
  public String getOfferId() {
    return offerId;
  }

  @Override
  public String getAgentId() {
    return agentId;
  }

  @Override
  public String getHostname() {
    return hostname;
  }

  @Override
  public ResourceBag getTotalResources() {
    return total;
  }

  @Override
  public List<Integer> getAvailablePorts() {
    return ports;
  }

  @Override
  public Optional<Instant> getUnavailabilityStart() {
    return unavailable;
  }

  @Override
  public boolean isDedicated() {
    return dedicated;
  }

  @Override
  public ResourceBag getResources(boolean isRevocable) {
    // Resource policy flags may be initialized after static test fixtures are built.
    return isRevocable
        ? revocable.filter(entry -> entry.getKey().isRevocable())
            .add(total.filter(entry -> !entry.getKey().isRevocable()))
        : nonRevocable;
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof TestOffer other)) {
      return false;
    }
    return offerId.equals(other.offerId) && agentId.equals(other.agentId)
        && hostname.equals(other.hostname) && total.equals(other.total)
        && revocable.equals(other.revocable) && nonRevocable.equals(other.nonRevocable)
        && ports.equals(other.ports) && unavailable.equals(other.unavailable)
        && dedicated == other.dedicated;
  }

  @Override
  public int hashCode() {
    return Objects.hash(offerId, agentId, hostname, total, revocable, nonRevocable,
        ports, unavailable, dedicated);
  }
}
