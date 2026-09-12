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
import java.util.List;
import java.util.Optional;

import org.apache.aurora.scheduler.resources.ResourceBag;

/** Resource and identity views needed by scheduling, independent of transport. */
public interface ExecutionOffer {
  String getOfferId();
  String getAgentId();
  String getHostname();
  ResourceBag getTotalResources();
  ResourceBag getResources(boolean revocable);
  /** Ordered port values, retaining overlapping ranges and resource entries. */
  List<Integer> getAvailablePorts();
  Optional<Instant> getUnavailabilityStart();
  boolean isDedicated();
}
