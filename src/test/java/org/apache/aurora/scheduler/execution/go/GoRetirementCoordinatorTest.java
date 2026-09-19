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
package org.apache.aurora.scheduler.execution.go;

import java.util.List;

import org.apache.aurora.scheduler.execution.go.GoRetirementCoordinator.TicketRange;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GoRetirementCoordinatorTest {
  @Test
  public void mergesOverlapAndAdjacencyWithoutBridgingGaps() {
    List<TicketRange> input = List.of(new TicketRange(7, 9), new TicketRange(2, 4),
        new TicketRange(1, 2), new TicketRange(4, 5), new TicketRange(12, 12));
    assertEquals(List.of(new TicketRange(1, 5), new TicketRange(7, 9), new TicketRange(12, 12)),
        GoRetirementCoordinator.merge(input));
    assertEquals(new TicketRange(7, 9), input.getFirst());
  }

  @Test
  public void boundsAndMaximumTicketDoNotOverflow() {
    assertThrows(IllegalArgumentException.class, () -> new TicketRange(0, 1));
    assertThrows(IllegalArgumentException.class, () -> new TicketRange(2, 1));
    TicketRange maximum = new TicketRange(Long.MAX_VALUE - 1, Long.MAX_VALUE);
    assertEquals(List.of(maximum), GoRetirementCoordinator.merge(
        List.of(maximum, new TicketRange(Long.MAX_VALUE, Long.MAX_VALUE))));
    assertTrue(maximum.contains(Long.MAX_VALUE));
    assertTrue(maximum.contains(new TicketRange(Long.MAX_VALUE, Long.MAX_VALUE)));
    assertFalse(maximum.contains(Long.MAX_VALUE - 2));
    assertEquals(List.of(), GoRetirementCoordinator.merge(List.of()));
  }
}
