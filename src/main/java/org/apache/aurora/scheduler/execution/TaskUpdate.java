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

/** An update receipt whose decoding and acknowledgement retain their transaction ordering. */
public interface TaskUpdate {
  /** Decode inside the existing status batch transaction, not on the callback thread. */
  TaskObservation observe();

  /** Acknowledge only after the entire status batch commits successfully. */
  void acknowledge();
}
