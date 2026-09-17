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

import java.io.IOException;
import java.util.Optional;

/** Bounded retained process output, addressed exclusively by scheduler task identity. */
public interface TaskLogReader {
  int MAX_PAGE_BYTES = 65536;
  long MAX_OFFSET = 16L << 20;

  record Page(String taskId, String stream, long offset, long nextOffset, boolean hasMore,
              boolean truncated, boolean complete, String data) { }

  Optional<Page> readLog(String taskId, String stream, long offset, int limit)
      throws IOException, InterruptedException;
}
