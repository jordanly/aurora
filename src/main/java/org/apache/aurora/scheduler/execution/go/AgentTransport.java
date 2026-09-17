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

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;

/** Bounded exchanges with an authenticated, enrolled agent. */
interface AgentTransport extends AutoCloseable {
  JsonNode request(GoAgentConfig.Node node, String path, JsonNode body,
                   String epoch, String session) throws IOException, InterruptedException;
  default Watch watch(GoAgentConfig.Node node, long after, String epoch, String session)
      throws IOException, InterruptedException {
    throw new IOException("Watch transport unavailable");
  }

  interface Watch extends AutoCloseable {
    JsonNode next() throws IOException, InterruptedException;
    @Override void close() throws IOException;
  }

  final class ResponseException extends IOException {
    private final int httpStatus;

    ResponseException(int status) {
      super("Agent returned HTTP " + status);
      httpStatus = status;
    }

    int status() {
      return httpStatus;
    }
  }

  @Override void close();
}
