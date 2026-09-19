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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.metadata.NearestFit;
import org.apache.aurora.scheduler.scheduling.TaskGroup;
import org.apache.aurora.scheduler.scheduling.TaskGroups;

/**
 * Servlet that exposes detailed information about tasks that are pending.
 */
@Path("/pendingtasks")
public class PendingTasks {

  private final TaskGroups taskGroups;
  private final NearestFit nearestFit;

  @Inject
  PendingTasks(TaskGroups taskGroups, NearestFit nearestFit) {
    this.taskGroups = Objects.requireNonNull(taskGroups);
    this.nearestFit = Objects.requireNonNull(nearestFit);
  }

  /**
   * Returns information about pending tasks.
   *
   * @return HTTP response.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getOffers() throws IOException {
    Iterable<TaskGroup> groups = taskGroups.getGroups();
    Map<TaskGroupKey, List<String>> taskGroupReasonMap = nearestFit.getPendingReasons(groups);

    ObjectMapper mapper = new ObjectMapper();
    ArrayNode jsonNode = mapper.createArrayNode();

    // Add the attribute "reason" to each serialized taskgroup
    for (TaskGroup group : groups) {
      ObjectNode pendingTask = (ObjectNode) mapper.valueToTree(group);

      pendingTask.put("reason", taskGroupReasonMap.get(group.getKey()).toString());
      jsonNode.add(pendingTask);
    }
    return Response.ok(jsonNode).build();
  }

}
