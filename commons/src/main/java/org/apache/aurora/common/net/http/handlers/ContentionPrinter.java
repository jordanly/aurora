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
package org.apache.aurora.common.net.http.handlers;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * HTTP request handler that prints information about blocked threads.
 *
 * @author William Farner
 */
@Path("/contention")
public class ContentionPrinter {
  private final ThreadMXBean bean;

  public ContentionPrinter() {
    this(ManagementFactory.getThreadMXBean());
  }

  ContentionPrinter(ThreadMXBean bean) {
    this.bean = bean;
    if (bean.isThreadContentionMonitoringSupported()) {
      bean.setThreadContentionMonitoringEnabled(true);
    }
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getContention() {
    List<String> lines = Lists.newLinkedList();
    // ThreadInfo carries its own stack: avoid joining unrelated snapshots of live threads.
    Map<Long, ThreadInfo> threads = new HashMap<>();
    for (ThreadInfo thread : bean.dumpAllThreads(false, false)) {
      if (thread != null) {
        threads.put(thread.getThreadId(), thread);
      }
    }

    Set<Long> lockOwners = Sets.newHashSet();

    lines.add("Locked threads:");
    for (ThreadInfo t : threads.values()) {
      switch (t.getThreadState()) {
        case BLOCKED:
        case WAITING:
        case TIMED_WAITING:
          lines.addAll(getThreadInfo(t));
          if (t.getLockOwnerId() != -1) lockOwners.add(t.getLockOwnerId());
          break;
      }
    }

    if (lockOwners.size() > 0) {
      lines.add("\nLock Owners");
      for (Long owner : lockOwners) {
        ThreadInfo thread = threads.get(owner);
        if (thread != null) {
          lines.addAll(getThreadInfo(thread));
        }
      }
    }

    return String.join("\n", lines);
  }

  private static List<String> getThreadInfo(ThreadInfo t) {
    List<String> lines = Lists.newLinkedList();

    lines.add(String.format("'%s' Id=%d %s",
        t.getThreadName(), t.getThreadId(), t.getThreadState()));
    lines.add("Waiting for lock: " + t.getLockName());
    lines.add("Lock is currently held by thread: " + t.getLockOwnerName());
    lines.add("Wait time: " + t.getBlockedTime() + " ms.");
    for (StackTraceElement s : t.getStackTrace()) {
      lines.add("    " + s);
    }
    lines.add("\n");

    return lines;
  }
}
