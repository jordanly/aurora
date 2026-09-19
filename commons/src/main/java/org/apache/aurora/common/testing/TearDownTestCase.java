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
package org.apache.aurora.common.testing;

import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Lists;

import org.junit.After;

/**
 * A base class for unit tests to easily add teardown actions.
 */
public class TearDownTestCase {
  private final LinkedList<TearDown> actions = new LinkedList<>();

  public final void addTearDown(TearDown tearDown) {
    actions.addFirst(tearDown);
  }

  @After
  public final void tearDown() {
    List<Throwable> failures = Lists.newArrayList();
    try {
      for (TearDown action : actions) {
        try {
          action.tearDown();
        } catch (Exception | AssertionError failure) {
          failures.add(failure);
        }
      }
    } finally {
      actions.clear();
    }
    if (!failures.isEmpty()) {
      AssertionError failure = new AssertionError("Tear down did not complete cleanly",
          failures.getFirst());
      failures.stream().skip(1).forEach(failure::addSuppressed);
      throw failure;
    }
  }

  public interface TearDown {
    void tearDown() throws Exception;
  }
}
