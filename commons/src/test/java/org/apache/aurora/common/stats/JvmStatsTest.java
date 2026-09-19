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

package org.apache.aurora.common.stats;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JvmStatsTest {
  @Test
  public void testExportsOnlySafeDiagnostics() {
    String property = "aurora.test.synthetic.credential";
    String previous = System.getProperty(property);
    System.setProperty(property, "synthetic-diagnostic-secret");
    try {
      JvmStats.export();
      Map<String, Object> exported = new HashMap<>();
      for (Stat<?> stat : Stats.getVariables()) {
        exported.put(stat.getName(), stat.read());
      }
      assertFalse(exported.containsKey("jvm_input_arguments"));
      assertFalse(exported.containsKey("jvm_prop_" + Stats.normalizeName(property)));
      assertFalse(exported.keySet().stream().anyMatch(name -> name.startsWith("system_env_")));
      assertFalse(exported.values().contains("synthetic-diagnostic-secret"));
      assertEquals(System.getProperty("java.version"), exported.get("jvm_prop_java.version"));
      assertTrue(exported.get("jvm_memory_free_mb") instanceof Number);
      assertTrue(exported.get("jvm_threads_active") instanceof Number);
    } finally {
      if (previous == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previous);
      }
      Stats.flush();
    }
  }
}
