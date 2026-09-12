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
package org.apache.aurora.scheduler;

import java.util.UUID;

import com.google.inject.Guice;

import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.TaskIdGenerator.TaskIdGeneratorImpl;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TaskIdGeneratorTest {
  @Test
  public void testIdentityFormatAndUniqueSuffixWithoutClockBinding() {
    TaskIdGenerator generator = Guice.createInjector().getInstance(TaskIdGeneratorImpl.class);
    ITaskConfig task = ITaskConfig.build(
        new TaskConfig().setJob(new JobKey("r/ole", "e nv", "j.ob")));
    String first = generator.generate(task, 7);
    String second = generator.generate(task, 7);
    String prefix = "r-ole-e-nv-j-ob-7-";

    assertTrue(first.startsWith(prefix));
    assertTrue(second.startsWith(prefix));
    UUID.fromString(first.substring(prefix.length()));
    UUID.fromString(second.substring(prefix.length()));
    assertNotEquals(first, second);
  }
}
