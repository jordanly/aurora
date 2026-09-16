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
package org.apache.aurora.scheduler.cron.quartz;

import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.sqlite.SqliteStorage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CronSqliteValidationTest {
  @Rule
  public TemporaryFolder temporary = new TemporaryFolder();

  @Test
  public void validationDoesNotPoisonEnclosingWrite() throws Exception {
    try (var storage = SqliteStorage.open(temporary.getRoot().toPath().resolve("cron.db"))) {
      var failures = new AtomicInteger();
      storage.setWriteFailureHandler(failure -> failures.incrementAndGet());
      Scheduler scheduler = createMock(Scheduler.class);
      replay(scheduler);
      var manager = new CronJobManagerImpl(storage, scheduler, TimeZone.getTimeZone("GMT"));
      var job = QuartzTestUtil.makeSanitizedCronJob();
      assertTrue(storage.write(stores -> {
        try {
          manager.updateJob(job);
          return false;
        } catch (CronException expected) {
          return true;
        }
      }));
      storage.write(stores -> {
        stores.getCronJobStore().saveAcceptedJob(job.getSanitizedConfig().getJobConfig());
        return null;
      });
      assertTrue(storage.write(stores -> {
        try {
          manager.createJob(job);
          return false;
        } catch (CronException expected) {
          return true;
        }
      }));
      storage.write(stores -> {
        stores.getSchedulerStore().saveFrameworkId("still-writable");
        return null;
      });
      assertEquals(0, failures.get());
      verify(scheduler);
    }
  }

  @Test
  public void quartzFailureAfterMutationStillFailsClosed() throws Exception {
    try (var storage = SqliteStorage.open(temporary.getRoot().toPath().resolve("failure.db"))) {
      var failures = new AtomicInteger();
      storage.setWriteFailureHandler(failure -> failures.incrementAndGet());
      Scheduler scheduler = createMock(Scheduler.class);
      expect(scheduler.scheduleJob(anyObject(JobDetail.class), anyObject(Trigger.class)))
          .andThrow(new SchedulerException("unavailable"));
      replay(scheduler);
      var manager = new CronJobManagerImpl(storage, scheduler, TimeZone.getTimeZone("GMT"));
      try {
        storage.write(stores -> {
          try {
            manager.createJob(QuartzTestUtil.makeSanitizedCronJob());
          } catch (CronException expected) {
            // Mirrors the API's catch inside its outer storage callback.
          }
          return null;
        });
        fail("Expected rollback-only transaction");
      } catch (StorageException expected) {
        assertEquals(1, failures.get());
      }
      assertTrue(storage.read(stores ->
          !stores.getCronJobStore().fetchJobs().iterator().hasNext()));
      try {
        storage.write(stores -> null);
        fail("Expected restart requirement");
      } catch (StorageException expected) {
        assertTrue(expected.getMessage().contains("restart required"));
      }
      verify(scheduler);
    }
  }
}
