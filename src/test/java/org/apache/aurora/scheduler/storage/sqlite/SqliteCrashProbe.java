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
package org.apache.aurora.scheduler.storage.sqlite;

import java.nio.file.Path;

import org.apache.aurora.scheduler.resources.ResourceTestUtil;
import org.apache.aurora.scheduler.storage.sqlite.SqliteEffects.Command;
import org.apache.aurora.scheduler.storage.sqlite.SqliteEffects.ReceiptKey;

/** Child-process crash fixture: intentionally exits without closing JDBC or scheduler ownership. */
public final class SqliteCrashProbe {
  private SqliteCrashProbe() {
  }

  public static void main(String[] args) throws Exception {
    SqliteStorage storage = SqliteStorage.open(Path.of(args[0]));
    storage.write("crash-operation", stores -> {
      stores.getQuotaStore().saveQuota("crash-role", ResourceTestUtil.aggregate(2, 512, 100));
      storage.effects().enqueue(new Command("crash-command", "agent", "task", "LAUNCH", 1,
          new byte[] {1, 2}));
      storage.effects().recordReceipt(new ReceiptKey("agent", "incarnation", 0), 1,
          new byte[] {3, 4});
      if ("before".equals(args[1])) {
        Runtime.getRuntime().halt(17);
      }
      return null;
    });
    Runtime.getRuntime().halt(18);
  }
}
