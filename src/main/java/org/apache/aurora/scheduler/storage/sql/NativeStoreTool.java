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
package org.apache.aurora.scheduler.storage.sql;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

/** Tiny independent native-library qualification entrypoint; not a scheduler runtime. */
public final class NativeStoreTool {
  private NativeStoreTool() { }

  public static void main(String[] args) throws Exception {
    if (args.length != 2 || !("self-check".equals(args[0])
        || "crash-before".equals(args[0]) || "crash-after".equals(args[0]))) {
      throw new IllegalArgumentException("Usage: NativeStoreTool self-check STATE_DIR");
    }
    Path directory = Paths.get(args[1]);
    NativeSqlStore.JobKey job = new NativeSqlStore.JobKey("lab", "test", "qualification");
    boolean preexisting;
    try (NativeSqlStore store = new NativeSqlStore(directory, "lab", "recovery-1")) {
      preexisting = store.read(tx -> tx.jobBody(job) != null);
      store.write(tx -> {
        if (tx.jobBody(job) == null) { tx.createJob(job, "18446744073709551615", "batch", "job"); }
        if ("crash-before".equals(args[0])) { Runtime.getRuntime().halt(71); }
        return null;
      });
      if ("crash-after".equals(args[0])) { Runtime.getRuntime().halt(72); }
      try {
        store.write(tx -> {
          tx.addInstance(job, "rolled-back");
          try { store.write(inner -> { throw new AssertionError("nested failure"); }); }
          catch (AssertionError expected) { /* outer must still roll back */ }
          return null;
        });
        throw new AssertionError("Rollback-only commit accepted");
      } catch (SQLException expected) { /* expected */ }
    }
    try (NativeSqlStore store = new NativeSqlStore(directory, "lab", "recovery-1")) {
      store.read(tx -> {
        if (!"job".equals(tx.jobBody(job)) || tx.hasInstance(job, "rolled-back")) {
          throw new AssertionError("Reopen/rollback check failed");
        }
        return null;
      });
    }
    System.out.println("NATIVE_SQL_OK sqlite-jdbc=3.53.4.0 WAL FULL reopen rollback-only "
        + "arch=" + System.getProperty("os.arch") + " java=" + System.getProperty("java.version")
        + " preexisting=" + preexisting);
  }
}
