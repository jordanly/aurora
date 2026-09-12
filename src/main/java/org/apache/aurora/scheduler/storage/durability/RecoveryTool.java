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
package org.apache.aurora.scheduler.storage.durability;

import java.nio.file.Path;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import org.apache.aurora.scheduler.storage.sqlite.SqliteRecovery;
import org.slf4j.LoggerFactory;

/** Offline, non-overwriting recovery into a new SQLite database. */
public final class RecoveryTool {
  private RecoveryTool() { }

  enum Endpoint { BACKUP, SQLITE }

  @Parameters(separators = "=")
  private static class Options {
    @Parameter(names = "-from", required = true,
        description = "BACKUP for a drained Thrift snapshot; SQLITE for a standalone backup")
    Endpoint from;

    @Parameter(names = "-to", required = true, description = "Destination type (SQLITE only)")
    Endpoint to;

    @Parameter(names = "-backup", required = true, description = "Read-only source backup file")
    String backup;

    @Parameter(names = "-sqlite-destination", required = true,
        description = "New file in an existing offline directory; never replaces a database")
    String destination;

    @Parameter(names = "--help", help = true, description = "Print usage")
    boolean help;
  }

  public static void main(String[] args) {
    Options options = new Options();
    JCommander parser = JCommander.newBuilder().programName(RecoveryTool.class.getName())
        .addObject(options).build();
    parser.parse(args);
    if (options.help) {
      parser.usage();
      System.exit(1);
      return;
    }
    if (options.to != Endpoint.SQLITE) {
      throw new IllegalArgumentException("Only -to=SQLITE is supported; backups are read-only");
    }
    Path source = Path.of(options.backup);
    Path destination = Path.of(options.destination);
    if (options.from == Endpoint.BACKUP) {
      SqliteRecovery.importSnapshot(source, destination);
    } else {
      SqliteRecovery.restoreBackup(source, destination);
    }
    LoggerFactory.getLogger(RecoveryTool.class).info(
        "Recovered {} into new SQLite database {}", options.from, destination);
  }
}
