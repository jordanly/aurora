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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.aurora.scheduler.storage.Storage.StorageException;

/** Verifies and atomically publishes a snapshot while the caller retains its owner/write guards. */
final class SqliteBackup {
  @FunctionalInterface
  interface SnapshotWriter {
    void write(Path snapshot) throws SQLException;
  }

  private SqliteBackup() { }

  static void create(Path path, Path destination, SnapshotWriter writer) {
    Path temporaryDirectory = null;
    Throwable primary = null;
    try {
      Path requested = destination.toAbsolutePath().normalize();
      Path requestedParent = requested.getParent();
      if (requestedParent == null) {
        throw new StorageException("SQLite backup path must name a file: " + requested);
      }
      Path targetParent = requestedParent.toRealPath();
      Path target = targetParent.resolve(requested.getFileName());
      if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)
          || target.equals(path)
          || target.equals(path.resolveSibling(path.getFileName() + "-wal"))
          || target.equals(path.resolveSibling(path.getFileName() + "-shm"))
          || target.equals(path.resolveSibling(path.getFileName() + "-journal"))
          || target.equals(path.resolveSibling(path.getFileName() + ".owner"))) {
        throw new StorageException("Backup destination must be a new independent file: " + target);
      }
      temporaryDirectory = Files.createTempDirectory(targetParent, ".aurora-backup-");
      Path snapshot = temporaryDirectory.resolve("snapshot.db");
      writer.write(snapshot);
      try (Connection verification = DriverManager.getConnection(
          "jdbc:sqlite:" + snapshot.toUri().toASCIIString());
           Statement statement = verification.createStatement();
           ResultSet rows = statement.executeQuery("PRAGMA integrity_check")) {
        if (!rows.next() || !"ok".equals(rows.getString(1)) || rows.next()) {
          throw new StorageException("SQLite backup failed integrity verification");
        }
      }
      // Link publication is atomic and cannot overwrite a destination created concurrently.
      // Both paths are on the destination filesystem; the temporary link is removed afterward.
      Files.createLink(target, snapshot);
      Files.delete(snapshot);
      try (FileChannel directory = FileChannel.open(targetParent, StandardOpenOption.READ)) {
        directory.force(true);
      }
    } catch (IOException | SQLException e) {
      primary = e;
      throw new StorageException("Unable to create SQLite backup", e);
    } catch (RuntimeException | Error e) {
      primary = e;
      throw e;
    } finally {
      try {
        if (temporaryDirectory != null) {
          Files.deleteIfExists(temporaryDirectory.resolve("snapshot.db"));
          Files.deleteIfExists(temporaryDirectory.resolve("snapshot.db-wal"));
          Files.deleteIfExists(temporaryDirectory.resolve("snapshot.db-shm"));
          Files.deleteIfExists(temporaryDirectory);
        }
      } catch (IOException cleanup) {
        if (primary != null) {
          suppress(primary, cleanup);
        } else {
          throw new StorageException("Unable to clean up SQLite backup workspace", cleanup);
        }
      }
    }
  }

  // Throwable rejects self-suppression by identity, regardless of an exception's equals method.
  @SuppressWarnings("PMD.CompareObjectsWithEquals")
  private static void suppress(Throwable primary, Throwable cleanup) {
    if (primary != cleanup) {
      primary.addSuppressed(cleanup);
    }
  }

}
