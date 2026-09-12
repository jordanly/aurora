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

import org.apache.aurora.scheduler.state.StateManagerImplTest;
import org.apache.aurora.scheduler.storage.Storage;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/** Runs the existing state-manager scenarios against the transactional store. */
public class SqliteStateManagerImplTest extends StateManagerImplTest {
  @Rule
  public TemporaryFolder temporary = new TemporaryFolder();

  @Override
  protected Storage createStorage() {
    SqliteStorage sqlite = SqliteStorage.open(temporary.getRoot().toPath().resolve("state.db"));
    addTearDown(sqlite::close);
    return sqlite;
  }
}
