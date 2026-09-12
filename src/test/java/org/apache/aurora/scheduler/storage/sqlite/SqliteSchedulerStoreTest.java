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

import com.google.inject.AbstractModule;
import com.google.inject.Module;

import org.apache.aurora.scheduler.storage.AbstractSchedulerStoreTest;
import org.apache.aurora.scheduler.storage.Storage;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class SqliteSchedulerStoreTest extends AbstractSchedulerStoreTest {
  @Rule
  public TemporaryFolder temporary = new TemporaryFolder();

  private SqliteStorage storage;

  @Override
  protected Module getStorageModule() {
    storage = openStorage();
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(Storage.class).toInstance(storage);
      }
    };
  }

  @After
  public void closeStorage() {
    storage.close();
  }

  private SqliteStorage openStorage() {
    return SqliteStorage.open(temporary.getRoot().toPath().resolve("storage.db"));
  }
}
