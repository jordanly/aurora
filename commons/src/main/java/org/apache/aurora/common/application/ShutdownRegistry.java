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
package org.apache.aurora.common.application;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.base.ExceptionalCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A shutdown action controller. It executes actions in the reverse order they were registered, and
 * logs a warning for every shutdown action that fails, but doesn't prevent completion of subsequent
 * actions or the normal completion of the {@code execute()} method.
 *
 * @author Attila Szegedi
 */
public interface ShutdownRegistry {

  /**
   * Adds an action to the shutdown registry.
   *
   * @param action Action to register.
   * @param <E> Exception type thrown by the action.
   * @param <T> Type of command.
   */
  <E extends Exception, T extends ExceptionalCommand<E>> void addAction(T action);

  /**
   * Implementation of a shutdown registry.
   */
  public static class ShutdownRegistryImpl implements ShutdownRegistry, Command {
    private static final Logger LOG = LoggerFactory.getLogger(ShutdownRegistryImpl.class);

    private final List<ExceptionalCommand<? extends Exception>> actions = Lists.newLinkedList();

    private boolean completed = false;

    /**
     * Registers an action to execute during {@link #execute()}. It is an error to call this method
     * after calling {@link #execute()}.
     *
     * @param action the action to add to the list of actions to execute during execution
     */
    @Override
    public synchronized <E extends Exception, T extends ExceptionalCommand<E>> void addAction(
        T action) {
      Preconditions.checkState(!completed);
      actions.add(java.util.Objects.requireNonNull(action));
    }

    /**
     * Executes an application shutdown stage by executing all registered actions. This method can
     * be called multiple times but will only execute the registered actions the first time.
     *
     * This sends output to System.out because logging is unreliable during JVM shutdown, which
     * this class may be used for.
     */
    @Override
    public void execute() {
      List<ExceptionalCommand<? extends Exception>> pending;
      synchronized (this) {
        if (completed) {
          return;
        }
        completed = true;
        pending = List.copyOf(actions.reversed());
        actions.clear();
      }
      LOG.info("Executing {} shutdown commands.", pending.size());
      for (ExceptionalCommand<? extends Exception> action : pending) {
        // Each hook runs outside the registry lock, including hooks that re-enter shutdown.
        // SUPPRESS CHECKSTYLE:OFF IllegalCatch
        try {
          action.execute();
        } catch (Exception e) {
          LOG.warn("Shutdown action failed.", e);
        }
        // SUPPRESS CHECKSTYLE:ON IllegalCatch
      }
    }
  }
}
