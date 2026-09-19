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

package org.apache.aurora.scheduler.testing;

import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.read.ListAppender;

/** Captures formatted messages and attached exceptions at every log level. */
public final class LogCapture implements AutoCloseable {
  private final Logger logger;
  private final Level previousLevel;
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

  public LogCapture(Class<?> type) {
    logger = (Logger) LoggerFactory.getLogger(type);
    previousLevel = logger.getLevel();
    logger.setLevel(Level.TRACE);
    appender.start();
    logger.addAppender(appender);
  }

  public String messages() {
    return appender.list.stream()
        .map(event -> event.getFormattedMessage()
            + (event.getThrowableProxy() == null ? ""
                : ThrowableProxyUtil.asString(event.getThrowableProxy())))
        .collect(Collectors.joining("\n"));
  }

  @Override
  public void close() {
    logger.detachAppender(appender);
    appender.stop();
    logger.setLevel(previousLevel);
  }
}
