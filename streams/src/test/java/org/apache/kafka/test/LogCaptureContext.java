/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.test.appender.ListAppender;

/**
 * <p>This class provides an isolated logging context for logging tests. You can also set the logging
 * level of the loggers for a given context differently.
 *
 * <p>By default, the context uses the definition in src/test/resources/log4j2.properties:
 * <pre>
 *     // Creates a logging context with default configurations
 *     try (final LogCaptureContext logCaptureContext = LogCaptureContext.create()) {
 *         ...
 *     }
 * </pre>
 *
 * <p>You can override the default logging levels by passing a map from the logger name to the desired level, like:
 * <pre>
 *     // A logging context with default configuration, but 'foo.bar' logger's level is set to WARN.
 *     try (final LogCaptureContext logCaptureContext = LogCaptureContext.create(
 *         Collections.singletonMap("foo.bar", "WARN")
 *     )) {
 *         ...
 *     }
 * </pre>
 *
 * <p>Since the logging messages are appended asynchronously, you should wait until the appender process
 * the given messages with {@link #setLatch(int)} and {@link #await(long, TimeUnit)} methods, like:
 * <pre>
 *     try (final LogCaptureContext logCaptureContext = LogCaptureContext.create(...)) {
 *         // We expect there will be at least 5 logging messages.
 *         logCaptureContext.setLatch(5);
 *
 *         // The routine to test ...
 *
 *         // Wait for the appender to finish processing the logging messages, 10 seconds in maximum.
 *         logCaptureContext.await(10L, TimeUnit.SECONDS);
 *         assertThat(
 *             logCaptureContext.getMessages(),
 *             hasItem("the logging message is appended"));
 *     }
 * </pre>
 *
 * <p>Note: The tests may hang up if you set the messages count too high.
 */
public class LogCaptureContext implements AutoCloseable {
    private final ListAppender listAppender;
    private final Map<String, Level> prevLevelMap = new HashMap<>();

    public static LogCaptureContext create() {
        return create(new HashMap<>());
    }

    public static LogCaptureContext create(final Map<String, String> levelMap) {
        return new LogCaptureContext(levelMap);
    }

    private LogCaptureContext(final Map<String, String> levelMap) {
        final LoggerContext loggerContext = LoggerContext.getContext(false);
        listAppender = ListAppender.createAppender("logger-context-" + TestUtils.randomString(8),
            false, false, PatternLayout.newBuilder().withPattern("%p %m %throwable").build(), null);
        listAppender.start();
        loggerContext.getConfiguration().addAppender(listAppender);
        loggerContext.getRootLogger().addAppender(listAppender);

        for (final String loggerName : levelMap.keySet()) {
            final Logger logger = loggerContext.getLogger(loggerName);

            // Store the previous logger level
            this.prevLevelMap.put(loggerName, logger.getLevel());

            // Change the logger level
            logger.setLevel(Level.getLevel(levelMap.get(loggerName)));
        }
    }

    /**
     * Set the expected number of events.
     *
     * @param size number of expected logging events
     */
    public void setLatch(final int size) {
        this.listAppender.countDownLatch = new CountDownLatch(size);
    }

    /**
     * Wait for the appender to finish processing the expected number of events.
     *
     * @throws InterruptedException
     */
    public void await() throws InterruptedException {
        await(10, TimeUnit.SECONDS);
    }

    /**
     * Wait for the appender to finish processing the expected number of events.
     *
     * @throws InterruptedException
     */
    public void await(final long l, final TimeUnit timeUnit) throws InterruptedException {
        this.listAppender.countDownLatch.await(l, timeUnit);
    }

    /**
     * Returns the appended log messages.
     *
     * @return appended log messages
     */
    public List<String> getMessages() {
        return listAppender.getMessages();
    }

    @Override
    public void close() {
        final LoggerContext loggerContext = LoggerContext.getContext(false);
        loggerContext.getRootLogger().removeAppender(listAppender);
        listAppender.stop();

        for (final String loggerName : this.prevLevelMap.keySet()) {
            final Logger logger = loggerContext.getLogger(loggerName);

            // Restore previous logger level
            logger.setLevel(this.prevLevelMap.get(loggerName));
        }
    }
}
