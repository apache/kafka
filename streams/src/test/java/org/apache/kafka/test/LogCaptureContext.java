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

public class LogCaptureContext implements AutoCloseable {
    private final ListAppender listAppender;
    private final Map<String, Level> prevLevelMap = new HashMap<>();

    public static LogCaptureContext create(final String name) {
        return create(name, new HashMap<>());
    }

    public static LogCaptureContext create(final String name, final Map<String, String> levelMap) {
        return new LogCaptureContext(name, levelMap);
    }

    private LogCaptureContext(final String name, final Map<String, String> levelMap) {
        final LoggerContext loggerContext = LoggerContext.getContext(false);
        listAppender = ListAppender.createAppender(name, false, false,
            PatternLayout.newBuilder().withPattern("%p %m %throwable").build(), null);
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

    public void setLatch(final int size) {
        this.listAppender.countDownLatch = new CountDownLatch(size);
    }

    public void await(final long l, final TimeUnit timeUnit) throws InterruptedException {
        this.listAppender.countDownLatch.await(l, timeUnit);
    }

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
