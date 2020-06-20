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
package org.apache.kafka.streams.processor.internals.testutil;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class LogCaptureAppender extends AppenderSkeleton implements AutoCloseable {
    private final List<LoggingEvent> events = new LinkedList<>();

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static class Event {
        private final String level;
        private final String message;
        private final Optional<String> throwableInfo;

        Event(final String level, final String message, final Optional<String> throwableInfo) {
            this.level = level;
            this.message = message;
            this.throwableInfo = throwableInfo;
        }

        public String getLevel() {
            return level;
        }

        public String getMessage() {
            return message;
        }

        public Optional<String> getThrowableInfo() {
            return throwableInfo;
        }
    }

    public static LogCaptureAppender createAndRegister() {
        final LogCaptureAppender logCaptureAppender = new LogCaptureAppender();
        Logger.getRootLogger().addAppender(logCaptureAppender);
        return logCaptureAppender;
    }

    public static LogCaptureAppender createAndRegister(final Class<?> clazz) {
        final LogCaptureAppender logCaptureAppender = new LogCaptureAppender();
        Logger.getLogger(clazz).addAppender(logCaptureAppender);
        return logCaptureAppender;
    }

    public static void setClassLoggerToDebug(final Class<?> clazz) {
        Logger.getLogger(clazz).setLevel(Level.DEBUG);
    }

    public static void unregister(final LogCaptureAppender logCaptureAppender) {
        Logger.getRootLogger().removeAppender(logCaptureAppender);
    }

    @Override
    protected void append(final LoggingEvent event) {
        synchronized (events) {
            events.add(event);
        }
    }

    public List<String> getMessages() {
        final LinkedList<String> result = new LinkedList<>();
        synchronized (events) {
            for (final LoggingEvent event : events) {
                result.add(event.getRenderedMessage());
            }
        }
        return result;
    }

    public List<Event> getEvents() {
        final LinkedList<Event> result = new LinkedList<>();
        synchronized (events) {
            for (final LoggingEvent event : events) {
                final String[] throwableStrRep = event.getThrowableStrRep();
                final Optional<String> throwableString;
                if (throwableStrRep == null) {
                    throwableString = Optional.empty();
                } else {
                    final StringBuilder throwableStringBuilder = new StringBuilder();

                    for (final String s : throwableStrRep) {
                        throwableStringBuilder.append(s);
                    }

                    throwableString = Optional.of(throwableStringBuilder.toString());
                }

                result.add(new Event(event.getLevel().toString(), event.getRenderedMessage(), throwableString));
            }
        }
        return result;
    }

    @Override
    public void close() {
        unregister(this);
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }
}
