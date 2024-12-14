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
package org.apache.kafka.common.utils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class LogCaptureAppender extends AbstractAppender implements AutoCloseable {
    private final List<LogEvent> events = new LinkedList<>();
    private final List<LogLevelChange> logLevelChanges = new LinkedList<>();
    private final List<org.apache.logging.log4j.core.Logger> loggers = new ArrayList<>();

    public static class LogLevelChange {
        private final Level originalLevel;
        private final Class<?> clazz;

        public LogLevelChange(final Level originalLevel, final Class<?> clazz) {
            this.originalLevel = originalLevel;
            this.clazz = clazz;
        }
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static class Event {
        private final String level;
        private final String message;
        private final Optional<String> throwableInfo;
        private final Optional<String> throwableClassName;

        Event(final String level, final String message, final Optional<String> throwableInfo, final Optional<String> throwableClassName) {
            this.level = level;
            this.message = message;
            this.throwableInfo = throwableInfo;
            this.throwableClassName = throwableClassName;
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

        public Optional<String> getThrowableClassName() {
            return throwableClassName;
        }
    }

    public LogCaptureAppender(String name) {
        super(name, null, null, true, Property.EMPTY_ARRAY);
    }

    public static LogCaptureAppender createAndRegister() {
        final LogCaptureAppender logCaptureAppender = new LogCaptureAppender("LogCaptureAppender");
        Logger logger = LogManager.getRootLogger();
        logCaptureAppender.addToLogger(logger);
        return logCaptureAppender;
    }

    public static LogCaptureAppender createAndRegister(final Class<?> clazz) {
        final LogCaptureAppender logCaptureAppender = new LogCaptureAppender("LogCaptureAppender");
        Logger logger = LogManager.getLogger(clazz);
        logCaptureAppender.addToLogger(logger);
        return logCaptureAppender;
    }

    public void addToLogger(Logger logger) {
        org.apache.logging.log4j.core.Logger coreLogger = (org.apache.logging.log4j.core.Logger) logger;
        this.start();
        coreLogger.addAppender(this);
        loggers.add(coreLogger);
    }

    public void setClassLogger(final Class<?> clazz, Level level) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        String loggerName = clazz.getName();
        LoggerConfig loggerConfig = config.getLoggerConfig(loggerName);

        Level originalLevel = loggerConfig.getLevel();
        logLevelChanges.add(new LogLevelChange(originalLevel, clazz));

        if (!loggerConfig.getName().equals(loggerName)) {
            LoggerConfig newLoggerConfig = new LoggerConfig(loggerName, level, true);
            config.addLogger(loggerName, newLoggerConfig);
        } else {
            loggerConfig.setLevel(level);
        }
        ctx.updateLoggers();
    }

    @Override
    public void append(final LogEvent event) {
        synchronized (events) {
            events.add(event.toImmutable());
        }
    }

    public List<String> getMessages(String level) {
        return getEvents().stream()
                .filter(e -> level.equals(e.getLevel()))
                .map(Event::getMessage)
                .collect(Collectors.toList());
    }

    public List<String> getMessages() {
        final List<String> result = new LinkedList<>();
        synchronized (events) {
            for (final LogEvent event : events) {
                result.add(event.getMessage().getFormattedMessage());
            }
        }
        return result;
    }

    public List<Event> getEvents() {
        final LinkedList<Event> result = new LinkedList<>();
        synchronized (events) {
            for (final LogEvent event : events) {
                final Throwable throwable = event.getThrown();
                final Optional<String> throwableString;
                final Optional<String> throwableClassName;
                if (throwable == null) {
                    throwableString = Optional.empty();
                    throwableClassName = Optional.empty();
                } else {
                    StringWriter stringWriter = new StringWriter();
                    PrintWriter printWriter = new PrintWriter(stringWriter);
                    throwable.printStackTrace(printWriter);
                    throwableString = Optional.of(stringWriter.toString());
                    throwableClassName = Optional.of(throwable.getClass().getName());
                }

                result.add(new Event(
                    event.getLevel().toString(),
                    event.getMessage().getFormattedMessage(),
                    throwableString,
                    throwableClassName));
            }
        }
        return result;
    }

    @Override
    public void close() {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();

        for (final LogLevelChange logLevelChange : logLevelChanges) {
            String loggerName = logLevelChange.clazz.getName();
            LoggerConfig loggerConfig = config.getLoggerConfig(loggerName);
            if (!loggerConfig.getName().equals(loggerName)) {
                LoggerConfig newLoggerConfig = new LoggerConfig(loggerName, logLevelChange.originalLevel, true);
                config.addLogger(loggerName, newLoggerConfig);
            } else {
                loggerConfig.setLevel(logLevelChange.originalLevel);
            }
        }
        logLevelChanges.clear();
        ctx.updateLoggers();

        unregister();
    }

    public void unregister() {
        for (org.apache.logging.log4j.core.Logger logger : loggers) {
            logger.removeAppender(this);
        }
        loggers.clear();
        this.stop();
    }
}
