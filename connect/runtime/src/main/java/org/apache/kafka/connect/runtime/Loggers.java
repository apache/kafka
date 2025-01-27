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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.rest.entities.LoggerLevel;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manages logging levels on a single worker. Supports dynamic adjustment and querying
 * of logging levels.
 * <p>
 * This class is thread-safe; concurrent calls to all of its public methods from any number
 * of threads are permitted.
 */
public abstract class Loggers {

    private static final Logger log = LoggerFactory.getLogger(Loggers.class);

    private static final String ROOT_LOGGER_NAME = "root";

    /**
     * Log4j uses "root" (case-insensitive) as name of the root logger.
     * Note: In log4j, the root logger's name was "root" and Kafka also followed that name for dynamic logging control feature.
     * <p>
     * While log4j2 changed the root logger's name to empty string (see: [[LogManager.ROOT_LOGGER_NAME]]),
     * for backward-compatibility purposes, we accept both empty string and "root" as valid root logger names.
     * This is why we have a dedicated definition that includes both values.
     * </p>
     */
    private static final List<String> VALID_ROOT_LOGGER_NAMES = List.of(LogManager.ROOT_LOGGER_NAME, ROOT_LOGGER_NAME);

    final Time time;

    /**
     * Maps logger names to their last modification timestamps.
     * Note: The logger name "root" refers to the actual root logger of log4j2.
     */
    final Map<String, Long> lastModifiedTimes;

    /**
     * Creates a {@link Loggers} instance appropriate for the current environment.
     *
     * @param time A time source.
     * @return A new {@link Loggers} instance, never {@link null}.
     */
    public static Loggers newInstance(Time time) {
        Objects.requireNonNull(time);
        try {
            return new Log4jLoggers(time);
        } catch (ClassCastException | LinkageError e) {
            log.info("No supported logging implementation found. Logging configuration endpoint will be disabled.");
            return new NoOpLoggers(time);
        } catch (Exception e) {
            log.warn("A problem occurred, while initializing the logging controller. Logging configuration endpoint will be disabled.", e);
            return new NoOpLoggers(time);
        }
    }

    private Loggers(Time time) {
        this.time = time;
        this.lastModifiedTimes = new ConcurrentHashMap<>();
    }

    /**
     * Retrieve the current level for a single logger.
     *
     * @param loggerName the name of the logger to retrieve the level for; may not be null
     * @return the current level (falling back on the effective level if necessary) of the logger,
     * or null if no logger with the specified name exists
     */
    public abstract LoggerLevel level(String loggerName);

    /**
     * Retrieve the current levels of all known loggers
     *
     * @return the levels of all known loggers; may be empty, but never null
     */
    public abstract Map<String, LoggerLevel> allLevels();

    /**
     * Set the level for the specified logger and all of its children
     *
     * @param namespace the name of the logger to adjust along with its children; may not be null
     * @param level     the level to set for the logger and its children; may not be null
     * @return all loggers that were affected by this action, sorted by their natural ordering;
     * may be empty, but never null
     */
    public abstract List<String> setLevel(String namespace, String level);

    public abstract boolean isValidLevel(String level);

    static class Log4jLoggers extends Loggers {

        // package-private for testing
        final LoggerContext loggerContext;

        // Package-private for testing
        Log4jLoggers(Time time) {
            super(time);
            loggerContext = (LoggerContext) LogManager.getContext(false);
        }

        @Override
        public LoggerLevel level(String logger) {
            Objects.requireNonNull(logger, "Logger may not be null");

            org.apache.logging.log4j.Logger foundLogger = null;
            if (isValidRootLoggerName(logger)) {
                foundLogger = rootLogger();
            } else {
                var currentLoggers = currentLoggers().values();
                // search within existing loggers for the given name.
                // using LogManger.getLogger() will create a logger if it doesn't exist
                // (potential leak since these don't get cleaned up).
                for (org.apache.logging.log4j.Logger currentLogger : currentLoggers) {
                    if (logger.equals(currentLogger.getName())) {
                        foundLogger = currentLogger;
                        break;
                    }
                }
            }

            if (foundLogger == null) {
                log.warn("Unable to find level for logger {}", logger);
                return null;
            }

            return loggerLevel(foundLogger);
        }

        @Override
        public Map<String, LoggerLevel> allLevels() {
            return currentLoggers()
                    .values()
                    .stream()
                    .filter(logger -> !logger.getLevel().equals(Level.OFF))
                    .collect(Collectors.toMap(
                            this::getLoggerName,
                            this::loggerLevel,
                            (existing, replacing) -> replacing,
                            TreeMap::new)
                    );
        }

        @Override
        public List<String> setLevel(String namespace, String level) {
            Objects.requireNonNull(namespace, "Logging namespace may not be null");
            Objects.requireNonNull(level, "Level may not be null");
            String internalNameSpace = isValidRootLoggerName(namespace) ? LogManager.ROOT_LOGGER_NAME : namespace;

            log.info("Setting level of namespace {} and children to {}", internalNameSpace, level);

            var loggers = loggers(internalNameSpace);
            var nameToLevel = allLevels();

            List<String> result = new ArrayList<>();
            Configurator.setAllLevels(internalNameSpace, Level.valueOf(level));
            for (org.apache.logging.log4j.Logger logger : loggers) {
                // We need to track level changes for each logger and record their update timestamps  to ensure this method
                // correctly returns only the loggers whose levels were actually modified.
                String name = getLoggerName(logger);
                String newLevel = logger.getLevel().name();
                String oldLevel = nameToLevel.getOrDefault(name, new LoggerLevel("", time.milliseconds())).level();
                if (!newLevel.equalsIgnoreCase(oldLevel)) {
                    lastModifiedTimes.put(name, time.milliseconds());
                    result.add(name);
                }
            }
            Collections.sort(result);

            return result;
        }

        @Override
        public boolean isValidLevel(String level) {
            return !level.isEmpty() && Level.getLevel(level) != null;
        }

        /**
         * Retrieve all known loggers within a given namespace, creating an ancestor logger for that
         * namespace if one does not already exist
         *
         * @param namespace the namespace that the loggers should fall under; may not be null
         * @return all loggers that fall under the given namespace; never null, and will always contain
         * at least one logger (the ancestor logger for the namespace)
         */
        private Collection<org.apache.logging.log4j.core.Logger> loggers(String namespace) {
            Objects.requireNonNull(namespace, "Logging namespace may not be null");

            if (isValidRootLoggerName(namespace)) {
                return currentLoggers().values();
            }

            var result = new ArrayList<org.apache.logging.log4j.core.Logger>();
            var nameToLogger = currentLoggers();
            var ancestorLogger = lookupLogger(namespace);
            var currentLoggers = nameToLogger.values();

            boolean present = false;
            for (org.apache.logging.log4j.core.Logger currentLogger : currentLoggers) {
                if (currentLogger.getName().startsWith(namespace)) {
                    result.add(currentLogger);
                }
                if (namespace.equals(currentLogger.getName())) {
                    present = true;
                }
            }

            if (!present) {
                result.add(ancestorLogger);
            }

            return result;
        }

        // visible for testing
        org.apache.logging.log4j.core.Logger lookupLogger(String logger) {
            return loggerContext.getLogger(isValidRootLoggerName(logger) ? LogManager.ROOT_LOGGER_NAME : logger);
        }

        // visible for testing
        Map<String, org.apache.logging.log4j.core.Logger> currentLoggers() {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            var results = new HashMap<String, org.apache.logging.log4j.core.Logger>();
            context.getConfiguration().getLoggers().forEach((name, logger) -> results.put(name, loggerContext.getLogger(name)));
            context.getLoggerRegistry().getLoggers().forEach(logger -> results.put(logger.getName(), logger));
            return results;
        }

        // visible for testing
        org.apache.logging.log4j.Logger rootLogger() {
            return LogManager.getRootLogger();
        }

        private LoggerLevel loggerLevel(org.apache.logging.log4j.Logger logger) {
            Long lastModified = lastModifiedTimes.get(getLoggerName(logger));
            return new LoggerLevel(Objects.toString(logger.getLevel()), lastModified);
        }

        private boolean isValidRootLoggerName(String namespace) {
            return VALID_ROOT_LOGGER_NAMES.stream()
                    .anyMatch(rootLoggerNames -> rootLoggerNames.equalsIgnoreCase(namespace));
        }

        /**
         * Converts logger name to ensure backward compatibility between Log4j 1 and Log4j 2.
         * If the logger name is empty (Log4j 2 root logger representation), converts it to "root" (Log4j 1 style).
         * Otherwise, returns the original logger name.
         *
         * @param loggerName The name of the logger.
         * @return The logger name - returns "root" for empty string, otherwise returns the original logger name
         */
        private String getLoggerName(String loggerName) {
            return loggerName.equals(LogManager.ROOT_LOGGER_NAME) ? ROOT_LOGGER_NAME : loggerName;
        }

        /**
         * Converts logger name to ensure backward compatibility between Log4j 1 and Log4j 2.
         * If the logger name is empty (Log4j 2 root logger representation), converts it to "root" (Log4j 1 style).
         * Otherwise, returns the original logger name.
         *
         * @param logger The logger instance to get the name from
         * @return The logger name - returns "root" for empty string, otherwise returns the original logger name
         */
        private String getLoggerName(org.apache.logging.log4j.Logger logger) {
            return getLoggerName(logger.getName());
        }

    }

    private static class NoOpLoggers extends Loggers {

        private NoOpLoggers(Time time) {
            super(time);
        }

        @Override
        public LoggerLevel level(String loggerName) {
            return new LoggerLevel("OFF", 0L);
        }

        @Override
        public Map<String, LoggerLevel> allLevels() {
            return Map.of();
        }

        @Override
        public List<String> setLevel(String loggerName, String level) {
            return List.of();
        }

        @Override
        public boolean isValidLevel(String level) {
            return "OFF".equals(level);
        }
    }
}
