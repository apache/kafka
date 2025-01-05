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
import org.apache.logging.log4j.core.config.LoggerConfig;
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
import java.util.stream.Collectors;

/**
 * Manages logging levels on a single worker. Supports dynamic adjustment and querying
 * of logging levels.
 * <p>
 * This class is thread-safe; concurrent calls to all of its public methods from any number
 * of threads are permitted.
 */
public class Loggers {

    private static final Logger log = LoggerFactory.getLogger(Loggers.class);

    /**
     * Log4j uses "root" (case-insensitive) as name of the root logger.
     * Note: In log4j, the root logger's name was "root" and Kafka also followed that name for dynamic logging control feature.
     *
     * The root logger's name is changed in log4j2 to empty string (see: [[LogManager.ROOT_LOGGER_NAME]]) but for backward-
     * compatibility. Kafka keeps its original root logger name. It is why here is a dedicated definition for the root logger name.
     */
    private static final String ROOT_LOGGER_NAME = "root";

    private final Time time;
    private final Map<String, Long> lastModifiedTimes;

    public Loggers(Time time) {
        this.time = time;
        this.lastModifiedTimes = new HashMap<>();
    }

    /**
     * Retrieve the current level for a single logger.
     * @param logger the name of the logger to retrieve the level for; may not be null
     * @return the current level (falling back on the effective level if necessary) of the logger,
     * or null if no logger with the specified name exists
     */
    public synchronized LoggerLevel level(String logger) {
        Objects.requireNonNull(logger, "Logger may not be null");

        org.apache.logging.log4j.Logger foundLogger = null;
        if (ROOT_LOGGER_NAME.equalsIgnoreCase(logger)) {
            foundLogger = rootLogger();
        } else {
            List<org.apache.logging.log4j.Logger> currentLoggers = currentLoggers();
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

    /**
     * Retrieve the current levels of all known loggers
     * @return the levels of all known loggers; may be empty, but never null
     */
    public synchronized Map<String, LoggerLevel> allLevels() {
        Map<String, LoggerLevel> result = new TreeMap<>();

        currentLoggers().stream()
                .filter(logger -> !logger.getLevel().equals(Level.OFF))
                .forEach(logger -> result.put(logger.getName(), loggerLevel(logger)));

        org.apache.logging.log4j.Logger root = rootLogger();
        if (!root.getLevel().equals(Level.OFF)) {
            result.put(ROOT_LOGGER_NAME, loggerLevel(root));
        }

        return result;
    }

    /**
     * Set the level for the specified logger and all of its children
     * @param namespace the name of the logger to adjust along with its children; may not be null
     * @param level the level to set for the logger and its children; may not be null
     * @return all loggers that were affected by this action, sorted by their natural ordering;
     * may be empty, but never null
     */
    public synchronized List<String> setLevel(String namespace, Level level) {
        Objects.requireNonNull(namespace, "Logging namespace may not be null");
        Objects.requireNonNull(level, "Level may not be null");

        log.info("Setting level of namespace {} and children to {}", namespace, level);
        List<org.apache.logging.log4j.Logger> childLoggers = loggers(namespace);

        List<String> result = new ArrayList<>();
        for (org.apache.logging.log4j.Logger logger: childLoggers) {
            setLevel(logger, level);
            result.add(logger.getName());
        }
        Collections.sort(result);

        return result;
    }

    /**
     * Retrieve all known loggers within a given namespace, creating an ancestor logger for that
     * namespace if one does not already exist
     * @param namespace the namespace that the loggers should fall under; may not be null
     * @return all loggers that fall under the given namespace; never null, and will always contain
     * at least one logger (the ancestor logger for the namespace)
     */
    private synchronized List<org.apache.logging.log4j.Logger> loggers(String namespace) {
        Objects.requireNonNull(namespace, "Logging namespace may not be null");

        if (ROOT_LOGGER_NAME.equalsIgnoreCase(namespace)) {
            List<org.apache.logging.log4j.Logger> result = currentLoggers();
            result.add(rootLogger());
            return result;
        }

        List<org.apache.logging.log4j.Logger> result = new ArrayList<>();
        org.apache.logging.log4j.Logger ancestorLogger = lookupLogger(namespace);
        List<org.apache.logging.log4j.Logger> currentLoggers = currentLoggers();
        boolean present = false;
        for (org.apache.logging.log4j.Logger currentLogger : currentLoggers) {
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
    org.apache.logging.log4j.Logger lookupLogger(String logger) {
        return LogManager.getLogger(logger);
    }

    List<org.apache.logging.log4j.Logger> currentLoggers() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Collection<LoggerConfig> loggerConfigs = context.getConfiguration().getLoggers().values();
        return loggerConfigs.stream()
            .map(LoggerConfig::getName)
            .distinct()
            .map(LogManager::getLogger)
            .collect(Collectors.toCollection(ArrayList::new));
    }

    // visible for testing
    org.apache.logging.log4j.Logger rootLogger() {
        return LogManager.getRootLogger();
    }

    private void setLevel(org.apache.logging.log4j.Logger logger, Level level) {
        String loggerName = logger.getName();
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        LoggerConfig loggerConfig = context.getConfiguration().getLoggerConfig(loggerName);
        Level currentLevel = loggerConfig.getLevel();

        if (level.equals(currentLevel)) {
            log.debug("Skipping update for logger {} since its level is already {}", loggerName, level);
            return;
        }

        log.debug("Setting level of logger {} (excluding children) to {}", loggerName, level);
        Configurator.setLevel(loggerName, level);
        lastModifiedTimes.put(loggerName, time.milliseconds());
    }

    private LoggerLevel loggerLevel(org.apache.logging.log4j.Logger logger) {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        LoggerConfig loggerConfig = context.getConfiguration().getLoggerConfig(logger.getName());
        Level level = loggerConfig.getLevel();
        Long lastModified = lastModifiedTimes.get(logger.getName());
        return new LoggerLevel(Objects.toString(level), lastModified);
    }
}
