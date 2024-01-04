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
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

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

        org.apache.log4j.Logger foundLogger = null;
        if (ROOT_LOGGER_NAME.equalsIgnoreCase(logger)) {
            foundLogger = rootLogger();
        } else {
            Enumeration<org.apache.log4j.Logger> en = currentLoggers();
            // search within existing loggers for the given name.
            // using LogManger.getLogger() will create a logger if it doesn't exist
            // (potential leak since these don't get cleaned up).
            while (en.hasMoreElements()) {
                org.apache.log4j.Logger l = en.nextElement();
                if (logger.equals(l.getName())) {
                    foundLogger = l;
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

        Enumeration<org.apache.log4j.Logger> enumeration = currentLoggers();
        Collections.list(enumeration)
                .stream()
                .filter(logger -> logger.getLevel() != null)
                .forEach(logger -> result.put(logger.getName(), loggerLevel(logger)));

        org.apache.log4j.Logger root = rootLogger();
        if (root.getLevel() != null) {
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
        List<org.apache.log4j.Logger> childLoggers = loggers(namespace);

        List<String> result = new ArrayList<>();
        for (org.apache.log4j.Logger logger: childLoggers) {
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
    private synchronized List<org.apache.log4j.Logger> loggers(String namespace) {
        Objects.requireNonNull(namespace, "Logging namespace may not be null");

        if (ROOT_LOGGER_NAME.equalsIgnoreCase(namespace)) {
            List<org.apache.log4j.Logger> result = Collections.list(currentLoggers());
            result.add(rootLogger());
            return result;
        }

        List<org.apache.log4j.Logger> result = new ArrayList<>();
        org.apache.log4j.Logger ancestorLogger = lookupLogger(namespace);
        Enumeration<org.apache.log4j.Logger> en = currentLoggers();
        boolean present = false;
        while (en.hasMoreElements()) {
            org.apache.log4j.Logger current = en.nextElement();
            if (current.getName().startsWith(namespace)) {
                result.add(current);
            }
            if (namespace.equals(current.getName())) {
                present = true;
            }
        }

        if (!present) {
            result.add(ancestorLogger);
        }

        return result;
    }

    // visible for testing
    org.apache.log4j.Logger lookupLogger(String logger) {
        return LogManager.getLogger(logger);
    }

    @SuppressWarnings("unchecked")
    // visible for testing
    Enumeration<org.apache.log4j.Logger> currentLoggers() {
        return LogManager.getCurrentLoggers();
    }

    // visible for testing
    org.apache.log4j.Logger rootLogger() {
        return LogManager.getRootLogger();
    }

    private void setLevel(org.apache.log4j.Logger logger, Level level) {
        Level currentLevel = logger.getLevel();
        if (currentLevel == null)
            currentLevel = logger.getEffectiveLevel();

        if (level.equals(currentLevel)) {
            log.debug("Skipping update for logger {} since its level is already {}", logger.getName(), level);
            return;
        }

        log.debug("Setting level of logger {} (excluding children) to {}", logger.getName(), level);
        logger.setLevel(level);
        lastModifiedTimes.put(logger.getName(), time.milliseconds());
    }

    private LoggerLevel loggerLevel(org.apache.log4j.Logger logger) {
        Level level = logger.getLevel();
        if (level == null)
            level = logger.getEffectiveLevel();

        Long lastModified = lastModifiedTimes.get(logger.getName());
        return new LoggerLevel(Objects.toString(level), lastModified);
    }

}
