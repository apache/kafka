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
package org.apache.kafka.server.logger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * An MBean that allows the user to dynamically alter log4j levels at runtime.
 * The companion object contains the singleton instance of this class and
 * registers the MBean. The {@code kafka.utils.Logging} trait forces initialization
 * of the companion object.
 */
public class LoggingController implements LoggingControllerMBean {

    private static final Logger LOGGER = LogManager.getLogger(LoggingController.class);

    /**
     * Note: In Log4j 1, the root logger's name was "root" and Kafka also followed that name for dynamic logging control feature.
     * The root logger's name is changed in log4j2 to empty string (see: {@link LogManager#ROOT_LOGGER_NAME}) but for backward-compatibility.
     * Kafka keeps its original root logger name. It is why here is a dedicated definition for the root logger name.
     */
    public static final String ROOT_LOGGER = "root";

    private static final LoggingControllerDelegate DELEGATE;

    static {
        LoggingControllerDelegate tempDelegate;
        try {
            tempDelegate = new Log4jCoreController();
        } catch (ClassCastException | LinkageError e) {
            LOGGER.info("No supported logging implementation found. Logging configuration endpoint will be disabled.");
            tempDelegate = new NoOpController();
        } catch (Exception e) {
            LOGGER.warn("A problem occurred, while initializing the logging controller. Logging configuration endpoint will be disabled.", e);
            tempDelegate = new NoOpController();
        }
        DELEGATE = tempDelegate;
    }

    /**
     * Returns a map of the log4j loggers and their assigned log level.
     * If a logger does not have a log level assigned, we return the log level of the first ancestor with a level configured.
     */
    public static Map<String, String> loggers() {
        return DELEGATE.loggers();
    }

    /**
     * Sets the log level of a particular logger. If the given logLevel is not an available level
     * (i.e., one of OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL) it falls back to DEBUG.
     *
     * @see Level#toLevel(String, Level)
     */
    public static boolean logLevel(String loggerName, String logLevel) {
        return DELEGATE.logLevel(loggerName, logLevel);
    }

    public static boolean unsetLogLevel(String loggerName) {
        return DELEGATE.unsetLogLevel(loggerName);
    }

    public static boolean loggerExists(String loggerName) {
        return DELEGATE.loggerExists(loggerName);
    }

    @Override
    public List<String> getLoggers() {
        return LoggingController.loggers()
                .entrySet()
                .stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .toList();
    }

    @Override
    public String getLogLevel(String loggerName) {
        return LoggingController.loggers().getOrDefault(loggerName, "No such logger.");
    }

    @Override
    public boolean setLogLevel(String loggerName, String level) {
        return LoggingController.logLevel(loggerName, level);
    }
}
