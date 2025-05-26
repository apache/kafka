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

import org.apache.kafka.common.utils.Utils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

class Log4jCoreController implements LoggingControllerDelegate {
    private final LoggerContext logContext;

    public Log4jCoreController() {
        this.logContext = (LoggerContext) LogManager.getContext(false);
    }

    @Override
    public Map<String, String> loggers() {
        String rootLoggerLevel = logContext.getRootLogger().getLevel().toString();

        Map<String, String> result = new HashMap<>();
        // Loggers defined in the configuration
        for (LoggerConfig logger : logContext.getConfiguration().getLoggers().values()) {
            if (!logger.getName().equals(LogManager.ROOT_LOGGER_NAME)) {
                result.put(logger.getName(), logger.getLevel().toString());
            }
        }
        // Loggers actually running
        for (Logger logger : logContext.getLoggers()) {
            if (!logger.getName().equals(LogManager.ROOT_LOGGER_NAME)) {
                result.put(logger.getName(), logger.getLevel().toString());
            }
        }
        // Add root logger
        result.put(LoggingController.ROOT_LOGGER, rootLoggerLevel);
        return result;
    }

    @Override
    public boolean logLevel(String loggerName, String logLevel) {
        if (Utils.isBlank(loggerName) || Utils.isBlank(logLevel))
            return false;

        Level level = Level.toLevel(logLevel.toUpperCase(Locale.ROOT));

        if (loggerName.equals(LoggingController.ROOT_LOGGER)) {
            Configurator.setLevel(LogManager.ROOT_LOGGER_NAME, level);
            return true;
        }
        if (loggerExists(loggerName) && level != null) {
            Configurator.setLevel(loggerName, level);
            return true;
        }
        return false;
    }

    @Override
    public boolean unsetLogLevel(String loggerName) {
        Level nullLevel = null;
        if (loggerName.equals(LoggingController.ROOT_LOGGER)) {
            Configurator.setLevel(LogManager.ROOT_LOGGER_NAME, nullLevel);
            return true;
        }
        if (loggerExists(loggerName)) {
            Configurator.setLevel(loggerName, nullLevel);
            return true;
        }
        return false;
    }
}
