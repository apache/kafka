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
package org.apache.kafka.common.logging;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;

/**
 * An MBean to control log levels of lo4j loggers.
 *
 * <p />Note: The changes made by this rest extension are not persisted across worker restarts.
 */
public class Log4jController implements Log4jControllerMBean {

    @Override
    public List<String> getLoggers() {
        List<String> logLevels = new ArrayList<>();
        Enumeration loggers = LogManager.getCurrentLoggers();
        while (loggers.hasMoreElements()) {
            Logger logger = (Logger) loggers.nextElement();
            if (logger != null) {
                Level currentLevel = getCurrentLevel(logger);
                logLevels.add(String.format("%s=%s", logger.getName(), currentLevel));
            }
        }
        Collections.sort(logLevels);
        logLevels.add(0, "root=" + LogManager.getRootLogger().getLevel());
        return logLevels;
    }

    /**
     * get the log level of a logger given its name.
     * @param name name of the logger whose logging level is desired
     * @return the logging level (the effective level is returned if the immediate level of this logger is not set).
     */
    @Override
    public String getLogLevel(String name) {
        if (name.trim().isEmpty()) {
            return null;
        }

        Logger logger = loggerByName(name);
        if (logger == null) {
            return null;
        }

        Level level = getCurrentLevel(logger);
        return String.valueOf(level);
    }

    /**
     * set the log level of a logger given its name.
     * @param name name of the logger
     * @param level the level to be set
     * @return true, if level was successfully set; false otherwise.
     */
    @Override
    public Boolean setLogLevel(String name, String level) {
        if (name.trim().isEmpty() || level.trim().isEmpty()) {
            return false;
        }

        Logger log = newLogger(name);
        if (log == null) {
            return false;
        } else {
            log.setLevel(Level.toLevel(level.toUpperCase(Locale.ROOT)));
            return true;
        }
    }

    private Level getCurrentLevel(Logger logger) {
        Level level = logger.getLevel();
        if (level == null) {
            return logger.getEffectiveLevel();
        } else {
            return level;
        }
    }

    private Logger loggerByName(String name) {
        if ("root".equals(name)) {
            return Logger.getRootLogger();
        } else {
            return LogManager.exists(name);
        }
    }

    private Logger newLogger(String name) {
        if ("root".equals(name)) {
            return Logger.getRootLogger();
        } else {
            return LogManager.getLogger(name);
        }
    }
}
