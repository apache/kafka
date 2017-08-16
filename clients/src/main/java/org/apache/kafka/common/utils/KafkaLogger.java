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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaLogger {
    private final Logger logger;
    private final String logPrefix;

    public KafkaLogger(Class<?> clazz, String logPrefix) {
        this.logger = LoggerFactory.getLogger(clazz);
        this.logPrefix = logPrefix;
    }

    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    public void trace(String message) {
        if (logger.isTraceEnabled())
            logger.trace(logPrefix + message);
    }

    public void trace(String message, Object arg) {
        if (logger.isTraceEnabled())
            logger.trace(logPrefix + message, arg);
    }

    public void trace(String message, Object arg1, Object arg2) {
        if (logger.isTraceEnabled())
            logger.trace(logPrefix + message, arg1, arg2);
    }

    public void trace(String message, Object... args) {
        if (logger.isTraceEnabled())
            logger.trace(logPrefix + message, args);
    }

    public void debug(String message) {
        if (logger.isDebugEnabled())
            logger.debug(logPrefix + message);
    }

    public void debug(String message, Object arg) {
        if (logger.isDebugEnabled())
            logger.debug(logPrefix + message, arg);
    }

    public void debug(String message, Object arg1, Object arg2) {
        if (logger.isDebugEnabled())
            logger.debug(logPrefix + message, arg1, arg2);
    }
    
    public void debug(String message, Object... args) {
        if (logger.isDebugEnabled())
            logger.debug(logPrefix + message, args);
    }

    public void warn(String message) {
        logger.warn(logPrefix + message);
    }

    public void warn(String message, Object arg) {
        logger.warn(logPrefix + message, arg);
    }

    public void warn(String message, Object arg1, Object arg2) {
        logger.warn(logPrefix + message, arg1, arg2);
    }

    public void warn(String message, Object... args) {
        logger.warn(logPrefix + message, args);
    }

    public void error(String message) {
        logger.error(logPrefix + message);
    }

    public void error(String message, Object arg) {
        logger.error(logPrefix + message, arg);
    }

    public void error(String message, Object arg1, Object arg2) {
        logger.error(logPrefix + message, arg1, arg2);
    }

    public void error(String message, Object... args) {
        logger.error(logPrefix + message, args);
    }

    public void info(String message) {
        logger.info(logPrefix + message);
    }

    public void info(String message, Object arg) {
        logger.info(logPrefix + message, arg);
    }

    public void info(String message, Object arg1, Object arg2) {
        logger.info(logPrefix + message, arg1, arg2);
    }

    public void info(String message, Object... args) {
        logger.info(logPrefix + message, args);
    }

}
