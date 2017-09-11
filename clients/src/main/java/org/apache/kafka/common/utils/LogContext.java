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
import org.slf4j.Marker;

/**
 * This class provides a way to instrument loggers with a common context which can be used to
 * automatically enrich log messages. For example, in the KafkaConsumer, it is often useful to know
 * the groupId of the consumer, so this can be added to a context object which can then be passed to
 * all of the dependent components in order to build new loggers. This removes the need to manually
 * add the groupId to each message.
 */
public class LogContext {

    private final String logPrefix;

    public LogContext(String logPrefix) {
        this.logPrefix = logPrefix == null ? "" : logPrefix;
    }

    public LogContext() {
        this("");
    }

    public Logger logger(Class<?> clazz) {
        return new KafkaLogger(clazz, logPrefix);
    }

    public String logPrefix() {
        return logPrefix;
    }

    private static class KafkaLogger implements Logger {
        private final Logger logger;
        private final String logPrefix;

        public KafkaLogger(Class<?> clazz, String logPrefix) {
            this.logger = LoggerFactory.getLogger(clazz);
            this.logPrefix = logPrefix;
        }

        @Override
        public String getName() {
            return logger.getName();
        }

        @Override
        public boolean isTraceEnabled() {
            return logger.isTraceEnabled();
        }

        @Override
        public boolean isTraceEnabled(Marker marker) {
            return logger.isTraceEnabled(marker);
        }

        @Override
        public boolean isDebugEnabled() {
            return logger.isDebugEnabled();
        }

        @Override
        public boolean isDebugEnabled(Marker marker) {
            return logger.isDebugEnabled(marker);
        }

        @Override
        public boolean isInfoEnabled() {
            return logger.isInfoEnabled();
        }

        @Override
        public boolean isInfoEnabled(Marker marker) {
            return logger.isInfoEnabled(marker);
        }

        @Override
        public boolean isWarnEnabled() {
            return logger.isWarnEnabled();
        }

        @Override
        public boolean isWarnEnabled(Marker marker) {
            return logger.isWarnEnabled(marker);
        }

        @Override
        public boolean isErrorEnabled() {
            return logger.isErrorEnabled();
        }

        @Override
        public boolean isErrorEnabled(Marker marker) {
            return logger.isErrorEnabled(marker);
        }

        @Override
        public void trace(String message) {
            if (logger.isTraceEnabled())
                logger.trace(logPrefix + message);
        }

        @Override
        public void trace(String message, Object arg) {
            if (logger.isTraceEnabled())
                logger.trace(logPrefix + message, arg);
        }

        @Override
        public void trace(String message, Object arg1, Object arg2) {
            if (logger.isTraceEnabled())
                logger.trace(logPrefix + message, arg1, arg2);
        }

        @Override
        public void trace(String message, Object... args) {
            if (logger.isTraceEnabled())
                logger.trace(logPrefix + message, args);
        }

        @Override
        public void trace(String msg, Throwable t) {
            if (logger.isTraceEnabled())
                logger.trace(logPrefix + msg, t);
        }

        @Override
        public void trace(Marker marker, String msg) {
            if (logger.isTraceEnabled())
                logger.trace(marker, logPrefix + msg);
        }

        @Override
        public void trace(Marker marker, String format, Object arg) {
            if (logger.isTraceEnabled())
                logger.trace(marker, logPrefix + format, arg);
        }

        @Override
        public void trace(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isTraceEnabled())
                logger.trace(marker, logPrefix + format, arg1, arg2);
        }

        @Override
        public void trace(Marker marker, String format, Object... argArray) {
            if (logger.isTraceEnabled())
                logger.trace(marker, logPrefix + format, argArray);
        }

        @Override
        public void trace(Marker marker, String msg, Throwable t) {
            if (logger.isTraceEnabled())
                logger.trace(marker, logPrefix + msg, t);
        }

        @Override
        public void debug(String message) {
            if (logger.isDebugEnabled())
                logger.debug(logPrefix + message);
        }

        @Override
        public void debug(String message, Object arg) {
            if (logger.isDebugEnabled())
                logger.debug(logPrefix + message, arg);
        }

        @Override
        public void debug(String message, Object arg1, Object arg2) {
            if (logger.isDebugEnabled())
                logger.debug(logPrefix + message, arg1, arg2);
        }

        @Override
        public void debug(String message, Object... args) {
            if (logger.isDebugEnabled())
                logger.debug(logPrefix + message, args);
        }

        @Override
        public void debug(String msg, Throwable t) {
            if (logger.isDebugEnabled())
                logger.debug(logPrefix + msg, t);
        }

        @Override
        public void debug(Marker marker, String msg) {
            if (logger.isDebugEnabled())
                logger.debug(marker, logPrefix + msg);
        }

        @Override
        public void debug(Marker marker, String format, Object arg) {
            if (logger.isDebugEnabled())
                logger.debug(marker, logPrefix + format, arg);
        }

        @Override
        public void debug(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isDebugEnabled())
                logger.debug(marker, logPrefix + format, arg1, arg2);
        }

        @Override
        public void debug(Marker marker, String format, Object... arguments) {
            if (logger.isDebugEnabled())
                logger.debug(marker, logPrefix + format, arguments);
        }

        @Override
        public void debug(Marker marker, String msg, Throwable t) {
            if (logger.isDebugEnabled())
                logger.debug(marker, logPrefix + msg, t);
        }

        @Override
        public void warn(String message) {
            logger.warn(logPrefix + message);
        }

        @Override
        public void warn(String message, Object arg) {
            logger.warn(logPrefix + message, arg);
        }

        @Override
        public void warn(String message, Object arg1, Object arg2) {
            logger.warn(logPrefix + message, arg1, arg2);
        }

        @Override
        public void warn(String message, Object... args) {
            logger.warn(logPrefix + message, args);
        }

        @Override
        public void warn(String msg, Throwable t) {
            logger.warn(logPrefix + msg, t);
        }

        @Override
        public void warn(Marker marker, String msg) {
            logger.warn(marker, logPrefix + msg);
        }

        @Override
        public void warn(Marker marker, String format, Object arg) {
            logger.warn(marker, logPrefix + format, arg);
        }

        @Override
        public void warn(Marker marker, String format, Object arg1, Object arg2) {
            logger.warn(marker, logPrefix + format, arg1, arg2);
        }

        @Override
        public void warn(Marker marker, String format, Object... arguments) {
            logger.warn(marker, logPrefix + format, arguments);
        }

        @Override
        public void warn(Marker marker, String msg, Throwable t) {
            logger.warn(marker, logPrefix + msg, t);
        }

        @Override
        public void error(String message) {
            logger.error(logPrefix + message);
        }

        @Override
        public void error(String message, Object arg) {
            logger.error(logPrefix + message, arg);
        }

        @Override
        public void error(String message, Object arg1, Object arg2) {
            logger.error(logPrefix + message, arg1, arg2);
        }

        @Override
        public void error(String message, Object... args) {
            logger.error(logPrefix + message, args);
        }

        @Override
        public void error(String msg, Throwable t) {
            logger.error(logPrefix + msg, t);
        }

        @Override
        public void error(Marker marker, String msg) {
            logger.error(marker, logPrefix + msg);
        }

        @Override
        public void error(Marker marker, String format, Object arg) {
            logger.error(marker, logPrefix + format, arg);
        }

        @Override
        public void error(Marker marker, String format, Object arg1, Object arg2) {
            logger.error(marker, logPrefix + format, arg1, arg2);
        }

        @Override
        public void error(Marker marker, String format, Object... arguments) {
            logger.error(marker, logPrefix + format, arguments);
        }

        @Override
        public void error(Marker marker, String msg, Throwable t) {
            logger.error(marker, logPrefix + msg, t);
        }

        @Override
        public void info(String message) {
            logger.info(logPrefix + message);
        }

        @Override
        public void info(String message, Object arg) {
            logger.info(logPrefix + message, arg);
        }

        @Override
        public void info(String message, Object arg1, Object arg2) {
            logger.info(logPrefix + message, arg1, arg2);
        }

        @Override
        public void info(String message, Object... args) {
            logger.info(logPrefix + message, args);
        }

        @Override
        public void info(String msg, Throwable t) {
            logger.info(logPrefix + msg, t);
        }

        @Override
        public void info(Marker marker, String msg) {
            logger.info(marker, logPrefix + msg);
        }

        @Override
        public void info(Marker marker, String format, Object arg) {
            logger.info(marker, logPrefix + format, arg);
        }

        @Override
        public void info(Marker marker, String format, Object arg1, Object arg2) {
            logger.info(marker, logPrefix + format, arg1, arg2);
        }

        @Override
        public void info(Marker marker, String format, Object... arguments) {
            logger.info(marker, logPrefix + format, arguments);
        }

        @Override
        public void info(Marker marker, String msg, Throwable t) {
            logger.info(marker, logPrefix + msg, t);
        }

    }
}
