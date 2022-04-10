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
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;
import org.slf4j.spi.LocationAwareLogger;

/**
 * This class provides a way to instrument loggers with a common context which can be used to
 * automatically enrich log messages. For example, in the KafkaConsumer, it is often useful to know
 * the groupId of the consumer, so this can be added to a context object which can then be passed to
 * all of the dependent components in order to build new loggers. This removes the need to manually
 * add the groupId to each message.
 * @see LogContext
 */
public abstract class AbstractLogContext {

    @FunctionalInterface
    public interface Context {
        String addContext(String message);
    }

    private static final Context EMPTY_CONTEXT = message -> message;

    private final Context context;

    protected AbstractLogContext(Context context) {
        this.context = context == null ? EMPTY_CONTEXT : context;
    }

    public Logger logger(Class<?> clazz) {
        Logger logger = LoggerFactory.getLogger(clazz);
        if (logger instanceof LocationAwareLogger) {
            return new LocationAwareKafkaLogger(context, (LocationAwareLogger) logger);
        } else {
            return new LocationIgnorantKafkaLogger(context, logger);
        }
    }

    private static abstract class AbstractKafkaLogger implements Logger {
        private final Context context;

        protected AbstractKafkaLogger(final Context context) {
            this.context = context;
        }

        protected String addContext(final String message) {
            return context.addContext(message);
        }
    }

    private static class LocationAwareKafkaLogger extends AbstractKafkaLogger {
        private final LocationAwareLogger logger;
        private final String fqcn;

        LocationAwareKafkaLogger(Context context, LocationAwareLogger logger) {
            super(context);
            this.logger = logger;
            this.fqcn = LocationAwareKafkaLogger.class.getName();
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
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, message, null, null);
            }
        }

        @Override
        public void trace(String format, Object arg) {
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void trace(String format, Object arg1, Object arg2) {
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void trace(String format, Object... args) {
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, format, args, null);
            }
        }

        @Override
        public void trace(String msg, Throwable t) {
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, msg, null, t);
            }
        }

        @Override
        public void trace(Marker marker, String msg) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, msg, null, null);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object... argArray) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, format, argArray, null);
            }
        }

        @Override
        public void trace(Marker marker, String msg, Throwable t) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, msg, null, t);
            }
        }

        @Override
        public void debug(String message) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, message, null, null);
            }
        }

        @Override
        public void debug(String format, Object arg) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void debug(String format, Object arg1, Object arg2) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void debug(String format, Object... args) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, format, args, null);
            }
        }

        @Override
        public void debug(String msg, Throwable t) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, msg, null, t);
            }
        }

        @Override
        public void debug(Marker marker, String msg) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, msg, null, null);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object... arguments) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, format, arguments, null);
            }
        }

        @Override
        public void debug(Marker marker, String msg, Throwable t) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, msg, null, t);
            }
        }

        @Override
        public void warn(String message) {
            writeLog(null, LocationAwareLogger.WARN_INT, message, null, null);
        }

        @Override
        public void warn(String format, Object arg) {
            writeLog(null, LocationAwareLogger.WARN_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void warn(String message, Object arg1, Object arg2) {
            writeLog(null, LocationAwareLogger.WARN_INT, message, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void warn(String format, Object... args) {
            writeLog(null, LocationAwareLogger.WARN_INT, format, args, null);
        }

        @Override
        public void warn(String msg, Throwable t) {
            writeLog(null, LocationAwareLogger.WARN_INT, msg, null, t);
        }

        @Override
        public void warn(Marker marker, String msg) {
            writeLog(marker, LocationAwareLogger.WARN_INT, msg, null, null);
        }

        @Override
        public void warn(Marker marker, String format, Object arg) {
            writeLog(marker, LocationAwareLogger.WARN_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void warn(Marker marker, String format, Object arg1, Object arg2) {
            writeLog(marker, LocationAwareLogger.WARN_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void warn(Marker marker, String format, Object... arguments) {
            writeLog(marker, LocationAwareLogger.WARN_INT, format, arguments, null);
        }

        @Override
        public void warn(Marker marker, String msg, Throwable t) {
            writeLog(marker, LocationAwareLogger.WARN_INT, msg, null, t);
        }

        @Override
        public void error(String message) {
            writeLog(null, LocationAwareLogger.ERROR_INT, message, null, null);
        }

        @Override
        public void error(String format, Object arg) {
            writeLog(null, LocationAwareLogger.ERROR_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void error(String format, Object arg1, Object arg2) {
            writeLog(null, LocationAwareLogger.ERROR_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void error(String format, Object... args) {
            writeLog(null, LocationAwareLogger.ERROR_INT, format, args, null);
        }

        @Override
        public void error(String msg, Throwable t) {
            writeLog(null, LocationAwareLogger.ERROR_INT, msg, null, t);
        }

        @Override
        public void error(Marker marker, String msg) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, msg, null, null);
        }

        @Override
        public void error(Marker marker, String format, Object arg) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void error(Marker marker, String format, Object arg1, Object arg2) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void error(Marker marker, String format, Object... arguments) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, format, arguments, null);
        }

        @Override
        public void error(Marker marker, String msg, Throwable t) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, msg, null, t);
        }

        @Override
        public void info(String msg) {
            writeLog(null, LocationAwareLogger.INFO_INT, msg, null, null);
        }

        @Override
        public void info(String format, Object arg) {
            writeLog(null, LocationAwareLogger.INFO_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void info(String format, Object arg1, Object arg2) {
            writeLog(null, LocationAwareLogger.INFO_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void info(String format, Object... args) {
            writeLog(null, LocationAwareLogger.INFO_INT, format, args, null);
        }

        @Override
        public void info(String msg, Throwable t) {
            writeLog(null, LocationAwareLogger.INFO_INT, msg, null, t);
        }

        @Override
        public void info(Marker marker, String msg) {
            writeLog(marker, LocationAwareLogger.INFO_INT, msg, null, null);
        }

        @Override
        public void info(Marker marker, String format, Object arg) {
            writeLog(marker, LocationAwareLogger.INFO_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void info(Marker marker, String format, Object arg1, Object arg2) {
            writeLog(marker, LocationAwareLogger.INFO_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void info(Marker marker, String format, Object... arguments) {
            writeLog(marker, LocationAwareLogger.INFO_INT, format, arguments, null);
        }

        @Override
        public void info(Marker marker, String msg, Throwable t) {
            writeLog(marker, LocationAwareLogger.INFO_INT, msg, null, t);
        }

        private void writeLog(Marker marker, int level, String format, Object[] args, Throwable exception) {
            String message = format;
            if (args != null && args.length > 0) {
                FormattingTuple formatted = MessageFormatter.arrayFormat(format, args);
                if (exception == null && formatted.getThrowable() != null) {
                    exception = formatted.getThrowable();
                }
                message = formatted.getMessage();
            }
            logger.log(marker, fqcn, level, addContext(message), null, exception);
        }
    }

    private static class LocationIgnorantKafkaLogger extends AbstractKafkaLogger {
        private final Logger logger;

        LocationIgnorantKafkaLogger(Context context, Logger logger) {
            super(context);
            this.logger = logger;
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
            if (logger.isTraceEnabled()) {
                logger.trace(addContext(message));
            }
        }

        @Override
        public void trace(String message, Object arg) {
            if (logger.isTraceEnabled()) {
                logger.trace(addContext(message), arg);
            }
        }

        @Override
        public void trace(String message, Object arg1, Object arg2) {
            if (logger.isTraceEnabled()) {
                logger.trace(addContext(message), arg1, arg2);
            }
        }

        @Override
        public void trace(String message, Object... args) {
            if (logger.isTraceEnabled()) {
                logger.trace(addContext(message), args);
            }
        }

        @Override
        public void trace(String msg, Throwable t) {
            if (logger.isTraceEnabled()) {
                logger.trace(addContext(msg), t);
            }
        }

        @Override
        public void trace(Marker marker, String msg) {
            if (logger.isTraceEnabled()) {
                logger.trace(marker, addContext(msg));
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg) {
            if (logger.isTraceEnabled()) {
                logger.trace(marker, addContext(format), arg);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isTraceEnabled()) {
                logger.trace(marker, addContext(format), arg1, arg2);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object... argArray) {
            if (logger.isTraceEnabled()) {
                logger.trace(marker, addContext(format), argArray);
            }
        }

        @Override
        public void trace(Marker marker, String msg, Throwable t) {
            if (logger.isTraceEnabled()) {
                logger.trace(marker, addContext(msg), t);
            }
        }

        @Override
        public void debug(String message) {
            if (logger.isDebugEnabled()) {
                logger.debug(addContext(message));
            }
        }

        @Override
        public void debug(String message, Object arg) {
            if (logger.isDebugEnabled()) {
                logger.debug(addContext(message), arg);
            }
        }

        @Override
        public void debug(String message, Object arg1, Object arg2) {
            if (logger.isDebugEnabled()) {
                logger.debug(addContext(message), arg1, arg2);
            }
        }

        @Override
        public void debug(String message, Object... args) {
            if (logger.isDebugEnabled()) {
                logger.debug(addContext(message), args);
            }
        }

        @Override
        public void debug(String msg, Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug(addContext(msg), t);
            }
        }

        @Override
        public void debug(Marker marker, String msg) {
            if (logger.isDebugEnabled()) {
                logger.debug(marker, addContext(msg));
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg) {
            if (logger.isDebugEnabled()) {
                logger.debug(marker, addContext(format), arg);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isDebugEnabled()) {
                logger.debug(marker, addContext(format), arg1, arg2);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object... arguments) {
            if (logger.isDebugEnabled()) {
                logger.debug(marker, addContext(format), arguments);
            }
        }

        @Override
        public void debug(Marker marker, String msg, Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug(marker, addContext(msg), t);
            }
        }

        @Override
        public void warn(String message) {
            logger.warn(addContext(message));
        }

        @Override
        public void warn(String message, Object arg) {
            logger.warn(addContext(message), arg);
        }

        @Override
        public void warn(String message, Object arg1, Object arg2) {
            logger.warn(addContext(message), arg1, arg2);
        }

        @Override
        public void warn(String message, Object... args) {
            logger.warn(addContext(message), args);
        }

        @Override
        public void warn(String msg, Throwable t) {
            logger.warn(addContext(msg), t);
        }

        @Override
        public void warn(Marker marker, String msg) {
            logger.warn(marker, addContext(msg));
        }

        @Override
        public void warn(Marker marker, String format, Object arg) {
            logger.warn(marker, addContext(format), arg);
        }

        @Override
        public void warn(Marker marker, String format, Object arg1, Object arg2) {
            logger.warn(marker, addContext(format), arg1, arg2);
        }

        @Override
        public void warn(Marker marker, String format, Object... arguments) {
            logger.warn(marker, addContext(format), arguments);
        }

        @Override
        public void warn(Marker marker, String msg, Throwable t) {
            logger.warn(marker, addContext(msg), t);
        }

        @Override
        public void error(String message) {
            logger.error(addContext(message));
        }

        @Override
        public void error(String message, Object arg) {
            logger.error(addContext(message), arg);
        }

        @Override
        public void error(String message, Object arg1, Object arg2) {
            logger.error(addContext(message), arg1, arg2);
        }

        @Override
        public void error(String message, Object... args) {
            logger.error(addContext(message), args);
        }

        @Override
        public void error(String msg, Throwable t) {
            logger.error(addContext(msg), t);
        }

        @Override
        public void error(Marker marker, String msg) {
            logger.error(marker, addContext(msg));
        }

        @Override
        public void error(Marker marker, String format, Object arg) {
            logger.error(marker, addContext(format), arg);
        }

        @Override
        public void error(Marker marker, String format, Object arg1, Object arg2) {
            logger.error(marker, addContext(format), arg1, arg2);
        }

        @Override
        public void error(Marker marker, String format, Object... arguments) {
            logger.error(marker, addContext(format), arguments);
        }

        @Override
        public void error(Marker marker, String msg, Throwable t) {
            logger.error(marker, addContext(msg), t);
        }

        @Override
        public void info(String message) {
            logger.info(addContext(message));
        }

        @Override
        public void info(String message, Object arg) {
            logger.info(addContext(message), arg);
        }

        @Override
        public void info(String message, Object arg1, Object arg2) {
            logger.info(addContext(message), arg1, arg2);
        }

        @Override
        public void info(String message, Object... args) {
            logger.info(addContext(message), args);
        }

        @Override
        public void info(String msg, Throwable t) {
            logger.info(addContext(msg), t);
        }

        @Override
        public void info(Marker marker, String msg) {
            logger.info(marker, addContext(msg));
        }

        @Override
        public void info(Marker marker, String format, Object arg) {
            logger.info(marker, addContext(format), arg);
        }

        @Override
        public void info(Marker marker, String format, Object arg1, Object arg2) {
            logger.info(marker, addContext(format), arg1, arg2);
        }

        @Override
        public void info(Marker marker, String format, Object... arguments) {
            logger.info(marker, addContext(format), arguments);
        }

        @Override
        public void info(Marker marker, String msg, Throwable t) {
            logger.info(marker, addContext(msg), t);
        }

    }

}
