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
package org.apache.kafka.connect.runtime.errors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Writes errors and their context to application logs.
 */
public class LogReporter implements ErrorReporter {

    private static final Logger log = LoggerFactory.getLogger(LogReporter.class);

    public static final String PREFIX = "errors.log";

    public static final String LOG_ENABLE = "enable";
    public static final String LOG_ENABLE_DOC = "If true, log to application logs the errors and the information describing where they occurred.";
    public static final boolean LOG_ENABLE_DEFAULT = false;

    public static final String LOG_INCLUDE_MESSAGES = "include.messages";
    public static final String LOG_INCLUDE_MESSAGES_DOC = "If true, include in the application log the Connect key, value, and other details of records that resulted in errors and failures.";
    public static final boolean LOG_INCLUDE_MESSAGES_DEFAULT = false;

    private final ConnectorTaskId id;

    private LogReporterConfig config;
    private ErrorHandlingMetrics errorHandlingMetrics;

    public LogReporter(ConnectorTaskId id) {
        this.id = id;
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
                .define(LOG_ENABLE, ConfigDef.Type.BOOLEAN, LOG_ENABLE_DEFAULT, ConfigDef.Importance.MEDIUM, LOG_ENABLE_DOC)
                .define(LOG_INCLUDE_MESSAGES, ConfigDef.Type.BOOLEAN, LOG_INCLUDE_MESSAGES_DEFAULT, ConfigDef.Importance.MEDIUM, LOG_INCLUDE_MESSAGES_DOC);

    /**
     * Log error context.
     *
     * @param context the processing context.
     */
    @Override
    public void report(ProcessingContext context) {
        if (!config.isEnabled()) {
            return;
        }

        if (!context.failed()) {
            return;
        }

        log.error(message(context), context.error());
        errorHandlingMetrics.recordErrorLogged();
    }

    @Override
    public void metrics(ErrorHandlingMetrics errorHandlingMetrics) {
        this.errorHandlingMetrics = errorHandlingMetrics;
    }

    // Visible for testing
    String message(ProcessingContext context) {
        return String.format("Error encountered in task %s. %s", String.valueOf(id), context.toString(config.canLogMessages()));
    }

    @Override
    public void configure(Map<String, ?> configs) {
        config = new LogReporterConfig(configs);
    }

    private static class LogReporterConfig extends AbstractConfig {
        public LogReporterConfig(Map<?, ?> originals) {
            super(CONFIG_DEF, originals, true);
        }

        /**
         * @return true, if logging of error context is desired; false otherwise.
         */
        public boolean isEnabled() {
            return getBoolean(LOG_ENABLE);
        }

        /**
         * @return if false, the connect record which caused the exception is not logged.
         */
        public boolean canLogMessages() {
            return getBoolean(LOG_INCLUDE_MESSAGES);
        }
    }

}
