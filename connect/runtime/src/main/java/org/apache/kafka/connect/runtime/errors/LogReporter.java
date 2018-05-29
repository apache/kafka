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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Log error context with application logs.
 */
public class LogReporter implements ErrorReporter {

    private static final Logger log = LoggerFactory.getLogger(LogReporter.class);

    public static final String PREFIX = "errors.log.";

    public static final String LOG_ENABLE = "enable";
    public static final String LOG_ENABLE_DOC = "Log the error context along with the other application logs.";
    public static final boolean LOG_ENABLE_DEFAULT = false;

    public static final String LOG_INCLUDE_MESSAGES = "include.messages";
    public static final String LOG_INCLUDE_MESSAGES_DOC = "Include the Connect Record which failed to process in the log.";
    public static final boolean LOG_INCLUDE_MESSAGES_DEFAULT = false;

    private final ConnectorTaskId id;

    private LogReporterConfig config;
    private ErrorHandlingMetrics errorHandlingMetrics;

    public LogReporter(ConnectorTaskId id) {
        this.id = id;
    }

    static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(LOG_ENABLE, ConfigDef.Type.BOOLEAN, LOG_ENABLE_DEFAULT, ConfigDef.Importance.MEDIUM, LOG_ENABLE_DOC)
                .define(LOG_INCLUDE_MESSAGES, ConfigDef.Type.BOOLEAN, LOG_INCLUDE_MESSAGES_DEFAULT, ConfigDef.Importance.MEDIUM, LOG_INCLUDE_MESSAGES_DOC);
    }

    /**
     * Log error context.
     *
     * @param context the processing context.
     */
    public void report(ProcessingContext context) {
        if (!config.isEnabled()) {
            return;
        }

        if (!context.failed()) {
            return;
        }

        StringBuilder builder = message(context);
        log.error(builder.toString(), context.error());
        errorHandlingMetrics.recordErrorLogged();
    }

    @Override
    public void setMetrics(ErrorHandlingMetrics errorHandlingMetrics) {
        this.errorHandlingMetrics = errorHandlingMetrics;
    }

    // Visible for testing
    StringBuilder message(ProcessingContext context) {
        StringBuilder builder = new StringBuilder("Error encountered in task ");
        builder.append(id).append(" while performing ");
        builder.append(context.stage().name());
        builder.append(" operation with class '");
        builder.append(context.executingClass());
        builder.append('\'');
        if (config.canLogMessages() && context.sourceRecord() != null) {
            builder.append(", with record = ");
            builder.append(context.sourceRecord());
        } else if (config.canLogMessages() && context.consumerRecord() != null) {
            ConsumerRecord<byte[], byte[]> msg = context.consumerRecord();
            builder.append(", msg.topic='").append(msg.topic()).append('\'');
            builder.append(", msg.partition=").append(msg.partition());
            builder.append(", msg.offset=").append(msg.offset());
            builder.append(", msg.timestamp=").append(msg.timestamp());
            builder.append(", msg.timestampType=").append(msg.timestampType());
        }
        builder.append('.');
        return builder;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        config = new LogReporterConfig(configs);
    }

    private static class LogReporterConfig extends AbstractConfig {
        public LogReporterConfig(Map<?, ?> originals) {
            super(getConfigDef(), originals, true);
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
