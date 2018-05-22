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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogReporter implements ErrorReporter {

    private static final Logger log = LoggerFactory.getLogger(LogReporter.class);

    public static final String PREFIX = "errors.log.";

    public static final String LOG_ENABLE = "enable";
    public static final String LOG_ENABLE_DOC = "Log the error context along with the other application logs.";
    public static final boolean LOG_ENABLE_DEFAULT = false;

    public static final String LOG_INCLUDE_MESSAGES = "include.messages";
    public static final String LOG_INCLUDE_MESSAGES_DOC = "Include the Connect Record which failed to process in the log.";
    public static final boolean LOG_INCLUDE_MESSAGES_DEFAULT = false;

    private LogReporterConfig config;

    static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(LOG_ENABLE, ConfigDef.Type.BOOLEAN, LOG_ENABLE_DEFAULT, ConfigDef.Importance.MEDIUM, LOG_ENABLE_DOC)
                .define(LOG_INCLUDE_MESSAGES, ConfigDef.Type.BOOLEAN, LOG_INCLUDE_MESSAGES_DEFAULT, ConfigDef.Importance.MEDIUM, LOG_INCLUDE_MESSAGES_DOC);
    }

    public void report(ProcessingContext context) {
        if (!config.isEnabled()) {
            return;
        }

        if (context.result().success()) {
            return;
        }

        StringBuilder builder = new StringBuilder("Error encountered while performing ");
        builder.append(context.stage().name());
        builder.append(" operation with class '");
        builder.append(context.executingClass());
        builder.append('\'');
        if (config.canLogMessages() && context.sourceRecord() != null) {
            builder.append(", with record = ");
            builder.append(context.sourceRecord());
        }
        builder.append('.');
        log.error(builder.toString(), context.result().error());
    }

    @Override
    public void configure(Map<String, ?> configs) {
        config = new LogReporterConfig(configs);
    }

    private static class LogReporterConfig extends AbstractConfig {
        public LogReporterConfig(Map<?, ?> originals) {
            super(getConfigDef(), originals, true);
        }

        public boolean isEnabled() {
            return getBoolean(LOG_ENABLE);
        }

        public boolean canLogMessages() {
            return getBoolean(LOG_INCLUDE_MESSAGES);
        }
    }

}
