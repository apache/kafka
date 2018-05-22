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
package org.apache.kafka.connect.runtime.errors.impl;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogReporter extends ErrorReporter {

    private static final Logger log = LoggerFactory.getLogger(LogReporter.class);

    public static final String LOG_INCLUDE_CONFIGS = "log.include.configs";
    public static final String LOG_INCLUDE_CONFIGS_DOC = "Include the (worker, connector) configs in the log.";
    public static final boolean LOG_INCLUDE_CONFIGS_DEFAULT = false;

    public static final String LOG_INCLUDE_MESSAGES = "log.include.messages";
    public static final String LOG_INCLUDE_MESSAGES_DOC = "Include the Connect Record which failed to process in the log.";
    public static final boolean LOG_INCLUDE_MESSAGES_DEFAULT = false;

    private LogReporterConfig config;

    static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(LOG_INCLUDE_CONFIGS, ConfigDef.Type.BOOLEAN, LOG_INCLUDE_CONFIGS_DEFAULT, ConfigDef.Importance.HIGH, LOG_INCLUDE_CONFIGS_DOC)
                .define(LOG_INCLUDE_MESSAGES, ConfigDef.Type.BOOLEAN, LOG_INCLUDE_MESSAGES_DEFAULT, ConfigDef.Importance.HIGH, LOG_INCLUDE_MESSAGES_DOC);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        config = new LogReporterConfig(configs);
    }

    @Override
    public void report(ProcessingContext context) {
        log.info("Error processing record. Context={}", createLogString(context));
    }

    public String createLogString(ProcessingContext context) {
        return String.valueOf(context.toStruct());
    }

    static class LogReporterConfig extends AbstractConfig {
        public LogReporterConfig(Map<?, ?> originals) {
            super(getConfigDef(), originals);
        }

        public boolean includeConfigs() {
            return getBoolean(LOG_INCLUDE_CONFIGS);
        }

        public boolean includeMessages() {
            return getBoolean(LOG_INCLUDE_MESSAGES);
        }
    }
}
