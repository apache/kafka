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
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReporterFactory {

    private static final Logger log = LoggerFactory.getLogger(ReporterFactory.class);

    public static final String DLQ_ENABLE = "dlq.enable";
    public static final String DLQ_ENABLE_DOC = "Log the error context along with the other application logs.";
    public static final boolean DLQ_ENABLE_DEFAULT = false;

    public static final String LOG_ENABLE = "log.enable";
    public static final String LOG_ENABLE_DOC = "Log the error context along with the other application logs.";
    public static final boolean LOG_ENABLE_DEFAULT = false;

    static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(DLQ_ENABLE, ConfigDef.Type.BOOLEAN, DLQ_ENABLE_DEFAULT, ConfigDef.Importance.HIGH, DLQ_ENABLE_DOC)
                .define(LOG_ENABLE, ConfigDef.Type.BOOLEAN, LOG_ENABLE_DEFAULT, ConfigDef.Importance.HIGH, LOG_ENABLE_DOC);
    }

    public List<ErrorReporter> forConfig(Map<String, Object> workerProducerProps, ConnectorConfig connConfig) {
        Map<String, Object> configs = new HashMap<>();
        for (Map.Entry<String, Object> e: workerProducerProps.entrySet()) {
            configs.put(DLQReporter.DLQ_PRODUCER_PROPERTIES + "." + e.getKey(), e.getValue());
        }
        configs.putAll(connConfig.errorHandlerConfig());

        ReporterFactoryConfig config = new ReporterFactoryConfig(getConfigDef(), configs);
        List<ErrorReporter> reporters = new ArrayList<>(3);
        log.info("Adding metrics reporter for reporting errors");
        reporters.add(new ErrorMetricsReporter());
        if (config.isDlqReporterEnabled()) {
            log.info("Adding DLQ reporter for reporting errors");
            DLQReporter reporter = new DLQReporter();
            reporter.configure(configs);
            reporter.initialize();
            reporters.add(reporter);
        }
        if (config.isLogReporterEnabled()) {
            log.info("Adding Log reporter for reporting errors");
            LogReporter reporter = new LogReporter();
            reporter.configure(configs);
            reporter.initialize();
            reporters.add(reporter);
        }
        return reporters;
    }

    static class ReporterFactoryConfig extends AbstractConfig {

        public ReporterFactoryConfig(ConfigDef definition, Map<?, ?> originals) {
            super(definition, originals, false);
        }

        public boolean isLogReporterEnabled() {
            return getBoolean(LOG_ENABLE);
        }

        public boolean isDlqReporterEnabled() {
            return getBoolean(DLQ_ENABLE);
        }
    }
}
