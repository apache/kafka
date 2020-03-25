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

import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Writes errors and their context to application logs.
 */
public class LogReporter implements ErrorReporter {

    private static final Logger log = LoggerFactory.getLogger(LogReporter.class);

    private final ConnectorTaskId id;
    private final ConnectorConfig connConfig;
    private final ErrorHandlingMetrics errorHandlingMetrics;

    public LogReporter(ConnectorTaskId id, ConnectorConfig connConfig, ErrorHandlingMetrics errorHandlingMetrics) {
        Objects.requireNonNull(id);
        Objects.requireNonNull(connConfig);
        Objects.requireNonNull(errorHandlingMetrics);

        this.id = id;
        this.connConfig = connConfig;
        this.errorHandlingMetrics = errorHandlingMetrics;
    }

    /**
     * Log error context.
     *
     * @param context the processing context.
     */
    @Override
    public void report(ProcessingContext context) {
        if (!connConfig.enableErrorLog()) {
            return;
        }

        if (!context.failed()) {
            return;
        }

        log.error(message(context), context.error());
        errorHandlingMetrics.recordErrorLogged();
    }

    // Visible for testing
    String message(ProcessingContext context) {
        return String.format("Error encountered in task %s. %s", String.valueOf(id),
                context.toString(connConfig.includeRecordDetailsInErrorLog()));
    }

}
