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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Writes errors and their context to application logs.
 */
public abstract class LogReporter<T> implements ErrorReporter<T> {

    private static final Logger log = LoggerFactory.getLogger(LogReporter.class);
    private static final Future<RecordMetadata> COMPLETED = CompletableFuture.completedFuture(null);

    private final ConnectorTaskId id;
    private final ConnectorConfig connConfig;
    private final ErrorHandlingMetrics errorHandlingMetrics;

    private LogReporter(ConnectorTaskId id, ConnectorConfig connConfig, ErrorHandlingMetrics errorHandlingMetrics) {
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
    public Future<RecordMetadata> report(ProcessingContext<T> context) {
        if (!connConfig.enableErrorLog()) {
            return COMPLETED;
        }

        if (!context.failed()) {
            return COMPLETED;
        }

        log.error(message(context), context.error());
        errorHandlingMetrics.recordErrorLogged();
        return COMPLETED;
    }

    // Visible for testing
    String message(ProcessingContext<T> context) {
        return String.format("Error encountered in task %s. %s", id,
                toString(context, connConfig.includeRecordDetailsInErrorLog()));
    }

    private String toString(ProcessingContext<T> context, boolean includeMessage) {
        StringBuilder builder = new StringBuilder();
        builder.append("Executing stage '");
        builder.append(context.stage().name());
        builder.append("' with class '");
        builder.append(context.executingClass() == null ? "null" : context.executingClass().getName());
        builder.append('\'');
        if (includeMessage) {
            appendMessage(builder, context.original());
        }
        builder.append('.');
        return builder.toString();
    }

    protected abstract void appendMessage(StringBuilder builder, T original);

    public static class Sink extends LogReporter<ConsumerRecord<byte[], byte[]>> {

        public Sink(ConnectorTaskId id, ConnectorConfig connConfig, ErrorHandlingMetrics errorHandlingMetrics) {
            super(id, connConfig, errorHandlingMetrics);
        }

        @Override
        protected void appendMessage(StringBuilder builder, ConsumerRecord<byte[], byte[]> msg) {
            builder.append(", where consumed record is ");
            builder.append("{topic='").append(msg.topic()).append('\'');
            builder.append(", partition=").append(msg.partition());
            builder.append(", offset=").append(msg.offset());
            if (msg.timestampType() == TimestampType.CREATE_TIME || msg.timestampType() == TimestampType.LOG_APPEND_TIME) {
                builder.append(", timestamp=").append(msg.timestamp());
                builder.append(", timestampType=").append(msg.timestampType());
            }
            builder.append("}");
        }
    }

    public static class Source extends LogReporter<SourceRecord> {

        public Source(ConnectorTaskId id, ConnectorConfig connConfig, ErrorHandlingMetrics errorHandlingMetrics) {
            super(id, connConfig, errorHandlingMetrics);
        }

        @Override
        protected void appendMessage(StringBuilder builder, SourceRecord original) {
            builder.append(", where source record is = ");
            builder.append(original);
        }
    }
}
