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

package org.apache.kafka.streams.errors.internals;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ErrorHandlerContext;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;

/**
 * {@code ExceptionHandlerUtils} Contains utilities method that could be used by all exception handlers
 */
public class ExceptionHandlerUtils {
    public static final String HEADER_ERRORS_EXCEPTION_NAME = "__streams.errors.exception";
    public static final String HEADER_ERRORS_STACKTRACE_NAME = "__streams.errors.stacktrace";
    public static final String HEADER_ERRORS_EXCEPTION_MESSAGE_NAME = "__streams.errors.message";
    public static final String HEADER_ERRORS_TOPIC_NAME = "__streams.errors.topic";
    public static final String HEADER_ERRORS_PARTITION_NAME = "__streams.errors.partition";
    public static final String HEADER_ERRORS_OFFSET_NAME = "__streams.errors.offset";


    public static boolean shouldBuildDeadLetterQueueRecord(final String deadLetterQueueTopicName) {
        return deadLetterQueueTopicName != null;
    }

    /**
     * If required, return Dead Letter Queue records for the provided exception
     *
     * @param key Serialized key for the records
     * @param value Serialized value for the records
     * @param context ErrorHandlerContext of the exception
     * @param exception Thrown exception
     * @return A list of Dead Letter Queue records to produce
     */
    public static List<ProducerRecord<byte[], byte[]>> maybeBuildDeadLetterQueueRecords(final String deadLetterQueueTopicName,
                                                                                 final byte[] key,
                                                                                 final byte[] value,
                                                                                 final ErrorHandlerContext context,
                                                                                 final Exception exception) {
        if (!shouldBuildDeadLetterQueueRecord(deadLetterQueueTopicName)) {
            return Collections.emptyList();
        }

        return Collections.singletonList(buildDeadLetterQueueRecord(deadLetterQueueTopicName, key, value, context, exception));
    }


    /**
     * Build dead letter queue record for the provided exception.
     *
     * @param key Serialized key for the record.
     * @param value Serialized value for the record.
     * @param context error handler context of the exception.
     * @return A dead letter queue record to produce.
     */
    public static ProducerRecord<byte[], byte[]> buildDeadLetterQueueRecord(final String deadLetterQueueTopicName,
                                                                     final byte[] key,
                                                                     final byte[] value,
                                                                     final ErrorHandlerContext context,
                                                                     final Exception e) {
        if (deadLetterQueueTopicName == null) {
            throw new InvalidConfigurationException(String.format("%s cannot be null while building dead letter queue record", StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG));
        }
        final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(deadLetterQueueTopicName, null, context.timestamp(), key, value);
        // Copy original headers from record that causes exception
        if (context.headers() != null) {
            for (final Header header : context.headers()) {
                producerRecord.headers().add(header);
            }
        }
        final StringWriter stackTraceStringWriter = new StringWriter();
        final PrintWriter stackTracePrintWriter = new PrintWriter(stackTraceStringWriter);
        e.printStackTrace(stackTracePrintWriter);

        try (final StringSerializer stringSerializer = new StringSerializer()) {
            producerRecord.headers().add(HEADER_ERRORS_EXCEPTION_NAME, stringSerializer.serialize(null, e.getClass().getName()));
            producerRecord.headers().add(HEADER_ERRORS_EXCEPTION_MESSAGE_NAME, stringSerializer.serialize(null, e.getMessage()));
            producerRecord.headers().add(HEADER_ERRORS_STACKTRACE_NAME, stringSerializer.serialize(null, stackTraceStringWriter.toString()));
            producerRecord.headers().add(HEADER_ERRORS_TOPIC_NAME, stringSerializer.serialize(null, context.topic()));
            producerRecord.headers().add(HEADER_ERRORS_PARTITION_NAME, stringSerializer.serialize(null, String.valueOf(context.partition())));
            producerRecord.headers().add(HEADER_ERRORS_OFFSET_NAME, stringSerializer.serialize(null, String.valueOf(context.offset())));
        }

        return producerRecord;
    }
}