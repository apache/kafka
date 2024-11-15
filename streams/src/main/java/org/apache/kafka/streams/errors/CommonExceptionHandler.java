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

package org.apache.kafka.streams.errors;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Map;

public class CommonExceptionHandler implements Configurable {
    protected String deadLetterQueueTopicName = null;
    public static final String HEADER_ERRORS_EXCEPTION_NAME = "__streams.errors.exception";
    public static final String HEADER_ERRORS_STACKTRACE_NAME = "__streams.errors.stacktrace";
    public static final String HEADER_ERRORS_EXCEPTION_MESSAGE_NAME = "__streams.errors.message";
    public static final String HEADER_ERRORS_TOPIC_NAME = "__streams.errors.topic";
    public static final String HEADER_ERRORS_PARTITION_NAME = "__streams.errors.partition";
    public static final String HEADER_ERRORS_OFFSET_NAME = "__streams.errors.offset";

    @Override
    public void configure(final Map<String, ?> configs) {
        if (configs.containsKey(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG)) {
            setDeadLetterQueueTopicName(String.valueOf(configs.get(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG)));
        }
    }

    public boolean shouldBuildDeadLetterQueueRecord() {
        return this.deadLetterQueueTopicName != null;
    }

    public Iterable<ProducerRecord<byte[], byte[]>> maybeBuildDeadLetterQueueRecords(final byte[] key,
                                                                                     final byte[] value,
                                                                                     final ErrorHandlerContext context,
                                                                                     final Exception exception) {
        if (!shouldBuildDeadLetterQueueRecord()) {
            return Collections.emptyList();
        }

        return Collections.singleton(buildDeadLetterQueueRecord(key, value, context, exception));
    }

    public ProducerRecord<byte[], byte[]> buildDeadLetterQueueRecord(final byte[] key,
                                                                     final byte[] value,
                                                                     final ErrorHandlerContext context,
                                                                     final Exception e) {
        if (deadLetterQueueTopicName == null) {
            throw new InvalidConfigurationException(String.format("%s can not be null while building DeadLetterQueue record", StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG));
        }
        final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(deadLetterQueueTopicName, null, context.timestamp(), key, value);
        final StringWriter stackStraceStringWriter = new StringWriter();
        final PrintWriter stackTracePrintWriter = new PrintWriter(stackStraceStringWriter);
        e.printStackTrace(stackTracePrintWriter);

        try (final StringSerializer stringSerializer = new StringSerializer()) {
            producerRecord.headers().add(HEADER_ERRORS_EXCEPTION_NAME, stringSerializer.serialize(null, e.toString()));
            producerRecord.headers().add(HEADER_ERRORS_EXCEPTION_MESSAGE_NAME, stringSerializer.serialize(null, e.getMessage()));
            producerRecord.headers().add(HEADER_ERRORS_STACKTRACE_NAME, stringSerializer.serialize(null, stackStraceStringWriter.toString()));
            producerRecord.headers().add(HEADER_ERRORS_TOPIC_NAME, stringSerializer.serialize(null, context.topic()));
            producerRecord.headers().add(HEADER_ERRORS_PARTITION_NAME, stringSerializer.serialize(null, String.valueOf(context.partition())));
            producerRecord.headers().add(HEADER_ERRORS_OFFSET_NAME, stringSerializer.serialize(null, String.valueOf(context.offset())));
        }

        return producerRecord;
    }

    public String getDeadLetterQueueTopicName() {
        return deadLetterQueueTopicName;
    }

    public void setDeadLetterQueueTopicName(final String deadLetterQueueTopicName) {
        this.deadLetterQueueTopicName = deadLetterQueueTopicName;
    }
}