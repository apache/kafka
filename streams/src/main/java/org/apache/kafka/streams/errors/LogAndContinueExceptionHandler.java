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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.StreamsConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.maybeBuildDeadLetterQueueRecords;

/**
 * Deserialization handler that logs a deserialization exception and then
 * signals the processing pipeline to continue processing more records.
 */
public class LogAndContinueExceptionHandler implements DeserializationExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(LogAndContinueExceptionHandler.class);
    private String deadLetterQueueTopic = null;

    @Override
    public Response handleError(final ErrorHandlerContext context,
                                final ConsumerRecord<byte[], byte[]> record,
                                final Exception exception) {
        log.warn(
            "Exception caught during Deserialization, taskId: {}, topic: {}, partition: {}, offset: {}",
            context.taskId(),
            record.topic(),
            record.partition(),
            record.offset(),
            exception
        );

        return Response.resume(maybeBuildDeadLetterQueueRecords(deadLetterQueueTopic, context.sourceRawKey(), context.sourceRawValue(), context, exception));
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        if (configs.get(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG) != null)
            deadLetterQueueTopic = String.valueOf(configs.get(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG));
    }
}
