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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.internals.DefaultErrorHandlerContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;

import org.slf4j.Logger;

import java.util.Objects;

import static org.apache.kafka.streams.StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;

public class RecordDeserializer {
    private final Logger log;
    private final SourceNode<?, ?> sourceNode;
    private final Sensor droppedRecordsSensor;
    private final DeserializationExceptionHandler deserializationExceptionHandler;

    RecordDeserializer(final SourceNode<?, ?> sourceNode,
                       final DeserializationExceptionHandler deserializationExceptionHandler,
                       final LogContext logContext,
                       final Sensor droppedRecordsSensor) {
        this.sourceNode = sourceNode;
        this.deserializationExceptionHandler = deserializationExceptionHandler;
        this.log = logContext.logger(RecordDeserializer.class);
        this.droppedRecordsSensor = droppedRecordsSensor;
    }

    /**
     * @throws StreamsException if a deserialization error occurs and the deserialization callback returns
     *                          {@link DeserializationHandlerResponse#FAIL FAIL}
     *                          or throws an exception itself
     */
    ConsumerRecord<Object, Object> deserialize(final ProcessorContext<?, ?> processorContext,
                                               final ConsumerRecord<byte[], byte[]> rawRecord) {

        try {
            return new ConsumerRecord<>(
                rawRecord.topic(),
                rawRecord.partition(),
                rawRecord.offset(),
                rawRecord.timestamp(),
                TimestampType.CREATE_TIME,
                rawRecord.serializedKeySize(),
                rawRecord.serializedValueSize(),
                sourceNode.deserializeKey(rawRecord.topic(), rawRecord.headers(), rawRecord.key()),
                sourceNode.deserializeValue(rawRecord.topic(), rawRecord.headers(), rawRecord.value()),
                rawRecord.headers(),
                rawRecord.leaderEpoch()
            );
        } catch (final RuntimeException deserializationException) {
            handleDeserializationFailure(deserializationExceptionHandler, processorContext, deserializationException, rawRecord, log, droppedRecordsSensor, sourceNode().name());
            return null; //  'handleDeserializationFailure' would either throw or swallow -- if we swallow we need to skip the record by returning 'null'
        }
    }

    public static void handleDeserializationFailure(final DeserializationExceptionHandler deserializationExceptionHandler,
                                                    final ProcessorContext<?, ?> processorContext,
                                                    final RuntimeException deserializationException,
                                                    final ConsumerRecord<byte[], byte[]> rawRecord,
                                                    final Logger log,
                                                    final Sensor droppedRecordsSensor,
                                                    final String sourceNodeName) {

        final DefaultErrorHandlerContext errorHandlerContext = new DefaultErrorHandlerContext(
            (InternalProcessorContext<?, ?>) processorContext,
            rawRecord.topic(),
            rawRecord.partition(),
            rawRecord.offset(),
            rawRecord.headers(),
            sourceNodeName,
            processorContext.taskId(),
            rawRecord.timestamp());

        final DeserializationHandlerResponse response;
        try {
            response = Objects.requireNonNull(
                deserializationExceptionHandler.handle(errorHandlerContext, rawRecord, deserializationException),
                "Invalid DeserializationExceptionHandler response."
            );
        } catch (final RuntimeException fatalUserException) {
            log.error(
                "Deserialization error callback failed after deserialization error for record {}",
                rawRecord,
                deserializationException
            );
            throw new StreamsException("Fatal user code error in deserialization error callback", fatalUserException);
        }

        if (response == DeserializationHandlerResponse.FAIL) {
            throw new StreamsException("Deserialization exception handler is set to fail upon" +
                " a deserialization error. If you would rather have the streaming pipeline" +
                " continue after a deserialization error, please set the " +
                DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG + " appropriately.",
                deserializationException);
        } else {
            log.warn(
                "Skipping record due to deserialization error. topic=[{}] partition=[{}] offset=[{}]",
                rawRecord.topic(),
                rawRecord.partition(),
                rawRecord.offset(),
                deserializationException
            );
            droppedRecordsSensor.record();
        }
    }


    SourceNode<?, ?> sourceNode() {
        return sourceNode;
    }
}
