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
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;

import static java.lang.String.format;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;

class RecordDeserializer {
    private final SourceNode sourceNode;
    private final DeserializationExceptionHandler deserializationExceptionHandler;

    RecordDeserializer(final SourceNode sourceNode,
                       final DeserializationExceptionHandler deserializationExceptionHandler) {
        this.sourceNode = sourceNode;
        this.deserializationExceptionHandler = deserializationExceptionHandler;
    }

    /**
     * @throws StreamsException if a deserialization exception occurs
     */
    @SuppressWarnings("deprecation")
    ConsumerRecord<Object, Object> deserialize(final ConsumerRecord<byte[], byte[]> rawRecord) {
        final Object key;
        try {
            key = sourceNode.deserializeKey(rawRecord.topic(), rawRecord.headers(), rawRecord.key());
        } catch (Exception e) {
            throw new StreamsException(format("Failed to deserialize key for record. topic=%s, partition=%d, offset=%d",
                                              rawRecord.topic(), rawRecord.partition(), rawRecord.offset()), e);
        }

        final Object value;
        try {
            value = sourceNode.deserializeValue(rawRecord.topic(), rawRecord.headers(), rawRecord.value());
        } catch (Exception e) {
            throw new StreamsException(format("Failed to deserialize value for record. topic=%s, partition=%d, offset=%d",
                                              rawRecord.topic(), rawRecord.partition(), rawRecord.offset()), e);
        }

        return new ConsumerRecord<>(rawRecord.topic(),
                                    rawRecord.partition(),
                                    rawRecord.offset(),
                                    rawRecord.timestamp(),
                                    TimestampType.CREATE_TIME,
                                    rawRecord.checksum(),
                                    rawRecord.serializedKeySize(),
                                    rawRecord.serializedValueSize(),
                                    key,
                                    value);

    }

    /**
     * @throws StreamsException if a deserialization error occurs and the deserialization callback returns
     * {@link DeserializationExceptionHandler.DeserializationHandlerResponse#FAIL FAIL} or throws an exception itself
     */
    ConsumerRecord<Object, Object> tryDeserialize(final ProcessorContext processorContext,
                                                  final ConsumerRecord<byte[], byte[]> rawRecord) {

        // catch and process if we have a deserialization handler
        try {
            return deserialize(rawRecord);
        } catch (final Exception deserializationException) {
            final DeserializationExceptionHandler.DeserializationHandlerResponse response;
            try {
                response = deserializationExceptionHandler.handle(processorContext, rawRecord, deserializationException);
            } catch (final RuntimeException fatalUserException) {
                throw new StreamsException("Fatal user code error in deserialization callback.", fatalUserException);
            }

            if (response == DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL) {
                throw new StreamsException("Deserialization exception handler is set to fail upon" +
                    " a deserialization error. If you would rather have the streaming pipeline" +
                    " continue after a deserialization error, please set the " +
                    DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG + " appropriately.",
                    deserializationException);
            } else {
                sourceNode.nodeMetrics.sourceNodeSkippedDueToDeserializationError.record();
            }
        }
        return null;
    }

    SourceNode sourceNode() {
        return sourceNode;
    }
}
