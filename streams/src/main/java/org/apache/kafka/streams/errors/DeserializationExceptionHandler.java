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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.errors.internals.DefaultErrorHandlerContext;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collections;
import java.util.List;

/**
 * Interface that specifies how an exception from source node deserialization
 * (e.g., reading from Kafka) should be handled.
 */
public interface DeserializationExceptionHandler extends Configurable {

    /**
     * Inspect a record and the exception received.
     *
     * <p> Note, that the passed in {@link ProcessorContext} only allows to access metadata like the task ID.
     * However, it cannot be used to emit records via {@link ProcessorContext#forward(Object, Object)};
     * calling {@code forward()} (and some other methods) would result in a runtime exception.
     *
     * @param context
     *     Processor context.
     * @param record
     *     Record that failed deserialization.
     * @param exception
     *     The actual exception.
     *
     * @return Whether to continue or stop processing.
     *
     * @deprecated Since 3.9. Use {@link #handle(ErrorHandlerContext, ConsumerRecord, Exception)} instead.
     */
    @Deprecated
    default DeserializationHandlerResponse handle(final ProcessorContext context,
                                                  final ConsumerRecord<byte[], byte[]> record,
                                                  final Exception exception) {
        throw new UnsupportedOperationException();
    }

    /**
     * Inspect a record and the exception received.
     *
     * @param context
     *     Error handler context.
     * @param record
     *     Record that failed deserialization.
     * @param exception
     *     The actual exception.
     *
     * @return Whether to continue or stop processing.
     *
     * @deprecated Use {@link #handleError(ErrorHandlerContext, ConsumerRecord, Exception)} instead.
     */
    @Deprecated
    default DeserializationHandlerResponse handle(final ErrorHandlerContext context,
                                                  final ConsumerRecord<byte[], byte[]> record,
                                                  final Exception exception) {
        return handle(((DefaultErrorHandlerContext) context).processorContext().orElse(null), record, exception);
    }

    /**
     * Inspects a record and the exception received during deserialization.
     *
     * @param context
     *     Error handler context.
     * @param record
     *     Record that failed deserialization.
     * @param exception
     *     The actual exception.
     *
     * @return a {@link DeserializationExceptionResponse} object
     */
    default DeserializationExceptionResponse handleError(final ErrorHandlerContext context, final ConsumerRecord<byte[], byte[]> record, final Exception exception) {
        return new DeserializationExceptionResponse(handle(context, record, exception), Collections.emptyList());
    }
    /**
     * Enumeration that describes the response from the exception handler.
     */
    enum DeserializationHandlerResponse {
        /** Continue processing. */
        CONTINUE(0, "CONTINUE"),
        /** Fail processing. */
        FAIL(1, "FAIL");

        /**
         * An english description for the used option. This is for debugging only and may change.
         */
        public final String name;

        /**
         * The permanent and immutable id for the used option. This can't change ever.
         */
        public final int id;

        DeserializationHandlerResponse(final int id, final String name) {
            this.id = id;
            this.name = name;
        }
    }

    /**
     * Represents the result of handling a deserialization exception.
     * <p>
     * The {@code Response} class encapsulates a {@link ProcessingExceptionHandler.ProcessingHandlerResponse},
     * indicating whether processing should continue or fail, along with an optional list of
     * {@link ProducerRecord} instances to be sent to a dead letter queue.
     * </p>
     */
    class DeserializationExceptionResponse {

        private DeserializationHandlerResponse deserializationHandlerResponse;

        private List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords;

        /**
         * Constructs a new {@code DeserializationExceptionResponse} object.
         *
         * @param deserializationHandlerResponse the response indicating whether processing should continue or fail;
         *                                  must not be {@code null}.
         * @param deadLetterQueueRecords    the list of records to be sent to the dead letter queue; may be {@code null}.
         */
        private DeserializationExceptionResponse(final DeserializationHandlerResponse deserializationHandlerResponse,
                                                 final List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords) {
            this.deserializationHandlerResponse = deserializationHandlerResponse;
            this.deadLetterQueueRecords = deadLetterQueueRecords;
        }

        /**
         * Creates a {@code DeserializationExceptionResponse} indicating that processing should fail.
         *
         * @param deadLetterQueueRecords the list of records to be sent to the dead letter queue; may be {@code null}.
         * @return a {@code DeserializationExceptionResponse} with a {@link DeserializationHandlerResponse#FAIL} status.
         */
        public static DeserializationExceptionResponse failProcessing(final List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords) {
            return new DeserializationExceptionResponse(DeserializationHandlerResponse.FAIL, deadLetterQueueRecords);
        }

        /**
         * Creates a {@code DeserializationExceptionResponse} indicating that processing should fail.
         *
         * @return a {@code DeserializationExceptionResponse} with a {@link DeserializationHandlerResponse#FAIL} status.
         */
        public static DeserializationExceptionResponse failProcessing() {
            return failProcessing(Collections.emptyList());
        }

        /**
         * Creates a {@code DeserializationExceptionResponse} indicating that processing should continue.
         *
         * @param deadLetterQueueRecords the list of records to be sent to the dead letter queue; may be {@code null}.
         * @return a {@code DeserializationExceptionResponse} with a {@link DeserializationHandlerResponse#CONTINUE} status.
         */
        public static DeserializationExceptionResponse continueProcessing(final List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords) {
            return new DeserializationExceptionResponse(DeserializationHandlerResponse.CONTINUE, deadLetterQueueRecords);
        }

        /**
         * Creates a {@code DeserializationExceptionResponse} indicating that processing should continue.
         *
         * @return a {@code DeserializationExceptionResponse} with a {@link DeserializationHandlerResponse#CONTINUE} status.
         */
        public static DeserializationExceptionResponse continueProcessing() {
            return continueProcessing(Collections.emptyList());
        }

        /**
         * Retrieves the deserialization handler response.
         *
         * @return the {@link DeserializationHandlerResponse} indicating whether processing should continue or fail.
         */
        public DeserializationHandlerResponse response() {
            return deserializationHandlerResponse;
        }

        /**
         * Retrieves an unmodifiable list of records to be sent to the dead letter queue.
         * <p>
         * If the list is {@code null}, an empty list is returned.
         * </p>
         *
         * @return an unmodifiable list of {@link ProducerRecord} instances
         *         for the dead letter queue, or an empty list if no records are available.
         */
        public List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords() {
            if (deadLetterQueueRecords == null) {
                return Collections.emptyList();
            }
            return Collections.unmodifiableList(deadLetterQueueRecords);
        }
    }
}
