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
import org.apache.kafka.streams.processor.api.Record;

import java.util.Collections;
import java.util.List;

/**
 * An interface that allows user code to inspect a record that has failed processing
 */
public interface ProcessingExceptionHandler extends Configurable {

    /**
     * Inspect a record and the exception received
     *
     * @param context
     *     Processing context metadata.
     * @param record
     *     Record where the exception occurred.
     * @param exception
     *     The actual exception.
     *
     * @return Whether to continue or stop processing.
     * @deprecated Use {@link #handleError(ErrorHandlerContext, Record, Exception)} instead.
     */
    @Deprecated
    default ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
        throw new UnsupportedOperationException();
    };

    /**
     * Inspects a record and the exception received during processing.
     *
     * @param context
     *     Processing context metadata.
     * @param record
     *     Record where the exception occurred.
     * @param exception
     *     The actual exception.
     *
     * @return a {@link ProcessingExceptionResponse} object
     */
    default ProcessingExceptionResponse handleError(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
        return new ProcessingExceptionResponse(handle(context, record, exception), Collections.emptyList());
    }


    enum ProcessingHandlerResponse {
        /** Continue processing. */
        CONTINUE(1, "CONTINUE"),
        /** Fail processing. */
        FAIL(2, "FAIL");

        /**
         * An english description for the used option. This is for debugging only and may change.
         */
        public final String name;

        /**
         * The permanent and immutable id for the used option. This can't change ever.
         */
        public final int id;

        ProcessingHandlerResponse(final int id, final String name) {
            this.id = id;
            this.name = name;
        }
    }

    /**
     * Represents the result of handling a processing exception.
     * <p>
     * The {@code Response} class encapsulates a {@link ProcessingHandlerResponse},
     * indicating whether processing should continue or fail, along with an optional list of
     * {@link org.apache.kafka.clients.producer.ProducerRecord} instances to be sent to a dead letter queue.
     * </p>
     */
    class ProcessingExceptionResponse {

        private ProcessingHandlerResponse processingHandlerResponse;

        private List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords;

        /**
         * Constructs a new {@code ProcessingExceptionResponse} object.
         *
         * @param processingHandlerResponse the response indicating whether processing should continue or fail;
         *                                  must not be {@code null}.
         * @param deadLetterQueueRecords    the list of records to be sent to the dead letter queue; may be {@code null}.
         */
        private ProcessingExceptionResponse(final ProcessingHandlerResponse processingHandlerResponse,
                                            final List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords) {
            this.processingHandlerResponse = processingHandlerResponse;
            this.deadLetterQueueRecords = deadLetterQueueRecords;
        }

        /**
         * Creates a {@code ProcessingExceptionResponse} indicating that processing should fail.
         *
         * @param deadLetterQueueRecords the list of records to be sent to the dead letter queue; may be {@code null}.
         * @return a {@code ProcessingExceptionResponse} with a {@link ProcessingHandlerResponse#FAIL} status.
         */
        public static ProcessingExceptionResponse failProcessing(final List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords) {
            return new ProcessingExceptionResponse(ProcessingHandlerResponse.FAIL, deadLetterQueueRecords);
        }

        /**
         * Creates a {@code ProcessingExceptionResponse} indicating that processing should fail.
         *
         * @return a {@code ProcessingExceptionResponse} with a {@link ProcessingHandlerResponse#FAIL} status.
         */
        public static ProcessingExceptionResponse failProcessing() {
            return failProcessing(Collections.emptyList());
        }

        /**
         * Creates a {@code ProcessingExceptionResponse} indicating that processing should continue.
         *
         * @param deadLetterQueueRecords the list of records to be sent to the dead letter queue; may be {@code null}.
         * @return a {@code Response} with a {@link ProcessingHandlerResponse#CONTINUE} status.
         */
        public static ProcessingExceptionResponse continueProcessing(final List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords) {
            return new ProcessingExceptionResponse(ProcessingHandlerResponse.CONTINUE, deadLetterQueueRecords);
        }

        /**
         * Creates a {@code ProcessingExceptionResponse} indicating that processing should continue.
         *
         * @return a {@code ProcessingExceptionResponse} with a {@link ProcessingHandlerResponse#CONTINUE} status.
         */
        public static ProcessingExceptionResponse continueProcessing() {
            return continueProcessing(Collections.emptyList());
        }

        /**
         * Retrieves the processing handler response.
         *
         * @return the {@link ProcessingHandlerResponse} indicating whether processing should continue or fail.
         */
        public ProcessingHandlerResponse response() {
            return processingHandlerResponse;
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
