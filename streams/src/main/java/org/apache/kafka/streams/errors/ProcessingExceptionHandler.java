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
     * @return a {@link Response} object
     */
    default Response handleError(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
        return new Response(ProcessingExceptionHandler.Result.from(handle(context, record, exception)), Collections.emptyList());
    }

    @Deprecated
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
     * Enumeration that describes the response from the exception handler.
     */
    enum Result {
        /** Resume processing. */
        RESUME(1, "RESUME"),
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

        Result(final int id, final String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * Converts the deprecated enum ProcessingHandlerResponse into the new Result enum.
         *
         * @param value the old DeserializationHandlerResponse enum value
         * @return a {@link ProcessingExceptionHandler.Result} enum value
         * @throws IllegalArgumentException if the provided value does not map to a valid {@link ProcessingExceptionHandler.Result}
         */
        private static ProcessingExceptionHandler.Result from(final ProcessingHandlerResponse value) {
            switch (value) {
                case FAIL:
                    return Result.FAIL;
                case CONTINUE:
                    return Result.RESUME;
                default:
                    throw new IllegalArgumentException("No Result enum found for old value: " + value);
            }
        }
    }

    /**
     * Represents the result of handling a processing exception.
     * <p>
     * The {@code Response} class encapsulates a {@link Result},
     * indicating whether processing should continue or fail, along with an optional list of
     * {@link org.apache.kafka.clients.producer.ProducerRecord} instances to be sent to a dead letter queue.
     * </p>
     */
    class Response {

        private Result result;

        private List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords;

        /**
         * Constructs a new {@code ProcessingExceptionResponse} object.
         *
         * @param result the result indicating whether processing should continue or fail;
         *                                  must not be {@code null}.
         * @param deadLetterQueueRecords    the list of records to be sent to the dead letter queue; may be {@code null}.
         */
        private Response(final Result result,
                         final List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords) {
            this.result = result;
            this.deadLetterQueueRecords = deadLetterQueueRecords;
        }

        /**
         * Creates a {@code Response} indicating that processing should fail.
         *
         * @param deadLetterQueueRecords the list of records to be sent to the dead letter queue; may be {@code null}.
         * @return a {@code Response} with a {@link ProcessingExceptionHandler.Result#FAIL} status.
         */
        public static Response fail(final List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords) {
            return new Response(Result.FAIL, deadLetterQueueRecords);
        }

        /**
         * Creates a {@code Response} indicating that processing should fail.
         *
         * @return a {@code Response} with a {@link ProcessingExceptionHandler.Result#FAIL} status.
         */
        public static Response fail() {
            return fail(Collections.emptyList());
        }

        /**
         * Creates a {@code Response} indicating that processing should continue.
         *
         * @param deadLetterQueueRecords the list of records to be sent to the dead letter queue; may be {@code null}.
         * @return a {@code Response} with a {@link ProcessingExceptionHandler.Result#RESUME} status.
         */
        public static Response resume(final List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords) {
            return new Response(Result.RESUME, deadLetterQueueRecords);
        }

        /**
         * Creates a {@code Response} indicating that processing should continue.
         *
         * @return a {@code Response} with a {@link ProcessingExceptionHandler.Result#RESUME} status.
         */
        public static Response resume() {
            return resume(Collections.emptyList());
        }

        /**
         * Retrieves the processing handler result.
         *
         * @return the {@link Result} indicating whether processing should continue or fail.
         */
        public Result result() {
            return result;
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
