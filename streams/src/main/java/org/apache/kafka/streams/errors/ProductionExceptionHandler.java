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

import java.util.Collections;
import java.util.List;

/**
 * Interface that specifies how an exception when attempting to produce a result to
 * Kafka should be handled.
 */
public interface ProductionExceptionHandler extends Configurable {
    /**
     * Inspect a record that we attempted to produce, and the exception that resulted
     * from attempting to produce it and determine to continue or stop processing.
     *
     * @param record
     *     The record that failed to produce.
     * @param exception
     *     The exception that occurred during production.
     *
     * @return Whether to continue or stop processing, or retry the failed operation.
     *
     * @deprecated Since 3.9. Use {@link #handle(ErrorHandlerContext, ProducerRecord, Exception)} instead.
     */
    @Deprecated
    default ProductionExceptionHandlerResponse handle(final ProducerRecord<byte[], byte[]> record,
                                                      final Exception exception) {
        throw new UnsupportedOperationException();
    }

    /**
     * Inspect a record that we attempted to produce, and the exception that resulted
     * from attempting to produce it and determine to continue or stop processing.
     *
     * @param context
     *     The error handler context metadata.
     * @param record
     *     The record that failed to produce.
     * @param exception
     *     The exception that occurred during production.
     *
     * @return Whether to continue or stop processing, or retry the failed operation.
     * @deprecated Use {@link #handleError(ErrorHandlerContext, ProducerRecord, Exception)} instead.
     */
    @Deprecated
    default ProductionExceptionHandlerResponse handle(final ErrorHandlerContext context,
                                                      final ProducerRecord<byte[], byte[]> record,
                                                      final Exception exception) {
        throw new UnsupportedOperationException();
    }

    /**
     * Inspect a record that we attempted to produce, and the exception that resulted
     * from attempting to produce it and determine to continue or stop processing.
     *
     * @param context
     *     The error handler context metadata.
     * @param record
     *     The record that failed to produce.
     * @param exception
     *     The exception that occurred during production.
     *
     * @return a {@link Response} object
     */
    default Response handleError(final ErrorHandlerContext context,
                                 final ProducerRecord<byte[], byte[]> record,
                                 final Exception exception) {
        return new Response(Result.from(handle(context, record, exception)), Collections.emptyList());
    }

    /**
     * Handles serialization exception and determine if the process should continue. The default implementation is to
     * fail the process.
     *
     * @param record
     *     The record that failed to serialize.
     * @param exception
     *     The exception that occurred during serialization.
     *
     * @return Whether to continue or stop processing, or retry the failed operation.
     *
     * @deprecated Since 3.9. Use {@link #handleSerializationException(ErrorHandlerContext, ProducerRecord, Exception, SerializationExceptionOrigin)} instead.
     */
    @SuppressWarnings({"rawtypes", "unused"})
    @Deprecated
    default ProductionExceptionHandlerResponse handleSerializationException(final ProducerRecord record,
                                                                            final Exception exception) {
        return ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL;
    }

    /**
     * Handles serialization exception and determine if the process should continue. The default implementation is to
     * fail the process.
     *
     * @param context
     *     The error handler context metadata.
     * @param record
     *     The record that failed to serialize.
     * @param exception
     *     The exception that occurred during serialization.
     * @param origin
     *     The origin of the serialization exception.
     *
     * @return Whether to continue or stop processing, or retry the failed operation.
     *
     * @deprecated Use {@link #handleSerializationError(ErrorHandlerContext, ProducerRecord, Exception, SerializationExceptionOrigin)} instead.
     */
    @SuppressWarnings("rawtypes")
    @Deprecated
    default ProductionExceptionHandlerResponse handleSerializationException(final ErrorHandlerContext context,
                                                                            final ProducerRecord record,
                                                                            final Exception exception,
                                                                            final SerializationExceptionOrigin origin) {
        return handleSerializationException(record, exception);
    }

    /**
     * Handles serialization exception and determine if the process should continue. The default implementation is to
     * fail the process.
     *
     * @param context
     *     The error handler context metadata.
     * @param record
     *     The record that failed to serialize.
     * @param exception
     *     The exception that occurred during serialization.
     * @param origin
     *     The origin of the serialization exception.
     *
     * @return a {@link Response} object
     */
    @SuppressWarnings("rawtypes")
    default Response handleSerializationError(final ErrorHandlerContext context,
                                              final ProducerRecord record,
                                              final Exception exception,
                                              final SerializationExceptionOrigin origin) {
        return new Response(Result.from(handleSerializationException(context, record, exception, origin)), Collections.emptyList());
    }

    @Deprecated
    enum ProductionExceptionHandlerResponse {
        /** Continue processing.
         *
         * <p> For this case, output records which could not be written successfully are lost.
         * Use this option only if you can tolerate data loss.
         */
        CONTINUE(0, "CONTINUE"),
        /** Fail processing.
         *
         * <p> Kafka Streams will raise an exception and the {@code StreamsThread} will fail.
         * No offsets (for {@link org.apache.kafka.streams.StreamsConfig#AT_LEAST_ONCE at-least-once}) or transactions
         * (for {@link org.apache.kafka.streams.StreamsConfig#EXACTLY_ONCE_V2 exactly-once}) will be committed.
         */
        FAIL(1, "FAIL"),
        /** Retry the failed operation.
         *
         * <p> Retrying might imply that a {@link TaskCorruptedException} exception is thrown, and that the retry
         * is started from the last committed offset.
         *
         * <p> <b>NOTE:</b> {@code RETRY} is only a valid return value for
         * {@link org.apache.kafka.common.errors.RetriableException retriable exceptions}.
         * If {@code RETRY} is returned for a non-retriable exception it will be interpreted as {@link #FAIL}.
         */
        RETRY(2, "RETRY");

        /**
         * An english description for the used option. This is for debugging only and may change.
         */
        public final String name;

        /**
         * The permanent and immutable id for the used option. This can't change ever.
         */
        public final int id;

        ProductionExceptionHandlerResponse(final int id,
                                           final String name) {
            this.id = id;
            this.name = name;
        }
    }

    /**
     * Enumeration that describes the response from the exception handler.
     */
    enum Result {
        /** Resume processing.
         *
         * <p> For this case, output records which could not be written successfully are lost.
         * Use this option only if you can tolerate data loss.
         */
        RESUME(0, "RESUME"),
        /** Fail processing.
         *
         * <p> Kafka Streams will raise an exception and the {@code StreamsThread} will fail.
         * No offsets (for {@link org.apache.kafka.streams.StreamsConfig#AT_LEAST_ONCE at-least-once}) or transactions
         * (for {@link org.apache.kafka.streams.StreamsConfig#EXACTLY_ONCE_V2 exactly-once}) will be committed.
         */
        FAIL(1, "FAIL"),
        /** Retry the failed operation.
         *
         * <p> Retrying might imply that a {@link TaskCorruptedException} exception is thrown, and that the retry
         * is started from the last committed offset.
         *
         * <p> <b>NOTE:</b> {@code RETRY} is only a valid return value for
         * {@link org.apache.kafka.common.errors.RetriableException retriable exceptions}.
         * If {@code RETRY} is returned for a non-retriable exception it will be interpreted as {@link #FAIL}.
         */
        RETRY(2, "RETRY");

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
         * Converts the deprecated enum ProductionExceptionHandlerResponse into the new Result enum.
         *
         * @param value the old ProductionExceptionHandlerResponse enum value
         * @return a {@link ProductionExceptionHandler.Result} enum value
         * @throws IllegalArgumentException if the provided value does not map to a valid {@link ProductionExceptionHandler.Result}
         */
        @Deprecated
        public static ProductionExceptionHandler.Result from(final ProductionExceptionHandlerResponse value) {
            switch (value) {
                case FAIL:
                    return Result.FAIL;
                case CONTINUE:
                    return Result.RESUME;
                case RETRY:
                    return Result.RETRY;
                default:
                    throw new IllegalArgumentException("No Result enum found for old value: " + value);
            }
        }
    }

    enum SerializationExceptionOrigin {
        /** Serialization exception occurred during serialization of the key. */
        KEY,
        /** Serialization exception occurred during serialization of the value. */
        VALUE
    }

    /**
     * Represents the result of handling a production exception.
     * <p>
     * The {@code Response} class encapsulates a {@link ProductionExceptionHandlerResponse},
     * indicating whether processing should continue or fail, along with an optional list of
     * {@link ProducerRecord} instances to be sent to a dead letter queue.
     * </p>
     */
    class Response {

        private Result result;

        private List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords;

        /**
         * Constructs a new {@code Response} object.
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
         * @return a {@code Response} with a {@link ProductionExceptionHandler.Result#FAIL} status.
         */
        public static Response fail(final List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords) {
            return new Response(Result.FAIL, deadLetterQueueRecords);
        }

        /**
         * Creates a {@code Response} indicating that processing should fail.
         *
         * @return a {@code Response} with a {@link ProductionExceptionHandler.Result#FAIL} status.
         */
        public static Response fail() {
            return fail(Collections.emptyList());
        }

        /**
         * Creates a {@code Response} indicating that processing should continue.
         *
         * @param deadLetterQueueRecords the list of records to be sent to the dead letter queue; may be {@code null}.
         * @return a {@code Response} with a {@link ProductionExceptionHandler.Result#RESUME} status.
         */
        public static Response resume(final List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords) {
            return new Response(Result.RESUME, deadLetterQueueRecords);
        }

        /**
         * Creates a {@code Response} indicating that processing should continue.
         *
         * @return a {@code Response} with a {@link ProductionExceptionHandler.Result#RESUME} status.
         */
        public static Response resume() {
            return resume(Collections.emptyList());
        }

        /**
         * Creates a {@code Response} indicating that processing should retry.
         *
         * @return a {@code Response} with a {@link ProductionExceptionHandler.Result#RETRY} status.
         */
        public static Response retry() {
            return new Response(Result.RETRY, Collections.emptyList());
        }

        /**
         * Retrieves the production exception handler result.
         *
         * @return the {@link Result} indicating whether processing should continue, fail or retry.
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
