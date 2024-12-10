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
     * @return a {@link ProductionExceptionResponse} object
     */
    default ProductionExceptionResponse handleError(final ErrorHandlerContext context,
                                                    final ProducerRecord<byte[], byte[]> record,
                                                    final Exception exception) {
        final ProductionExceptionHandlerResponse response =  handle(context, record, exception);
        if (ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL == response) {
            return ProductionExceptionResponse.failProcessing();
        } else if (ProductionExceptionHandler.ProductionExceptionHandlerResponse.RETRY == response) {
            return ProductionExceptionResponse.retryProcessing();
        }
        return ProductionExceptionResponse.continueProcessing();
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
     * @return a {@link ProductionExceptionResponse} object
     */
    @SuppressWarnings("rawtypes")
    default ProductionExceptionResponse handleSerializationError(final ErrorHandlerContext context,
                                                                 final ProducerRecord record,
                                                                 final Exception exception,
                                                                 final SerializationExceptionOrigin origin) {
        final ProductionExceptionHandlerResponse response =  handleSerializationException(context, record, exception, origin);
        if (ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL == response) {
            return ProductionExceptionResponse.failProcessing();
        } else if (ProductionExceptionHandler.ProductionExceptionHandlerResponse.RETRY == response) {
            return ProductionExceptionResponse.retryProcessing();
        }
        return ProductionExceptionResponse.continueProcessing();
    }

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
    class ProductionExceptionResponse {

        private ProductionExceptionHandlerResponse productionExceptionHandlerResponse;

        private List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords;

        /**
         * Constructs a new {@code ProductionExceptionResponse} object.
         *
         * @param productionExceptionHandlerResponse the response indicating whether processing should continue or fail;
         *                                  must not be {@code null}.
         * @param deadLetterQueueRecords    the list of records to be sent to the dead letter queue; may be {@code null}.
         */
        private ProductionExceptionResponse(final ProductionExceptionHandlerResponse productionExceptionHandlerResponse,
                                            final List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords) {
            this.productionExceptionHandlerResponse = productionExceptionHandlerResponse;
            this.deadLetterQueueRecords = deadLetterQueueRecords;
        }

        /**
         * Creates a {@code ProductionExceptionResponse} indicating that processing should fail.
         *
         * @param deadLetterQueueRecords the list of records to be sent to the dead letter queue; may be {@code null}.
         * @return a {@code ProductionExceptionResponse} with a {@link DeserializationExceptionHandler.DeserializationHandlerResponse#FAIL} status.
         */
        public static ProductionExceptionResponse failProcessing(final List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords) {
            return new ProductionExceptionResponse(ProductionExceptionHandlerResponse.FAIL, deadLetterQueueRecords);
        }

        /**
         * Creates a {@code ProductionExceptionResponse} indicating that processing should fail.
         *
         * @return a {@code ProductionExceptionResponse} with a {@link DeserializationExceptionHandler.DeserializationHandlerResponse#FAIL} status.
         */
        public static ProductionExceptionResponse failProcessing() {
            return new ProductionExceptionResponse(ProductionExceptionHandlerResponse.FAIL, Collections.emptyList());
        }

        /**
         * Creates a {@code ProductionExceptionResponse} indicating that processing should continue.
         *
         * @param deadLetterQueueRecords the list of records to be sent to the dead letter queue; may be {@code null}.
         * @return a {@code ProductionExceptionResponse} with a {@link DeserializationExceptionHandler.DeserializationHandlerResponse#CONTINUE} status.
         */
        public static ProductionExceptionResponse continueProcessing(final List<ProducerRecord<byte[], byte[]>> deadLetterQueueRecords) {
            return new ProductionExceptionResponse(ProductionExceptionHandlerResponse.CONTINUE, deadLetterQueueRecords);
        }

        /**
         * Creates a {@code ProductionExceptionResponse} indicating that processing should continue.
         *
         * @return a {@code ProductionExceptionResponse} with a {@link DeserializationExceptionHandler.DeserializationHandlerResponse#CONTINUE} status.
         */
        public static ProductionExceptionResponse continueProcessing() {
            return new ProductionExceptionResponse(ProductionExceptionHandlerResponse.CONTINUE, Collections.emptyList());
        }

        /**
         * Creates a {@code ProductionExceptionResponse} indicating that processing should retry.
         *
         * @return a {@code ProductionExceptionResponse} with a {@link DeserializationExceptionHandler.DeserializationHandlerResponse#CONTINUE} status.
         */
        public static ProductionExceptionResponse retryProcessing() {
            return new ProductionExceptionResponse(ProductionExceptionHandlerResponse.RETRY, Collections.emptyList());
        }

        /**
         * Retrieves the production exception handler response.
         *
         * @return the {@link ProductionExceptionHandlerResponse} indicating whether processing should continue or fail.
         */
        public ProductionExceptionHandlerResponse response() {
            return productionExceptionHandlerResponse;
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
