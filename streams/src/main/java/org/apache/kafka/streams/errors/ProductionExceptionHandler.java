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
     */
    default ProductionExceptionHandlerResponse handle(final ErrorHandlerContext context,
                                                      final ProducerRecord<byte[], byte[]> record,
                                                      final Exception exception) {
        return handle(record, exception);
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
        return ProductionExceptionHandlerResponse.FAIL;
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
     */
    @SuppressWarnings("rawtypes")
    default ProductionExceptionHandlerResponse handleSerializationException(final ErrorHandlerContext context,
                                                                            final ProducerRecord record,
                                                                            final Exception exception,
                                                                            final SerializationExceptionOrigin origin) {
        return handleSerializationException(record, exception);
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
}
