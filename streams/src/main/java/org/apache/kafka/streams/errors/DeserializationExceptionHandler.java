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
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.errors.internals.DefaultErrorHandlerContext;
import org.apache.kafka.streams.processor.ProcessorContext;

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
     */
    default DeserializationHandlerResponse handle(final ErrorHandlerContext context,
                                                  final ConsumerRecord<byte[], byte[]> record,
                                                  final Exception exception) {
        return handle(((DefaultErrorHandlerContext) context).processorContext().orElse(null), record, exception);
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

}
