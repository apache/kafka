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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.processor.api.Record;

/**
 * An interface that allows user code to inspect a record that has failed processing
 */
public interface ProcessingExceptionHandler extends Configurable {
    /**
     * Inspect a record and the exception received
     *
     * @param context processing context metadata
     * @param record record where the exception occurred
     * @param exception the actual exception
     */
    ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception);

    enum ProcessingHandlerResponse {
        /* continue with processing */
        CONTINUE(1, "CONTINUE"),
        /* fail the processing and stop */
        FAIL(2, "FAIL");

        /**
         * the permanent and immutable name of processing exception response
         */
        public final String name;

        /**
         * the permanent and immutable id of processing exception response
         */
        public final int id;

        ProcessingHandlerResponse(final int id, final String name) {
            this.id = id;
            this.name = name;
        }
    }
}
