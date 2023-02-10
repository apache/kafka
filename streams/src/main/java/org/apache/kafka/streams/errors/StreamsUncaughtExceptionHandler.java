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

public interface StreamsUncaughtExceptionHandler {
    /**
     * Inspect the exception received in a stream thread and respond with an action.
     * @param exception the actual exception
     */
    StreamThreadExceptionResponse handle(final Throwable exception);

    /**
     * Enumeration that describes the response from the exception handler.
     */
    enum StreamThreadExceptionResponse {
        REPLACE_THREAD(0, "REPLACE_THREAD"),
        SHUTDOWN_CLIENT(1, "SHUTDOWN_KAFKA_STREAMS_CLIENT"),
        SHUTDOWN_APPLICATION(2, "SHUTDOWN_KAFKA_STREAMS_APPLICATION");

        /** an english description of the api--this is for debugging and can change */
        public final String name;

        /** the permanent and immutable id of an API--this can't change ever */
        public final int id;

        StreamThreadExceptionResponse(final int id, final String name) {
            this.id = id;
            this.name = name;
        }
    }
}
