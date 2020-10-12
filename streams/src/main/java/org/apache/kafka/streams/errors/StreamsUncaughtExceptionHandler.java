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
    StreamsUncaughtExceptionHandler.StreamsUncaughtExceptionHandlerResponse handle(final Throwable exception);


    /**
     * Inspect the exception received in a global stream thread.
     * @param exception the actual exception
     */
    default StreamsUncaughtExceptionHandler.StreamsUncaughtExceptionHandlerResponseGlobalThread handleExceptionInGlobalThread(final Throwable exception) {
        return StreamsUncaughtExceptionHandlerResponseGlobalThread.SHUTDOWN_KAFKA_STREAMS_CLIENT;
    }

    /**
     * Enumeration that describes the response from the exception handler.
     */
    enum StreamsUncaughtExceptionHandlerResponse {
        SHUTDOWN_STREAM_THREAD(0, "SHUTDOWN_STREAM_THREAD"),
//        REPLACE_STREAM_THREAD(1, "REPLACE_STREAM_THREAD"),
        SHUTDOWN_KAFKA_STREAMS_CLIENT(2, "SHUTDOWN_KAFKA_STREAMS_CLIENT"),
        SHUTDOWN_KAFKA_STREAMS_APPLICATION(3, "SHUTDOWN_KAFKA_STREAMS_APPLICATION");

        /** an english description of the api--this is for debugging and can change */
        public final String name;

        /** the permanent and immutable id of an API--this can't change ever */
        public final int id;

        StreamsUncaughtExceptionHandlerResponse(final int id, final String name) {
            this.id = id;
            this.name = name;
        }
    }

    /**
     * Enumeration that describes the response from the exception handler.
     */
    enum StreamsUncaughtExceptionHandlerResponseGlobalThread {
        SHUTDOWN_KAFKA_STREAMS_CLIENT(2, "SHUTDOWN_KAFKA_STREAMS_CLIENT");

        /** an english description of the api--this is for debugging and can change */
        public final String name;

        /** the permanent and immutable id of an API--this can't change ever */
        public final int id;

        StreamsUncaughtExceptionHandlerResponseGlobalThread(final int id, final String name) {
            this.id = id;
            this.name = name;
        }
    }
}
