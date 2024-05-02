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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.Configurable;

/**
 * Interface that specifies how an the RecordTooLargeException and/or UnknownTopicOrPartitionException should be handled.
 * The accepted responses for RecordTooLargeException are FAIL and SWALLOW. Therefore, RETRY will be interpreted and executed as FAIL.
 */
public interface ProducerExceptionHandler extends Configurable {

    /**
     * Determine whether to stop processing, keep retrying internally, or swallow the error by dropping the record.
     * For RecordTooLargeException RETRY will be interpreted and executed as FAIL.
     *
     * @param record The record that failed to produce
     * @param exception The exception that occurred during production
     */
    Response handle(final ProducerRecord record, final Exception exception);

    enum Response {
        /* stop processing: fail */
        FAIL(0, "FAIL"),
        /* continue: keep retrying */
        RETRY(1, "RETRY"),
        /* continue: swallow the error */
        SWALLOW(2, "SWALLOW");

        /**
         * an english description of the api--this is for debugging and can change
         */
        public final String name;

        /**
         * the permanent and immutable id of an API--this can't change ever
         */
        public final int id;

        Response(final int id, final String name) {
            this.id = id;
            this.name = name;
        }
    }
}
