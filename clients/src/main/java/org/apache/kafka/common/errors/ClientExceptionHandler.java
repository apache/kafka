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

package org.apache.kafka.common.errors;

/**
 * Interface that specifies how an exception should be handled.
 */
public interface ClientExceptionHandler {

    ClientExceptionHandlerResponse handle(final Exception exception);

    enum ClientExceptionHandlerResponse {
        /* continue processing */
        CONTINUE(0, "CONTINUE"),
        /* fail processing */
        FAIL(1, "FAIL");

        /**
         * an english description of the api--this is for debugging and can change
         */
        public final String name;

        /**
         * the permanent and immutable id of an API--this can't change ever
         */
        public final int id;

        ClientExceptionHandlerResponse(final int id,
                                           final String name) {
            this.id = id;
            this.name = name;
        }
    }
}
