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
 * An error code on the server for which the client doesn't have a corresponding error.
 * UnknownErrorCodeException can only occur on the client side when client's library version is lower than the server's version.
 * This exception should be retriable to handle the scenario that the corresponding new exception defined in the server side is retriable.
 */
public class UnknownErrorCodeException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public UnknownErrorCodeException() {
    }

    public UnknownErrorCodeException(String message) {
        super(message);
    }

    public UnknownErrorCodeException(Throwable cause) {
        super(cause);
    }

    public UnknownErrorCodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
