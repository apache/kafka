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

package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;

import org.slf4j.Logger;

import java.util.function.BiFunction;

public class CoordinatorOperationExceptionHelper {
    /**
     * This is the handler commonly used by all the operations that requires to convert errors to
     * coordinator errors. The handler also handles and logs unexpected errors.
     *
     * @param operationName     The name of the operation.
     * @param operationInput    The operation's input for logging purposes.
     * @param exception         The exception to handle.
     * @param handler           A function which takes an Errors and a String and builds the expected
     *                          output. The String can be null. Note that the function could further
     *                          transform the error depending on the context.
     * @return The output built by the handler.
     * @param <IN> The type of the operation input. It must be a toString'able object.
     * @param <OUT> The type of the value returned by handler.
     */
    public static <IN, OUT> OUT handleOperationException(
        String operationName,
        IN operationInput,
        Throwable exception,
        BiFunction<Errors, String, OUT> handler,
        Logger log
    ) {
        ApiError apiError = ApiError.fromThrowable(exception);

        switch (apiError.error()) {
            case UNKNOWN_SERVER_ERROR:
                log.error("Operation {} with {} hit an unexpected exception: {}.",
                    operationName, operationInput, exception.getMessage(), exception);
                return handler.apply(Errors.UNKNOWN_SERVER_ERROR, null);

            case NETWORK_EXCEPTION:
                // When committing offsets transactionally, we now verify the transaction with the
                // transaction coordinator. Verification can fail with `NETWORK_EXCEPTION`, a
                // retriable error which older clients may not expect and retry correctly. We
                // translate the error to `COORDINATOR_LOAD_IN_PROGRESS` because it causes clients
                // to retry the request without an unnecessary coordinator lookup.
                return handler.apply(Errors.COORDINATOR_LOAD_IN_PROGRESS, null);

            case UNKNOWN_TOPIC_OR_PARTITION:
            case NOT_ENOUGH_REPLICAS:
            case REQUEST_TIMED_OUT:
                return handler.apply(Errors.COORDINATOR_NOT_AVAILABLE, null);

            case NOT_LEADER_OR_FOLLOWER:
            case KAFKA_STORAGE_ERROR:
                return handler.apply(Errors.NOT_COORDINATOR, null);

            case MESSAGE_TOO_LARGE:
            case RECORD_LIST_TOO_LARGE:
            case INVALID_FETCH_SIZE:
                return handler.apply(Errors.UNKNOWN_SERVER_ERROR, null);

            default:
                return handler.apply(apiError.error(), apiError.message());
        }
    }
}
