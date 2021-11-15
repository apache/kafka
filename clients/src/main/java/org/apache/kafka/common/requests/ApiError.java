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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.protocol.Errors;

import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Encapsulates an error code (via the Errors enum) and an optional message. Generally, the optional message is only
 * defined if it adds information over the default message associated with the error code.
 *
 * This is an internal class (like every class in the requests package).
 */
public class ApiError {

    public static final ApiError NONE = new ApiError(Errors.NONE, null);

    private final Errors error;
    private final String message;

    public static ApiError fromThrowable(Throwable t) {
        Throwable throwableToBeEncoded = t;
        // Get the underlying cause for common exception types from the concurrent library.
        // This is useful to handle cases where exceptions may be raised from a future or a
        // completion stage (as might be the case for requests sent to the controller in `ControllerApis`)
        if (t instanceof CompletionException || t instanceof ExecutionException) {
            throwableToBeEncoded = t.getCause();
        }
        // Avoid populating the error message if it's a generic one. Also don't populate error
        // message for UNKNOWN_SERVER_ERROR to ensure we don't leak sensitive information.
        Errors error = Errors.forException(throwableToBeEncoded);
        String message = error == Errors.UNKNOWN_SERVER_ERROR ||
            error.message().equals(throwableToBeEncoded.getMessage()) ? null : throwableToBeEncoded.getMessage();
        return new ApiError(error, message);
    }

    public ApiError(Errors error) {
        this(error, error.message());
    }

    public ApiError(Errors error, String message) {
        this.error = error;
        this.message = message;
    }

    public ApiError(short code, String message) {
        this.error = Errors.forCode(code);
        this.message = message;
    }

    public boolean is(Errors error) {
        return this.error == error;
    }

    public boolean isFailure() {
        return !isSuccess();
    }

    public boolean isSuccess() {
        return is(Errors.NONE);
    }

    public Errors error() {
        return error;
    }

    /**
     * Return the optional error message or null. Consider using {@link #messageWithFallback()} instead.
     */
    public String message() {
        return message;
    }

    /**
     * If `message` is defined, return it. Otherwise fallback to the default error message associated with the error
     * code.
     */
    public String messageWithFallback() {
        if (message == null)
            return error.message();
        return message;
    }

    public ApiException exception() {
        return error.exception(message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(error, message);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ApiError)) {
            return false;
        }
        ApiError other = (ApiError) o;
        return Objects.equals(error, other.error) &&
            Objects.equals(message, other.message);
    }

    @Override
    public String toString() {
        return "ApiError(error=" + error + ", message=" + message + ")";
    }
}
