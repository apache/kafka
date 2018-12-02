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
import org.apache.kafka.common.protocol.types.Struct;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.ERROR_MESSAGE;

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
        // Avoid populating the error message if it's a generic one
        Errors error = Errors.forException(t);
        String message = error.message().equals(t.getMessage()) ? null : t.getMessage();
        return new ApiError(error, message);
    }

    public ApiError(Struct struct) {
        error = Errors.forCode(struct.get(ERROR_CODE));
        // In some cases, the error message field was introduced in newer version
        message = struct.getOrElse(ERROR_MESSAGE, null);
    }

    public ApiError(Errors error, String message) {
        this.error = error;
        this.message = message;
    }

    public void write(Struct struct) {
        struct.set(ERROR_CODE, error.code());
        if (error != Errors.NONE)
            struct.setIfExists(ERROR_MESSAGE, message);
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
    public String toString() {
        return "ApiError(error=" + error + ", message=" + message + ")";
    }
}
