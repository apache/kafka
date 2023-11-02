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

package org.apache.kafka.controller;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;

import java.util.Objects;


public class ResultOrError<T> {
    private final ApiError error;
    private final T result;

    public ResultOrError(Errors error, String message) {
        this(new ApiError(error, message));
    }

    public ResultOrError(ApiError error) {
        Objects.requireNonNull(error);
        this.error = error;
        this.result = null;
    }

    public ResultOrError(T result) {
        this.error = null;
        this.result = result;
    }

    public static <T> ResultOrError<T> of(T result) {
        return new ResultOrError<>(result);
    }

    public static <T> ResultOrError<T> of(ApiError error) {
        return new ResultOrError<>(error);
    }

    public boolean isError() {
        return error != null;
    }

    public boolean isResult() {
        return error == null;
    }

    public ApiError error() {
        return error;
    }

    public T result() {
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!o.getClass().equals(getClass()))) {
            return false;
        }
        ResultOrError other = (ResultOrError) o;
        return Objects.equals(error, other.error) &&
            Objects.equals(result, other.result);
    }

    @Override
    public int hashCode() {
        return Objects.hash(error, result);
    }

    @Override
    public String toString() {
        if (error == null) {
            return "ResultOrError(" + result + ")";
        } else {
            return "ResultOrError(" + error + ")";
        }
    }
}
