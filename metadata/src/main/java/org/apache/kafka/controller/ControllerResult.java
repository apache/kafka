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

import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


class ControllerResult<T> {
    private final List<ApiMessageAndVersion> records;
    private final T response;
    private final boolean isAtomic;

    protected ControllerResult(List<ApiMessageAndVersion> records, T response, boolean isAtomic) {
        Objects.requireNonNull(records);
        this.records = records;
        this.response = response;
        this.isAtomic = isAtomic;
    }

    public List<ApiMessageAndVersion> records() {
        return records;
    }

    public T response() {
        return response;
    }

    public boolean isAtomic() {
        return isAtomic;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!o.getClass().equals(getClass()))) {
            return false;
        }
        ControllerResult other = (ControllerResult) o;
        return records.equals(other.records) &&
            Objects.equals(response, other.response) &&
            Objects.equals(isAtomic, other.isAtomic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(records, response, isAtomic);
    }

    @Override
    public String toString() {
        return String.format(
            "ControllerResult(records=%s, response=%s, isAtomic=%s)",
            records.stream().map(ApiMessageAndVersion::toString).collect(Collectors.joining(",")),
            response,
            isAtomic
        );
    }

    public ControllerResult<T> withoutRecords() {
        return new ControllerResult<>(Collections.emptyList(), response, false);
    }

    public static <T> ControllerResult<T> atomicOf(List<ApiMessageAndVersion> records, T response) {
        return new ControllerResult<>(records, response, true);
    }

    public static <T> ControllerResult<T> of(List<ApiMessageAndVersion> records, T response) {
        return new ControllerResult<>(records, response, false);
    }
}
