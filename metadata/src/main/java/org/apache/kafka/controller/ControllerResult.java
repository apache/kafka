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

import org.apache.kafka.common.protocol.ApiMessageAndVersion;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

class ControllerResult<T> {
    private final List<ApiMessageAndVersion> records;
    private final T response;

    static <T> ControllerResult<T> of(T response) {
        return new ControllerResult<>(Collections.emptyList(), response);
    }

    ControllerResult(List<ApiMessageAndVersion> records,
                     T response) {
        Objects.requireNonNull(records);
        Objects.requireNonNull(response);
        this.records = records;
        this.response = response;
    }

    public List<ApiMessageAndVersion> records() {
        return records;
    }

    public T response() {
        return response;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!o.getClass().equals(getClass()))) {
            return false;
        }
        ControllerResult other = (ControllerResult) o;
        return records.equals(other.records) &&
            response.equals(other.response);
    }

    @Override
    public int hashCode() {
        return Objects.hash(records, response);
    }
}
