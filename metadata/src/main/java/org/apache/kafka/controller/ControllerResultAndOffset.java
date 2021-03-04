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

import org.apache.kafka.metadata.ApiMessageAndVersion;

import java.util.Objects;
import java.util.stream.Collectors;


final class ControllerResultAndOffset<T> extends ControllerResult<T> {
    private final long offset;

    private ControllerResultAndOffset(long offset, ControllerResult<T> result) {
        super(result.records(), result.response(), result.isAtomic());
        this.offset = offset;
    }

    public long offset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!o.getClass().equals(getClass()))) {
            return false;
        }
        ControllerResultAndOffset other = (ControllerResultAndOffset) o;
        return records().equals(other.records()) &&
            response().equals(other.response()) &&
            isAtomic() == other.isAtomic() &&
            offset == other.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(records(), response(), isAtomic(), offset);
    }

    @Override
    public String toString() {
        return String.format(
            "ControllerResultAndOffset(records=%s, response=%s, isAtomic=%s, offset=%s)",
            String.join(",", records().stream().map(ApiMessageAndVersion::toString).collect(Collectors.toList())),
            response(),
            isAtomic(),
            offset
        );
    }

    public static <T> ControllerResultAndOffset<T> of(long offset, ControllerResult<T> result) {
        return new ControllerResultAndOffset<>(offset, result);
    }
}
