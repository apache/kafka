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

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.Objects;

/**
 * A Record which contains an {{@link ApiMessage}} as key and
 * an {{@link ApiMessageAndVersion}} as value. The value could be null to
 * represent a tombstone.
 *
 * This class is immutable.
 */
public class CoordinatorRecord {

    public static CoordinatorRecord record(
        ApiMessage key,
        ApiMessageAndVersion value
    ) {
        return new CoordinatorRecord(key, value);
    }

    public static CoordinatorRecord tombstone(
        ApiMessage key
    ) {
        return new CoordinatorRecord(key, null);
    }

    /**
     * The key of the record.
     */
    private final ApiMessage key;

    /**
     * The value of the record or null if the record is
     * a tombstone.
     */
    private final ApiMessageAndVersion value;

    /**
     * Constructs a CoordinatorRecord.
     *
     * @param key   A non-null key.
     * @param value A key or null.
     */
    private CoordinatorRecord(
        ApiMessage key,
        ApiMessageAndVersion value
    ) {
        this.key = Objects.requireNonNull(key);
        if (key.apiKey() < 0) {
            throw new IllegalArgumentException("The key must have a type.");
        }

        this.value = value;
        if (value != null) {
            if (value.message().apiKey() < 0) {
                throw new IllegalArgumentException("The value must have a type.");
            }
            if (value.message().apiKey() != key.apiKey()) {
                throw new IllegalArgumentException("The key and the value must have the same type.");
            }
        }
    }

    /**
     * @return The key.
     */
    public ApiMessage key() {
        return this.key;
    }

    /**
     * @return The value or null.
     */
    public ApiMessageAndVersion value() {
        return this.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CoordinatorRecord that = (CoordinatorRecord) o;
        return Objects.equals(key, that.key) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "CoordinatorRecord(key=" + key + ", value=" + value + ")";
    }
}
