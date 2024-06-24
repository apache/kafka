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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.Objects;

/**
 * A Record which contains an {{@link ApiMessageAndVersion}} as key and
 * an {{@link ApiMessageAndVersion}} as value. The value could be null to
 * represent a tombstone.
 *
 * This class is immutable.
 */
public class CoordinatorRecord {
    /**
     * The key of the record.
     */
    private final ApiMessageAndVersion key;

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
    public CoordinatorRecord(
        ApiMessageAndVersion key,
        ApiMessageAndVersion value
    ) {
        this.key = Objects.requireNonNull(key);
        this.value = value;
    }

    /**
     * @return The key.
     */
    public ApiMessageAndVersion key() {
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

        CoordinatorRecord record = (CoordinatorRecord) o;

        if (!Objects.equals(key, record.key)) return false;
        return Objects.equals(value, record.value);
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CoordinatorRecord(key=" + key + ", value=" + value + ")";
    }
}
