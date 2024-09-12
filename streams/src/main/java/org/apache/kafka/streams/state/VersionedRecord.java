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
package org.apache.kafka.streams.state;

import java.util.Objects;
import java.util.Optional;

/**
 * Combines a value (from a key-value record) with a timestamp, for use as the return type
 * from {@link VersionedKeyValueStore#get(Object, long)} and related methods.
 *
 * @param <V> The value type
 */
public final class VersionedRecord<V> {
    private final V value;
    private final long timestamp;
    private final Optional<Long> validTo;

    /**
     * Create a new {@link VersionedRecord} instance. {@code value} cannot be {@code null}.
     *
     * @param value      The value
     * @param timestamp  The type of the result returned by this query.
     */
    public VersionedRecord(final V value, final long timestamp) {
        this.value = Objects.requireNonNull(value, "value cannot be null.");
        this.timestamp = timestamp;
        this.validTo = Optional.empty();
    }

    /**
     * Create a new {@link VersionedRecord} instance. {@code value} cannot be {@code null}.
     *
     * @param value      The value
     * @param timestamp  The timestamp
     * @param validTo    The exclusive upper bound of the validity interval
     */
    public VersionedRecord(final V value, final long timestamp, final long validTo) {
        this.value = Objects.requireNonNull(value);
        this.timestamp = timestamp;
        this.validTo = Optional.of(validTo);
    }


    public V value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }

    public Optional<Long> validTo() {
        return validTo;
    }

    @Override
    public String toString() {
        return "<" + value + "," + timestamp + "," + validTo + ">";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final VersionedRecord<?> that = (VersionedRecord<?>) o;
        return timestamp == that.timestamp && validTo == that.validTo &&
            Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, timestamp, validTo);
    }
}