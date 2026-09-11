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

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.Objects;
import java.util.Optional;

/**
 * Combines a value (from a key-value record) with a timestamp, for use as the return type
 * from {@link VersionedKeyValueStore#get(Object, long)} and related methods.
 *
 * @param <V> The value type
 */
@InterfaceAudience.Public
public final class VersionedRecord<V> {
    private final V value;
    private final long timestamp;
    private final Optional<Long> validTo;
    private final Headers headers;

    /**
     * Create a new {@link VersionedRecord} instance. {@code value} cannot be {@code null}.
     *
     * @param value      The value
     * @param timestamp  The type of the result returned by this query.
     */
    public VersionedRecord(final V value, final long timestamp) {
        this(value, timestamp, Optional.empty(), new RecordHeaders());
    }

    /**
     * Create a new {@link VersionedRecord} instance. {@code value} cannot be {@code null}.
     *
     * @param value      The value
     * @param timestamp  The timestamp
     * @param validTo    The exclusive upper bound of the validity interval
     */
    public VersionedRecord(final V value, final long timestamp, final long validTo) {
        this(value, timestamp, Optional.of(validTo), new RecordHeaders());
    }

    /**
     * Create a new {@link VersionedRecord} instance with headers. {@code value} cannot be {@code null}.
     *
     * @param value      The value
     * @param timestamp  The timestamp
     * @param headers    The record headers
     */
    public VersionedRecord(final V value, final long timestamp, final Headers headers) {
        this(value, timestamp, Optional.empty(), headers);
    }

    /**
     * Create a new {@link VersionedRecord} instance with headers. {@code value} cannot be {@code null}.
     *
     * @param value      The value
     * @param timestamp  The timestamp
     * @param validTo    The exclusive upper bound of the validity interval
     * @param headers    The record headers
     */
    public VersionedRecord(final V value, final long timestamp, final long validTo, final Headers headers) {
        this(value, timestamp, Optional.of(validTo), headers);
    }

    private VersionedRecord(final V value, final long timestamp, final Optional<Long> validTo, final Headers headers) {
        this.value = Objects.requireNonNull(value, "value cannot be null.");
        this.timestamp = timestamp;
        this.validTo = validTo;
        this.headers = Objects.requireNonNull(headers, "headers cannot be null.");
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

    /**
     * @return the record headers. Never {@code null} (returns empty headers if none were set).
     */
    public Headers headers() {
        return headers;
    }

    @Override
    public String toString() {
        return "<" + value + "," + timestamp + "," + validTo + "," + headers + ">";
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
        return timestamp == that.timestamp && Objects.equals(validTo, that.validTo) &&
            Objects.equals(value, that.value) &&
            Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, timestamp, validTo, headers);
    }
}
