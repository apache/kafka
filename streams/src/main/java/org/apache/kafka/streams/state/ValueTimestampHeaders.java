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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.Objects;

/**
 * Combines a value with its timestamp and associated record headers.
 *
 * @param <V> the value type
 */
public final class ValueTimestampHeaders<V> {

    private final V value;
    private final long timestamp;
    private final Headers headers;

    private ValueTimestampHeaders(final V value, final long timestamp, final Headers headers) {
        this.value = value;
        this.timestamp = timestamp;
        this.headers = headers == null ? new RecordHeaders() : headers;
    }

    /**
     * Create a new {@link ValueTimestampHeaders} instance if the provided {@code value} is not {@code null}.
     *
     * @param value     the value
     * @param timestamp the timestamp
     * @param headers   the headers (may be {@code null}, treated as empty)
     * @param <V>       the type of the value
     * @return a new {@link ValueTimestampHeaders} instance if the provided {@code value} is not {@code null};
     * otherwise {@code null} is returned
     */
    public static <V> ValueTimestampHeaders<V> make(final V value,
                                                    final long timestamp,
                                                    final Headers headers) {
        if (value == null) {
            return null;
        }
        return new ValueTimestampHeaders<>(value, timestamp, headers);
    }

    /**
     * Create a new {@link ValueTimestampHeaders} instance.
     * The provided {@code value} may be {@code null}.
     *
     * @param value     the value (may be {@code null})
     * @param timestamp the timestamp
     * @param headers   the headers (may be {@code null}, treated as empty)
     * @param <V>       the type of the value
     * @return a new {@link ValueTimestampHeaders} instance
     */
    public static <V> ValueTimestampHeaders<V> makeAllowNullable(final V value,
                                                                 final long timestamp,
                                                                 final Headers headers) {
        return new ValueTimestampHeaders<>(value, timestamp, headers);
    }

    /**
     * Return the wrapped {@code value} of the given {@code valueTimestampHeaders} parameter
     * if the parameter is not {@code null}.
     *
     * @param valueTimestampHeaders a {@link ValueTimestampHeaders} instance; can be {@code null}
     * @param <V>                   the type of the value
     * @return the wrapped {@code value} of {@code valueTimestampHeaders} if not {@code null}; otherwise {@code null}
     */
    public static <V> V getValueOrNull(final ValueTimestampHeaders<V> valueTimestampHeaders) {
        return valueTimestampHeaders == null ? null : valueTimestampHeaders.value;
    }

    public V value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }

    public Headers headers() {
        return headers;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ValueTimestampHeaders)) {
            return false;
        }
        final ValueTimestampHeaders<?> that = (ValueTimestampHeaders<?>) o;
        return timestamp == that.timestamp
            && Objects.equals(value, that.value)
            && Objects.equals(this.headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, timestamp, headers);
    }

    @Override
    public String toString() {
        return "ValueTimestampHeaders{" +
            "value=" + value +
            ", timestamp=" + timestamp +
            ", headers=" + headers +
            '}';
    }
}
