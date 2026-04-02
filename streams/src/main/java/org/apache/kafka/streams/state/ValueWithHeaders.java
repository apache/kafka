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

import java.util.Objects;

/**
 * Combines a value with its associated record headers.
 * This is used by SessionStoreWithHeaders and WindowStoreWithHeaders to store values along with headers.
 *
 * @param <V> the value type
 */
public final class ValueWithHeaders<V> {

    private final V value;
    private final Headers headers;

    private ValueWithHeaders(final V value, final Headers headers) {
        Objects.requireNonNull(headers, "headers must not be null");
        this.value = value;
        this.headers = headers;
    }

    /**
     * Create a new {@link ValueWithHeaders} instance if the provided {@code value} is not {@code null}.
     *
     * @param value   the value
     * @param headers the headers (may be {@code null}, treated as empty)
     * @param <V>     the type of the value
     * @return a new {@link ValueWithHeaders} instance if the provided {@code value} is not {@code null};
     * otherwise {@code null} is returned
     */
    public static <V> ValueWithHeaders<V> make(final V value, final Headers headers) {
        if (value == null) {
            return null;
        }
        return new ValueWithHeaders<>(value, headers);
    }

    /**
     * Create a new {@link ValueWithHeaders} instance.
     * The provided {@code value} may be {@code null}.
     *
     * @param value   the value (may be {@code null})
     * @param headers the headers (may be {@code null}, treated as empty)
     * @param <V>     the type of the value
     * @return a new {@link ValueWithHeaders} instance
     */
    public static <V> ValueWithHeaders<V> makeAllowNullable(final V value, final Headers headers) {
        return new ValueWithHeaders<>(value, headers);
    }

    /**
     * Return the wrapped {@code value} of the given {@code valueWithHeaders} parameter
     * if the parameter is not {@code null}.
     *
     * @param valueWithHeaders an {@link ValueWithHeaders} instance; can be {@code null}
     * @param <V>              the type of the value
     * @return the wrapped {@code value} of {@code valueWithHeaders} if not {@code null}; otherwise {@code null}
     */
    public static <V> V getValueOrNull(final ValueWithHeaders<V> valueWithHeaders) {
        return valueWithHeaders == null ? null : valueWithHeaders.value;
    }

    public V value() {
        return value;
    }

    public Headers headers() {
        return headers;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ValueWithHeaders)) {
            return false;
        }
        final ValueWithHeaders<?> that = (ValueWithHeaders<?>) o;
        return Objects.equals(value, that.value)
            && Objects.equals(this.headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, headers);
    }

    @Override
    public String toString() {
        return "ValueWithHeaders{" +
            "value=" + value +
            ", headers=" + headers +
            '}';
    }
}
