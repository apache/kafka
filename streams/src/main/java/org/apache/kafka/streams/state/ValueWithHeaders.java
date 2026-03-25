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
 * Combines an aggregated value with its associated record headers.
 * This is used by SessionStoreWithHeaders to store session aggregations along with headers.
 *
 * @param <V> the aggregation type
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
     * Create a new {@link ValueWithHeaders} instance if the provided {@code aggregation} is not {@code null}.
     *
     * @param aggregation the aggregation
     * @param headers     the headers (may be {@code null}, treated as empty)
     * @param <AGG>       the type of the aggregation
     * @return a new {@link ValueWithHeaders} instance if the provided {@code aggregation} is not {@code null};
     * otherwise {@code null} is returned
     */
    public static <AGG> ValueWithHeaders<AGG> make(final AGG aggregation, final Headers headers) {
        if (aggregation == null) {
            return null;
        }
        return new ValueWithHeaders<>(aggregation, headers);
    }

    /**
     * Create a new {@link ValueWithHeaders} instance.
     * The provided {@code aggregation} may be {@code null}.
     *
     * @param aggregation the aggregation (may be {@code null})
     * @param headers     the headers (may be {@code null}, treated as empty)
     * @param <AGG>       the type of the aggregation
     * @return a new {@link ValueWithHeaders} instance
     */
    public static <AGG> ValueWithHeaders<AGG> makeAllowNullable(final AGG aggregation, final Headers headers) {
        return new ValueWithHeaders<>(aggregation, headers);
    }

    /**
     * Return the wrapped {@code aggregation} of the given {@code valueWithHeaders} parameter
     * if the parameter is not {@code null}.
     *
     * @param valueWithHeaders an {@link ValueWithHeaders} instance; can be {@code null}
     * @param <AGG>                  the type of the aggregation
     * @return the wrapped {@code aggregation} of {@code valueWithHeaders} if not {@code null}; otherwise {@code null}
     */
    public static <AGG> AGG getValueOrNull(final ValueWithHeaders<AGG> valueWithHeaders) {
        return valueWithHeaders == null ? null : valueWithHeaders.value;
    }

    public V aggregation() {
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
            "aggregation=" + value +
            ", headers=" + headers +
            '}';
    }
}
