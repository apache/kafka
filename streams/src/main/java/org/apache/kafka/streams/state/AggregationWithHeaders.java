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
 * @param <AGG> the aggregation type
 */
public final class AggregationWithHeaders<AGG> {

    private final AGG aggregation;
    private final Headers headers;

    private AggregationWithHeaders(final AGG aggregation, final Headers headers) {
        Objects.requireNonNull(headers, "headers must not be null");
        this.aggregation = aggregation;
        this.headers = headers;
    }

    /**
     * Create a new {@link AggregationWithHeaders} instance if the provided {@code aggregation} is not {@code null}.
     *
     * @param aggregation the aggregation
     * @param headers     the headers (may be {@code null}, treated as empty)
     * @param <AGG>       the type of the aggregation
     * @return a new {@link AggregationWithHeaders} instance if the provided {@code aggregation} is not {@code null};
     * otherwise {@code null} is returned
     */
    public static <AGG> AggregationWithHeaders<AGG> make(final AGG aggregation, final Headers headers) {
        if (aggregation == null) {
            return null;
        }
        return new AggregationWithHeaders<>(aggregation, headers);
    }

    /**
     * Create a new {@link AggregationWithHeaders} instance.
     * The provided {@code aggregation} may be {@code null}.
     *
     * @param aggregation the aggregation (may be {@code null})
     * @param headers     the headers (may be {@code null}, treated as empty)
     * @param <AGG>       the type of the aggregation
     * @return a new {@link AggregationWithHeaders} instance
     */
    public static <AGG> AggregationWithHeaders<AGG> makeAllowNullable(final AGG aggregation, final Headers headers) {
        return new AggregationWithHeaders<>(aggregation, headers);
    }

    /**
     * Return the wrapped {@code aggregation} of the given {@code aggregationWithHeaders} parameter
     * if the parameter is not {@code null}.
     *
     * @param aggregationWithHeaders an {@link AggregationWithHeaders} instance; can be {@code null}
     * @param <AGG>                  the type of the aggregation
     * @return the wrapped {@code aggregation} of {@code aggregationWithHeaders} if not {@code null}; otherwise {@code null}
     */
    public static <AGG> AGG getAggregationOrNull(final AggregationWithHeaders<AGG> aggregationWithHeaders) {
        return aggregationWithHeaders == null ? null : aggregationWithHeaders.aggregation;
    }

    public AGG aggregation() {
        return aggregation;
    }

    public Headers headers() {
        return headers;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AggregationWithHeaders)) {
            return false;
        }
        final AggregationWithHeaders<?> that = (AggregationWithHeaders<?>) o;
        return Objects.equals(aggregation, that.aggregation)
            && Objects.equals(this.headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregation, headers);
    }

    @Override
    public String toString() {
        return "AggregationWithHeaders{" +
            "aggregation=" + aggregation +
            ", headers=" + headers +
            '}';
    }
}
