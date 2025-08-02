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
package org.apache.kafka.streams.query;

import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Optional;

/**
 * A range query that supports pagination to reduce memory usage and improve performance
 * when dealing with large result sets.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
@Evolving
public final class PaginatedRangeQuery<K, V> implements Query<KeyValueIterator<K, V>> {

    private final Optional<K> lowerBound;
    private final Optional<K> upperBound;
    private final int pageSize;
    private final Optional<K> startKey;
    private final ResultOrder resultOrder;

    /**
     * Creates a paginated range query.
     *
     * @param lowerBound optional lower bound for the range
     * @param upperBound optional upper bound for the range
     * @param pageSize maximum number of results per page
     */
    public PaginatedRangeQuery(final Optional<K> lowerBound, 
                              final Optional<K> upperBound, 
                              final int pageSize) {
        this(lowerBound, upperBound, pageSize, Optional.empty(), ResultOrder.ASCENDING);
    }

    /**
     * Creates a paginated range query with a starting key for pagination.
     *
     * @param lowerBound optional lower bound for the range
     * @param upperBound optional upper bound for the range
     * @param pageSize maximum number of results per page
     * @param startKey starting key for pagination (exclusive)
     * @param resultOrder the order of results
     */
    public PaginatedRangeQuery(final Optional<K> lowerBound,
                              final Optional<K> upperBound,
                              final int pageSize,
                              final Optional<K> startKey,
                              final ResultOrder resultOrder) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("Page size must be positive");
        }
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.pageSize = pageSize;
        this.startKey = startKey;
        this.resultOrder = resultOrder;
    }

    /**
     * Returns the lower bound of the range.
     *
     * @return the lower bound
     */
    public Optional<K> getLowerBound() {
        return lowerBound;
    }

    /**
     * Returns the upper bound of the range.
     *
     * @return the upper bound
     */
    public Optional<K> getUpperBound() {
        return upperBound;
    }

    /**
     * Returns the page size.
     *
     * @return the maximum number of results per page
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Returns the starting key for pagination.
     *
     * @return the starting key (exclusive)
     */
    public Optional<K> getStartKey() {
        return startKey;
    }

    /**
     * Returns the result order.
     *
     * @return the result order
     */
    public ResultOrder getResultOrder() {
        return resultOrder;
    }

    /**
     * Creates a new query for the next page using the provided last key.
     *
     * @param lastKey the last key from the current page
     * @return a new query for the next page
     */
    public PaginatedRangeQuery<K, V> nextPage(final K lastKey) {
        return new PaginatedRangeQuery<>(
            lowerBound,
            upperBound,
            pageSize,
            Optional.of(lastKey),
            resultOrder
        );
    }

    /**
     * Creates a query with a different page size.
     *
     * @param newPageSize the new page size
     * @return a new query with the specified page size
     */
    public PaginatedRangeQuery<K, V> withPageSize(final int newPageSize) {
        return new PaginatedRangeQuery<>(
            lowerBound,
            upperBound,
            newPageSize,
            startKey,
            resultOrder
        );
    }

    /**
     * Creates a query with a different result order.
     *
     * @param newOrder the new result order
     * @return a new query with the specified order
     */
    public PaginatedRangeQuery<K, V> withOrder(final ResultOrder newOrder) {
        return new PaginatedRangeQuery<>(
            lowerBound,
            upperBound,
            pageSize,
            startKey,
            newOrder
        );
    }

    @Override
    public String toString() {
        return "PaginatedRangeQuery{" +
                "lowerBound=" + lowerBound +
                ", upperBound=" + upperBound +
                ", pageSize=" + pageSize +
                ", startKey=" + startKey +
                ", resultOrder=" + resultOrder +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaginatedRangeQuery<?, ?> that = (PaginatedRangeQuery<?, ?>) o;
        return pageSize == that.pageSize &&
                lowerBound.equals(that.lowerBound) &&
                upperBound.equals(that.upperBound) &&
                startKey.equals(that.startKey) &&
                resultOrder == that.resultOrder;
    }

    @Override
    public int hashCode() {
        int result = lowerBound.hashCode();
        result = 31 * result + upperBound.hashCode();
        result = 31 * result + pageSize;
        result = 31 * result + startKey.hashCode();
        result = 31 * result + resultOrder.hashCode();
        return result;
    }
}