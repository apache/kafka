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

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;
import org.apache.kafka.streams.state.ReadOnlyRecordIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;

import java.util.Optional;

/**
 * Interactive query for issuing range queries and scans over a
 * {@link TimestampedKeyValueStoreWithHeaders}, returning each record together with its headers.
 *
 * <p>This is the headers-aware parallel of {@link TimestampedRangeQuery}: it returns a
 * {@link ReadOnlyRecordIterator} of {@link ReadOnlyRecord} elements, each carrying the key, value,
 * timestamp, and headers, whereas {@link TimestampedRangeQuery} returns a
 * {@link org.apache.kafka.streams.state.KeyValueIterator} of
 * {@link org.apache.kafka.streams.state.ValueAndTimestamp} (value and timestamp only, no headers).
 *
 * <p>A range query retrieves a set of records, specified using an upper and/or lower bound on the
 * keys. A scan query (no bounds) retrieves all records contained in the store. Keys' order is based
 * on the serialized {@code byte[]} of the keys, not the 'logical' key order.
 *
 * <p>Headers are persisted and returned only when the store is backed by a native headers store,
 * i.e. built with a KIP-1271 {@code WithHeaders} byte-store supplier (e.g.
 * {@code Stores.persistentTimestampedKeyValueStoreWithHeaders}). A {@code WithHeaders} store built
 * over a legacy (non-headers) supplier cannot persist headers, so the store-served reads come back
 * with empty {@code headers()}.
 *
 * <p>Against a plain store not built with the {@code WithHeaders} builder at all, this query type is
 * unsupported and fails with {@link FailureReason#UNKNOWN_QUERY_TYPE}.
 *
 * <p>Each element is a {@link ReadOnlyRecord}, whose timestamp is non-negative. If the backing store
 * does not persist timestamps -- for example a {@code WithHeaders} store built over a plain
 * {@link org.apache.kafka.streams.state.KeyValueStore} supplier, which surfaces every entry with
 * {@code NO_TIMESTAMP} ({@code -1}) -- that entry cannot be represented, so advancing the returned
 * {@link ReadOnlyRecordIterator} throws {@link org.apache.kafka.streams.errors.StreamsException} at
 * that entry. Back the store with {@code Stores.persistentTimestampedKeyValueStoreWithHeaders(...)} to
 * persist timestamps and headers, or use {@link TimestampedRangeQuery} if headers are not needed.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
@Evolving
@InterfaceAudience.Public
public final class TimestampedRangeWithHeadersQuery<K, V> implements Query<ReadOnlyRecordIterator<K, V>> {

    private final Optional<K> lower;
    private final Optional<K> upper;
    private final ResultOrder order;

    private TimestampedRangeWithHeadersQuery(final Optional<K> lower, final Optional<K> upper, final ResultOrder order) {
        this.lower = lower;
        this.upper = upper;
        this.order = order;
    }

    /**
     * Interactive range query using a lower and upper bound to filter the keys returned.
     * @param lower The key that specifies the lower bound of the range
     * @param upper The key that specifies the upper bound of the range
     * @param <K> The key type
     * @param <V> The value type
     */
    public static <K, V> TimestampedRangeWithHeadersQuery<K, V> withRange(final K lower, final K upper) {
        return new TimestampedRangeWithHeadersQuery<>(Optional.ofNullable(lower), Optional.ofNullable(upper), ResultOrder.ANY);
    }

    /**
     * Interactive range query using an upper bound to filter the keys returned.
     * @param upper The key that specifies the upper bound of the range
     * @param <K> The key type
     * @param <V> The value type
     */
    public static <K, V> TimestampedRangeWithHeadersQuery<K, V> withUpperBound(final K upper) {
        return new TimestampedRangeWithHeadersQuery<>(Optional.empty(), Optional.of(upper), ResultOrder.ANY);
    }

    /**
     * Interactive range query using a lower bound to filter the keys returned.
     * @param lower The key that specifies the lower bound of the range
     * @param <K> The key type
     * @param <V> The value type
     */
    public static <K, V> TimestampedRangeWithHeadersQuery<K, V> withLowerBound(final K lower) {
        return new TimestampedRangeWithHeadersQuery<>(Optional.of(lower), Optional.empty(), ResultOrder.ANY);
    }

    /**
     * Interactive scan query that returns all records in the store.
     * @param <K> The key type
     * @param <V> The value type
     */
    public static <K, V> TimestampedRangeWithHeadersQuery<K, V> withNoBounds() {
        return new TimestampedRangeWithHeadersQuery<>(Optional.empty(), Optional.empty(), ResultOrder.ANY);
    }

    /**
     * Set the query to return the serialized {@code byte[]} of the keys in descending order.
     * Order is based on the serialized {@code byte[]} of the keys, not the 'logical' key order.
     * @return a new query instance with the descending flag set.
     */
    public TimestampedRangeWithHeadersQuery<K, V> withDescendingKeys() {
        return new TimestampedRangeWithHeadersQuery<>(this.lower, this.upper, ResultOrder.DESCENDING);
    }

    /**
     * Set the query to return the serialized {@code byte[]} of the keys in ascending order.
     * Order is based on the serialized {@code byte[]} of the keys, not the 'logical' key order.
     * @return a new query instance with the ascending flag set.
     */
    public TimestampedRangeWithHeadersQuery<K, V> withAscendingKeys() {
        return new TimestampedRangeWithHeadersQuery<>(this.lower, this.upper, ResultOrder.ASCENDING);
    }

    /**
     * The lower bound of the query, if specified.
     */
    public Optional<K> lowerBound() {
        return lower;
    }

    /**
     * The upper bound of the query, if specified.
     */
    public Optional<K> upperBound() {
        return upper;
    }

    /**
     * Determines if the serialized {@code byte[]} of the keys in ascending or descending or unordered order.
     * Order is based on the serialized {@code byte[]} of the keys, not the 'logical' key order.
     * @return the order of the returned records based on the serialized {@code byte[]} of the keys
     *         (can be unordered, ascending, or descending).
     */
    public ResultOrder resultOrder() {
        return order;
    }
}
