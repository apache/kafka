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
 * Interactive query for issuing range queries and scans over KeyValue stores.
 * <p>
 *  A range query retrieves a set of records, specified using an upper and/or lower bound on the keys.
 * <p>
 *  A scan query retrieves all records contained in the store.
 * <p>
 *  Keys' order is based on the serialized byte[] of the keys, not the 'logical' key order.
 * @param <K> Type of keys
 * @param <V> Type of values
 */
@Evolving
public final class RangeQuery<K, V> implements Query<KeyValueIterator<K, V>> {


    private final Optional<K> lower;
    private final Optional<K> upper;
    private final ResultOrder order;

    private RangeQuery(final Optional<K> lower, final Optional<K> upper, final ResultOrder order) {
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
    public static <K, V> RangeQuery<K, V> withRange(final K lower, final K upper) {
        return new RangeQuery<>(Optional.ofNullable(lower), Optional.ofNullable(upper), ResultOrder.ANY);
    }

    /**
     * Determines if the serialized byte[] of the keys in ascending or descending or unordered order.
     * Order is based on the serialized byte[] of the keys, not the 'logical' key order.
     * @return return the order of returned records based on the serialized byte[] of the keys (can be unordered, or in ascending or in descending order).
     */
    public ResultOrder resultOrder() {
        return order;
    }

    /**
     * Set the query to return the serialized byte[] of the keys in descending order.
     * Order is based on the serialized byte[] of the keys, not the 'logical' key order.
     * @return a new RangeQuery instance with descending flag set.
     */
    public RangeQuery<K, V> withDescendingKeys() {
        return new RangeQuery<>(this.lower, this.upper, ResultOrder.DESCENDING);
    }

    /**
     * Set the query to return the serialized byte[] of the keys in ascending order.
     * Order is based on the serialized byte[] of the keys, not the 'logical' key order.
     * @return a new RangeQuery instance with ascending flag set.
     */
    public RangeQuery<K, V> withAscendingKeys() {
        return new RangeQuery<>(this.lower, this.upper, ResultOrder.ASCENDING);
    }

    /**
     * Interactive range query using an upper bound to filter the keys returned.
     * If both {@code <K,V>} are null, RangQuery returns a full range scan.
     * @param upper The key that specifies the upper bound of the range
     * @param <K> The key type
     * @param <V> The value type
     */
    public static <K, V> RangeQuery<K, V> withUpperBound(final K upper) {
        return new RangeQuery<>(Optional.empty(), Optional.of(upper), ResultOrder.ANY);
    }

    /**
     * Interactive range query using a lower bound to filter the keys returned.
     * @param lower The key that specifies the lower bound of the range
     * @param <K> The key type
     * @param <V> The value type
     */
    public static <K, V> RangeQuery<K, V> withLowerBound(final K lower) {
        return new RangeQuery<>(Optional.of(lower), Optional.empty(), ResultOrder.ANY);
    }

    /**
     * Interactive scan query that returns all records in the store.
     * @param <K> The key type
     * @param <V> The value type
     */
    public static <K, V> RangeQuery<K, V> withNoBounds() {
        return new RangeQuery<>(Optional.empty(), Optional.empty(), ResultOrder.ANY);
    }

    /**
     * The lower bound of the query, if specified.
     */
    public Optional<K> getLowerBound() {
        return lower;
    }

    /**
     * The upper bound of the query, if specified
     */
    public Optional<K> getUpperBound() {
        return upper;
    }
}