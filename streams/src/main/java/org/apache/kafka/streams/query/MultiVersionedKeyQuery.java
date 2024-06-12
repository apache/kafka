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

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.streams.state.VersionedRecordIterator;

/**
 * Interactive query for retrieving a set of records with the same specified key and different timestamps within the specified time range.
 * No ordering is guaranteed for the results, but the results can be sorted by timestamp (in ascending or descending order) by calling the corresponding defined methods.
 *
 *  @param <K> The type of the key.
 *  @param <V> The type of the result returned by this query.
 */
@Evolving
public final class MultiVersionedKeyQuery<K, V> implements Query<VersionedRecordIterator<V>> {

    private final K key;
    private final Optional<Instant> fromTime;
    private final Optional<Instant> toTime;
    private final ResultOrder order;

    private MultiVersionedKeyQuery(final K key, final Optional<Instant> fromTime, final Optional<Instant> toTime, final ResultOrder order) {
        this.key = key;
        this.fromTime = fromTime;
        this.toTime = toTime;
        this.order = order;
    }

  /**
   * Creates a query that will retrieve the set of records identified by {@code key} if any exists
   * (or {@code null} otherwise).
   *
   * <p>
   * While the query by default returns the all the record versions of the specified {@code key}, setting
   * the {@code fromTimestamp} (by calling the {@link #fromTime(Instant)} method), and the {@code toTimestamp}
   * (by calling the {@link #toTime(Instant)} method) makes the query to return the record versions associated
   * to the specified time range.
   *
   * @param key The specified key by the query
   * @param <K> The type of the key
   * @param <V> The type of the value that will be retrieved
   * @throws NullPointerException if {@code key} is null
   */
    public static <K, V> MultiVersionedKeyQuery<K, V> withKey(final K key) {
        Objects.requireNonNull(key, "key cannot be null.");
        return new MultiVersionedKeyQuery<>(key, Optional.empty(), Optional.empty(), ResultOrder.ANY);
    }

    /**
     * Specifies the starting time point for the key query.
     * <p>
     * The key query returns all the records that are still existing in the time range starting from the timestamp {@code fromTime}. There can
     * be records which have been inserted before the {@code fromTime} and are still valid in the query specified time range (the whole time range
     * or even partially). The key query in fact returns all the records that have NOT become tombstone at or after {@code fromTime}.
     *
     * @param fromTime The starting time point
     * If {@code fromTime} is null, it will be considered as negative infinity, ie, no lower bound
     */
    public MultiVersionedKeyQuery<K, V> fromTime(final Instant fromTime) {
        return new MultiVersionedKeyQuery<>(key, Optional.ofNullable(fromTime), toTime, order);
    }

    /**
     * Specifies the ending time point for the key query.
     * The key query returns all the records that have timestamp &lt;= toTime.
     *
     * @param toTime The ending time point
     * If @param toTime is null, will be considered as positive infinity, ie, no upper bound
     */
    public MultiVersionedKeyQuery<K, V> toTime(final Instant toTime) {
        return new MultiVersionedKeyQuery<>(key, fromTime, Optional.ofNullable(toTime), order);
    }

    /**
     * Specifies the order of the returned records by the query as descending by timestamp.
     */
    public MultiVersionedKeyQuery<K, V> withDescendingTimestamps() {
        return new MultiVersionedKeyQuery<>(key, fromTime, toTime, ResultOrder.DESCENDING);
    }

    /**
     * Specifies the order of the returned records by the query as ascending by timestamp.
     */
    public MultiVersionedKeyQuery<K, V> withAscendingTimestamps() {
        return new MultiVersionedKeyQuery<>(key, fromTime, toTime, ResultOrder.ASCENDING);
    }

    /**
     * The key that was specified for this query.
     * @return The specified {@code key} of the query.
     */
    public K key() {
        return key;
    }

    /**
     * The starting time point of the query, if specified
     * @return The specified {@code fromTime} of the query.
     */
    public Optional<Instant> fromTime() {
        return fromTime;
    }

    /**
     * The ending time point of the query, if specified
     * @return The specified {@code toTime} of the query.
     */
    public Optional<Instant> toTime() {
        return toTime;
    }

    /**
     * The order of the returned records by timestamp.
     * @return the order of returned records based on timestamp (can be unordered, or in ascending, or in descending order of timestamps).
     */
    public ResultOrder resultOrder() {
        return order;
    }
}
