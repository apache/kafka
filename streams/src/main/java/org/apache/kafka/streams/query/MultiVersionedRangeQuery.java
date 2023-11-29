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
import java.util.Optional;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.VersionedRecord;

/**
 * Interactive query for retrieving a set of records with keys within a specified key range and time range.
 */

@Evolving
public final class MultiVersionedRangeQuery<K, V> implements Query<KeyValueIterator<K, VersionedRecord<V>>> {
    private final Optional<K> lower;
    private final Optional<K> upper;
    private final Optional<Instant> fromTime;

    private final Optional<Instant> toTime;
    private final ResultOrder keyOrder;
    private final ResultOrder timestampOrder;

    private MultiVersionedRangeQuery(final Optional<K> lower, final Optional<K> upper,
                                     final Optional<Instant> fromTime, final Optional<Instant> toTime,
                                     final ResultOrder keyOrder, final ResultOrder timestampOrder) {
        this.lower = lower;
        this.upper = upper;
        this.fromTime = fromTime;
        this.toTime = toTime;
        this.keyOrder = keyOrder;
        this.timestampOrder = timestampOrder;
    }

    /**
    * Interactive range query using a lower and upper bound to filter the keys returned. * For each
    * key the records valid within the specified time range are returned. * In case the time range is
    * not specified just the latest record for each key is returned.
    * @param lower The key that specifies the lower bound of the range
    * @param upper The key that specifies the upper bound of the range
    * @param <K> The key type
    * @param <V> The value type
    */
    public static <K, V> MultiVersionedRangeQuery<K, V> withKeyRange(final K lower, final K upper) {
        return new MultiVersionedRangeQuery<>(Optional.of(lower), Optional.of(upper), Optional.empty(), Optional.empty(), ResultOrder.ANY, ResultOrder.ANY);
    }


    /**
    * Interactive range query using a lower bound to filter the keys returned. * For each key the
    * records valid within the specified time range are returned. * In case the time range is not
    * specified just the latest record for each key is returned.
    * @param lower The key that specifies the lower bound of the range
    * @param <K>   The key type
    * @param <V>   The value type
    */
    public static <K, V> MultiVersionedRangeQuery<K, V> withLowerKeyBound(final K lower) {
        return new MultiVersionedRangeQuery<>(Optional.of(lower), Optional.empty(), Optional.empty(), Optional.empty(), ResultOrder.ANY, ResultOrder.ANY);
    }

    /**
    * Interactive range query using a lower bound to filter the keys returned. * For each key the
    * records valid within the specified time range are returned. * In case the time range is not
    * specified just the latest record for each key is returned.
    * @param upper The key that specifies the lower bound of the range
    * @param <K>   The key type
    * @param <V>   The value type
    */
    public static <K, V> MultiVersionedRangeQuery<K, V> withUpperKeyBound(final K upper) {
        return new MultiVersionedRangeQuery<>(Optional.empty(), Optional.of(upper), Optional.empty(), Optional.empty(), ResultOrder.ANY, ResultOrder.ANY);
    }

    /**
    * Interactive scan query that returns all records in the store. * For each key the records valid
    * within the specified time range are returned. * In case the time range is not specified just
    * the latest record for each key is returned.
    * @param <K> The key type
    * @param <V> The value type
    */
    public static <K, V> MultiVersionedRangeQuery<K, V> allKeys() {
        return new MultiVersionedRangeQuery<>(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), ResultOrder.ANY, ResultOrder.ANY);
    }

    /**
    * Specifies the starting time point for the key query. The range query returns all the records
    * that are valid in the time range starting from the timestamp {@code fromTimestamp}.
    * @param fromTime The starting time point
    */
    public MultiVersionedRangeQuery<K, V> fromTime(final Instant fromTime) {
        if (fromTime == null) {
            return new MultiVersionedRangeQuery<>(lower, upper, Optional.empty(), toTime, keyOrder, timestampOrder);
        }
        return new MultiVersionedRangeQuery<>(lower, upper, Optional.of(fromTime), toTime, keyOrder, timestampOrder);
    }

    /**
    * Specifies the ending time point for the key query. The range query returns all the records that
    * have timestamp <= {@code asOfTimestamp}.
    * @param toTime The ending time point
    */
    public MultiVersionedRangeQuery<K, V> toTime(final Instant toTime) {
        if (toTime == null) {
            return new MultiVersionedRangeQuery<>(lower, upper, fromTime, Optional.empty(),  keyOrder, timestampOrder);
        }
        return new MultiVersionedRangeQuery<>(lower, upper, fromTime, Optional.of(toTime), keyOrder, timestampOrder);
    }

    /**
    * Specifies the order of keys as ascending.
    */
    public MultiVersionedRangeQuery<K, V> withAscendingKeys() {
        return new MultiVersionedRangeQuery<>(lower, upper, fromTime, toTime, ResultOrder.ASCENDING, timestampOrder);
    }

    /**
    * Specifies the order of keys as descending.
    */
    public MultiVersionedRangeQuery<K, V> withDescendingKeys() {
        return new MultiVersionedRangeQuery<>(lower, upper, fromTime, toTime, ResultOrder.DESCENDING, timestampOrder);
    }

    /**
    * Specifies the order of the timestamps as ascending.
    */
    public MultiVersionedRangeQuery<K, V> withAscendingTimestamps() {
        return new MultiVersionedRangeQuery<>(lower, upper, fromTime, toTime, keyOrder, ResultOrder.ASCENDING);
    }

    /**
    * Specifies the order of the timestamps as descending.
    */
    public MultiVersionedRangeQuery<K, V> withDescendingTimestamps() {
        return new MultiVersionedRangeQuery<>(lower, upper, fromTime, toTime, keyOrder, ResultOrder.DESCENDING);
    }

    /**
    * The lower bound of the query, if specified.
    */
    public Optional<K> lowerKeyBound() {
        return lower;
    }

    /**
    * The upper bound of the query, if specified
    */
    public Optional<K> upperKeyBound() {
        return upper;
    }

    /**
    * The starting time point of the query, if specified
    */
    public Optional<Instant> fromTime() {
        return fromTime;
    }

    /**
    * The ending time point of the query, if specified
    */
    public Optional<Instant> toTime() {
        return toTime;
    }

    /**
    * @return the order of keys
    */
    public ResultOrder keyOrder() {
        return keyOrder;
    }

    /**
    * @return the order of timestamps
    */
    public ResultOrder timestampOrder() {
        return timestampOrder;
    }
}