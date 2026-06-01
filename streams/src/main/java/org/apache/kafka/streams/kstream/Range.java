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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Defines a range of records around an anchor record, and fetches them from the buffer store.
 * Built-in implementations are provided via {@link EventTimeRange} and {@link EventCountRange}.
 * Custom subclasses can be provided to define arbitrary range logic.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public abstract class Range<K, V> {

    private final long gracePeriodMs;

    /**
     * @param gracePeriodMs the grace period in milliseconds. Must not be negative.
     */
    protected Range(final long gracePeriodMs) {
        this.gracePeriodMs = gracePeriodMs;
    }

    /**
     * Fetch the records that fall within this range for the given anchor record.
     * The anchor record itself should be included in the returned iterator.
     * Records should be ordered by their timestamps in ascending order.
     *
     * <p>The framework guarantees that the returned iterator will be closed after aggregation
     * completes, even if the aggregator exits early or throws an exception.
     *
     * <p>Note: headers are not preserved in the returned records, as they are not stored in the
     * underlying {@link WindowStore}. Headers are only available on the {@code anchor} record.
     *
     * @param anchor the record that triggered the range evaluation
     * @param store  the buffer store holding records for the anchor's group key
     * @return a closeable iterator of records that fall within the defined range
     */
    public abstract CloseableIterator<Record<K, V>> fetch(Record<K, V> anchor, ReadOnlyWindowStore<K, V> store);

    /**
     * @return the grace period in milliseconds. Records arriving after stream time has advanced
     * beyond the range's natural boundary plus this value will be dropped.
     */
    public long gracePeriodMs() {
        return gracePeriodMs;
    }

    /**
     * The minimum retention the buffer {@link WindowStore} must be configured with, excluding
     * the grace period. Implementations should return the oldest a record can be relative to an
     * anchor's timestamp and still fall within the range (e.g. {@code before} for
     * {@link EventTimeRange}, {@code maxTimeBefore} for {@link EventCountRange}).
     *
     * @return the range-specific retention in milliseconds, excluding grace period
     */
    protected abstract long rangeRetentionMs();

    /**
     * The minimum retention the buffer {@link WindowStore} must be configured with to correctly
     * serve this range, including the grace period. This is what {@code rangeOver()} validates
     * against the {@link Materialized} retention.
     *
     * @return the total required retention in milliseconds
     */
    public long retentionMs() {
        return rangeRetentionMs() + gracePeriodMs;
    }
}
