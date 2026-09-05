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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;

/**
 * A time-based {@link Range} that includes all records whose timestamps fall within
 * [{@code anchor.timestamp() - before}, {@code anchor.timestamp()}].
 *
 * <p>Use {@link #ofTimeBoundsWithNoGrace} or {@link #ofTimeBoundsAndGrace} to create instances.
 * Use {@link #withMaxRecords(int)} to cap the number of included records.
 * Use {@link #withLookAhead(Duration)} to include lookahead for out-of-order anchor context resolution.
 */
public final class EventTimeRange<K, V> extends Range<K, V> {

    private final long beforeMs;
    private long afterMs = 0L;
    private int maxRecords = Integer.MAX_VALUE;

    private EventTimeRange(final long beforeMs, final long gracePeriodMs) {
        super(gracePeriodMs);
        this.beforeMs = beforeMs;
    }

    /**
     * Create an {@link EventTimeRange} spanning {@code before} time prior to the anchor
     * record's timestamp, capping the upper boundary strictly at the anchor record (Current Row).
     * Late-arriving records past the stream time watermark are dropped.
     * Use {@link #ofTimeBoundsAndGrace(Duration, Duration)} to tolerate late arrivals.
     * Use {@link #withMaxRecords(int)} to cap the number of records included.
     *
     * @param before the time before the anchor record's timestamp that defines the start of the
     *               range. Must not be negative.
     * @return a new {@link EventTimeRange} with no grace period
     * @throws IllegalArgumentException if the duration is negative or cannot be represented as
     *                                  {@code long} milliseconds
     */
    public static <K, V> EventTimeRange<K, V> ofTimeBoundsWithNoGrace(final Duration before) {
        final long beforeMs = validateNonNegativeMs(before, "before");
        return new EventTimeRange<>(beforeMs, 0L);
    }

    /**
     * Create an {@link EventTimeRange} spanning {@code before} time prior to the anchor
     * record's timestamp, accepting late records up to {@code grace} beyond the range boundary.
     * Use {@link #withMaxRecords(int)} to cap the number of records included.
     *
     * @param before the time before the anchor record's timestamp that defines the start of the
     *               range. Must not be negative.
     * @param grace  the grace period to tolerate late-arriving records. Must not be negative.
     * @return a new {@link EventTimeRange} with the specified grace period
     * @throws IllegalArgumentException if any duration is negative or cannot be represented as
     *                                  {@code long} milliseconds
     */
    public static <K, V> EventTimeRange<K, V> ofTimeBoundsAndGrace(final Duration before, final Duration grace) {
        final long beforeMs = validateNonNegativeMs(before, "before");
        final long graceMs = validateNonNegativeMs(grace, "grace");
        return new EventTimeRange<>(beforeMs, graceMs);
    }

    /**
     * Cap the number of records included in the range. If more records fall within the time
     * boundaries, the newest records are dropped.
     *
     * @param maxRecords the maximum number of records to include. Must be positive.
     * @return this {@link EventTimeRange}
     * @throws IllegalArgumentException if {@code maxRecords} is not positive
     */
    public EventTimeRange<K, V> withMaxRecords(final int maxRecords) {
        if (maxRecords <= 0) {
            throw new IllegalArgumentException("maxRecords must be positive, got: " + maxRecords);
        }
        this.maxRecords = maxRecords;
        return this;
    }

    /**
     * Enable a forward-looking window over newer records that have already been
     * buffered in the state store (useful for out-of-order anchor context resolution).
     *
     * @param after the time after the anchor record's timestamp to look forward. Must not be negative.
     * @return this {@link EventTimeRange} instance
     * @throws IllegalArgumentException if the duration is negative or can't be represented as {@code long milliseconds}
     */
    public EventTimeRange<K, V> withLookAhead(final Duration after) {
        this.afterMs = validateNonNegativeMs(after, "after");
        return this;
    }

    @Override
    public RangedRecordIterator<Record<K, V>> fetch(final Record<K, V> anchor, final ReadOnlyWindowStore<K, V> store) {
        final Instant from = Instant.ofEpochMilli(anchor.timestamp() - beforeMs);
        final Instant to = Instant.ofEpochMilli(anchor.timestamp() + afterMs);
        return new LimitingWindowStoreIterator<>(store.fetch(anchor.key(), from, to), anchor.key(), maxRecords);
    }

    @Override
    protected long rangeRetentionMs() {
        return beforeMs;
    }

    private static long validateNonNegativeMs(final Duration duration, final String name) {
        final long ms = ApiUtils.validateMillisecondDuration(duration, name);
        if (ms < 0) {
            throw new IllegalArgumentException(name + " must not be negative, got: " + duration);
        }
        return ms;
    }

    private static final class LimitingWindowStoreIterator<K, V> implements RangedRecordIterator<Record<K, V>> {

        private final WindowStoreIterator<V> inner;
        private final K key;
        private final int limit;
        private int count = 0;
        private boolean done = false;

        LimitingWindowStoreIterator(final WindowStoreIterator<V> inner, final K key, final int limit) {
            this.inner = inner;
            this.key = key;
            this.limit = limit;
        }

        @Override
        public boolean hasNext() {
            if (done) return false;
            if (count >= limit || !inner.hasNext()) {
                close();
                return false;
            }
            return true;
        }

        @Override
        public Record<K, V> next() {
            if (!hasNext()) throw new NoSuchElementException();
            final KeyValue<Long, V> kv = inner.next();
            count++;
            return new Record<>(key, kv.value, kv.key);
        }

        @Override
        public void close() {
            if (!done) {
                inner.close();
                done = true;
            }
        }
    }
}
