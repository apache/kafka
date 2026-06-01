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
 * [{@code anchor.timestamp() - before}, {@code anchor.timestamp() + after}].
 *
 * <p>Use {@link #ofTimeBoundsWithNoGrace} or {@link #ofTimeBoundsAndGrace} to create instances.
 * Use {@link #withMaxRecords(int)} to cap the number of included records.
 */
public final class EventTimeRange<K, V> extends Range<K, V> {

    private final long beforeMs;
    private final long afterMs;
    private int maxRecords = Integer.MAX_VALUE;

    private EventTimeRange(final long beforeMs, final long afterMs, final long gracePeriodMs) {
        super(gracePeriodMs);
        this.beforeMs = beforeMs;
        this.afterMs = afterMs;
    }

    /**
     * Create an {@link EventTimeRange} spanning {@code before} time before and {@code after} time
     * after the anchor record's timestamp. Records that arrive out of order are immediately dropped.
     * Use {@link #ofTimeBoundsAndGrace} to tolerate late arrivals.
     * Use {@link #withMaxRecords(int)} to cap the number of records included.
     *
     * @param before the time before the anchor record's timestamp that defines the start of the
     *               range. Must not be negative.
     * @param after  the time after the anchor record's timestamp that defines the end of the range.
     *               Must not be negative.
     * @return a new {@link EventTimeRange} with no grace period
     * @throws IllegalArgumentException if either duration is negative or cannot be represented as
     *                                  {@code long} milliseconds
     */
    public static <K, V> EventTimeRange<K, V> ofTimeBoundsWithNoGrace(final Duration before, final Duration after) {
        final long beforeMs = validateNonNegativeMs(before, "before");
        final long afterMs = validateNonNegativeMs(after, "after");
        return new EventTimeRange<>(beforeMs, afterMs, 0L);
    }

    /**
     * Create an {@link EventTimeRange} spanning {@code before} time before and {@code after} time
     * after the anchor record's timestamp, accepting late records up to {@code grace} beyond the
     * range boundary. Use {@link #withMaxRecords(int)} to cap the number of records included.
     *
     * @param before the time before the anchor record's timestamp that defines the start of the
     *               range. Must not be negative.
     * @param after  the time after the anchor record's timestamp that defines the end of the range.
     *               Must not be negative.
     * @param grace  the grace period to tolerate late-arriving records. Must not be negative.
     * @return a new {@link EventTimeRange} with the specified grace period
     * @throws IllegalArgumentException if any duration is negative or cannot be represented as
     *                                  {@code long} milliseconds
     */
    public static <K, V> EventTimeRange<K, V> ofTimeBoundsAndGrace(final Duration before, final Duration after, final Duration grace) {
        final long beforeMs = validateNonNegativeMs(before, "before");
        final long afterMs = validateNonNegativeMs(after, "after");
        final long graceMs = validateNonNegativeMs(grace, "grace");
        return new EventTimeRange<>(beforeMs, afterMs, graceMs);
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

    @Override
    public CloseableIterator<Record<K, V>> fetch(final Record<K, V> anchor, final ReadOnlyWindowStore<K, V> store) {
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

    private static final class LimitingWindowStoreIterator<K, V> implements CloseableIterator<Record<K, V>> {

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
