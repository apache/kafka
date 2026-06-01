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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A count-based {@link Range} that includes up to {@code before} records preceding the anchor and
 * up to {@code after} records following it in event-time order. A required time floor
 * {@code maxTimeBefore} prevents looking back arbitrarily far.
 *
 * <p>Use {@link #ofCountBoundsWithNoGrace} or {@link #ofCountBoundsAndGrace} to create instances.
 * Use {@link #withMaxTimeAfter(Duration)} to add an optional time ceiling on the forward direction.
 */
public final class EventCountRange<K, V> extends Range<K, V> {

    private final int before;
    private final int after;
    private final long maxTimeBeforeMs;
    private Long maxTimeAfterMs = null;

    private EventCountRange(final int before, final int after, final long maxTimeBeforeMs, final long gracePeriodMs) {
        super(gracePeriodMs);
        this.before = before;
        this.after = after;
        this.maxTimeBeforeMs = maxTimeBeforeMs;
    }

    /**
     * Create an {@link EventCountRange} including {@code before} records before and {@code after}
     * records after the anchor record in event-time order. {@code maxTimeBefore} sets a required
     * time floor: records older than {@code anchor.timestamp() - maxTimeBefore} are excluded
     * regardless of count. Records that arrive out of order are immediately dropped.
     * Use {@link #ofCountBoundsAndGrace} to tolerate late arrivals.
     * Use {@link #withMaxTimeAfter(Duration)} to add an optional time ceiling.
     *
     * @param before        the number of records before the anchor to include. Must not be negative.
     * @param after         the number of records after the anchor to include. Must not be negative.
     * @param maxTimeBefore the maximum time before the anchor's timestamp to look back. Must not be
     *                      negative.
     * @return a new {@link EventCountRange} with no grace period
     * @throws IllegalArgumentException if any count is negative or the duration cannot be
     *                                  represented as {@code long} milliseconds
     */
    public static <K, V> EventCountRange<K, V> ofCountBoundsWithNoGrace(final int before, final int after, final Duration maxTimeBefore) {
        validateNonNegativeCount(before, "before");
        validateNonNegativeCount(after, "after");
        final long maxTimeBeforeMs = validateNonNegativeMs(maxTimeBefore, "maxTimeBefore");
        return new EventCountRange<>(before, after, maxTimeBeforeMs, 0L);
    }

    /**
     * Create an {@link EventCountRange} including {@code before} records before and {@code after}
     * records after the anchor record in event-time order, accepting late records up to
     * {@code grace} beyond the range boundary. {@code maxTimeBefore} sets a required time floor.
     * Use {@link #withMaxTimeAfter(Duration)} to add an optional time ceiling.
     *
     * @param before        the number of records before the anchor to include. Must not be negative.
     * @param after         the number of records after the anchor to include. Must not be negative.
     * @param maxTimeBefore the maximum time before the anchor's timestamp to look back. Must not be
     *                      negative.
     * @param grace         the grace period to tolerate late-arriving records. Must not be negative.
     * @return a new {@link EventCountRange} with the specified grace period
     * @throws IllegalArgumentException if any count is negative or any duration cannot be
     *                                  represented as {@code long} milliseconds
     */
    public static <K, V> EventCountRange<K, V> ofCountBoundsAndGrace(final int before, final int after, final Duration maxTimeBefore, final Duration grace) {
        validateNonNegativeCount(before, "before");
        validateNonNegativeCount(after, "after");
        final long maxTimeBeforeMs = validateNonNegativeMs(maxTimeBefore, "maxTimeBefore");
        final long graceMs = validateNonNegativeMs(grace, "grace");
        return new EventCountRange<>(before, after, maxTimeBeforeMs, graceMs);
    }

    /**
     * Set an optional time ceiling on the forward direction. Records newer than
     * {@code anchor.timestamp() + maxTimeAfter} are excluded regardless of count.
     *
     * @param maxTimeAfter the maximum time after the anchor's timestamp to look forward. Must not
     *                     be negative.
     * @return this {@link EventCountRange}
     * @throws IllegalArgumentException if the duration is negative or cannot be represented as
     *                                  {@code long} milliseconds
     */
    public EventCountRange<K, V> withMaxTimeAfter(final Duration maxTimeAfter) {
        this.maxTimeAfterMs = validateNonNegativeMs(maxTimeAfter, "maxTimeAfter");
        return this;
    }

    @Override
    public CloseableIterator<Record<K, V>> fetch(final Record<K, V> anchor, final ReadOnlyWindowStore<K, V> store) {
        final long from = anchor.timestamp() - maxTimeBeforeMs;
        final long forwardTo = maxTimeAfterMs != null ? anchor.timestamp() + maxTimeAfterMs : Long.MAX_VALUE;

        // Backward fetch: collect `before+1` records (anchor + up to `before` before it), oldest first
        final List<Record<K, V>> backwardRecords = new ArrayList<>(before + 1);
        try (final WindowStoreIterator<V> backIt = store.backwardFetch(
                anchor.key(),
                Instant.ofEpochMilli(from),
                Instant.ofEpochMilli(anchor.timestamp()))) {
            int count = 0;
            while (backIt.hasNext() && count <= before) {
                final KeyValue<Long, V> kv = backIt.next();
                backwardRecords.add(0, new Record<>(anchor.key(), kv.value, kv.key));
                count++;
            }
        }

        // Forward fetch: up to `after` records strictly after the anchor's timestamp
        final WindowStoreIterator<V> forwardIt = store.fetch(
            anchor.key(),
            Instant.ofEpochMilli(anchor.timestamp() + 1),
            Instant.ofEpochMilli(forwardTo)
        );

        return new ConcatenatingIterator<>(backwardRecords.iterator(), forwardIt, anchor.key(), after);
    }

    @Override
    protected long rangeRetentionMs() {
        return maxTimeBeforeMs;
    }

    private static void validateNonNegativeCount(final int count, final String name) {
        if (count < 0) {
            throw new IllegalArgumentException(name + " must not be negative, got: " + count);
        }
    }

    private static long validateNonNegativeMs(final Duration duration, final String name) {
        final long ms = ApiUtils.validateMillisecondDuration(duration, name);
        if (ms < 0) {
            throw new IllegalArgumentException(name + " must not be negative, got: " + duration);
        }
        return ms;
    }

    private static final class ConcatenatingIterator<K, V> implements CloseableIterator<Record<K, V>> {

        private final Iterator<Record<K, V>> backwardPart;
        private final WindowStoreIterator<V> forwardIt;
        private final K key;
        private final int afterLimit;
        private int forwardCount = 0;
        private boolean forwardDone = false;

        ConcatenatingIterator(
            final Iterator<Record<K, V>> backwardPart,
            final WindowStoreIterator<V> forwardIt,
            final K key,
            final int afterLimit
        ) {
            this.backwardPart = backwardPart;
            this.forwardIt = forwardIt;
            this.key = key;
            this.afterLimit = afterLimit;
        }

        @Override
        public boolean hasNext() {
            if (backwardPart.hasNext()) return true;
            if (forwardDone) return false;
            if (forwardCount >= afterLimit || !forwardIt.hasNext()) {
                close();
                return false;
            }
            return true;
        }

        @Override
        public void close() {
            if (!forwardDone) {
                forwardIt.close();
                forwardDone = true;
            }
        }

        @Override
        public Record<K, V> next() {
            if (!hasNext()) throw new NoSuchElementException();
            if (backwardPart.hasNext()) return backwardPart.next();
            final KeyValue<Long, V> kv = forwardIt.next();
            forwardCount++;
            return new Record<>(key, kv.value, kv.key);
        }
    }
}
