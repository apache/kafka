/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.HashMap;
import java.util.Map;

/**
 * The fixed-size time-based window specifications used for aggregations.
 * <p>
 * The semantics of time-based aggregation windows are: Every T1 (advance) milliseconds, compute the aggregate total for
 * T2 (size) milliseconds.
 * <ul>
 *     <li> If {@code advance < size} a hopping windows is defined:<br />
 *          it discretize a stream into overlapping windows, which implies that a record maybe contained in one and or
 *          more "adjacent" windows.</li>
 *     <li> If {@code advance == size} a tumbling window is defined:<br />
 *          it discretize a stream into non-overlapping windows, which implies that a record is only ever contained in
 *          one and only one tumbling window.</li>
 * </ul>
 * Thus, the specified {@link TimeWindow}s are aligned to the epoch.
 * Aligned to the epoch means, that the first window starts at timestamp zero.
 * For example, hopping windows with size of 5000ms and advance of 3000ms, have window boundaries
 * [0;5000),[3000;8000),... and not [1000;6000),[4000;9000),... or even something "random" like [1452;6452),[4452;9452),...
 * <p>
 * For time semantics, see {@link TimestampExtractor}.
 *
 * @see SessionWindows
 * @see UnlimitedWindows
 * @see JoinWindows
 * @see KGroupedStream#count(Windows, String)
 * @see KGroupedStream#count(Windows, org.apache.kafka.streams.processor.StateStoreSupplier)
 * @see KGroupedStream#reduce(Reducer, Windows, String)
 * @see KGroupedStream#reduce(Reducer, Windows, org.apache.kafka.streams.processor.StateStoreSupplier)
 * @see KGroupedStream#aggregate(Initializer, Aggregator, Windows, org.apache.kafka.common.serialization.Serde, String)
 * @see KGroupedStream#aggregate(Initializer, Aggregator, Windows, org.apache.kafka.streams.processor.StateStoreSupplier)
 * @see TimestampExtractor
 */
@InterfaceStability.Unstable
public final class TimeWindows extends Windows<TimeWindow> {

    /** The size of the windows in milliseconds. */
    public final long sizeMs;

    /**
     * The size of the window's advance interval in milliseconds, i.e., by how much a window moves forward relative to
     * the previous one.
     */
    public final long advanceMs;

    private TimeWindows(final long sizeMs, final long advanceMs) {
        this.sizeMs = sizeMs;
        this.advanceMs = advanceMs;
    }

    /**
     * Return a window definition with the given window size, and with the advance interval being equal to the window
     * size.
     * The time interval represented by the the N-th window is: {@code [N * size, N * size + size)}.
     * <p>
     * This provides the semantics of tumbling windows, which are fixed-sized, gap-less, non-overlapping windows.
     * Tumbling windows are a special case of hopping windows with {@code advance == size}.
     *
     * @param sizeMs The size of the window in milliseconds
     * @return a new window definition with default maintain duration of 1 day
     * @throws IllegalArgumentException if the specified window size is zero or negative
     */
    public static TimeWindows of(final long sizeMs) throws IllegalArgumentException {
        if (sizeMs <= 0) {
            throw new IllegalArgumentException("Window size (sizeMs) must be larger than zero.");
        }
        return new TimeWindows(sizeMs, sizeMs);
    }

    /**
     * Return a window definition with the original size, but advance ("hop") the window by the given interval, which
     * specifies by how much a window moves forward relative to the previous one.
     * The time interval represented by the the N-th window is: {@code [N * advance, N * advance + size)}.
     * <p>
     * This provides the semantics of hopping windows, which are fixed-sized, overlapping windows.
     *
     * @param advanceMs The advance interval ("hop") in milliseconds of the window, with the requirement that
     *                  {@code 0 < advanceMs &le; sizeMs}.
     * @return a new window definition with default maintain duration of 1 day
     * @throws IllegalArgumentException if the advance interval is negative, zero, or larger-or-equal the window size
     */
    public TimeWindows advanceBy(final long advanceMs) {
        if (advanceMs <= 0 || advanceMs > sizeMs) {
            throw new IllegalArgumentException(String.format("AdvanceMs must lie within interval (0, %d].", sizeMs));
        }
        return new TimeWindows(sizeMs, advanceMs);
    }

    @Override
    public Map<Long, TimeWindow> windowsFor(final long timestamp) {
        long windowStart = (Math.max(0, timestamp - sizeMs + advanceMs) / advanceMs) * advanceMs;
        final Map<Long, TimeWindow> windows = new HashMap<>();
        while (windowStart <= timestamp) {
            final TimeWindow window = new TimeWindow(windowStart, windowStart + sizeMs);
            windows.put(windowStart, window);
            windowStart += advanceMs;
        }
        return windows;
    }

    @Override
    public long size() {
        return sizeMs;
    }

    /**
     * @param durationMs the window retention time
     * @return itself
     * @throws IllegalArgumentException if {@code duration} is smaller than the window size
     */
    @Override
    public TimeWindows until(final long durationMs) throws IllegalArgumentException {
        if (durationMs < sizeMs) {
            throw new IllegalArgumentException("Window retention time (durationMs) cannot be smaller than the window size.");
        }
        super.until(durationMs);
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * For {@code TimeWindows} the maintain duration is at least as small as the window size.
     *
     * @return the window maintain duration
     */
    @Override
    public long maintainMs() {
        return Math.max(super.maintainMs(), sizeMs);
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof TimeWindows)) {
            return false;
        }
        final TimeWindows other = (TimeWindows) o;
        return sizeMs == other.sizeMs && advanceMs == other.advanceMs;
    }

    @Override
    public int hashCode() {
        int result = (int) (sizeMs ^ (sizeMs >>> 32));
        result = 31 * result + (int) (advanceMs ^ (advanceMs >>> 32));
        return result;
    }

}
