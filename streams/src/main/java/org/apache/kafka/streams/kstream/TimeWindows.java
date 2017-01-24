/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.util.HashMap;
import java.util.Map;

/**
 * The time-based window specifications used for aggregations.
 * <p>
 * The semantics of a time-based window are: Every T1 (advance) time-units, compute the aggregate total for T2 (size) time-units.
 * <ul>
 *     <li> If {@code advance < size} a hopping windows is defined: <br />
 *          it discretize a stream into overlapping windows, which implies that a record maybe contained in one and or more "adjacent" windows.</li>
 *     <li> If {@code advance == size} a tumbling window is defined:<br />
 *          it discretize a stream into non-overlapping windows, which implies that a record is only ever contained in one and only one tumbling window.</li>
 * </ul>
 */
public class TimeWindows extends Windows<TimeWindow> {

    /**
     * The size of the window, i.e. how long a window lasts.
     * The window size's effective time unit is determined by the semantics of the topology's
     * configured {@link org.apache.kafka.streams.processor.TimestampExtractor}.
     */
    public final long sizeMs;

    /**
     * The size of the window's advance interval, i.e. by how much a window moves forward relative
     * to the previous one. The interval's effective time unit is determined by the semantics of
     * the topology's configured {@link org.apache.kafka.streams.processor.TimestampExtractor}.
     */
    public final long advanceMs;

    private TimeWindows(final long sizeMs, final long advanceMs) throws IllegalArgumentException {
        this.sizeMs = sizeMs;
        this.advanceMs = advanceMs;
    }

    /**
     * Returns a window definition with the given window size, and with the advance interval being
     * equal to the window size. Think: [N * size, N * size + size), with N denoting the N-th
     * window.
     *
     * This provides the semantics of tumbling windows, which are fixed-sized, gap-less,
     * non-overlapping windows. Tumbling windows are a specialization of hopping windows.
     *
     * @param size The size of the window, with the requirement that size &gt; 0.
     *             The window size's effective time unit is determined by the semantics of the
     *             topology's configured {@link org.apache.kafka.streams.processor.TimestampExtractor}.
     * @return a new window definition
     */
    public static TimeWindows of(final long sizeMs) throws IllegalArgumentException {
        if (sizeMs <= 0) {
            throw new IllegalArgumentException("Window sizeMs must be larger than zero.");
        }
        return new TimeWindows(sizeMs, sizeMs);
    }

    /**
     * Returns a window definition with the original size, but advance ("hop") the window by the given
     * interval, which specifies by how much a window moves forward relative to the previous one.
     * Think: [N * advanceInterval, N * advanceInterval + size), with N denoting the N-th window.
     *
     * This provides the semantics of hopping windows, which are fixed-sized, overlapping windows.
     *
     * @param interval The advance interval ("hop") of the window, with the requirement that
     *                 0 &lt; interval &le; size. The interval's effective time unit is
     *                 determined by the semantics of the topology's configured
     *                 {@link org.apache.kafka.streams.processor.TimestampExtractor}.
     * @return a new window definition
     */
    public TimeWindows advanceBy(final long advanceMs) {
        if (advanceMs <= 0 || advanceMs > sizeMs) {
            throw new IllegalArgumentException(String.format("AdvanceMs must lie within interval (0, %d]", sizeMs));
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

    public TimeWindows until(final long durationMs) throws IllegalArgumentException {
        if (durationMs < sizeMs) {
            throw new IllegalArgumentException("Window retention time (durationMs) cannot be smaller than the window size.");
        }
        super.until(durationMs);
        return this;
    }

    @Override
    public long maintainMs() {
        return Math.max(super.maintainMs(), sizeMs);
    }

    @Override
    public final boolean equals(final Object o) {
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