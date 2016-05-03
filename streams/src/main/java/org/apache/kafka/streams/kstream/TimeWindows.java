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
    public final long size;

    /**
     * The size of the window's advance interval, i.e. by how much a window moves forward relative
     * to the previous one. The interval's effective time unit is determined by the semantics of
     * the topology's configured {@link org.apache.kafka.streams.processor.TimestampExtractor}.
     */
    public final long advance;

    private TimeWindows(String name, long size, long advance) {
        super(name);
        if (size <= 0) {
            throw new IllegalArgumentException("window size must be > 0 (you provided " + size + ")");
        }
        this.size = size;
        if (!(0 < advance && advance <= size)) {
            throw new IllegalArgumentException(
                String.format("advance interval (%d) must lie within interval (0, %d]", advance, size));
        }
        this.advance = advance;
    }

    /**
     * Returns a window definition with the given window size, and with the advance interval being
     * equal to the window size. Think: [N * size, N * size + size), with N denoting the N-th
     * window.
     *
     * This provides the semantics of tumbling windows, which are fixed-sized, gap-less,
     * non-overlapping windows. Tumbling windows are a specialization of hopping windows.
     *
     * @param name The name of the window. Must not be null or empty.
     * @param size The size of the window, with the requirement that size &gt; 0.
     *             The window size's effective time unit is determined by the semantics of the
     *             topology's configured {@link org.apache.kafka.streams.processor.TimestampExtractor}.
     * @return a new window definition
     */
    public static TimeWindows of(String name, long size) {
        return new TimeWindows(name, size, size);
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
    public TimeWindows advanceBy(long interval) {
        return new TimeWindows(this.name, this.size, interval);
    }

    @Override
    public Map<Long, TimeWindow> windowsFor(long timestamp) {
        long enclosed = (size - 1) / advance;
        long windowStart = Math.max(0, timestamp - timestamp % advance - enclosed * advance);

        Map<Long, TimeWindow> windows = new HashMap<>();
        while (windowStart <= timestamp) {
            TimeWindow window = new TimeWindow(windowStart, windowStart + this.size);
            windows.put(windowStart, window);
            windowStart += this.advance;
        }
        return windows;
    }

    @Override
    public final boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof TimeWindows)) {
            return false;
        }
        TimeWindows other = (TimeWindows) o;
        return this.size == other.size && this.advance == other.advance;
    }

    @Override
    public int hashCode() {
        int result = (int) (size ^ (size >>> 32));
        result = 31 * result + (int) (advance ^ (advance >>> 32));
        return result;
    }

}