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
 */
public class TimeWindows extends Windows<TimeWindow> {

    /**
     * The size of the window, i.e. how long a window lasts.
     * The window size's effective time unit is determined by the semantics of the topology's
     * configured {@link org.apache.kafka.streams.processor.TimestampExtractor}.
     */
    public final long size;

    /**
     * The size of the window's hop, i.e. by how much a window moves forward relative to the previous one.
     * The hop size's effective time unit is determined by the semantics of the topology's
     * configured {@link org.apache.kafka.streams.processor.TimestampExtractor}.
     */
    public final long hop;

    private TimeWindows(String name, long size, long hop) {
        super(name);
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0 (you provided " + size + ")");
        }
        this.size = size;
        if (!(0 < hop && hop <= size)) {
            throw new IllegalArgumentException(String.format("hop (%d) must lie within interval (0, %d]", hop, size));
        }
        this.hop = hop;
    }

    /**
     * Returns a window definition with the given window size, and with the hop size being equal to
     * the window size.  Think: [N * size, N * size + size), with N denoting the N-th window.
     *
     * This provides the semantics of tumbling windows, which are fixed-sized, gap-less,
     * non-overlapping windows.  Tumbling windows are a specialization of hopping windows.
     *
     * @param name The name of the window.  Must not be null or empty.
     * @param size The size of the window, with the requirement that size &gt; 0.
     *             The window size's effective time unit is determined by the semantics of the
     *             topology's configured {@link org.apache.kafka.streams.processor.TimestampExtractor}.
     * @return a new window definition
     */
    public static TimeWindows of(String name, long size) {
        return new TimeWindows(name, size, size);
    }

    /**
     * Returns a window definition with the original size, but shift ("hop") the window by the given
     * hop size, which specifies by how much a window moves forward relative to the previous one.
     * Think: [N * hop, N * hop + size), with N denoting the N-th window.
     *
     * This provides the semantics of hopping windows, which are fixed-sized, overlapping windows.
     *
     * @param hop The hop size of the window, with the requirement that 0 &lt; hop &le; size.
     *            The hop size's effective time unit is determined by the semantics of the
     *            topology's configured {@link org.apache.kafka.streams.processor.TimestampExtractor}.
     * @return a new window definition
     */
    public TimeWindows shiftedBy(long hop) {
        return new TimeWindows(this.name, this.size, hop);
    }

    // TODO: Document windowsFor()
    @Override
    public Map<Long, TimeWindow> windowsFor(long timestamp) {
        long windowStart = timestamp - timestamp % this.size;
        // We cannot use Collections.singleMap since it does not support remove() call.
        Map<Long, TimeWindow> windows = new HashMap<>();
        windows.put(windowStart, new TimeWindow(windowStart, windowStart + this.size));
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
        return this.size == other.size && this.hop == other.hop;
    }

    @Override
    public int hashCode() {
        int result = (int) (size ^ (size >>> 32));
        result = 31 * result + (int) (hop ^ (hop >>> 32));
        return result;
    }

}