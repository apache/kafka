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

import java.util.HashMap;
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

/**
 * The fixed-size time-based window specifications used for aggregations.
 * <p>
 * The semantics of sliding time-based aggregation windows are: Compute the aggregate total only for the current
 * window, which ends at the current stream time and begins {window size} milliseconds before. The lower and upper
 * time interval bounds of sliding windows are both inclusive.

 * Thus, the specified {@link TimeWindow} is not aligned to the epoch, only to the record timestamps.
 * <p>
 * For time semantics, see {@link TimestampExtractor}.
 *
 * @see TimeWindows
 * @see SessionWindows
 * @see UnlimitedWindows
 * @see JoinWindows
 * @see KGroupedStream#windowedBy(Windows)
 * @see TimestampExtractor
 */
public final class SlidingWindows extends Windows<TimeWindow> {

    /** The size of the window in milliseconds. */
    private final long sizeMs;

    private SlidingWindows(final long sizeMs) {
        this.sizeMs = sizeMs;
    }

    /**
     * Return a window definition with the given window size.
     * <p>
     * This provides the semantics of sliding windows, which is a single fixed-size window that advances with stream time
     *
     * @param size The size of the window
     * @return a new window definition
     * @throws IllegalArgumentException if the specified window size is zero or negative or can't be represented as {@code long milliseconds}
     */
    public static SlidingWindows of(final Duration size) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(size, "size");
        final long sizeMs = ApiUtils.validateMillisecondDuration(size, msgPrefix);
        if (sizeMs <= 0) {
            throw new IllegalArgumentException("Window size (sizeMs) must be larger than zero.");
        }

        return new SlidingWindows(sizeMs);
    }

    @Override
    public Map<Long, TimeWindow> windowsFor(final long timestamp) {
         final Map<Long, TimeWindow> windows = new HashMap<>();
         windows.put(timestamp, new TimeWindow(timestamp - sizeMs, timestamp));
         return windows;
    }

    @Override
    public long gracePeriodMs() {
        return 0;
    }

    @Override
    public long size() {
        return sizeMs;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SlidingWindows that = (SlidingWindows) o;
        return sizeMs == that.sizeMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sizeMs);
    }

    @Override
    public String toString() {
        return "SlidingWindows{" +
            ", sizeMs=" + sizeMs +
            '}';
    }
}
