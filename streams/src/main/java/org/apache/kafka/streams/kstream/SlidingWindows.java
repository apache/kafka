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

import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

/**
 * The fixed-size sliding window specifications used for aggregations.
 * <p>
 * The semantics of sliding aggregation windows are: Compute the aggregate total for the unique current window, from
 * the current stream time to T1 (size) milliseconds before. Both the lower and upper time interval bounds are inclusive.
 *
 * Thus, the specified {@link TimeWindow} is not aligned to the epoch, but to record timestamps.
 * Aligned to the epoch means, that the first window starts at timestamp zero.
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

public final class SlidingWindows {


    /** The size of the windows in milliseconds. */
    public final long sizeMs;

    /** The grace period in milliseconds. */
    private final long graceMs;

    private SlidingWindows(final long sizeMs, final long graceMs) {
        this.sizeMs = sizeMs;
        this.graceMs = graceMs;

    }

    /**
     * Return a window definition with the given window size and given window grace period
     * Records that come after set grace period will be ignored
     *
     * @param size, The size of the window
     * @param grace, The grace period to admit out-of-order events to a window
     * @return a new window definition
     * @throws IllegalArgumentException if the specified window size or grace is zero or negative or can't be represented as {@code long milliseconds}
     */
    public static SlidingWindows withSizeAndGrace(final Duration size, final Duration grace) throws IllegalArgumentException {
        final String msgPrefixSize = prepareMillisCheckFailMsgPrefix(size, "size");
        final long sizeMs = ApiUtils.validateMillisecondDuration(size, msgPrefixSize);
        if (sizeMs <= 0) {
            throw new IllegalArgumentException("Window size (size) must be larger than zero.");
        }
        final String msgPrefixGrace = prepareMillisCheckFailMsgPrefix(grace, "afterWindowEnd");
        final long graceMs = ApiUtils.validateMillisecondDuration(grace, msgPrefixGrace);
        if (graceMs < 0) {
            throw new IllegalArgumentException("Grace period must not be negative.");
        }

        return new SlidingWindows(sizeMs, graceMs);
    }


    public long size() {
        return sizeMs;
    }





    @SuppressWarnings("deprecation") // continuing to support Windows#maintainMs/segmentInterval in fallback mode
    public long gracePeriodMs() {
        return graceMs;
    }


    @SuppressWarnings("deprecation") // removing segments from Windows will fix this
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SlidingWindows that = (SlidingWindows) o;
        return  sizeMs == that.sizeMs &&
                graceMs == that.graceMs;
    }

    @SuppressWarnings("deprecation") // removing segments from Windows will fix this
    @Override
    public int hashCode() {
        return Objects.hash(sizeMs, graceMs);
    }

    @SuppressWarnings("deprecation") // removing segments from Windows will fix this
    @Override
    public String toString() {
        return "SlidingWindows{" +
                ", sizeMs=" + sizeMs +
                ", graceMs=" + graceMs +
                '}';
    }
}
