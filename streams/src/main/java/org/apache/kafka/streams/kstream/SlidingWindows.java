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
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;
import static org.apache.kafka.streams.kstream.internals.WindowingDefaults.DEFAULT_RETENTION_MS;

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

public final class SlidingWindows extends Windows<TimeWindow> {


    /** The size of the windows in milliseconds. */
    public final long sizeMs;

    /** The grace period in milliseconds. */
    private final long graceMs;

    private SlidingWindows(final long sizeMs, final long graceMs) {
        this.sizeMs = sizeMs;
        this.graceMs = graceMs;

    }

    public static SlidingWindows of(final Duration size) throws IllegalArgumentException{
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(size, "size");
        final long sizeMs = ApiUtils.validateMillisecondDuration(size, msgPrefix);
        if (sizeMs <= 0) {
            throw new IllegalArgumentException("Window size (size) must be larger than zero.");
        }
        return new SlidingWindows(sizeMs, -1);

    }

    @Override
    public Map<Long, TimeWindow> windowsFor(final long timestamp) {
        //put 2 windows that could be created by a new timestamp
        //potentially add 1+previous record
        //aggregate other windows inside of process, use fetchAll(timestamp-sizeMs, timestamp+sizeMs)
        //make sure process doesn't store windows w/nothing in them? or check that here...
        //or, if we do store windows w nothing in them, wouldn't need to recompute later hmm
        //but windows that are empty will likely be beyond our current record time for records coming in order
        //add window with the new timestamp at the end of the window
        //add window with the start 1ms after timestamp
        throw new UnsupportedOperationException("windowsFor() is not supported by SlidingWindows.");

    }

    @Override
    public long size() {
        return sizeMs;
    }


    /**
     * Reject out-of-order events that arrive more than {@code millisAfterWindowEnd}
     * after the end of its window.
     * <p>
     * Delay is defined as (stream_time - record_timestamp).
     *
     * @param afterWindowEnd The grace period to admit out-of-order events to a window.
     * @return this updated builder
     * @throws IllegalArgumentException if {@code afterWindowEnd} is negative or can't be represented as {@code long milliseconds}
     */
    @SuppressWarnings("deprecation") // will be fixed when we remove segments from Windows
    public SlidingWindows grace(final Duration afterWindowEnd) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(afterWindowEnd, "afterWindowEnd");
        final long afterWindowEndMs = ApiUtils.validateMillisecondDuration(afterWindowEnd, msgPrefix);
        if (afterWindowEndMs < 0) {
            throw new IllegalArgumentException("Grace period must not be negative.");
        }

        return new SlidingWindows(sizeMs, afterWindowEndMs);
    }

    @SuppressWarnings("deprecation") // continuing to support Windows#maintainMs/segmentInterval in fallback mode
    @Override
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
