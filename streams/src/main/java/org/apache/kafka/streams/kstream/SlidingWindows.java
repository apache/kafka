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

import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Duration;
import java.util.Objects;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;
import static org.apache.kafka.streams.internals.ApiUtils.validateMillisecondDuration;
import static org.apache.kafka.streams.kstream.Windows.NO_GRACE_PERIOD;

/**
 * A sliding window used for aggregating events.
 * <p>
 * Sliding Windows are defined based on a record's timestamp, the window size based on the given maximum time difference (inclusive) between
 * records in the same window, and the given window grace period. While the window is sliding over the input data stream, a new window is
 * created each time a record enters the sliding window or a record drops out of the sliding window.
 * <p>
 * Records that come after set grace period will be ignored, i.e., a window is closed when
 * {@code stream-time > window-end + grace-period}.
 * <p>
 * For example, if we have a time difference of 5000ms and the following data arrives:
 * <pre>
 * +--------------------------------------+
 * |    key    |    value    |    time    |
 * +-----------+-------------+------------+
 * |    A      |     1       |    8000    |
 * +-----------+-------------+------------+
 * |    A      |     2       |    9200    |
 * +-----------+-------------+------------+
 * |    A      |     3       |    12400   |
 * +-----------+-------------+------------+
 * </pre>
 * We'd have the following 5 windows:
 * <ul>
 *     <li>window {@code [3000;8000]} contains [1] (created when first record enters the window)</li>
 *     <li>window {@code [4200;9200]} contains [1,2] (created when second record enters the window)</li>
 *     <li>window {@code [7400;12400]} contains [1,2,3] (created when third record enters the window)</li>
 *     <li>window {@code [8001;13001]} contains [2,3] (created when the first record drops out of the window)</li>
 *     <li>window {@code [9201;14201]} contains [3] (created when the second record drops out of the window)</li>
 * </ul>
 *<p>
 * Note that while SlidingWindows are of a fixed size, as are {@link TimeWindows}, the start and end points of the window
 * depend on when events occur in the stream (i.e., event timestamps), similar to {@link SessionWindows}.
 * <p>
 * For time semantics, see {@link TimestampExtractor}.
 *
 * @see TimeWindows
 * @see SessionWindows
 * @see UnlimitedWindows
 * @see JoinWindows
 * @see KGroupedStream#windowedBy(SlidingWindows)
 * @see CogroupedKStream#windowedBy(SlidingWindows)
 * @see TimestampExtractor
 */

public final class SlidingWindows {

    /** The size of the windows in milliseconds, defined by the max time difference between records. */
    private final long timeDifferenceMs;

    /** The grace period in milliseconds. */
    private final long graceMs;

    private SlidingWindows(final long timeDifferenceMs, final long graceMs) {
        this.timeDifferenceMs = timeDifferenceMs;
        this.graceMs = graceMs;

        if (timeDifferenceMs < 0) {
            throw new IllegalArgumentException("Window time difference must not be negative.");
        }

        if (graceMs < 0) {
            throw new IllegalArgumentException("Window grace period must not be negative.");
        }
    }

    /**
     * Return a window definition with the window size based on the given maximum time difference (inclusive) between
     * records in the same window and given window grace period. Reject out-of-order events that arrive after {@code grace}.
     * A window is closed when {@code stream-time > window-end + grace-period}.
     * <p>
     * CAUTION: Using this method implicitly sets the grace period to zero, which means that any out-of-order
     * records arriving after the window ends are considered late and will be dropped.
     *
     * @param timeDifference the max time difference (inclusive) between two records in a window
     * @return a new window definition with no grace period. Note that this means out-of-order records arriving after the window end will be dropped
     * @throws IllegalArgumentException if the timeDifference is negative or can't be represented as {@code long milliseconds}
     */
    public static SlidingWindows ofTimeDifferenceWithNoGrace(final Duration timeDifference) throws IllegalArgumentException {
        return ofTimeDifferenceAndGrace(timeDifference, ofMillis(NO_GRACE_PERIOD));
    }

    /**
     * Return a window definition with the window size based on the given maximum time difference (inclusive) between
     * records in the same window and given window grace period. Reject out-of-order events that arrive after {@code afterWindowEnd}.
     * A window is closed when {@code stream-time > window-end + grace-period}.
     *
     * @param timeDifference the max time difference (inclusive) between two records in a window
     * @param afterWindowEnd  the grace period to admit out-of-order events to a window
     * @return a new window definition with the specified grace period
     * @throws IllegalArgumentException if the timeDifference or afterWindowEnd (grace period) is negative or can't be represented as {@code long milliseconds}
     */
    public static SlidingWindows ofTimeDifferenceAndGrace(final Duration timeDifference, final Duration afterWindowEnd) throws IllegalArgumentException {
        final String timeDifferenceMsgPrefix = prepareMillisCheckFailMsgPrefix(timeDifference, "timeDifference");
        final long timeDifferenceMs = validateMillisecondDuration(timeDifference, timeDifferenceMsgPrefix);
        final String afterWindowEndMsgPrefix = prepareMillisCheckFailMsgPrefix(afterWindowEnd, "afterWindowEnd");
        final long afterWindowEndMs = validateMillisecondDuration(afterWindowEnd, afterWindowEndMsgPrefix);

        return new SlidingWindows(timeDifferenceMs, afterWindowEndMs);
    }

    public long timeDifferenceMs() {
        return timeDifferenceMs;
    }

    public long gracePeriodMs() {
        return graceMs;
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
        return timeDifferenceMs == that.timeDifferenceMs &&
            graceMs == that.graceMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeDifferenceMs, graceMs);
    }

    @Override
    public String toString() {
        return "SlidingWindows{" +
            ", sizeMs=" + timeDifferenceMs +
            ", graceMs=" + graceMs +
            '}';
    }
}
