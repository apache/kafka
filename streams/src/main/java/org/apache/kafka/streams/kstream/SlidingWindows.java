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
import org.apache.kafka.streams.processor.TimestampExtractor;
import java.time.Duration;
import java.util.Objects;
import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

/**
 /**
 * A sliding window used for aggregating events.
 * <p>
 * Sliding Windows are defined based on a record's timestamp, window size based on the given maximum time difference (inclusive) between
 * records in the same window and given window grace period.
 *
 * While the window is sliding over the input data stream, a new window is created each time a record enters
 * the sliding window or a record drops out of the sliding window.
 *
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
 * - window {@code [3000;8000]} contains [1] (created when first record enters the window)
 * - window {@code [4200;9200]} contains [1,2] (created when second record enters the window)
 * - window {@code [7400;124000]} contains [1,2,3] (created when third record enters the window)
 * - window {@code [8001;130001]} contains [2,3] (created when the first record drops out of the window)
 * - window {@code [9201;142001]} contains [3] (created when the second record drops out of the window)
 *
 * Note that while SlidingWindows are of a fixed size {@link TimeWindows}, the start and end points
 * depend on when events are processed, similar to {@link SessionWindows}.
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

    /** The size of the windows in milliseconds, defined by the max time difference between records. */
    private final long timeDifferenceMs;

    /** The grace period in milliseconds. */
    private final long graceMs;

    private SlidingWindows(final long timeDifferenceMs, final long graceMs) {
        this.timeDifferenceMs = timeDifferenceMs;
        this.graceMs = graceMs;
    }

    /**
     * Return a window definition with the window size based on the given maximum time difference (inclusive) between
     * records in the same window and given window grace period.
     *
     * Reject out-of-order events that arrive after {@code grace}. A window is closed when {@code stream-time > window-end + grace-period}.
     *
     * @param timeDifference the max time difference (inclusive) between two records in a window
     * @param grace the grace period to admit out-of-order events to a window
     * @return a new window definition
     * @throws IllegalArgumentException if the specified window size or grace is zero or negative or can't be represented as {@code long milliseconds}
     */
    public static SlidingWindows withTimeDifferenceAndGrace(final Duration timeDifference, final Duration grace) throws IllegalArgumentException {
        final String msgPrefixSize = prepareMillisCheckFailMsgPrefix(timeDifference, "timeDifference");
        final long timeDifferenceMs = ApiUtils.validateMillisecondDuration(timeDifference, msgPrefixSize);
        if (timeDifferenceMs <= 0) {
            throw new IllegalArgumentException("Window timeDifference (timeDifference) must be larger than zero.");
        }
        final String msgPrefixGrace = prepareMillisCheckFailMsgPrefix(grace, "afterWindowEnd");
        final long graceMs = ApiUtils.validateMillisecondDuration(grace, msgPrefixGrace);
        if (graceMs < 0) {
            throw new IllegalArgumentException("Grace period must not be negative.");
        }
        return new SlidingWindows(timeDifferenceMs, graceMs);
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
