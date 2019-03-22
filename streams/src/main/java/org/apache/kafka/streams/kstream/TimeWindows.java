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
 * @see KGroupedStream#windowedBy(Windows)
 * @see TimestampExtractor
 */
public final class TimeWindows extends Windows<TimeWindow> {

    private final long maintainDurationMs;

    /** The size of the windows in milliseconds. */
    @SuppressWarnings("WeakerAccess")
    public final long sizeMs;

    /**
     * The size of the window's advance interval in milliseconds, i.e., by how much a window moves forward relative to
     * the previous one.
     */
    @SuppressWarnings("WeakerAccess")
    public final long advanceMs;
    private final long graceMs;

    private TimeWindows(final long sizeMs, final long advanceMs, final long graceMs, final long maintainDurationMs) {
        this.sizeMs = sizeMs;
        this.advanceMs = advanceMs;
        this.graceMs = graceMs;
        this.maintainDurationMs = maintainDurationMs;
    }

    /** Private constructor for preserving segments. Can be removed along with Windows.segments. **/
    @Deprecated
    private TimeWindows(final long sizeMs,
                        final long advanceMs,
                        final long graceMs,
                        final long maintainDurationMs,
                        final int segments) {
        super(segments);
        this.sizeMs = sizeMs;
        this.advanceMs = advanceMs;
        this.graceMs = graceMs;
        this.maintainDurationMs = maintainDurationMs;
    }

    /**
     * Return a window definition with the given window size, and with the advance interval being equal to the window
     * size.
     * The time interval represented by the N-th window is: {@code [N * size, N * size + size)}.
     * <p>
     * This provides the semantics of tumbling windows, which are fixed-sized, gap-less, non-overlapping windows.
     * Tumbling windows are a special case of hopping windows with {@code advance == size}.
     *
     * @param sizeMs The size of the window in milliseconds
     * @return a new window definition with default maintain duration of 1 day
     * @throws IllegalArgumentException if the specified window size is zero or negative
     * @deprecated Use {@link #of(Duration)} instead
     */
    @Deprecated
    public static TimeWindows of(final long sizeMs) throws IllegalArgumentException {
        if (sizeMs <= 0) {
            throw new IllegalArgumentException("Window size (sizeMs) must be larger than zero.");
        }
        // This is a static factory method, so we initialize grace and retention to the defaults.
        return new TimeWindows(sizeMs, sizeMs, -1, DEFAULT_RETENTION_MS);
    }

    /**
     * Return a window definition with the given window size, and with the advance interval being equal to the window
     * size.
     * The time interval represented by the N-th window is: {@code [N * size, N * size + size)}.
     * <p>
     * This provides the semantics of tumbling windows, which are fixed-sized, gap-less, non-overlapping windows.
     * Tumbling windows are a special case of hopping windows with {@code advance == size}.
     *
     * @param size The size of the window
     * @return a new window definition with default maintain duration of 1 day
     * @throws IllegalArgumentException if the specified window size is zero or negative or can't be represented as {@code long milliseconds}
     */
    @SuppressWarnings("deprecation") // removing #of(final long sizeMs) will fix this
    public static TimeWindows of(final Duration size) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(size, "size");
        return of(ApiUtils.validateMillisecondDuration(size, msgPrefix));
    }

    /**
     * Return a window definition with the original size, but advance ("hop") the window by the given interval, which
     * specifies by how much a window moves forward relative to the previous one.
     * The time interval represented by the N-th window is: {@code [N * advance, N * advance + size)}.
     * <p>
     * This provides the semantics of hopping windows, which are fixed-sized, overlapping windows.
     *
     * @param advanceMs The advance interval ("hop") in milliseconds of the window, with the requirement that {@code 0 < advanceMs <= sizeMs}.
     * @return a new window definition with default maintain duration of 1 day
     * @throws IllegalArgumentException if the advance interval is negative, zero, or larger than the window size
     * @deprecated Use {@link #advanceBy(Duration)} instead
     */
    @Deprecated
    public TimeWindows advanceBy(final long advanceMs) {
        if (advanceMs <= 0 || advanceMs > sizeMs) {
            throw new IllegalArgumentException(String.format("Window advancement interval should be more than zero " +
                    "and less than window duration which is %d ms, but given advancement interval is: %d ms", sizeMs, advanceMs));
        }
        return new TimeWindows(sizeMs, advanceMs, graceMs, maintainDurationMs, segments);
    }

    /**
     * Return a window definition with the original size, but advance ("hop") the window by the given interval, which
     * specifies by how much a window moves forward relative to the previous one.
     * The time interval represented by the N-th window is: {@code [N * advance, N * advance + size)}.
     * <p>
     * This provides the semantics of hopping windows, which are fixed-sized, overlapping windows.
     *
     * @param advance The advance interval ("hop") of the window, with the requirement that {@code 0 < advance.toMillis() <= sizeMs}.
     * @return a new window definition with default maintain duration of 1 day
     * @throws IllegalArgumentException if the advance interval is negative, zero, or larger than the window size
     */
    @SuppressWarnings("deprecation") // removing #advanceBy(final long advanceMs) will fix this
    public TimeWindows advanceBy(final Duration advance) {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(advance, "advance");
        return advanceBy(ApiUtils.validateMillisecondDuration(advance, msgPrefix));
    }

    @Override
    public Map<Long, TimeWindow> windowsFor(final long timestamp) {
        long windowStart = (Math.max(0, timestamp - sizeMs + advanceMs) / advanceMs) * advanceMs;
        final Map<Long, TimeWindow> windows = new LinkedHashMap<>();
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
     * Reject late events that arrive more than {@code millisAfterWindowEnd}
     * after the end of its window.
     *
     * Lateness is defined as (stream_time - record_timestamp).
     *
     * @param afterWindowEnd The grace period to admit late-arriving events to a window.
     * @return this updated builder
     * @throws IllegalArgumentException if {@code afterWindowEnd} is negative or can't be represented as {@code long milliseconds}
     */
    @SuppressWarnings("deprecation") // will be fixed when we remove segments from Windows
    public TimeWindows grace(final Duration afterWindowEnd) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(afterWindowEnd, "afterWindowEnd");
        final long afterWindowEndMs = ApiUtils.validateMillisecondDuration(afterWindowEnd, msgPrefix);
        if (afterWindowEndMs < 0) {
            throw new IllegalArgumentException("Grace period must not be negative.");
        }

        return new TimeWindows(sizeMs, advanceMs, afterWindowEndMs, maintainDurationMs, segments);
    }

    @SuppressWarnings("deprecation") // continuing to support Windows#maintainMs/segmentInterval in fallback mode
    @Override
    public long gracePeriodMs() {
        // NOTE: in the future, when we remove maintainMs,
        // we should default the grace period to 24h to maintain the default behavior,
        // or we can default to (24h - size) if you want to be super accurate.
        return graceMs != -1 ? graceMs : maintainMs() - size();
    }

    /**
     * @param durationMs the window retention time
     * @return itself
     * @throws IllegalArgumentException if {@code duration} is smaller than the window size
     *
     * @deprecated since 2.1. Use {@link Materialized#retention} or directly configure the retention in a store supplier
     *             and use {@link Materialized#as(WindowBytesStoreSupplier)}.
     */
    @Override
    @Deprecated
    public TimeWindows until(final long durationMs) throws IllegalArgumentException {
        if (durationMs < sizeMs) {
            throw new IllegalArgumentException("Window retention time (durationMs) cannot be smaller than the window size.");
        }
        return new TimeWindows(sizeMs, advanceMs, graceMs, durationMs, segments);
    }

    /**
     * {@inheritDoc}
     * <p>
     * For {@code TimeWindows} the maintain duration is at least as small as the window size.
     *
     * @return the window maintain duration
     * @deprecated since 2.1. Use {@link Materialized#retention} instead.
     */
    @Override
    @Deprecated
    public long maintainMs() {
        return Math.max(maintainDurationMs, sizeMs);
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
        final TimeWindows that = (TimeWindows) o;
        return maintainDurationMs == that.maintainDurationMs &&
            segments == that.segments &&
            sizeMs == that.sizeMs &&
            advanceMs == that.advanceMs &&
            graceMs == that.graceMs;
    }

    @SuppressWarnings("deprecation") // removing segments from Windows will fix this
    @Override
    public int hashCode() {
        return Objects.hash(maintainDurationMs, segments, sizeMs, advanceMs, graceMs);
    }

    @SuppressWarnings("deprecation") // removing segments from Windows will fix this
    @Override
    public String toString() {
        return "TimeWindows{" +
            "maintainDurationMs=" + maintainDurationMs +
            ", sizeMs=" + sizeMs +
            ", advanceMs=" + advanceMs +
            ", graceMs=" + graceMs +
            ", segments=" + segments +
            '}';
    }
}
