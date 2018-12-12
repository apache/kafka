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
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;

import java.time.Duration;
import java.util.Objects;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;
import static org.apache.kafka.streams.kstream.internals.WindowingDefaults.DEFAULT_RETENTION_MS;


/**
 * A session based window specification used for aggregating events into sessions.
 * <p>
 * Sessions represent a period of activity separated by a defined gap of inactivity.
 * Any events processed that fall within the inactivity gap of any existing sessions are merged into the existing sessions.
 * If the event falls outside of the session gap then a new session will be created.
 * <p>
 * For example, if we have a session gap of 5 and the following data arrives:
 * <pre>
 * +--------------------------------------+
 * |    key    |    value    |    time    |
 * +-----------+-------------+------------+
 * |    A      |     1       |     10     |
 * +-----------+-------------+------------+
 * |    A      |     2       |     12     |
 * +-----------+-------------+------------+
 * |    A      |     3       |     20     |
 * +-----------+-------------+------------+
 * </pre>
 * We'd have 2 sessions for key A.
 * One starting from time 10 and ending at time 12 and another starting and ending at time 20.
 * The length of the session is driven by the timestamps of the data within the session.
 * Thus, session windows are no fixed-size windows (c.f. {@link TimeWindows} and {@link JoinWindows}).
 * <p>
 * If we then received another record:
 * <pre>
 * +--------------------------------------+
 * |    key    |    value    |    time    |
 * +-----------+-------------+------------+
 * |    A      |     4       |     16     |
 * +-----------+-------------+------------+
 * </pre>
 * The previous 2 sessions would be merged into a single session with start time 10 and end time 20.
 * The aggregate value for this session would be the result of aggregating all 4 values.
 * <p>
 * For time semantics, see {@link TimestampExtractor}.
 *
 * @see TimeWindows
 * @see UnlimitedWindows
 * @see JoinWindows
 * @see KGroupedStream#windowedBy(SessionWindows)
 * @see TimestampExtractor
 */
public final class SessionWindows {

    private final long gapMs;
    private final long maintainDurationMs;
    private final long graceMs;


    private SessionWindows(final long gapMs, final long maintainDurationMs, final long graceMs) {
        this.gapMs = gapMs;
        this.maintainDurationMs = maintainDurationMs;
        this.graceMs = graceMs;
    }

    /**
     * Create a new window specification with the specified inactivity gap in milliseconds.
     *
     * @param inactivityGapMs the gap of inactivity between sessions in milliseconds
     * @return a new window specification with default maintain duration of 1 day
     *
     * @throws IllegalArgumentException if {@code inactivityGapMs} is zero or negative
     * @deprecated Use {@link #with(Duration)} instead.
     */
    @Deprecated
    public static SessionWindows with(final long inactivityGapMs) {
        if (inactivityGapMs <= 0) {
            throw new IllegalArgumentException("Gap time (inactivityGapMs) cannot be zero or negative.");
        }
        return new SessionWindows(inactivityGapMs, DEFAULT_RETENTION_MS, -1);
    }

    /**
     * Create a new window specification with the specified inactivity gap.
     *
     * @param inactivityGap the gap of inactivity between sessions
     * @return a new window specification with default maintain duration of 1 day
     *
     * @throws IllegalArgumentException if {@code inactivityGap} is zero or negative or can't be represented as {@code long milliseconds}
     */
    @SuppressWarnings("deprecation")
    public static SessionWindows with(final Duration inactivityGap) {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(inactivityGap, "inactivityGap");
        return with(ApiUtils.validateMillisecondDuration(inactivityGap, msgPrefix));
    }

    /**
     * Set the window maintain duration (retention time) in milliseconds.
     * This retention time is a guaranteed <i>lower bound</i> for how long a window will be maintained.
     *
     * @return itself
     * @throws IllegalArgumentException if {@code durationMs} is smaller than window gap
     *
     * @deprecated since 2.1. Use {@link Materialized#retention}
     *             or directly configure the retention in a store supplier and use
     *             {@link Materialized#as(SessionBytesStoreSupplier)}.
     */
    @Deprecated
    public SessionWindows until(final long durationMs) throws IllegalArgumentException {
        if (durationMs < gapMs) {
            throw new IllegalArgumentException("Window retention time (durationMs) cannot be smaller than window gap.");
        }

        return new SessionWindows(gapMs, durationMs, graceMs);
    }

    /**
     * Reject late events that arrive more than {@code afterWindowEnd}
     * after the end of its window.
     *
     * Note that new events may change the boundaries of session windows, so aggressive
     * close times can lead to surprising results in which a too-late event is rejected and then
     * a subsequent event moves the window boundary forward.
     *
     * @param afterWindowEnd The grace period to admit late-arriving events to a window.
     * @return this updated builder
     * @throws IllegalArgumentException if the {@code afterWindowEnd} is negative of can't be represented as {@code long milliseconds}
     */
    public SessionWindows grace(final Duration afterWindowEnd) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(afterWindowEnd, "afterWindowEnd");
        final long afterWindowEndMs = ApiUtils.validateMillisecondDuration(afterWindowEnd, msgPrefix);

        if (afterWindowEndMs < 0) {
            throw new IllegalArgumentException("Grace period must not be negative.");
        }

        return new SessionWindows(
            gapMs,
            maintainDurationMs,
            afterWindowEndMs
        );
    }

    @SuppressWarnings("deprecation") // continuing to support Windows#maintainMs/segmentInterval in fallback mode
    public long gracePeriodMs() {

        // NOTE: in the future, when we remove maintainMs,
        // we should default the grace period to 24h to maintain the default behavior,
        // or we can default to (24h - gapMs) if you want to be super accurate.
        return graceMs != -1 ? graceMs : maintainMs() - inactivityGap();
    }

    /**
     * Return the specified gap for the session windows in milliseconds.
     *
     * @return the inactivity gap of the specified windows
     */
    public long inactivityGap() {
        return gapMs;
    }

    /**
     * Return the window maintain duration (retention time) in milliseconds.
     * <p>
     * For {@code SessionWindows} the maintain duration is at least as small as the window gap.
     *
     * @return the window maintain duration
     * @deprecated since 2.1. Use {@link Materialized#retention} instead.
     */
    @Deprecated
    public long maintainMs() {
        return Math.max(maintainDurationMs, gapMs);
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SessionWindows that = (SessionWindows) o;
        return gapMs == that.gapMs &&
            maintainDurationMs == that.maintainDurationMs &&
            graceMs == that.graceMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(gapMs, maintainDurationMs, graceMs);
    }

    @Override
    public String toString() {
        return "SessionWindows{" +
            "gapMs=" + gapMs +
            ", maintainDurationMs=" + maintainDurationMs +
            ", graceMs=" + graceMs +
            '}';
    }
}
