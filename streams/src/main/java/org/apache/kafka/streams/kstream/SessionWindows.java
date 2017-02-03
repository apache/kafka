/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.streams.processor.TimestampExtractor;

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
 * @see KGroupedStream#count(SessionWindows, String)
 * @see KGroupedStream#count(SessionWindows, org.apache.kafka.streams.processor.StateStoreSupplier)
 * @see KGroupedStream#reduce(Reducer, SessionWindows, String)
 * @see KGroupedStream#reduce(Reducer, SessionWindows, org.apache.kafka.streams.processor.StateStoreSupplier)
 * @see KGroupedStream#aggregate(Initializer, Aggregator, Merger, SessionWindows, org.apache.kafka.common.serialization.Serde, String)
 * @see KGroupedStream#aggregate(Initializer, Aggregator, Merger, SessionWindows, org.apache.kafka.common.serialization.Serde, org.apache.kafka.streams.processor.StateStoreSupplier)
 * @see TimestampExtractor
 */
@InterfaceStability.Unstable
public final class SessionWindows {

    private final long gapMs;
    private long maintainDurationMs;

    private SessionWindows(final long gapMs) {
        this.gapMs = gapMs;
        maintainDurationMs = Windows.DEFAULT_MAINTAIN_DURATION_MS;
    }

    /**
     * Create a new window specification with the specified inactivity gap in milliseconds.
     *
     * @param inactivityGapMs the gap of inactivity between sessions in milliseconds
     * @return a new window specification with default maintain duration of 1 day
     *
     * @throws IllegalArgumentException if {@code inactivityGapMs} is zero or negative
     */
    public static SessionWindows with(final long inactivityGapMs) {
        if (inactivityGapMs <= 0) {
            throw new IllegalArgumentException("Gap time (inactivityGapMs) cannot be zero or negative.");
        }
        return new SessionWindows(inactivityGapMs);
    }

    /**
     * Set the window maintain duration (retention time) in milliseconds.
     * This retention time is a guaranteed <i>lower bound</i> for how long a window will be maintained.
     *
     * @return itself
     * @throws IllegalArgumentException if {@code durationMs} is smaller than window gap
     */
    public SessionWindows until(final long durationMs) throws IllegalArgumentException {
        if (durationMs < gapMs) {
            throw new IllegalArgumentException("Window retention time (durationMs) cannot be smaller than window gap.");
        }
        maintainDurationMs = durationMs;

        return this;
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
     */
    public long maintainMs() {
        return Math.max(maintainDurationMs, gapMs);
    }
}
