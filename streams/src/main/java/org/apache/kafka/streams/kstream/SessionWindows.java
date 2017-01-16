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

/**
 * A session based window specification used for aggregating events into sessions.
 * <p>
 * Sessions represent a period of activity separated by a defined gap of inactivity.
 * Any events processed that fall within the inactivity gap of any existing sessions
 * are merged into the existing sessions. If the event falls outside of the session gap
 * then a new session will be created.
 * <p>
 * For example, If we have a session gap of 5 and the following data arrives:
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
 * <p>
 * We'd have 2 sessions for key A. 1 starting from time 10 and ending at time 12 and another
 * starting and ending at time 20. The length of the session is driven by the timestamps of
 * the data within the session
 * <p>
 * If we then received another record:
 * <p>
 * <pre>
 * +--------------------------------------+
 * |    key    |    value    |    time    |
 * +-----------+-------------+------------+
 * |    A      |     4       |     16     |
 * +-----------+-------------+------------+
 * </pre>
 * <p>
 * The previous 2 sessions would be merged into a single session with start time 10 and end time 20.
 * The aggregate value for this session would be the result of aggregating all 4 values.
 */
@InterfaceStability.Unstable
public class SessionWindows {

    private final long gapMs;
    private long maintainDurationMs;

    private SessionWindows(final long gapMs, final long maintainDurationMs) {
        this.gapMs = gapMs;
        this.maintainDurationMs = maintainDurationMs;
    }

    /**
     * Create a new SessionWindows with the specified inactivity gap
     * @param inactivityGapMs  the gap of inactivity between sessions
     * @return a new SessionWindows with the provided inactivity gap
     * and default maintain duration
     */
    public static SessionWindows with(final long inactivityGapMs) {
        return new SessionWindows(inactivityGapMs, Windows.DEFAULT_MAINTAIN_DURATION);
    }

    /**
     * Set the window maintain duration in milliseconds of streams time.
     * This retention time is a guaranteed <i>lower bound</i> for how long a window will be maintained.
     *
     * @return  itself
     */
    public SessionWindows until(final long durationMs) {
        this.maintainDurationMs = durationMs;
        return this;
    }

    /**
     * @return the inactivityGap
     */
    public long inactivityGap() {
        return gapMs;
    }

    /**
     * @return the minimum amount of time a window will be maintained for.
     */
    public long maintainMs() {
        return maintainDurationMs;
    }
}
