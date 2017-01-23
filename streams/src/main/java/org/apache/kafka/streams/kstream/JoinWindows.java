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

import java.util.Map;

/**
 * The window specifications used for joins.
 * <p>
 * A {@link JoinWindows} instance defines a join over two stream on the same key and a maximum time difference.
 * In SQL-style you would express this join as
 * <pre>
 *     SELECT * FROM stream1, stream2
 *     WHERE
 *       stream1.key = stream2.key
 *       AND
 *       stream1.ts - before <= stream2.ts AND stream2.ts <= stream1.ts + after
 * </pre>
 * There are three different window configuration supported:
 * <ul>
 *     <li>before = after = time-difference</li>
 *     <li>before = 0 and after = time-difference</li>
 *     <li>before = time-difference and after = 0</li>
 * </ul>
 * A join is symmetric in the sense, that a join specification on the first stream returns the same result record as
 * a join specification on the second stream with flipped before and after values.
 * <p>
 * Both values (before and after) must not result in an "inverse" window,
 * i.e., lower-interval-bound must not be larger than upper-interval.bound.
 */
public class JoinWindows extends Windows<Window> {

    /** Maximum time difference for tuples that are before the join tuple. */
    public final long beforeMs;
    /** Maximum time difference for tuples that are after the join tuple. */
    public final long afterMs;

    private JoinWindows(final long beforeMs, final long afterMs) {
        if (beforeMs + afterMs < 0) {
            throw new IllegalArgumentException("Window interval (ie, beforeMs+afterMs) must not be negative");
        }
        this.afterMs = afterMs;
        this.beforeMs = beforeMs;
    }

    /**
     * Specifies that records of the same key are joinable if their timestamps are within {@code timeDifference}.
     * ({@code timeDifference} must not be negative)
     *
     * @param timeDifference    join window interval
     */
    public static JoinWindows of(final long timeDifferenceMs) throws IllegalArgumentException {
        return new JoinWindows(timeDifferenceMs, timeDifferenceMs);
    }

    /**
     * Specifies that records of the same key are joinable if their timestamps are within
     * the join window interval, and if the timestamp of a record from the secondary stream is
     * earlier than or equal to the timestamp of a record from the first stream.
     *
     * @param timeDifference    join window interval
     */
    public JoinWindows before(final long timeDifferenceMs) throws IllegalArgumentException {
        return new JoinWindows(timeDifferenceMs, afterMs);
    }

    /**
     * Specifies that records of the same key are joinable if their timestamps are within
     * the join window interval, and if the timestamp of a record from the secondary stream
     * is later than or equal to the timestamp of a record from the first stream.
     *
     * @param timeDifference    join window interval
     */
    public JoinWindows after(final long timeDifferenceMs) throws IllegalArgumentException {
        return new JoinWindows(beforeMs, timeDifferenceMs);
    }

    /**
     * Not supported by {@link JoinWindows}. Throws {@link UnsupportedOperationException}.
     */
    @Override
    public Map<Long, Window> windowsFor(final long timestamp) {
        throw new UnsupportedOperationException("windowsFor() is not supported in JoinWindows");
    }

    @Override
    public long size() {
        return beforeMs + afterMs;
    }

    @Override
    public JoinWindows until(final long durationMs) throws IllegalArgumentException {
        if (durationMs < size()) {
            throw new IllegalArgumentException("Window retention time (durationMs) cannot be smaller than the window size.");
        }
        super.until(durationMs);
        return this;
    }

    @Override
    public long maintainMs() {
        return Math.max(super.maintainMs(), size());
    }

    @Override
    public final boolean equals(final Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof JoinWindows)) {
            return false;
        }

        final JoinWindows other = (JoinWindows) o;
        return beforeMs == other.beforeMs && afterMs == other.afterMs;
    }

    @Override
    public int hashCode() {
        int result = (int) (beforeMs ^ (beforeMs >>> 32));
        result = 31 * result + (int) (afterMs ^ (afterMs >>> 32));
        return result;
    }

}
