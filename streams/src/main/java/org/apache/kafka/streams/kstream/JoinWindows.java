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

import org.apache.kafka.streams.kstream.internals.TimeWindow;

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
public class JoinWindows extends Windows<TimeWindow> {

    /** Maximum time difference for tuples that are before the join tuple. */
    public final long before;
    /** Maximum time difference for tuples that are after the join tuple. */
    public final long after;

    private JoinWindows(long before, long after) {
        super();

        if (before + after < 0) {
            throw new IllegalArgumentException("Window interval (ie, before+after) must not be negative");
        }
        this.after = after;
        this.before = before;
    }

    /**
     * Specifies that records of the same key are joinable if their timestamps are within {@code timeDifference}.
     * ({@code timeDifference} must not be negative)
     *
     * @param timeDifference    join window interval
     */
    public static JoinWindows of(long timeDifference) {
        return new JoinWindows(timeDifference, timeDifference);
    }

    /**
     * Specifies that records of the same key are joinable if their timestamps are within
     * the join window interval, and if the timestamp of a record from the secondary stream is
     * earlier than or equal to the timestamp of a record from the first stream.
     *
     * @param timeDifference    join window interval
     */
    public JoinWindows before(long timeDifference) {
        return new JoinWindows(timeDifference, this.after);
    }

    /**
     * Specifies that records of the same key are joinable if their timestamps are within
     * the join window interval, and if the timestamp of a record from the secondary stream
     * is later than or equal to the timestamp of a record from the first stream.
     *
     * @param timeDifference    join window interval
     */
    public JoinWindows after(long timeDifference) {
        return new JoinWindows(this.before, timeDifference);
    }

    /**
     * Not supported by {@link JoinWindows}. Throws {@link UnsupportedOperationException}.
     */
    @Override
    public Map<Long, TimeWindow> windowsFor(long timestamp) {
        throw new UnsupportedOperationException("windowsFor() is not supported in JoinWindows");
    }

    @Override
    public long size() {
        return after + before;
    }

    @Override
    public final boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof JoinWindows)) {
            return false;
        }

        JoinWindows other = (JoinWindows) o;
        return this.before == other.before && this.after == other.after;
    }

    @Override
    public int hashCode() {
        int result = (int) (before ^ (before >>> 32));
        result = 31 * result + (int) (after ^ (after >>> 32));
        return result;
    }

}
