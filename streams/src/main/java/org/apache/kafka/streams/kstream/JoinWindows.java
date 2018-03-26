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

import java.util.Map;

/**
 * The window specifications used for joins.
 * <p>
 * A {@code JoinWindows} instance defines a maximum time difference for a {@link KStream#join(KStream, ValueJoiner,
 * JoinWindows) join over two streams} on the same key.
 * In SQL-style you would express this join as
 * <pre>{@code
 *     SELECT * FROM stream1, stream2
 *     WHERE
 *       stream1.key = stream2.key
 *       AND
 *       stream1.ts - before <= stream2.ts AND stream2.ts <= stream1.ts + after
 * }</pre>
 * There are three different window configuration supported:
 * <ul>
 *     <li>before = after = time-difference</li>
 *     <li>before = 0 and after = time-difference</li>
 *     <li>before = time-difference and after = 0</li>
 * </ul>
 * A join is symmetric in the sense, that a join specification on the first stream returns the same result record as
 * a join specification on the second stream with flipped before and after values.
 * <p>
 * Both values (before and after) must not result in an "inverse" window, i.e., upper-interval bound cannot be smaller
 * than lower-interval bound.
 * <p>
 * {@code JoinWindows} are sliding windows, thus, they are aligned to the actual record timestamps.
 * This implies, that each input record defines its own window with start and end time being relative to the record's
 * timestamp.
 * <p>
 * For time semantics, see {@link TimestampExtractor}.
 *
 * @see TimeWindows
 * @see UnlimitedWindows
 * @see SessionWindows
 * @see KStream#join(KStream, ValueJoiner, JoinWindows)
 * @see KStream#join(KStream, ValueJoiner, JoinWindows, Joined)
 * @see KStream#leftJoin(KStream, ValueJoiner, JoinWindows)
 * @see KStream#leftJoin(KStream, ValueJoiner, JoinWindows, Joined)
 * @see KStream#outerJoin(KStream, ValueJoiner, JoinWindows)
 * @see KStream#outerJoin(KStream, ValueJoiner, JoinWindows, Joined)
 * @see TimestampExtractor
 */
public final class JoinWindows extends Windows<Window> {

    /** Maximum time difference for tuples that are before the join tuple. */
    public final long beforeMs;
    /** Maximum time difference for tuples that are after the join tuple. */
    public final long afterMs;

    private JoinWindows(final long beforeMs, final long afterMs) {
        if (beforeMs + afterMs < 0) {
            throw new IllegalArgumentException("Window interval (ie, beforeMs+afterMs) must not be negative.");
        }
        this.afterMs = afterMs;
        this.beforeMs = beforeMs;
    }

    /**
     * Specifies that records of the same key are joinable if their timestamps are within {@code timeDifferenceMs},
     * i.e., the timestamp of a record from the secondary stream is max {@code timeDifferenceMs} earlier or later than
     * the timestamp of the record from the primary stream.
     *
     * @param timeDifferenceMs join window interval in milliseconds
     * @throws IllegalArgumentException if {@code timeDifferenceMs} is negative
     */
    public static JoinWindows of(final long timeDifferenceMs) throws IllegalArgumentException {
        return new JoinWindows(timeDifferenceMs, timeDifferenceMs);
    }

    /**
     * Changes the start window boundary to {@code timeDifferenceMs} but keep the end window boundary as is.
     * Thus, records of the same key are joinable if the timestamp of a record from the secondary stream is at most
     * {@code timeDifferenceMs} earlier than the timestamp of the record from the primary stream.
     * {@code timeDifferenceMs} can be negative but it's absolute value must not be larger than current window "after"
     * value (which would result in a negative window size).
     *
     * @param timeDifferenceMs relative window start time in milliseconds
     * @throws IllegalArgumentException if the resulting window size is negative
     */
    public JoinWindows before(final long timeDifferenceMs) throws IllegalArgumentException {
        return new JoinWindows(timeDifferenceMs, afterMs);
    }

    /**
     * Changes the end window boundary to {@code timeDifferenceMs} but keep the start window boundary as is.
     * Thus, records of the same key are joinable if the timestamp of a record from the secondary stream is at most
     * {@code timeDifferenceMs} later than the timestamp of the record from the primary stream.
     * {@code timeDifferenceMs} can be negative but it's absolute value must not be larger than current window "before"
     * value (which would result in a negative window size).
     *
     * @param timeDifferenceMs relative window end time in milliseconds
     * @throws IllegalArgumentException if the resulting window size is negative
     */
    public JoinWindows after(final long timeDifferenceMs) throws IllegalArgumentException {
        return new JoinWindows(beforeMs, timeDifferenceMs);
    }

    /**
     * Not supported by {@code JoinWindows}.
     * Throws {@link UnsupportedOperationException}.
     *
     * @throws UnsupportedOperationException at every invocation
     */
    @Override
    public Map<Long, Window> windowsFor(final long timestamp) {
        throw new UnsupportedOperationException("windowsFor() is not supported by JoinWindows.");
    }

    @Override
    public long size() {
        return beforeMs + afterMs;
    }

    /**
     * @param durationMs the window retention time in milliseconds
     * @return itself
     * @throws IllegalArgumentException if {@code durationMs} is smaller than the window size
     */
    @Override
    public JoinWindows until(final long durationMs) throws IllegalArgumentException {
        if (durationMs < size()) {
            throw new IllegalArgumentException("Window retention time (durationMs) cannot be smaller than the window size.");
        }
        super.until(durationMs);
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * For {@link TimeWindows} the maintain duration is at least as small as the window size.
     *
     * @return the window maintain duration
     */
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
