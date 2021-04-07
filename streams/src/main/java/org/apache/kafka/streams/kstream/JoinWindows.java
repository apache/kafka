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
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;
import static org.apache.kafka.streams.internals.ApiUtils.validateMillisecondDuration;

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
 * @see KStream#join(KStream, ValueJoiner, JoinWindows, StreamJoined)
 * @see KStream#leftJoin(KStream, ValueJoiner, JoinWindows)
 * @see KStream#leftJoin(KStream, ValueJoiner, JoinWindows, StreamJoined)
 * @see KStream#outerJoin(KStream, ValueJoiner, JoinWindows)
 * @see KStream#outerJoin(KStream, ValueJoiner, JoinWindows, StreamJoined)
 * @see TimestampExtractor
 */
public final class JoinWindows extends Windows<Window> {

    /** Maximum time difference for tuples that are before the join tuple. */
    public final long beforeMs;
    /** Maximum time difference for tuples that are after the join tuple. */
    public final long afterMs;

    private final long graceMs;

    private JoinWindows(final long beforeMs,
                        final long afterMs,
                        final long graceMs) {
        if (beforeMs + afterMs < 0) {
            throw new IllegalArgumentException("Window interval (ie, beforeMs+afterMs) must not be negative.");
        }
        this.afterMs = afterMs;
        this.beforeMs = beforeMs;
        this.graceMs = graceMs;
    }

    /**
     * Specifies that records of the same key are joinable if their timestamps are within {@code timeDifference},
     * i.e., the timestamp of a record from the secondary stream is max {@code timeDifference} earlier or later than
     * the timestamp of the record from the primary stream.
     *
     * @param timeDifference join window interval
     * @throws IllegalArgumentException if {@code timeDifference} is negative or can't be represented as {@code long milliseconds}
     */
    public static JoinWindows of(final Duration timeDifference) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(timeDifference, "timeDifference");
        final long timeDifferenceMs = validateMillisecondDuration(timeDifference, msgPrefix);
        return new JoinWindows(timeDifferenceMs, timeDifferenceMs, DEFAULT_GRACE_PERIOD_MS);
    }

    /**
     * Changes the start window boundary to {@code timeDifference} but keep the end window boundary as is.
     * Thus, records of the same key are joinable if the timestamp of a record from the secondary stream is at most
     * {@code timeDifference} earlier than the timestamp of the record from the primary stream.
     * {@code timeDifference} can be negative but its absolute value must not be larger than current window "after"
     * value (which would result in a negative window size).
     *
     * @param timeDifference relative window start time
     * @throws IllegalArgumentException if the resulting window size is negative or {@code timeDifference} can't be represented as {@code long milliseconds}
     */
    public JoinWindows before(final Duration timeDifference) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(timeDifference, "timeDifference");
        final long timeDifferenceMs = validateMillisecondDuration(timeDifference, msgPrefix);
        return new JoinWindows(timeDifferenceMs, afterMs, DEFAULT_GRACE_PERIOD_MS);
    }

    /**
     * Changes the end window boundary to {@code timeDifference} but keep the start window boundary as is.
     * Thus, records of the same key are joinable if the timestamp of a record from the secondary stream is at most
     * {@code timeDifference} later than the timestamp of the record from the primary stream.
     * {@code timeDifference} can be negative but its absolute value must not be larger than current window "before"
     * value (which would result in a negative window size).
     *
     * @param timeDifference relative window end time
     * @throws IllegalArgumentException if the resulting window size is negative or {@code timeDifference} can't be represented as {@code long milliseconds}
     */
    public JoinWindows after(final Duration timeDifference) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(timeDifference, "timeDifference");
        final long timeDifferenceMs = validateMillisecondDuration(timeDifference, msgPrefix);
        return new JoinWindows(beforeMs, timeDifferenceMs, DEFAULT_GRACE_PERIOD_MS);
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
     * Reject out-of-order events that are delayed more than {@code afterWindowEnd}
     * after the end of its window.
     * <p>
     * Delay is defined as (stream_time - record_timestamp).
     *
     * @param afterWindowEnd The grace period to admit out-of-order events to a window.
     * @return this updated builder
     * @throws IllegalArgumentException if the {@code afterWindowEnd} is negative of can't be represented as {@code long milliseconds}
     */
    public JoinWindows grace(final Duration afterWindowEnd) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(afterWindowEnd, "afterWindowEnd");
        final long afterWindowEndMs = validateMillisecondDuration(afterWindowEnd, msgPrefix);
        if (afterWindowEndMs < 0) {
            throw new IllegalArgumentException("Grace period must not be negative.");
        }
        return new JoinWindows(beforeMs, afterMs, afterWindowEndMs);
    }

    @Override
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
        final JoinWindows that = (JoinWindows) o;
        return beforeMs == that.beforeMs &&
            afterMs == that.afterMs &&
            graceMs == that.graceMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(beforeMs, afterMs, graceMs);
    }

    @Override
    public String toString() {
        return "JoinWindows{" +
            "beforeMs=" + beforeMs +
            ", afterMs=" + afterMs +
            ", graceMs=" + graceMs +
            '}';
    }
}
