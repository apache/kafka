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
package org.apache.kafka.streams.processor.internals;

import java.util.Comparator;
import java.util.Objects;

/**
 * A type class suitable for representing offsets, offset sums, and offset differences (lags),
 * including semantically sound sentinel values.
 */
public final class OffsetLike {
    // this must be negative to distinguish a running active task from other kinds of tasks
    // which may be caught up to the same offsets
    private static final long LATEST_OFFSET = -2L;

    // Use a negative sentinel when we don't know the offset instead of skipping it to distinguish it from dirty state
    // and use -3 as the -1 sentinel may be taken by some producer errors and -2 in the
    // subscription means that the state is used by an active task and hence caught-up.
    public static final long OFFSET_UNKNOWN = -3L;

    private final long serialValue;

    public static OffsetLike unknownSentinel() {
        return new OffsetLike(OFFSET_UNKNOWN);
    }

    public static OffsetLike latestSentinel() {
        return new OffsetLike(LATEST_OFFSET);
    }

    public static OffsetLike realValue(final long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("non-sentinel offsets may not be negative");
        } else {
            return new OffsetLike(offset);
        }
    }

    public static OffsetLike maxValue() {
        return new OffsetLike(Long.MAX_VALUE);
    }

    public static OffsetLike fromSerialValue(final long serialValue) {
        return new OffsetLike(serialValue);
    }

    private OffsetLike(final long serialValue) {
        this.serialValue = serialValue;
    }

    public long serialValue() {
        return serialValue;
    }

    public boolean isLatestSentinel() {
        return serialValue == LATEST_OFFSET;
    }

    public boolean isUnknown() {
        return serialValue == OFFSET_UNKNOWN;
    }

    public boolean isRealValue() {
        return serialValue > 0;
    }

    public long realValue() {
        if (isRealValue()) {
            return serialValue;
        } else {
            throw new IllegalStateException("Expected not to get a sentinel offset, but got: " + serialValue);
        }
    }

    public static class CompareAsLags implements Comparator<OffsetLike> {
        @Override
        public int compare(final OffsetLike o1, final OffsetLike o2) {
            if (o1.isUnknown() || o2.isUnknown()) {
                throw new IllegalArgumentException("Unknown offsets are not comparable: " + o1 + ", " + o2);
            } else if (o1.isLatestSentinel() && o2.isLatestSentinel()) {
                return 0;
            } else if (o1.isLatestSentinel() && o2.isRealValue()) {
                return -1;
            } else {
                return Long.compare(o1.realValue(), o2.realValue());
            }
        }
    }

    @Override
    public String toString() {
        return "Offset{" +
            "serialValue=" + serialValue +
            '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final OffsetLike that = (OffsetLike) o;
        return serialValue == that.serialValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(serialValue);
    }
}
