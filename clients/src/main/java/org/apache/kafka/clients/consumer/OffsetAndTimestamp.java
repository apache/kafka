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
package org.apache.kafka.clients.consumer;

import java.util.Objects;
import java.util.Optional;

/**
 * A container class for offset and timestamp.
 */
public final class OffsetAndTimestamp {
    private final long timestamp;
    private final long offset;
    private final Optional<Integer> leaderEpoch;

    public OffsetAndTimestamp(long offset, long timestamp) {
        this(offset, timestamp, Optional.empty());
    }

    public OffsetAndTimestamp(long offset, long timestamp, Optional<Integer> leaderEpoch) {
        if (offset < 0)
            throw new IllegalArgumentException("Invalid negative offset");

        if (timestamp < 0)
            throw new IllegalArgumentException("Invalid negative timestamp");

        this.offset = offset;
        this.timestamp = timestamp;
        this.leaderEpoch = leaderEpoch;
    }

    public long timestamp() {
        return timestamp;
    }

    public long offset() {
        return offset;
    }

    /**
     * Get the leader epoch corresponding to the offset that was found (if one exists).
     * This can be provided to seek() to ensure that the log hasn't been truncated prior to fetching.
     *
     * @return The leader epoch or empty if it is not known
     */
    public Optional<Integer> leaderEpoch() {
        return leaderEpoch;
    }

    @Override
    public String toString() {
        return "(timestamp=" + timestamp +
                ", leaderEpoch=" + leaderEpoch.orElse(null) +
                ", offset=" + offset + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffsetAndTimestamp that = (OffsetAndTimestamp) o;
        return timestamp == that.timestamp &&
                offset == that.offset &&
                Objects.equals(leaderEpoch, that.leaderEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, offset, leaderEpoch);
    }
}
