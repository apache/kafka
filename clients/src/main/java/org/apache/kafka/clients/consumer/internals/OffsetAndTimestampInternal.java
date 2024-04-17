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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;

import java.util.Optional;

/**
 * Internal representation of {@link OffsetAndTimestamp} to allow negative timestamps and offset.
 */
public class OffsetAndTimestampInternal {
    private final long timestamp;
    private final long offset;
    private final Optional<Integer> leaderEpoch;

    public OffsetAndTimestampInternal(long offset, long timestamp, Optional<Integer> leaderEpoch) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.leaderEpoch = leaderEpoch;
    }

    long offset() {
        return offset;
    }

    long timestamp() {
        return timestamp;
    }

    Optional<Integer> leaderEpoch() {
        return leaderEpoch;
    }

    public OffsetAndTimestamp buildOffsetAndTimestamp() {
        return new OffsetAndTimestamp(offset, timestamp, leaderEpoch);
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + leaderEpoch.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OffsetAndTimestampInternal)) return false;

        OffsetAndTimestampInternal that = (OffsetAndTimestampInternal) o;

        if (timestamp != that.timestamp) return false;
        if (offset != that.offset) return false;
        return leaderEpoch.equals(that.leaderEpoch);
    }

    @Override
    public String toString() {
        return "OffsetAndTimestampInternal{" +
                "timestamp=" + timestamp +
                ", offset=" + offset +
                ", leaderEpoch=" + leaderEpoch +
                '}';
    }
}
