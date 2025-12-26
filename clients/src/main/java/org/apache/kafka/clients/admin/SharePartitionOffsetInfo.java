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

package org.apache.kafka.clients.admin;

import java.util.Objects;
import java.util.Optional;

/**
 * This class is used to contain the offset and lag information for a share-partition.
 */
public class SharePartitionOffsetInfo {
    private final long startOffset;
    private final Optional<Integer> leaderEpoch;
    private final Optional<Long> lag;

    /**
     * Construct a new SharePartitionOffsetInfo.
     *
     * @param startOffset The share-partition start offset. The start offset is the earliest offset
     *                    for in-flight records being evaluated for delivery to share consumers.
     *                    Some records after the start offset may already have completed delivery.
     * @param leaderEpoch The optional leader epoch of the share-partition
     * @param lag         The optional lag for the share-partition
     */
    public SharePartitionOffsetInfo(long startOffset, Optional<Integer> leaderEpoch, Optional<Long> lag) {
        this.startOffset = startOffset;
        this.leaderEpoch = leaderEpoch;
        this.lag = lag;
    }

    /**
     * Get the start offset for the share-partition. The start offset is the earliest offset for
     * in-flight records being evaluated for delivery to share consumers. Some records after the start
     * offset may already have completed delivery.
     *
     * @return The start offset of the partition read by the share group.
     */
    public long startOffset() {
        return startOffset;
    }

    /**
     * Get the leader epoch for the partition.
     *
     * @return The leader epoch of the partition.
     */
    public Optional<Integer> leaderEpoch() {
        return leaderEpoch;
    }

    /**
     * Get the lag for the partition.
     *
     * @return The lag of the partition.
     */
    public Optional<Long> lag() {
        return lag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SharePartitionOffsetInfo that = (SharePartitionOffsetInfo) o;
        return startOffset == that.startOffset &&
            Objects.equals(leaderEpoch, that.leaderEpoch) &&
            Objects.equals(lag, that.lag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startOffset, leaderEpoch, lag);
    }

    @Override
    public String toString() {
        return "SharePartitionOffsetInfo{" +
            "startOffset=" + startOffset +
            ", leaderEpoch=" + leaderEpoch.orElse(null) +
            ", lag=" + lag.orElse(null) +
            '}';
    }
}