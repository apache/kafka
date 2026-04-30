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
package org.apache.kafka.server.replica;

import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import java.util.Optional;

/**
 * @param logStartOffset              The log start offset value, kept in all replicas; for local replica it is the log's start offset, for remote replicas its value is only updated by follower fetch.
 * @param logEndOffsetMetadata        The log end offset value, kept in all replicas; for local replica it is the log's end offset, for remote replicas its value is only updated by follower fetch.
 * @param lastFetchLeaderLogEndOffset The log end offset value at the time the leader received the last FetchRequest from this follower. This is used to determine the lastCaughtUpTimeMs of the follower. It is reset by the leader when a LeaderAndIsr request is received and might be reset when the leader appends a record to its log.
 * @param lastFetchTimeMs             The time when the leader received the last FetchRequest from this follower. This is used to determine the lastCaughtUpTimeMs of the follower.
 * @param lastCaughtUpTimeMs          lastCaughtUpTimeMs is the largest time t such that the offset of most recent FetchRequest from this follower >= the LEO of leader at time t. This is used to determine the lag of this follower and ISR of this partition.
 * @param brokerEpoch                 The brokerEpoch is the epoch from the Fetch request.
 */
public record ReplicaState(
    long logStartOffset,
    LogOffsetMetadata logEndOffsetMetadata,
    long lastFetchLeaderLogEndOffset,
    long lastFetchTimeMs,
    long lastCaughtUpTimeMs,
    Optional<Long> brokerEpoch
) {
    public static final ReplicaState EMPTY = new ReplicaState(
        UnifiedLog.UNKNOWN_OFFSET,
        LogOffsetMetadata.UNKNOWN_OFFSET_METADATA,
        0L,
        0L,
        0L,
        Optional.empty()
    );

    /**
     * Returns the current log end offset of the replica.
     */
    public long logEndOffset() {
        return logEndOffsetMetadata.messageOffset;
    }

    /**
     * Returns true when the replica is considered as "caught-up". A replica is
     * considered "caught-up" when its log end offset is equals to the log end
     * offset of the leader OR when its last caught up time minus the current
     * time is smaller than the max replica lag.
     */
    public boolean isCaughtUp(
        long leaderEndOffset,
        long currentTimeMs,
        long replicaMaxLagMs) {
        return leaderEndOffset == logEndOffset() || currentTimeMs - lastCaughtUpTimeMs <= replicaMaxLagMs;
    }
}
