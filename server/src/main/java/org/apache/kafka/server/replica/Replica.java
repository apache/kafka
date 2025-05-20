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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class Replica {
    private static final Logger LOGGER = LoggerFactory.getLogger(Replica.class);
    private final int brokerId;
    private final TopicPartition topicPartition;
    private final MetadataCache metadataCache;
    private final AtomicReference<ReplicaState> replicaState;

    public Replica(int brokerId, TopicPartition topicPartition, MetadataCache metadataCache) {
        this.brokerId = brokerId;
        this.topicPartition = topicPartition;
        this.metadataCache = metadataCache;
        this.replicaState = new AtomicReference<>(ReplicaState.EMPTY);
    }

    public ReplicaState stateSnapshot() {
        return replicaState.get();
    }

    public int brokerId() {
        return brokerId;
    }

    /**
     * Update the replica's fetch state only if the broker epoch is -1 or it is larger or equal to the current broker
     * epoch. Otherwise, NOT_LEADER_OR_FOLLOWER exception will be thrown. This can fence fetch state update from a
     * stale request.
     *
     * If the FetchRequest reads up to the log end offset of the leader when the current fetch request is received,
     * set `lastCaughtUpTimeMs` to the time when the current fetch request was received.
     *
     * Else if the FetchRequest reads up to the log end offset of the leader when the previous fetch request was received,
     * set `lastCaughtUpTimeMs` to the time when the previous fetch request was received.
     *
     * This is needed to enforce the semantics of ISR, i.e. a replica is in ISR if and only if it lags behind leader's LEO
     * by at most `replicaLagTimeMaxMs`. These semantics allow a follower to be added to the ISR even if the offset of its
     * fetch request is always smaller than the leader's LEO, which can happen if small produce requests are received at
     * high frequency.
     */
    public void updateFetchStateOrThrow(
        LogOffsetMetadata followerFetchOffsetMetadata,
        long followerStartOffset,
        long followerFetchTimeMs,
        long leaderEndOffset,
        long brokerEpoch
    ) {
        replicaState.updateAndGet(currentReplicaState -> {
            var cachedBrokerEpoch = metadataCache.getAliveBrokerEpoch(brokerId);
            // Fence the update if it provides a stale broker epoch.
            if (brokerEpoch != -1 && cachedBrokerEpoch.filter(e -> e > brokerEpoch).isPresent()) {
                throw new NotLeaderOrFollowerException("Received stale fetch state update. broker epoch=" + brokerEpoch +
                    " vs expected=" + currentReplicaState.brokerEpoch());
            }

            long lastCaughtUpTime;
            if (followerFetchOffsetMetadata.messageOffset >= leaderEndOffset) {
                lastCaughtUpTime = Math.max(currentReplicaState.lastCaughtUpTimeMs(), followerFetchTimeMs);
            } else if (followerFetchOffsetMetadata.messageOffset >= currentReplicaState.lastFetchLeaderLogEndOffset()) {
                lastCaughtUpTime = Math.max(currentReplicaState.lastCaughtUpTimeMs(), currentReplicaState.lastFetchTimeMs());
            } else {
                lastCaughtUpTime = currentReplicaState.lastCaughtUpTimeMs();
            }

            return new ReplicaState(
                followerStartOffset,
                followerFetchOffsetMetadata,
                Math.max(leaderEndOffset, currentReplicaState.lastFetchLeaderLogEndOffset()),
                followerFetchTimeMs,
                lastCaughtUpTime,
                Optional.of(brokerEpoch)
            );
        });
    }

    /**
     * When the leader is elected or re-elected, the state of the follower is reinitialized
     * accordingly.
     */
    public void resetReplicaState(
        long currentTimeMs,
        long leaderEndOffset,
        boolean isNewLeader,
        boolean isFollowerInSync
    ) {
        replicaState.updateAndGet(currentReplicaState -> {
            // When the leader is elected or re-elected, the follower's last caught up time
            // is set to the current time if the follower is in the ISR, else to 0. The latter
            // is done to ensure that the high watermark is not hold back unnecessarily for
            // a follower which is not in the ISR anymore.
            long lastCaughtUpTimeMs = isFollowerInSync ? currentTimeMs : 0L;

            if (isNewLeader) {
                return new ReplicaState(
                    UnifiedLog.UNKNOWN_OFFSET,
                    LogOffsetMetadata.UNKNOWN_OFFSET_METADATA,
                    UnifiedLog.UNKNOWN_OFFSET,
                    0L,
                    lastCaughtUpTimeMs,
                    Optional.empty()
                );
            } else {
                return new ReplicaState(
                    currentReplicaState.logStartOffset(),
                    currentReplicaState.logEndOffsetMetadata(),
                    leaderEndOffset,
                    // When the leader is re-elected, the follower's last fetch time is
                    // set to the current time if the follower is in the ISR, else to 0.
                    // The latter is done to ensure that the follower is not brought back
                    // into the ISR before a fetch is received.
                    isFollowerInSync ? currentTimeMs : 0L,
                    lastCaughtUpTimeMs,
                    currentReplicaState.brokerEpoch()
                );
            }
        });
        LOGGER.trace("Reset state of replica to {}", this);
    }

    @Override
    public String toString() {
        ReplicaState replicaState = this.replicaState.get();
        return "Replica(replicaId=" + brokerId +
            ", topic=" + topicPartition.topic() +
            ", partition=" + topicPartition.partition() +
            ", lastCaughtUpTimeMs=" + replicaState.lastCaughtUpTimeMs() +
            ", logStartOffset=" + replicaState.logStartOffset() +
            ", logEndOffset=" + replicaState.logEndOffsetMetadata().messageOffset +
            ", logEndOffsetMetadata=" + replicaState.logEndOffsetMetadata() +
            ", lastFetchLeaderLogEndOffset=" + replicaState.lastFetchLeaderLogEndOffset() +
            ", brokerEpoch=" + replicaState.brokerEpoch().orElse(-2L) +
            ", lastFetchTimeMs=" + replicaState.lastFetchTimeMs() +
            ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Replica other = (Replica) o;
        return brokerId == other.brokerId && topicPartition.equals(other.topicPartition);
    }

    @Override
    public int hashCode() {
        return 31 + topicPartition.hashCode() + 17 * brokerId;
    }
}