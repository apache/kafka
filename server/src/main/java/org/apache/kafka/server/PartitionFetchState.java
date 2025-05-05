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


package org.apache.kafka.server;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import java.util.Optional;

/**
 * Class to keep partition offset and its state (truncatingLog, delayed)
 * This represents a partition as being either:
 * (1) Truncating its log, for example, having recently become a follower
 * (2) Delayed, for example, due to an error, where we subsequently back off a bit
 * (3) ReadyForFetch, the active state where the thread is actively fetching data.
 */
public class PartitionFetchState {
    private final Optional<Uuid> topicId;
    private final long fetchOffset;
    private final Optional<Long> lag;
    private final int currentLeaderEpoch;
    private final Optional<Long> delay;
    private final ReplicaState state;
    private final Optional<Integer> lastFetchedEpoch;
    private final Optional<Long> dueMs;


    public PartitionFetchState(Optional<Uuid> topicId,
                               long fetchOffset,
                               Optional<Long> lag,
                               int currentLeaderEpoch,
                               ReplicaState state,
                               Optional<Integer> lastFetchedEpoch) {
        this(topicId, fetchOffset, lag, currentLeaderEpoch,
                Optional.empty(), state, lastFetchedEpoch);
    }

    public PartitionFetchState(Optional<Uuid> topicId,
                               long fetchOffset,
                               Optional<Long> lag,
                               int currentLeaderEpoch,
                               Optional<Long> delay,
                               ReplicaState state,
                               Optional<Integer> lastFetchedEpoch) {
        this.topicId = topicId;
        this.fetchOffset = fetchOffset;
        this.lag = lag;
        this.currentLeaderEpoch = currentLeaderEpoch;
        this.delay = delay;
        this.state = state;
        this.lastFetchedEpoch = lastFetchedEpoch;

        // Initialize dueMs (equivalent to private val dueMs)
        this.dueMs = delay.map(aLong -> aLong + Time.SYSTEM.milliseconds());
    }

    public boolean isReadyForFetch() {
        return state == ReplicaState.FETCHING && !isDelayed();
    }

    public boolean isReplicaInSync() {
        return lag.isPresent() && lag.get() <= 0;
    }

    public boolean isTruncating() {
        return state == ReplicaState.TRUNCATING && !isDelayed();
    }

    public boolean isDelayed() {
        return dueMs.isPresent() && dueMs.get() > Time.SYSTEM.milliseconds();
    }

    public Optional<Uuid> topicId() {
        return topicId;
    }

    public long fetchOffset() {
        return fetchOffset;
    }

    public Optional<Long> lag() {
        return lag;
    }

    public int currentLeaderEpoch() {
        return currentLeaderEpoch;
    }

    public Optional<Long> delay() {
        return delay;
    }

    public ReplicaState state() {
        return state;
    }

    public Optional<Integer> lastFetchedEpoch() {
        return lastFetchedEpoch;
    }

    @Override
    public String toString() {
        return "FetchState(topicId=" + topicId +
                ", fetchOffset=" + fetchOffset +
                ", currentLeaderEpoch=" + currentLeaderEpoch +
                ", lastFetchedEpoch=" + lastFetchedEpoch +
                ", state=" + state +
                ", lag=" + lag +
                ", delay=" + delay.orElse(0L) + "ms)";
    }

    public PartitionFetchState updateTopicId(Optional<Uuid> newTopicId) {
        return new PartitionFetchState(newTopicId, this.fetchOffset, this.lag,
                this.currentLeaderEpoch, this.delay,
                this.state, this.lastFetchedEpoch);
    }
}
