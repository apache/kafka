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
package org.apache.kafka.server.partition;

import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.metadata.LeaderRecoveryState;

import java.util.Set;

/**
 * Represents a pending change to shrink the ISR of a partition.
 *
 * @param outOfSyncReplicaIds The set of replica IDs that are out of sync and will be removed from the ISR.
 * @param sentLeaderAndIsr The LeaderAndIsr object that was sent to the controller for this ISR shrinking.
 * @param lastCommittedState The last committed partition state before this ISR shrinking.
 */
public record PendingShrinkIsr(Set<Integer> outOfSyncReplicaIds,
                               LeaderAndIsr sentLeaderAndIsr,
                               CommittedPartitionState lastCommittedState) implements PendingPartitionChange {

    @Override
    public Set<Integer> isr() {
        return lastCommittedState.isr();
    }

    @Override
    public Set<Integer> maximalIsr() {
        return isr();
    }

    @Override
    public LeaderRecoveryState leaderRecoveryState() {
        return LeaderRecoveryState.RECOVERED;
    }

    @Override
    public boolean isInflight() {
        return true;
    }

    @Override
    public String toString() {
        return  "PendingShrinkIsr(outOfSyncReplicaIds=" + outOfSyncReplicaIds +
                ", sentLeaderAndIsr=" + sentLeaderAndIsr +
                ", leaderRecoveryState=" + leaderRecoveryState() +
                ", lastCommittedState=" + lastCommittedState +
                ")";
    }
}
