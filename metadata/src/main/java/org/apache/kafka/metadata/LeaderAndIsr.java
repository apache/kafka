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
package org.apache.kafka.metadata;

import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class LeaderAndIsr {
    public static final int INITIAL_LEADER_EPOCH = 0;
    public static final int INITIAL_PARTITION_EPOCH = 0;
    public static final int NO_LEADER = -1;
    public static final int NO_EPOCH = -1;
    public static final int LEADER_DURING_DELETE = -2;
    public static final int EPOCH_DURING_DELETE = -2;

    private final int leader;
    private final int leaderEpoch;
    private final LeaderRecoveryState leaderRecoveryState;
    private final List<BrokerState> isrWithBrokerEpoch;

    // The current epoch for the partition for KRaft controllers. The current ZK version for the
    // legacy controllers. The epoch is a monotonically increasing value which is incremented
    // after every partition change.
    private final int partitionEpoch;

    public LeaderAndIsr(int leader, List<Integer> isr) {
        this(leader, INITIAL_LEADER_EPOCH, isr, LeaderRecoveryState.RECOVERED, INITIAL_PARTITION_EPOCH);
    }

    public LeaderAndIsr(
            int leader,
            int leaderEpoch,
            List<Integer> isr,
            LeaderRecoveryState leaderRecoveryState,
            int partitionEpoch
    ) {
        this(
                leader,
                leaderEpoch,
                leaderRecoveryState,
                isr.stream().map(brokerId -> new BrokerState().setBrokerId(brokerId)).toList(),
                partitionEpoch
        );
    }

    public LeaderAndIsr(
            int leader,
            int leaderEpoch,
            LeaderRecoveryState leaderRecoveryState,
            List<BrokerState> isrWithBrokerEpoch,
            int partitionEpoch
    ) {
        this.leader = leader;
        this.leaderEpoch = leaderEpoch;
        this.leaderRecoveryState = leaderRecoveryState;
        this.isrWithBrokerEpoch = isrWithBrokerEpoch;
        this.partitionEpoch = partitionEpoch;
    }

    public static LeaderAndIsr duringDelete(List<Integer> isr) {
        return new LeaderAndIsr(LEADER_DURING_DELETE, isr);
    }

    public int leader() {
        return leader;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    public List<BrokerState> isrWithBrokerEpoch() {
        return isrWithBrokerEpoch;
    }

    public LeaderRecoveryState leaderRecoveryState() {
        return leaderRecoveryState;
    }

    public int partitionEpoch() {
        return partitionEpoch;
    }

    public LeaderAndIsr withPartitionEpoch(int partitionEpoch) {
        return new LeaderAndIsr(leader, leaderEpoch, leaderRecoveryState, isrWithBrokerEpoch, partitionEpoch);
    }

    public LeaderAndIsr newLeader(int leader) {
        return newLeaderAndIsrWithBrokerEpoch(leader, isrWithBrokerEpoch);
    }

    public LeaderAndIsr newLeaderAndIsr(int leader, List<Integer> isr) {
        return new LeaderAndIsr(leader, leaderEpoch + 1, isr, leaderRecoveryState, partitionEpoch);
    }

    private LeaderAndIsr newLeaderAndIsrWithBrokerEpoch(int leader, List<BrokerState> isrWithBrokerEpoch) {
        return new LeaderAndIsr(leader, leaderEpoch + 1, leaderRecoveryState, isrWithBrokerEpoch, partitionEpoch);
    }

    public LeaderAndIsr newRecoveringLeaderAndIsr(int leader, List<Integer> isr) {
        return new LeaderAndIsr(leader, leaderEpoch + 1, isr, LeaderRecoveryState.RECOVERING, partitionEpoch);
    }

    public LeaderAndIsr newEpoch() {
        return newLeaderAndIsrWithBrokerEpoch(leader, isrWithBrokerEpoch);
    }

    public Optional<Integer> leaderOpt() {
        return leader == LeaderAndIsr.NO_LEADER ? Optional.empty() : Optional.of(leader);
    }

    public List<Integer> isr() {
        return isrWithBrokerEpoch.stream()
                .map(BrokerState::brokerId)
                .toList();
    }

    public boolean equalsAllowStalePartitionEpoch(LeaderAndIsr other) {
        if (this == other) {
            return true;
        } else if (other == null) {
            return false;
        } else {
            return leader == other.leader &&
                    leaderEpoch == other.leaderEpoch &&
                    isrWithBrokerEpoch.equals(other.isrWithBrokerEpoch) &&
                    leaderRecoveryState == other.leaderRecoveryState &&
                    partitionEpoch <= other.partitionEpoch;
        }
    }

    @Override
    public String toString() {
        return "LeaderAndIsr(" +
                "leader=" + leader +
                ", leaderEpoch=" + leaderEpoch +
                ", isrWithBrokerEpoch=" + isrWithBrokerEpoch +
                ", leaderRecoveryState=" + leaderRecoveryState +
                ", partitionEpoch=" + partitionEpoch +
                ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LeaderAndIsr that = (LeaderAndIsr) o;
        return leader == that.leader && leaderEpoch == that.leaderEpoch && partitionEpoch == that.partitionEpoch &&
                leaderRecoveryState == that.leaderRecoveryState && Objects.equals(isrWithBrokerEpoch, that.isrWithBrokerEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leader, leaderEpoch, leaderRecoveryState, isrWithBrokerEpoch, partitionEpoch);
    }
}
