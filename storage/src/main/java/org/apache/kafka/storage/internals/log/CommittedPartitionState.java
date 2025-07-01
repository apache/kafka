package org.apache.kafka.storage.internals.log;

import org.apache.kafka.metadata.LeaderRecoveryState;

import java.util.Set;

public record CommittedPartitionState(Set<Integer> isr, LeaderRecoveryState leaderRecoveryState) implements PartitionState {

    @Override
    public Set<Integer> maximalIsr() {
        return isr;
    }

    @Override
    public Boolean isInflight() {
        return false;
    }

    @Override
    public String toString() {
        return "CommittedPartitionState(isr=" + isr +
                ", leaderRecoveryState=" + leaderRecoveryState +
                ")";
    }
}
