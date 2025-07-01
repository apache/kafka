package org.apache.kafka.storage.internals.log;

import org.apache.kafka.metadata.LeaderAndIsr;

import java.util.Set;

public record PendingShrinkIsr(Set<Integer> outOfSyncReplicaIds,
                               LeaderAndIsr sentLeaderAndIsr,
                               CommittedPartitionState lastCommittedState) implements PendingPartitionChange {


    @Override
    public CommittedPartitionState lastCommittedState() {
        return lastCommittedState;
    }

    @Override
    public void notifyListener(AlterPartitionListener alterPartitionListener) {
        alterPartitionListener.markIsrShrink();
    }

    @Override
    public Set<Integer> isr() {
        return lastCommittedState.isr();
    }

    @Override
    public Set<Integer> maximalIsr() {
        return isr();
    }

    @Override
    public Boolean isInflight() {
        return true;
    }

    @Override
    public String toString(){
        return  "PendingShrinkIsr(outOfSyncReplicaIds=" + outOfSyncReplicaIds +
                ", sentLeaderAndIsr=" + sentLeaderAndIsr +
                ", leaderRecoveryState=" + leaderRecoveryState() +
                ", lastCommittedState=" + lastCommittedState +
                ")";
    }
}
