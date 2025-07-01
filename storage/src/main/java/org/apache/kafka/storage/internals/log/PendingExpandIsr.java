package org.apache.kafka.storage.internals.log;

import org.apache.kafka.metadata.LeaderAndIsr;

import java.util.HashSet;
import java.util.Set;

public record PendingExpandIsr(int newInSyncReplicaId,
                               LeaderAndIsr sentLeaderAndIsr,
                               CommittedPartitionState lastCommittedState) implements PendingPartitionChange {

    @Override
    public CommittedPartitionState lastCommittedState() {
        return lastCommittedState;
    }

    @Override
    public void notifyListener(AlterPartitionListener alterPartitionListener) {
        alterPartitionListener.markIsrExpand();
    }

    @Override
    public Set<Integer> isr() {
        return lastCommittedState.isr();
    }

    @Override
    public Set<Integer> maximalIsr() {
        Set<Integer> newIsr = new HashSet<>(lastCommittedState.isr());
        newIsr.add(newInSyncReplicaId);
        return Set.copyOf(newIsr);
    }

    @Override
    public Boolean isInflight() {
        return true;
    }

    @Override
    public String toString(){
        return "PendingExpandIsr(newInSyncReplicaId=" + newInSyncReplicaId +
                ", sentLeaderAndIsr=" + sentLeaderAndIsr +
                ", leaderRecoveryState=" + leaderRecoveryState() +
                ", lastCommittedState=" + lastCommittedState +
                ")";
    }
}
