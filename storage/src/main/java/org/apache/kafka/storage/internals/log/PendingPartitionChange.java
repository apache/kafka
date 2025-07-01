package org.apache.kafka.storage.internals.log;

import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.metadata.LeaderRecoveryState;

public interface PendingPartitionChange extends PartitionState{
    CommittedPartitionState lastCommittedState();
    LeaderAndIsr sentLeaderAndIsr();

    default LeaderRecoveryState leaderRecoveryState(){
        return LeaderRecoveryState.RECOVERED;
    }

    void notifyListener(AlterPartitionListener alterPartitionListener);

}
