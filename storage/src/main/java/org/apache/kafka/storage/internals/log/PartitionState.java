package org.apache.kafka.storage.internals.log;

import org.apache.kafka.metadata.LeaderRecoveryState;

import java.util.Set;

public interface PartitionState {
    /**
     * Includes only the in-sync replicas which have been committed to ZK.
     */
    Set<Integer> isr();

    /**
     * This set may include un-committed ISR members following an expansion. This "effective" ISR is used for advancing
     * the high watermark as well as determining which replicas are required for acks=all produce requests.
     *
     * Only applicable as of IBP 2.7-IV2, for older versions this will return the committed ISR
     */
    Set<Integer> maximalIsr();

    /**
     * The leader recovery state. See the description for LeaderRecoveryState for details on the different values.
     */
    LeaderRecoveryState leaderRecoveryState();

    /**
     * Indicates if we have an AlterPartition request inflight.
     */
    Boolean isInflight();

}
