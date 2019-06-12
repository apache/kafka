package org.apache.kafka.common.replica;

import java.util.Collections;
import java.util.Set;

/**
 * View of a partition used by {@link ReplicaSelector} to determine a preferred replica.
 */
public interface PartitionView {
    Set<ReplicaView> replicas();

    class DefaultPartitionView implements PartitionView {
        private final Set<ReplicaView> replicas;

        public DefaultPartitionView(Set<ReplicaView> replicas) {
            this.replicas = Collections.unmodifiableSet(replicas);
        }

        @Override
        public Set<ReplicaView> replicas() {
            return replicas;
        }
    }
}
