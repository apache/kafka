package org.apache.kafka.coordinator.group.api.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;
import java.util.Set;

/**
 * The partition assignment for a consumer group member.
 */
@InterfaceStability.Unstable
public interface MemberAssignment {
    /**
     * @return Target partitions keyed by topic Ids.
     */
    Map<Uuid, Set<Integer>> partitions();
}
