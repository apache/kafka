package org.apache.kafka.streams.processor.internals.assignment;

import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;

public class RackAwareGraphConstructorFactory {

    static RackAwareGraphConstructor create(final AssignmentConfigs assignmentConfigs, final Map<Subtopology, Set<TaskId>> tasksForTopicGroup) {
        switch (assignmentConfigs.rackAwareAssignmentStrategy) {
            case StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC:
                return new MinTrafficGraphConstructor();
            case StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY:
                return new BalanceSubtopologyGraphConstructor(tasksForTopicGroup);
            default:
                throw new IllegalArgumentException("Rack aware assignment is disabled");
        }
    }

}
