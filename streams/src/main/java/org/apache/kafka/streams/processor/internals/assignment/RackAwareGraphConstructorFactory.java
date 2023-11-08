package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;

public class RackAwareGraphConstructorFactory {

    static RackAwareGraphConstructor create(final AssignmentConfigs assignmentConfigs) {
        switch (assignmentConfigs.rackAwareAssignmentStrategy) {
            case StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC:
                return new MinTrafficGraphConstructor();
            case StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY:
                return new BalanceSubtopologyGraphConstructor();
            default:
                throw new IllegalArgumentException("Rack aware assignment is disabled");
        }
    }

}
