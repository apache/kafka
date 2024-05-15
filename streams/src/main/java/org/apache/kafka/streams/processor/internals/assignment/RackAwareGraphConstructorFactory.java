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
package org.apache.kafka.streams.processor.internals.assignment;

import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;

public class RackAwareGraphConstructorFactory {

    static <T> RackAwareGraphConstructor<T> create(final AssignmentConfigs assignmentConfigs, final Map<Subtopology, Set<TaskId>> tasksForTopicGroup) {
        switch (assignmentConfigs.rackAwareAssignmentStrategy) {
            case StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC:
                return new MinTrafficGraphConstructor<T>();
            case StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY:
                return new BalanceSubtopologyGraphConstructor<T>(tasksForTopicGroup);
            default:
                throw new IllegalArgumentException("Rack aware assignment is disabled");
        }
    }

}
