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

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class RackAwareGraphConstructorFactory {

    static <T> RackAwareGraphConstructor<T> create(final AssignmentConfigs assignmentConfigs,
                                                   final Map<Subtopology, Set<TaskId>> tasksForTopicGroup) {
        return create(assignmentConfigs.rackAwareAssignmentStrategy(), new ArrayList<>(new TreeMap<>(tasksForTopicGroup).values()));
    }

    public static <T> RackAwareGraphConstructor<T> create(final String rackAwareAssignmentStrategy,
                                                          final Collection<TaskId> allTasks) {
        final Map<Integer, Set<TaskId>> tasksForTopologyId = new TreeMap<>();
        allTasks.forEach(taskId -> {
            tasksForTopologyId.computeIfAbsent(taskId.subtopology(), k -> new HashSet<>());
            tasksForTopologyId.get(taskId.subtopology()).add(taskId);
        });

        final List<Set<TaskId>> tasksForTopicGroup = new ArrayList<>();
        tasksForTopologyId.forEach((subtopology, taskIds) -> {
            tasksForTopicGroup.add(taskIds);
        });
        return create(rackAwareAssignmentStrategy, tasksForTopicGroup);
    }

    public static <T> RackAwareGraphConstructor<T> create(final String rackAwareAssignmentStrategy,
                                                          final List<Set<TaskId>> taskSetsPerTopicGroup) {
        switch (rackAwareAssignmentStrategy) {
            case StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC:
                return new MinTrafficGraphConstructor<T>();
            case StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY:
                return new BalanceSubtopologyGraphConstructor<T>(taskSetsPerTopicGroup);
            default:
                throw new IllegalArgumentException("Rack aware assignment is disabled");
        }
    }
}
