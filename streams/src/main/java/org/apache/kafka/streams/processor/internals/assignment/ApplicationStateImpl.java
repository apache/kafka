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

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;

public class ApplicationStateImpl implements ApplicationState {

    private final AssignmentConfigs assignmentConfigs;
    private final Set<TaskId> statelessTasks;
    private final Set<TaskId> statefulTasks;
    private final Set<TaskId> allTasks;
    private final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates;

    public ApplicationStateImpl(final AssignmentConfigs assignmentConfigs,
                                final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates,
                                final Set<TaskId> statefulTasks,
                                final Set<TaskId> statelessTasks) {
        this.assignmentConfigs = assignmentConfigs;
        this.kafkaStreamsStates = unmodifiableMap(kafkaStreamsStates);
        this.statefulTasks = unmodifiableSet(statefulTasks);
        this.statelessTasks = unmodifiableSet(statelessTasks);
        this.allTasks = unmodifiableSet(computeAllTasks(statelessTasks, statefulTasks));
    }

    @Override
    public Map<ProcessId, KafkaStreamsState> kafkaStreamsStates(final boolean computeTaskLags) {
        return kafkaStreamsStates;
    }

    @Override
    public AssignmentConfigs assignmentConfigs() {
        return assignmentConfigs;
    }

    @Override
    public Set<TaskId> allTasks() {
        return allTasks;
    }

    @Override
    public Set<TaskId> statefulTasks() {
        return statefulTasks;
    }

    @Override
    public Set<TaskId> statelessTasks() {
        return statelessTasks;
    }

    private static Set<TaskId> computeAllTasks(Set<TaskId> statelessTasks, Set<TaskId> statefulTasks) {
        final Set<TaskId> union = new HashSet<>(statefulTasks);
        union.addAll(statelessTasks);
        return union;
    }
}
