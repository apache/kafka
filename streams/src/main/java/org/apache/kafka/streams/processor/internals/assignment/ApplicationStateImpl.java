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

import static java.util.Collections.unmodifiableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor.ClientMetadata;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor;

public class ApplicationStateImpl implements ApplicationState {

    private final AssignmentConfigs assignmentConfigs;
    private final Set<TaskId> statelessTasks;
    private final Set<TaskId> statefulTasks;
    private final Set<TaskId> allTasks;
    private final Map<UUID, ClientMetadata> clientStates;

    public ApplicationStateImpl(final AssignmentConfigs assignmentConfigs,
                                final Set<TaskId> statefulTasks,
                                final Set<TaskId> statelessTasks,
                                final Map<UUID, ClientMetadata> clientStates) {
        this.assignmentConfigs = assignmentConfigs;
        this.statefulTasks = unmodifiableSet(statefulTasks);
        this.statelessTasks = unmodifiableSet(statelessTasks);
        this.allTasks = unmodifiableSet(computeAllTasks(statelessTasks, statefulTasks));
        this.clientStates = clientStates;
    }

    @Override
    public Map<ProcessId, KafkaStreamsState> kafkaStreamsStates(final boolean computeTaskLags) {
        final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates = new HashMap<>();
        for (final Map.Entry<UUID, StreamsPartitionAssignor.ClientMetadata> clientEntry : clientStates.entrySet()) {
            final ClientMetadata metadata = clientEntry.getValue();
            final ClientState clientState = metadata.state();
            final ProcessId processId = new ProcessId(clientEntry.getKey());
            final Map<TaskId, Long> taskLagTotals = computeTaskLags ? clientState.taskLagTotals() : null;
            final KafkaStreamsState kafkaStreamsState = new KafkaStreamsStateImpl(
                processId,
                clientState.capacity(),
                clientState.clientTags(),
                clientState.previousActiveTasks(),
                clientState.previousStandbyTasks(),
                clientState.taskIdsByPreviousConsumer(),
                Optional.ofNullable(metadata.hostInfo()),
                Optional.ofNullable(taskLagTotals)
            );
            kafkaStreamsStates.put(processId, kafkaStreamsState);
        }

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

    private static Set<TaskId> computeAllTasks(final Set<TaskId> statelessTasks, final Set<TaskId> statefulTasks) {
        final Set<TaskId> union = new HashSet<>(statefulTasks);
        union.addAll(statelessTasks);
        return union;
    }
}
