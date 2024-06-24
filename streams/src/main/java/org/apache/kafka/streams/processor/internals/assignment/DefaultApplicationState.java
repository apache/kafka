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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.streams.processor.assignment.TaskInfo;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor.ClientMetadata;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor;

public class DefaultApplicationState implements ApplicationState {

    private final AssignmentConfigs assignmentConfigs;
    private final Map<TaskId, TaskInfo> tasks;
    private final Map<ProcessId, ClientMetadata> clientStates;

    private final Map<Boolean, Map<ProcessId, KafkaStreamsState>> cachedKafkaStreamStates;

    public DefaultApplicationState(final AssignmentConfigs assignmentConfigs,
                                   final Map<TaskId, TaskInfo> tasks,
                                   final Map<ProcessId, ClientMetadata> clientStates) {
        this.assignmentConfigs = assignmentConfigs;
        this.tasks = unmodifiableMap(tasks);
        this.clientStates = clientStates;
        this.cachedKafkaStreamStates = new HashMap<>();
    }

    @Override
    public Map<ProcessId, KafkaStreamsState> kafkaStreamsStates(final boolean computeTaskLags) {
        if (cachedKafkaStreamStates.containsKey(computeTaskLags)) {
            return cachedKafkaStreamStates.get(computeTaskLags);
        }

        final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates = new HashMap<>();
        for (final Map.Entry<ProcessId, StreamsPartitionAssignor.ClientMetadata> clientEntry : clientStates.entrySet()) {
            final ClientMetadata metadata = clientEntry.getValue();
            final ClientState clientState = metadata.state();
            final ProcessId processId = clientEntry.getKey();
            final Map<TaskId, Long> taskLagTotals = computeTaskLags ? clientState.taskLagTotals() : null;
            final KafkaStreamsState kafkaStreamsState = new DefaultKafkaStreamsState(
                processId,
                clientState.capacity(),
                clientState.clientTags(),
                clientState.previousActiveTasks(),
                clientState.previousStandbyTasks(),
                clientState.taskIdsByPreviousConsumer(),
                Optional.ofNullable(metadata.hostInfo()),
                Optional.ofNullable(taskLagTotals),
                metadata.rackId()
            );
            kafkaStreamsStates.put(processId, kafkaStreamsState);
        }

        cachedKafkaStreamStates.put(computeTaskLags, kafkaStreamsStates);
        return kafkaStreamsStates;
    }

    @Override
    public AssignmentConfigs assignmentConfigs() {
        return assignmentConfigs;
    }

    @Override
    public Map<TaskId, TaskInfo> allTasks() {
        return tasks;
    }
}
