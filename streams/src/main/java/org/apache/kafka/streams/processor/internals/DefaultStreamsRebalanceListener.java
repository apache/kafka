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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.internals.StreamsRebalanceData;
import org.apache.kafka.clients.consumer.internals.StreamsRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.TaskId;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultStreamsRebalanceListener implements StreamsRebalanceListener {

    private final Logger log;
    private final Time time;
    private final StreamsRebalanceData streamsRebalanceData;
    private final TaskManager taskManager;
    private final StreamThread streamThread;

    public DefaultStreamsRebalanceListener(final Logger log,
                                           final Time time,
                                           final StreamsRebalanceData streamsRebalanceData,
                                           final StreamThread streamThread,
                                           final TaskManager taskManager) {
        this.log = log;
        this.time = time;
        this.streamsRebalanceData = streamsRebalanceData;
        this.streamThread = streamThread;
        this.taskManager = taskManager;
    }

    @Override
    public Optional<Exception> onTasksRevoked(final Set<StreamsRebalanceData.TaskId> tasks) {
        try {
            final Map<TaskId, Set<TopicPartition>> activeTasksToRevokeWithPartitions =
                pairWithTopicPartitions(tasks.stream());
            final Set<TopicPartition> partitionsToRevoke = activeTasksToRevokeWithPartitions.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

            final long start = time.milliseconds();
            try {
                log.info("Revoking active tasks {}.", tasks);
                taskManager.handleRevocation(partitionsToRevoke);
            } finally {
                log.info("partition revocation took {} ms.", time.milliseconds() - start);
            }
            if (streamThread.state() != StreamThread.State.PENDING_SHUTDOWN) {
                streamThread.setState(StreamThread.State.PARTITIONS_REVOKED);
            }
        } catch (final Exception exception) {
            return Optional.of(exception);
        }
        return Optional.empty();
    }

    @Override
    public Optional<Exception> onTasksAssigned(final StreamsRebalanceData.Assignment assignment) {
        try {
            final Map<TaskId, Set<TopicPartition>> activeTasksWithPartitions =
                pairWithTopicPartitions(assignment.activeTasks().stream());
            final Map<TaskId, Set<TopicPartition>> standbyTasksWithPartitions =
                pairWithTopicPartitions(Stream.concat(assignment.standbyTasks().stream(), assignment.warmupTasks().stream()));

            log.info("Processing new assignment {} from Streams Rebalance Protocol", assignment);

            taskManager.handleAssignment(activeTasksWithPartitions, standbyTasksWithPartitions);
            streamThread.setState(StreamThread.State.PARTITIONS_ASSIGNED);
            taskManager.handleRebalanceComplete();
            streamsRebalanceData.setReconciledAssignment(assignment);
        } catch (final Exception exception) {
            return Optional.of(exception);
        }
        return Optional.empty();
    }

    @Override
    public Optional<Exception> onAllTasksLost() {
        try {
            taskManager.handleLostAll();
            streamsRebalanceData.setReconciledAssignment(StreamsRebalanceData.Assignment.EMPTY);
        } catch (final Exception exception) {
            return Optional.of(exception);
        }
        return Optional.empty();
    }

    private Map<TaskId, Set<TopicPartition>> pairWithTopicPartitions(final Stream<StreamsRebalanceData.TaskId> taskIdStream) {
        return taskIdStream
            .collect(Collectors.toMap(
                this::toTaskId,
                task -> toTopicPartitions(task, streamsRebalanceData.subtopologies().get(task.subtopologyId()))
            ));
    }

    private TaskId toTaskId(final StreamsRebalanceData.TaskId task) {
        return new TaskId(Integer.parseInt(task.subtopologyId()), task.partitionId());
    }

    private Set<TopicPartition> toTopicPartitions(final StreamsRebalanceData.TaskId task,
                                                  final StreamsRebalanceData.Subtopology subTopology) {
        return
            Stream.concat(
                    subTopology.sourceTopics().stream(),
                    subTopology.repartitionSourceTopics().keySet().stream()
                )
                .map(t -> new TopicPartition(t, task.partitionId()))
                .collect(Collectors.toSet());
    }
}
