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
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.RebalanceListenerMetrics;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultStreamsRebalanceListener implements StreamsRebalanceListener {

    private final Logger log;
    private final Time time;
    private final StreamsRebalanceData streamsRebalanceData;
    private final TaskManager taskManager;
    private final StreamThread streamThread;
    private final Sensor tasksRevokedSensor;
    private final Sensor tasksAssignedSensor;
    private final Sensor tasksLostSensor;

    public DefaultStreamsRebalanceListener(final Logger log,
                                           final Time time,
                                           final StreamsRebalanceData streamsRebalanceData,
                                           final StreamThread streamThread,
                                           final TaskManager taskManager,
                                           final StreamsMetricsImpl streamsMetrics,
                                           final String threadId) {
        this.log = log;
        this.time = time;
        this.streamsRebalanceData = streamsRebalanceData;
        this.streamThread = streamThread;
        this.taskManager = taskManager;
        
        // Create sensors for rebalance metrics
        this.tasksRevokedSensor = RebalanceListenerMetrics.tasksRevokedSensor(threadId, streamsMetrics);
        this.tasksAssignedSensor = RebalanceListenerMetrics.tasksAssignedSensor(threadId, streamsMetrics);
        this.tasksLostSensor = RebalanceListenerMetrics.tasksLostSensor(threadId, streamsMetrics);
    }

    @Override
    public void onTasksRevoked(final Set<StreamsRebalanceData.TaskId> tasks) {
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
            final long latency = time.milliseconds() - start;
            tasksRevokedSensor.record(latency);
            log.info("partition revocation took {} ms.", latency);
        }
        if (streamThread.state() != StreamThread.State.PENDING_SHUTDOWN) {
            streamThread.setState(StreamThread.State.PARTITIONS_REVOKED);
        }
        streamThread.removePartitionsFromBufferOverflowTracking(partitionsToRevoke);
    }

    @Override
    public void onTasksAssigned(final StreamsRebalanceData.Assignment assignment) {
        log.debug("Received broker assignment {}", assignment);

        final long start = time.milliseconds();
        final StreamsRebalanceData.Assignment runningAssignment = deduplicateTasks(assignment);
        final Map<TaskId, Set<TopicPartition>> activeTasksWithPartitions =
            pairWithTopicPartitions(runningAssignment.activeTasks().stream());
        final Map<TaskId, Set<TopicPartition>> standbyTasksWithPartitions =
            pairWithTopicPartitions(Stream.concat(
                runningAssignment.standbyTasks().stream(),
                runningAssignment.warmupTasks().stream()
            ));

        log.info("Processing new assignment {} from Streams Rebalance Protocol", runningAssignment);

        try {
            streamThread.setStreamsGroupReady(assignment.isGroupReady());
            taskManager.handleAssignment(activeTasksWithPartitions, standbyTasksWithPartitions);
            streamThread.setState(StreamThread.State.PARTITIONS_ASSIGNED);
            taskManager.handleRebalanceComplete();
            streamsRebalanceData.setReconciledAssignment(runningAssignment);
        } finally {
            tasksAssignedSensor.record(time.milliseconds() - start);
        }
    }

    @Override
    public void onAllTasksLost() {
        final long start = time.milliseconds();
        try {
            taskManager.handleLostAll();
            streamsRebalanceData.setReconciledAssignment(StreamsRebalanceData.Assignment.EMPTY);
        } finally {
            tasksLostSensor.record(time.milliseconds() - start);
        }
    }

    private StreamsRebalanceData.Assignment deduplicateTasks(final StreamsRebalanceData.Assignment assignment) {
        // The overlaps below are only used to warn about them, so they are kept sorted to give the log a
        // deterministic and readable task order.
        final SortedSet<StreamsRebalanceData.TaskId> standbyAndWarmup = new TreeSet<>(assignment.standbyTasks());
        standbyAndWarmup.retainAll(assignment.warmupTasks());
        if (!standbyAndWarmup.isEmpty()) {
            log.warn("Tasks {} were assigned as standby tasks and as warm-up tasks. A standby task and a warm-up " +
                "task are the same thing on the client, so the two assignments are merged into a single task each.",
                standbyAndWarmup);
        }

        final SortedSet<StreamsRebalanceData.TaskId> activeAndReplica = new TreeSet<>(assignment.standbyTasks());
        activeAndReplica.addAll(assignment.warmupTasks());
        activeAndReplica.retainAll(assignment.activeTasks());
        if (!activeAndReplica.isEmpty()) {
            log.warn("Tasks {} were assigned as active tasks and also as standby or warm-up tasks. The standby and " +
                "warm-up assignment is ignored, and the tasks are run as active tasks.", activeAndReplica);
        }

        // The client runs one task per task id, so it reports one role for it. A task assigned as both standby and
        // warm-up is reported as a standby: the standby is a permanent part of the assignment, while the warm-up is
        // only injected on top of it and goes away again.
        final Set<StreamsRebalanceData.TaskId> standbyTasks = new HashSet<>(assignment.standbyTasks());
        standbyTasks.removeAll(assignment.activeTasks());
        final Set<StreamsRebalanceData.TaskId> warmupTasks = new HashSet<>(assignment.warmupTasks());
        warmupTasks.removeAll(assignment.activeTasks());
        warmupTasks.removeAll(standbyTasks);

        return new StreamsRebalanceData.Assignment(
            assignment.activeTasks(),
            standbyTasks,
            warmupTasks,
            assignment.isGroupReady()
        );
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
