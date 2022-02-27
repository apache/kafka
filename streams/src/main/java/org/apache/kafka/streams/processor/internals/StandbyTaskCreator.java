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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.filterMap;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.eosEnabled;

class StandbyTaskCreator {
    private final TopologyMetadata topologyMetadata;
    private final StreamsConfig applicationConfig;
    private final StreamsMetricsImpl streamsMetrics;
    private final StateDirectory stateDirectory;
    private final ChangelogReader storeChangelogReader;
    private final ThreadCache dummyCache;
    private final Logger log;
    private final Sensor createTaskSensor;

    // tasks may be assigned for a NamedTopology that is not yet known by this host, and saved for later creation
    private final Map<TaskId, Set<TopicPartition>> unknownTasksToBeCreated = new HashMap<>();

    StandbyTaskCreator(final TopologyMetadata topologyMetadata,
                       final StreamsConfig applicationConfig,
                       final StreamsMetricsImpl streamsMetrics,
                       final StateDirectory stateDirectory,
                       final ChangelogReader storeChangelogReader,
                       final String threadId,
                       final Logger log) {
        this.topologyMetadata = topologyMetadata;
        this.applicationConfig = applicationConfig;
        this.streamsMetrics = streamsMetrics;
        this.stateDirectory = stateDirectory;
        this.storeChangelogReader = storeChangelogReader;
        this.log = log;

        createTaskSensor = ThreadMetrics.createTaskSensor(threadId, streamsMetrics);

        dummyCache = new ThreadCache(
            new LogContext(String.format("stream-thread [%s] ", Thread.currentThread().getName())),
            0,
            streamsMetrics
        );
    }

    void removeRevokedUnknownTasks(final Set<TaskId> assignedTasks) {
        unknownTasksToBeCreated.keySet().retainAll(assignedTasks);
    }

    Map<TaskId, Set<TopicPartition>> uncreatedTasksForTopologies(final Set<String> currentTopologies) {
        return filterMap(unknownTasksToBeCreated, t -> currentTopologies.contains(t.getKey().topologyName()));
    }

    // TODO: change return type to `StandbyTask`
    Collection<Task> createTasks(final Map<TaskId, Set<TopicPartition>> tasksToBeCreated) {
        // TODO: change type to `StandbyTask`
        final List<Task> createdTasks = new ArrayList<>();
        final Map<TaskId, Set<TopicPartition>>  newUnknownTasks = new HashMap<>();

        for (final Map.Entry<TaskId, Set<TopicPartition>> newTaskAndPartitions : tasksToBeCreated.entrySet()) {
            final TaskId taskId = newTaskAndPartitions.getKey();
            final Set<TopicPartition> partitions = newTaskAndPartitions.getValue();

            // task belongs to a named topology that hasn't been added yet, wait until it has to create this
            if (taskId.topologyName() != null && !topologyMetadata.namedTopologiesView().contains(taskId.topologyName())) {
                newUnknownTasks.put(taskId, partitions);
                continue;
            }

            final ProcessorTopology topology = topologyMetadata.buildSubtopology(taskId);

            if (topology.hasStateWithChangelogs()) {
                final ProcessorStateManager stateManager = new ProcessorStateManager(
                    taskId,
                    Task.TaskType.STANDBY,
                    eosEnabled(applicationConfig),
                    getLogContext(taskId),
                    stateDirectory,
                    storeChangelogReader,
                    topology.storeToChangelogTopic(),
                    partitions
                );

                final InternalProcessorContext context = new ProcessorContextImpl(
                    taskId,
                    applicationConfig,
                    stateManager,
                    streamsMetrics,
                    dummyCache
                );

                createdTasks.add(createStandbyTask(taskId, partitions, topology, stateManager, context));
            } else {
                log.trace(
                    "Skipped standby task {} with assigned partitions {} " +
                        "since it does not have any state stores to materialize",
                    taskId, partitions
                );
            }
            unknownTasksToBeCreated.remove(taskId);
        }
        if (!newUnknownTasks.isEmpty()) {
            log.info("Delaying creation of tasks not yet known by this instance: {}", newUnknownTasks.keySet());
            unknownTasksToBeCreated.putAll(newUnknownTasks);
        }
        return createdTasks;
    }

    StandbyTask createStandbyTaskFromActive(final StreamTask streamTask,
                                            final Set<TopicPartition> inputPartitions) {
        final InternalProcessorContext context = streamTask.processorContext();
        final ProcessorStateManager stateManager = streamTask.stateMgr;

        streamTask.closeCleanAndRecycleState();
        stateManager.transitionTaskType(TaskType.STANDBY, getLogContext(streamTask.id()));

        return createStandbyTask(
            streamTask.id(),
            inputPartitions,
            topologyMetadata.buildSubtopology(streamTask.id),
            stateManager,
            context
        );
    }

    StandbyTask createStandbyTask(final TaskId taskId,
                                  final Set<TopicPartition> inputPartitions,
                                  final ProcessorTopology topology,
                                  final ProcessorStateManager stateManager,
                                  final InternalProcessorContext context) {
        final StandbyTask task = new StandbyTask(
            taskId,
            inputPartitions,
            topology,
            topologyMetadata.getTaskConfigFor(taskId),
            streamsMetrics,
            stateManager,
            stateDirectory,
            dummyCache,
            context
        );

        log.trace("Created task {} with assigned partitions {}", taskId, inputPartitions);
        createTaskSensor.record();
        return task;
    }

    private LogContext getLogContext(final TaskId taskId) {
        final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
        final String logPrefix = threadIdPrefix + String.format("%s [%s] ", "standby-task", taskId);
        return new LogContext(logPrefix);
    }
}
