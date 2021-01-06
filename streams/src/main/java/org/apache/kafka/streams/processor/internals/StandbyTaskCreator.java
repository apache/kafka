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
import java.util.List;
import java.util.Map;
import java.util.Set;

class StandbyTaskCreator {
    private final InternalTopologyBuilder builder;
    private final StreamsConfig config;
    private final StreamsMetricsImpl streamsMetrics;
    private final StateDirectory stateDirectory;
    private final ChangelogReader storeChangelogReader;
    private final ThreadCache dummyCache;
    private final Logger log;
    private final Sensor createTaskSensor;

    StandbyTaskCreator(final InternalTopologyBuilder builder,
                       final StreamsConfig config,
                       final StreamsMetricsImpl streamsMetrics,
                       final StateDirectory stateDirectory,
                       final ChangelogReader storeChangelogReader,
                       final String threadId,
                       final Logger log) {
        this.builder = builder;
        this.config = config;
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

    // TODO: change return type to `StandbyTask`
    Collection<Task> createTasks(final Map<TaskId, Set<TopicPartition>> tasksToBeCreated) {
        // TODO: change type to `StandbyTask`
        final List<Task> createdTasks = new ArrayList<>();
        for (final Map.Entry<TaskId, Set<TopicPartition>> newTaskAndPartitions : tasksToBeCreated.entrySet()) {
            final TaskId taskId = newTaskAndPartitions.getKey();
            final Set<TopicPartition> partitions = newTaskAndPartitions.getValue();

            final ProcessorTopology topology = builder.buildSubtopology(taskId.topicGroupId);

            if (topology.hasStateWithChangelogs()) {
                final ProcessorStateManager stateManager = new ProcessorStateManager(
                    taskId,
                    Task.TaskType.STANDBY,
                    StreamThread.eosEnabled(config),
                    getLogContext(taskId),
                    stateDirectory,
                    storeChangelogReader,
                    topology.storeToChangelogTopic(),
                    partitions
                );

                final InternalProcessorContext context = new ProcessorContextImpl(
                    taskId,
                    config,
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
            builder.buildSubtopology(streamTask.id.topicGroupId),
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
            config,
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

    public InternalTopologyBuilder builder() {
        return builder;
    }

    public StateDirectory stateDirectory() {
        return stateDirectory;
    }

    private LogContext getLogContext(final TaskId taskId) {
        final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
        final String logPrefix = threadIdPrefix + String.format("%s [%s] ", "standby-task", taskId);
        return new LogContext(logPrefix);
    }
}
