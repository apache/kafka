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
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;

class StandbyTaskCreator {
    private final InternalTopologyBuilder builder;
    private final StreamsConfig config;
    private final StreamsMetricsImpl streamsMetrics;
    private final StateDirectory stateDirectory;
    private final ChangelogReader storeChangelogReader;
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
    }

    Collection<Task> createTasks(final Map<TaskId, Set<TopicPartition>> tasksToBeCreated) {
        final List<Task> createdTasks = new ArrayList<>();
        for (final Map.Entry<TaskId, Set<TopicPartition>> newTaskAndPartitions : tasksToBeCreated.entrySet()) {
            final TaskId taskId = newTaskAndPartitions.getKey();
            final Set<TopicPartition> partitions = newTaskAndPartitions.getValue();

            final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
            final String logPrefix = threadIdPrefix + String.format("%s [%s] ", "standby-task", taskId);
            final LogContext logContext = new LogContext(logPrefix);

            final ProcessorTopology topology = builder.buildSubtopology(taskId.topicGroupId);

            if (topology.hasStateWithChangelogs()) {
                final ProcessorStateManager stateManager = new ProcessorStateManager(
                    taskId,
                    Task.TaskType.STANDBY,
                    EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)),
                    logContext,
                    stateDirectory,
                    storeChangelogReader,
                    topology.storeToChangelogTopic(),
                    partitions
                );

                final StandbyTask task = new StandbyTask(
                    taskId,
                    partitions,
                    topology,
                    config,
                    streamsMetrics,
                    stateManager,
                    stateDirectory
                );

                log.trace("Created task {} with assigned partitions {}", taskId, partitions);
                createdTasks.add(task);
                createTaskSensor.record();
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

    public InternalTopologyBuilder builder() {
        return builder;
    }

    public StateDirectory stateDirectory() {
        return stateDirectory;
    }
}
