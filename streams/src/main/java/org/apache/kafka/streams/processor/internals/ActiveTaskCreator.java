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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.internals.StreamsConfigUtils.eosEnabled;

class ActiveTaskCreator {
    private final TopologyMetadata topologyMetadata;
    private final StreamsConfig applicationConfig;
    private final StreamsMetricsImpl streamsMetrics;
    private final StateDirectory stateDirectory;
    private final ChangelogReader storeChangelogReader;
    private final ThreadCache cache;
    private final Time time;
    private final Logger log;
    private final Sensor createTaskSensor;
    private final boolean stateUpdaterEnabled;

    ActiveTaskCreator(final TopologyMetadata topologyMetadata,
                      final StreamsConfig applicationConfig,
                      final StreamsMetricsImpl streamsMetrics,
                      final StateDirectory stateDirectory,
                      final ChangelogReader storeChangelogReader,
                      final ThreadCache cache,
                      final Time time,
                      final String threadId,
                      final Logger log,
                      final boolean stateUpdaterEnabled) {
        this.topologyMetadata = topologyMetadata;
        this.applicationConfig = applicationConfig;
        this.streamsMetrics = streamsMetrics;
        this.stateDirectory = stateDirectory;
        this.storeChangelogReader = storeChangelogReader;
        this.cache = cache;
        this.time = time;
        this.log = log;
        this.stateUpdaterEnabled = stateUpdaterEnabled;

        createTaskSensor = ThreadMetrics.createTaskSensor(threadId, streamsMetrics);
    }

    // TODO: convert to StreamTask when we remove TaskManager#StateMachineTask with mocks
    public Collection<Task> createTasks(final Consumer<byte[], byte[]> consumer,
                                        final StreamsProducer producer,
                                        final Map<TaskId, Set<TopicPartition>> tasksToBeCreated) {
        final List<Task> createdTasks = new ArrayList<>();

        for (final Map.Entry<TaskId, Set<TopicPartition>> newTaskAndPartitions : tasksToBeCreated.entrySet()) {
            final TaskId taskId = newTaskAndPartitions.getKey();
            final LogContext logContext = getLogContext(taskId);
            final Set<TopicPartition> partitions = newTaskAndPartitions.getValue();
            final ProcessorTopology topology = topologyMetadata.buildSubtopology(taskId);

            final ProcessorStateManager stateManager = new ProcessorStateManager(
                taskId,
                Task.TaskType.ACTIVE,
                eosEnabled(applicationConfig),
                logContext,
                stateDirectory,
                storeChangelogReader,
                topology.storeToChangelogTopic(),
                partitions,
                stateUpdaterEnabled);

            final InternalProcessorContext<Object, Object> context = new ProcessorContextImpl(
                taskId,
                applicationConfig,
                stateManager,
                streamsMetrics,
                cache
            );

            createdTasks.add(
                createActiveTask(
                    taskId,
                    partitions,
                    consumer,
                    producer,
                    logContext,
                    topology,
                    stateManager,
                    context
                )
            );
        }
        return createdTasks;
    }

    private RecordCollector createRecordCollector(final TaskId taskId,
                                                  final LogContext logContext,
                                                  final ProcessorTopology topology,
                                                  final StreamsProducer producer) {
        return new RecordCollectorImpl(
            logContext,
            taskId,
            producer,
            applicationConfig.defaultProductionExceptionHandler(),
            streamsMetrics,
            topology
        );
    }

    /*
     * TODO: we pass in the new input partitions to validate if they still match,
     *       in the future we when we have fixed partitions -> tasks mapping,
     *       we should always reuse the input partition and hence no need validations
     */
    StreamTask createActiveTaskFromStandby(final StandbyTask standbyTask,
                                           final Set<TopicPartition> inputPartitions,
                                           final Consumer<byte[], byte[]> consumer,
                                           final StreamsProducer producer) {
        if (!inputPartitions.equals(standbyTask.inputPartitions)) {
            log.warn("Detected unmatched input partitions for task {} when recycling it from standby to active", standbyTask.id);
        }

        standbyTask.prepareRecycle();
        standbyTask.stateMgr.transitionTaskType(Task.TaskType.ACTIVE);

        final RecordCollector recordCollector = createRecordCollector(standbyTask.id, getLogContext(standbyTask.id), standbyTask.topology,
            producer);
        final StreamTask task = new StreamTask(
            standbyTask.id,
            inputPartitions,
            standbyTask.topology,
            consumer,
            standbyTask.config,
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            standbyTask.stateMgr,
            recordCollector,
            standbyTask.processorContext,
            standbyTask.logContext
        );

        log.trace("Created active task {} from recycled standby task with assigned partitions {}", task.id, inputPartitions);
        createTaskSensor.record();
        return task;
    }

    private StreamTask createActiveTask(final TaskId taskId,
                                        final Set<TopicPartition> inputPartitions,
                                        final Consumer<byte[], byte[]> consumer,
                                        final StreamsProducer producer,
                                        final LogContext logContext,
                                        final ProcessorTopology topology,
                                        final ProcessorStateManager stateManager,
                                        final InternalProcessorContext<Object, Object> context) {
        final RecordCollector recordCollector = createRecordCollector(taskId, logContext, topology, producer);

        final StreamTask task = new StreamTask(
            taskId,
            inputPartitions,
            topology,
            consumer,
            topologyMetadata.getTaskConfigFor(taskId),
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context,
            logContext
        );

        log.trace("Created active task {} with assigned partitions {}", taskId, inputPartitions);
        createTaskSensor.record();
        return task;
    }

    private LogContext getLogContext(final TaskId taskId) {
        final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
        final String logPrefix = threadIdPrefix + String.format("%s [%s] ", "task", taskId);
        return new LogContext(logPrefix);
    }
}
