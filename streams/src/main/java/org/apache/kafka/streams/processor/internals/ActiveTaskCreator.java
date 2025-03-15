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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.kafka.streams.internals.StreamsConfigUtils.eosEnabled;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.processingMode;
import static org.apache.kafka.streams.processor.internals.ClientUtils.producerClientId;

class ActiveTaskCreator {
    private final TopologyMetadata topologyMetadata;
    private final StreamsConfig applicationConfig;
    private final StreamsMetricsImpl streamsMetrics;
    private final StateDirectory stateDirectory;
    private final ChangelogReader storeChangelogReader;
    private final ThreadCache cache;
    private final Time time;
    private final KafkaClientSupplier clientSupplier;
    private final String threadId;
    private final int threadIdx;
    private final UUID processId;
    private final Logger log;
    private final Sensor createTaskSensor;
    private final StreamsProducer streamsProducer;
    private final boolean stateUpdaterEnabled;
    private final boolean processingThreadsEnabled;

    ActiveTaskCreator(final TopologyMetadata topologyMetadata,
                      final StreamsConfig applicationConfig,
                      final StreamsMetricsImpl streamsMetrics,
                      final StateDirectory stateDirectory,
                      final ChangelogReader storeChangelogReader,
                      final ThreadCache cache,
                      final Time time,
                      final KafkaClientSupplier clientSupplier,
                      final String threadId,
                      final int threadIdx,
                      final UUID processId,
                      final Logger log,
                      final boolean stateUpdaterEnabled,
                      final boolean processingThreadsEnabled) {
        this.topologyMetadata = topologyMetadata;
        this.applicationConfig = applicationConfig;
        this.streamsMetrics = streamsMetrics;
        this.stateDirectory = stateDirectory;
        this.storeChangelogReader = storeChangelogReader;
        this.cache = cache;
        this.time = time;
        this.clientSupplier = clientSupplier;
        this.threadId = threadId;
        this.threadIdx = threadIdx;
        this.processId = processId;
        this.log = log;
        this.stateUpdaterEnabled = stateUpdaterEnabled;
        this.processingThreadsEnabled = processingThreadsEnabled;

        createTaskSensor = ThreadMetrics.createTaskSensor(threadId, streamsMetrics);

        final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
        final LogContext logContext = new LogContext(threadIdPrefix);

        streamsProducer = new StreamsProducer(
            producer(),
            processingMode(applicationConfig),
            time,
            logContext
        );
    }

    private Producer<byte[], byte[]> producer() {
        final Map<String, Object> producerConfig = applicationConfig.getProducerConfigs(producerClientId(threadId));
        if (eosEnabled(applicationConfig)) {
            producerConfig.put(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                applicationConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG) + "-" + processId + "-" + threadIdx
            );
        }
        return clientSupplier.getProducer(producerConfig);
    }

    public void reInitializeProducer() {
        streamsProducer.resetProducer(producer());
    }

    StreamsProducer streamsProducer() {
        return streamsProducer;
    }

    // TODO: convert to StreamTask when we remove TaskManager#StateMachineTask with mocks
    public Collection<Task> createTasks(final Consumer<byte[], byte[]> consumer,
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
                                                  final ProcessorTopology topology) {
        return new RecordCollectorImpl(
            logContext,
            taskId,
            streamsProducer,
            applicationConfig.productionExceptionHandler(),
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
                                           final Consumer<byte[], byte[]> consumer) {
        if (!inputPartitions.equals(standbyTask.inputPartitions)) {
            log.warn("Detected unmatched input partitions for task {} when recycling it from standby to active", standbyTask.id);
        }

        standbyTask.prepareRecycle();
        standbyTask.stateMgr.transitionTaskType(Task.TaskType.ACTIVE, getLogContext(standbyTask.id));

        final RecordCollector recordCollector = createRecordCollector(standbyTask.id, getLogContext(standbyTask.id), standbyTask.topology);
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
            standbyTask.logContext,
            processingThreadsEnabled
        );

        log.trace("Created active task {} from recycled standby task with assigned partitions {}", task.id, inputPartitions);
        createTaskSensor.record();
        return task;
    }

    private StreamTask createActiveTask(final TaskId taskId,
                                        final Set<TopicPartition> inputPartitions,
                                        final Consumer<byte[], byte[]> consumer,
                                        final LogContext logContext,
                                        final ProcessorTopology topology,
                                        final ProcessorStateManager stateManager,
                                        final InternalProcessorContext<Object, Object> context) {
        final RecordCollector recordCollector = createRecordCollector(taskId, logContext, topology);

        final StreamTask task = new StreamTask(
            taskId,
            inputPartitions,
            topology,
            consumer,
            topologyMetadata.taskConfig(taskId),
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector,
            context,
            logContext,
            processingThreadsEnabled
        );

        log.trace("Created active task {} with assigned partitions {}", taskId, inputPartitions);
        createTaskSensor.record();
        return task;
    }

    void close() {
        try {
            streamsProducer.close();
        } catch (final RuntimeException e) {
            throw new StreamsException("Thread producer encounter error trying to close.", e);
        }
    }

    Map<MetricName, Metric> producerMetrics() {
        return ClientUtils.producerMetrics(Collections.singleton(streamsProducer));
    }

    String producerClientIds() {
        return producerClientId(threadId);
    }

    private LogContext getLogContext(final TaskId taskId) {
        final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
        final String logPrefix = threadIdPrefix + String.format("%s [%s] ", "stream-task", taskId);
        return new LogContext(logPrefix);
    }

    public double totalProducerBlockedTime() {
        return streamsProducer.totalBlockedTime();
    }
}
