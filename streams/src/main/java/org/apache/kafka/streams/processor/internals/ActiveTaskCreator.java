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
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.filterMap;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_ALPHA;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_V2;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.eosEnabled;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.processingMode;
import static org.apache.kafka.streams.processor.internals.ClientUtils.getTaskProducerClientId;
import static org.apache.kafka.streams.processor.internals.ClientUtils.getThreadProducerClientId;

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
    private final Logger log;
    private final Sensor createTaskSensor;
    private final StreamsProducer threadProducer;
    private final Map<TaskId, StreamsProducer> taskProducers;
    private final ProcessingMode processingMode;

    // Tasks may have been assigned for a NamedTopology that is not yet known by this host. When that occurs we stash
    // these unknown tasks until either the corresponding NamedTopology is added and we can create them at last, or
    // we receive a new assignment and they are revoked from the thread.
    private final Map<TaskId, Set<TopicPartition>> unknownTasksToBeCreated = new HashMap<>();

    ActiveTaskCreator(final TopologyMetadata topologyMetadata,
                      final StreamsConfig applicationConfig,
                      final StreamsMetricsImpl streamsMetrics,
                      final StateDirectory stateDirectory,
                      final ChangelogReader storeChangelogReader,
                      final ThreadCache cache,
                      final Time time,
                      final KafkaClientSupplier clientSupplier,
                      final String threadId,
                      final UUID processId,
                      final Logger log) {
        this.topologyMetadata = topologyMetadata;
        this.applicationConfig = applicationConfig;
        this.streamsMetrics = streamsMetrics;
        this.stateDirectory = stateDirectory;
        this.storeChangelogReader = storeChangelogReader;
        this.cache = cache;
        this.time = time;
        this.clientSupplier = clientSupplier;
        this.threadId = threadId;
        this.log = log;

        createTaskSensor = ThreadMetrics.createTaskSensor(threadId, streamsMetrics);
        processingMode = processingMode(applicationConfig);

        if (processingMode == EXACTLY_ONCE_ALPHA) {
            threadProducer = null;
            taskProducers = new HashMap<>();
        } else { // non-eos and eos-v2
            log.info("Creating thread producer client");

            final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
            final LogContext logContext = new LogContext(threadIdPrefix);

            threadProducer = new StreamsProducer(
                applicationConfig,
                threadId,
                clientSupplier,
                null,
                processId,
                logContext,
                time);
            taskProducers = Collections.emptyMap();
        }
    }

    public void reInitializeThreadProducer() {
        threadProducer.resetProducer();
    }

    StreamsProducer streamsProducerForTask(final TaskId taskId) {
        if (processingMode != EXACTLY_ONCE_ALPHA) {
            throw new IllegalStateException("Expected EXACTLY_ONCE to be enabled, but the processing mode was " + processingMode);
        }

        final StreamsProducer taskProducer = taskProducers.get(taskId);
        if (taskProducer == null) {
            throw new IllegalStateException("Unknown TaskId: " + taskId);
        }
        return taskProducer;
    }

    StreamsProducer threadProducer() {
        if (processingMode != EXACTLY_ONCE_V2) {
            throw new IllegalStateException("Expected EXACTLY_ONCE_V2 to be enabled, but the processing mode was " + processingMode);
        }
        return threadProducer;
    }

    void removeRevokedUnknownTasks(final Set<TaskId> assignedTasks) {
        unknownTasksToBeCreated.keySet().retainAll(assignedTasks);
    }

    Map<TaskId, Set<TopicPartition>> uncreatedTasksForTopologies(final Set<String> currentTopologies) {
        return filterMap(unknownTasksToBeCreated, t -> currentTopologies.contains(t.getKey().topologyName()));
    }

    // TODO: change return type to `StreamTask`
    Collection<Task> createTasks(final Consumer<byte[], byte[]> consumer,
                                 final Map<TaskId, Set<TopicPartition>> tasksToBeCreated) {
        // TODO: change type to `StreamTask`
        final List<Task> createdTasks = new ArrayList<>();
        final Map<TaskId, Set<TopicPartition>> newUnknownTasks = new HashMap<>();

        for (final Map.Entry<TaskId, Set<TopicPartition>> newTaskAndPartitions : tasksToBeCreated.entrySet()) {
            final TaskId taskId = newTaskAndPartitions.getKey();
            final Set<TopicPartition> partitions = newTaskAndPartitions.getValue();

            final LogContext logContext = getLogContext(taskId);

            // task belongs to a named topology that hasn't been added yet, wait until it has to create this
            if (taskId.topologyName() != null && !topologyMetadata.namedTopologiesView().contains(taskId.topologyName())) {
                newUnknownTasks.put(taskId, partitions);
                continue;
            }

            final ProcessorTopology topology = topologyMetadata.buildSubtopology(taskId);

            final ProcessorStateManager stateManager = new ProcessorStateManager(
                taskId,
                Task.TaskType.ACTIVE,
                eosEnabled(applicationConfig),
                logContext,
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
            unknownTasksToBeCreated.remove(taskId);
        }
        if (!newUnknownTasks.isEmpty()) {
            log.info("Delaying creation of tasks not yet known by this instance: {}", newUnknownTasks.keySet());
            unknownTasksToBeCreated.putAll(newUnknownTasks);
        }
        return createdTasks;
    }


    StreamTask createActiveTaskFromStandby(final StandbyTask standbyTask,
                                           final Set<TopicPartition> inputPartitions,
                                           final Consumer<byte[], byte[]> consumer) {
        final InternalProcessorContext context = standbyTask.processorContext();
        final ProcessorStateManager stateManager = standbyTask.stateMgr;
        final LogContext logContext = getLogContext(standbyTask.id);

        standbyTask.closeCleanAndRecycleState();
        stateManager.transitionTaskType(TaskType.ACTIVE, logContext);

        return createActiveTask(
            standbyTask.id,
            inputPartitions,
            consumer,
            logContext,
            topologyMetadata.buildSubtopology(standbyTask.id),
            stateManager,
            context
        );
    }

    private StreamTask createActiveTask(final TaskId taskId,
                                        final Set<TopicPartition> inputPartitions,
                                        final Consumer<byte[], byte[]> consumer,
                                        final LogContext logContext,
                                        final ProcessorTopology topology,
                                        final ProcessorStateManager stateManager,
                                        final InternalProcessorContext context) {
        final StreamsProducer streamsProducer;
        if (processingMode == ProcessingMode.EXACTLY_ONCE_ALPHA) {
            log.info("Creating producer client for task {}", taskId);
            streamsProducer = new StreamsProducer(
                applicationConfig,
                threadId,
                clientSupplier,
                taskId,
                null,
                logContext,
                time);
            taskProducers.put(taskId, streamsProducer);
        } else {
            streamsProducer = threadProducer;
        }

        final RecordCollector recordCollector = new RecordCollectorImpl(
            logContext,
            taskId,
            streamsProducer,
            applicationConfig.defaultProductionExceptionHandler(),
            streamsMetrics
        );

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

        log.trace("Created task {} with assigned partitions {}", taskId, inputPartitions);
        createTaskSensor.record();
        return task;
    }

    void closeThreadProducerIfNeeded() {
        if (threadProducer != null) {
            try {
                threadProducer.close();
            } catch (final RuntimeException e) {
                throw new StreamsException("Thread producer encounter error trying to close.", e);
            }
        }
    }

    void closeAndRemoveTaskProducerIfNeeded(final TaskId id) {
        final StreamsProducer taskProducer = taskProducers.remove(id);
        if (taskProducer != null) {
            try {
                taskProducer.close();
            } catch (final RuntimeException e) {
                throw new StreamsException("[" + id + "] task producer encounter error trying to close.", e, id);
            }
        }
    }

    Map<MetricName, Metric> producerMetrics() {
        // When EOS is turned on, each task will have its own producer client
        // and the producer object passed in here will be null. We would then iterate through
        // all the active tasks and add their metrics to the output metrics map.
        final Collection<StreamsProducer> producers = threadProducer != null ?
            Collections.singleton(threadProducer) :
            taskProducers.values();
        return ClientUtils.producerMetrics(producers);
    }

    Set<String> producerClientIds() {
        if (threadProducer != null) {
            return Collections.singleton(getThreadProducerClientId(threadId));
        } else {
            return taskProducers.keySet()
                                .stream()
                                .map(taskId -> getTaskProducerClientId(threadId, taskId))
                                .collect(Collectors.toSet());
        }
    }

    private LogContext getLogContext(final TaskId taskId) {
        final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
        final String logPrefix = threadIdPrefix + String.format("%s [%s] ", "task", taskId);
        return new LogContext(logPrefix);
    }

    public double totalProducerBlockedTime() {
        if (threadProducer != null) {
            return threadProducer.totalBlockedTime();
        }
        return taskProducers.values().stream()
            .mapToDouble(StreamsProducer::totalBlockedTime)
            .sum();
    }
}
