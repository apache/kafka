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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;

class ActiveTaskCreator {
    private final InternalTopologyBuilder builder;
    private final StreamsConfig config;
    private final StreamsMetricsImpl streamsMetrics;
    private final StateDirectory stateDirectory;
    private final ChangelogReader storeChangelogReader;
    private final ThreadCache cache;
    private final Time time;
    private final KafkaClientSupplier clientSupplier;
    private final String threadId;
    private final Logger log;
    private final Sensor createTaskSensor;
    private final String applicationId;
    private final Producer<byte[], byte[]> threadProducer;
    private final Map<TaskId, StreamsProducer> taskProducers;

    private static String getThreadProducerClientId(final String threadClientId) {
        return threadClientId + "-producer";
    }

    private static String getTaskProducerClientId(final String threadClientId, final TaskId taskId) {
        return threadClientId + "-" + taskId + "-producer";
    }

    ActiveTaskCreator(final InternalTopologyBuilder builder,
                      final StreamsConfig config,
                      final StreamsMetricsImpl streamsMetrics,
                      final StateDirectory stateDirectory,
                      final ChangelogReader storeChangelogReader,
                      final ThreadCache cache,
                      final Time time,
                      final KafkaClientSupplier clientSupplier,
                      final String threadId,
                      final Logger log) {
        this.builder = builder;
        this.config = config;
        this.streamsMetrics = streamsMetrics;
        this.stateDirectory = stateDirectory;
        this.storeChangelogReader = storeChangelogReader;
        this.cache = cache;
        this.time = time;
        this.clientSupplier = clientSupplier;
        this.threadId = threadId;
        this.log = log;

        createTaskSensor = ThreadMetrics.createTaskSensor(threadId, streamsMetrics);
        applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);

        if (EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))) {
            threadProducer = null;
            taskProducers = new HashMap<>();
        } else {
            log.info("Creating thread producer client");

            final String threadProducerClientId = getThreadProducerClientId(threadId);
            final Map<String, Object> producerConfigs = config.getProducerConfigs(threadProducerClientId);

            threadProducer = clientSupplier.getProducer(producerConfigs);
            taskProducers = Collections.emptyMap();
        }
    }

    StreamsProducer streamsProducerForTask(final TaskId taskId) {
        if (threadProducer != null) {
            throw new IllegalStateException("Producer per thread is used");
        }

        final StreamsProducer taskProducer = taskProducers.get(taskId);
        if (taskProducer == null) {
            throw new IllegalStateException("Unknown TaskId: " + taskId);
        }
        return taskProducer;
    }

    Collection<Task> createTasks(final Consumer<byte[], byte[]> consumer,
                                 final Map<TaskId, Set<TopicPartition>> tasksToBeCreated) {
        final List<Task> createdTasks = new ArrayList<>();
        for (final Map.Entry<TaskId, Set<TopicPartition>> newTaskAndPartitions : tasksToBeCreated.entrySet()) {
            final TaskId taskId = newTaskAndPartitions.getKey();
            final Set<TopicPartition> partitions = newTaskAndPartitions.getValue();

            final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
            final String logPrefix = threadIdPrefix + String.format("%s [%s] ", "task", taskId);
            final LogContext logContext = new LogContext(logPrefix);

            final ProcessorTopology topology = builder.buildSubtopology(taskId.topicGroupId);

            final ProcessorStateManager stateManager = new ProcessorStateManager(
                taskId,
                Task.TaskType.ACTIVE,
                EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)),
                logContext,
                stateDirectory,
                storeChangelogReader,
                topology.storeToChangelogTopic(),
                partitions
            );

            final StreamsProducer streamsProducer;
            if (threadProducer == null) {
                final String taskProducerClientId = getTaskProducerClientId(threadId, taskId);
                final Map<String, Object> producerConfigs = config.getProducerConfigs(taskProducerClientId);
                producerConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, applicationId + "-" + taskId);
                log.info("Creating producer client for task {}", taskId);
                streamsProducer = new StreamsProducer(
                    clientSupplier.getProducer(producerConfigs),
                    true,
                    applicationId,
                    logContext);
                taskProducers.put(taskId, streamsProducer);
            } else {
                streamsProducer = new StreamsProducer(threadProducer, false, null, logContext);
            }

            final RecordCollector recordCollector = new RecordCollectorImpl(
                logContext,
                taskId,
                streamsProducer,
                config.defaultProductionExceptionHandler(),
                streamsMetrics
            );

            final Task task = new StreamTask(
                taskId,
                partitions,
                topology,
                consumer,
                config,
                streamsMetrics,
                stateDirectory,
                cache,
                time,
                stateManager,
                recordCollector
            );

            log.trace("Created task {} with assigned partitions {}", taskId, partitions);
            createdTasks.add(task);
            createTaskSensor.record();
        }
        return createdTasks;
    }

    void closeThreadProducerIfNeeded() {
        if (threadProducer != null) {
            try {
                threadProducer.close();
            } catch (final RuntimeException e) {
                throw new StreamsException("Thread Producer encounter error trying to close", e);
            }
        }
    }

    void closeAndRemoveTaskProducerIfNeeded(final TaskId id) {
        final StreamsProducer taskProducer = taskProducers.remove(id);
        if (taskProducer != null) {
            try {
                taskProducer.kafkaProducer().close();
            } catch (final RuntimeException e) {
                throw new StreamsException("[" + id + "] Producer encounter error trying to close", e);
            }
        }
    }

    Map<MetricName, Metric> producerMetrics() {
        final Map<MetricName, Metric> result = new LinkedHashMap<>();
        if (threadProducer != null) {
            final Map<MetricName, ? extends Metric> producerMetrics = threadProducer.metrics();
            if (producerMetrics != null) {
                result.putAll(producerMetrics);
            }
        } else {
            // When EOS is turned on, each task will have its own producer client
            // and the producer object passed in here will be null. We would then iterate through
            // all the active tasks and add their metrics to the output metrics map.
            for (final Map.Entry<TaskId, StreamsProducer> entry : taskProducers.entrySet()) {
                final Map<MetricName, ? extends Metric> taskProducerMetrics = entry.getValue().kafkaProducer().metrics();
                result.putAll(taskProducerMetrics);
            }
        }
        return result;
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
}
