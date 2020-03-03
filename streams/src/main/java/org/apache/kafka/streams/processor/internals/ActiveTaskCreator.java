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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;

class ActiveTaskCreator extends AbstractTaskCreator<Task> {
    private final String threadId;
    private final ThreadCache cache;
    private final Producer<byte[], byte[]> threadProducer;
    private final KafkaClientSupplier clientSupplier;
    final Map<TaskId, Producer<byte[], byte[]>> taskProducers;
    private final Sensor createTaskSensor;

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
        super(
            builder,
            config,
            streamsMetrics,
            stateDirectory,
            storeChangelogReader,
            time,
            log);

        final boolean eosEnabled = EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
        if (!eosEnabled) {
            final Map<String, Object> producerConfigs = config.getProducerConfigs(StreamThread.getThreadProducerClientId(threadId));
            log.info("Creating thread producer client");
            this.threadProducer = clientSupplier.getProducer(producerConfigs);
        } else {
            this.threadProducer = null;
        }

        this.taskProducers = new HashMap<>();

        this.cache = cache;
        this.threadId = threadId;
        this.clientSupplier = clientSupplier;

        this.createTaskSensor = ThreadMetrics.createTaskSensor(threadId, streamsMetrics);
    }

    @Override
    StreamTask createTask(final Consumer<byte[], byte[]> mainConsumer,
                          final TaskId taskId,
                          final Set<TopicPartition> partitions) {
        createTaskSensor.record();

        final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
        final String logPrefix = threadIdPrefix + String.format("%s [%s] ", "task", taskId);
        final LogContext logContext = new LogContext(logPrefix);

        final ProcessorTopology topology = builder.buildSubtopology(taskId.topicGroupId);

        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            partitions,
            Task.TaskType.ACTIVE,
            stateDirectory,
            topology.storeToChangelogTopic(),
            storeChangelogReader,
            logContext);

        if (threadProducer == null) {
            // create one producer per task for EOS
            // TODO: after KIP-447 this would be removed
            final Map<String, Object> producerConfigs = config.getProducerConfigs(StreamThread.getTaskProducerClientId(threadId, taskId));
            producerConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, applicationId + "-" + taskId);
            log.info("Creating producer client for task {}", taskId);
            taskProducers.put(taskId, clientSupplier.getProducer(producerConfigs));
        }
        final RecordCollector recordCollector = new RecordCollectorImpl(
            logContext,
            taskId,
            mainConsumer,
            threadProducer != null ?
                new StreamsProducer(logContext, threadProducer) :
                new StreamsProducer(logContext, taskProducers.get(taskId), applicationId, taskId),
            config.defaultProductionExceptionHandler(),
            EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)),
            streamsMetrics);

        return new StreamTask(
            taskId,
            partitions,
            topology,
            mainConsumer,
            config,
            streamsMetrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            recordCollector);
    }

    public void close() {
        if (threadProducer != null) {
            try {
                threadProducer.close();
            } catch (final Throwable e) {
                log.error("Failed to close producer due to the following error:", e);
            }
        }
    }

    public Producer<byte[], byte[]> threadProducer() {
        return threadProducer;
    }

    public Map<TaskId, Producer<byte[], byte[]>> taskProducers() {
        return taskProducers;
    }
}
