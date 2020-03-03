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
import org.slf4j.Logger;

import java.util.Set;

class StandbyTaskCreator extends AbstractTaskCreator<StandbyTask> {
    private final Sensor createTaskSensor;

    StandbyTaskCreator(final InternalTopologyBuilder builder,
                       final StreamsConfig config,
                       final StreamsMetricsImpl streamsMetrics,
                       final StateDirectory stateDirectory,
                       final ChangelogReader storeChangelogReader,
                       final Time time,
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
        createTaskSensor = ThreadMetrics.createTaskSensor(threadId, streamsMetrics);
    }

    @Override
    StandbyTask createTask(final Consumer<byte[], byte[]> consumer,
                           final TaskId taskId,
                           final Set<TopicPartition> partitions) {
        createTaskSensor.record();

        final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
        final String logPrefix = threadIdPrefix + String.format("%s [%s] ", "standby-task", taskId);
        final LogContext logContext = new LogContext(logPrefix);

        final ProcessorTopology topology = builder.buildSubtopology(taskId.topicGroupId);

        if (topology.hasStateWithChangelogs()) {
            final ProcessorStateManager stateManager = new ProcessorStateManager(
                taskId,
                partitions,
                Task.TaskType.STANDBY,
                stateDirectory,
                topology.storeToChangelogTopic(),
                storeChangelogReader,
                logContext);

            return new StandbyTask(
                taskId,
                partitions,
                topology,
                config,
                streamsMetrics,
                stateManager,
                stateDirectory);
        } else {
            log.trace(
                "Skipped standby task {} with assigned partitions {} " +
                    "since it does not have any state stores to materialize",
                taskId, partitions
            );
            return null;
        }
    }
}
