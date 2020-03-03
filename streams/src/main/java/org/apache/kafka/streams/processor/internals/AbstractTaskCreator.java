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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

abstract class AbstractTaskCreator<T extends Task> {
    final String applicationId;
    final InternalTopologyBuilder builder;
    final StreamsConfig config;
    final StreamsMetricsImpl streamsMetrics;
    final StateDirectory stateDirectory;
    final ChangelogReader storeChangelogReader;
    final Time time;
    final Logger log;

    AbstractTaskCreator(final InternalTopologyBuilder builder,
                        final StreamsConfig config,
                        final StreamsMetricsImpl streamsMetrics,
                        final StateDirectory stateDirectory,
                        final ChangelogReader storeChangelogReader,
                        final Time time,
                        final Logger log) {
        this.applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        this.builder = builder;
        this.config = config;
        this.streamsMetrics = streamsMetrics;
        this.stateDirectory = stateDirectory;
        this.storeChangelogReader = storeChangelogReader;
        this.time = time;
        this.log = log;
    }

    public InternalTopologyBuilder builder() {
        return builder;
    }

    public StateDirectory stateDirectory() {
        return stateDirectory;
    }

    Collection<T> createTasks(final Consumer<byte[], byte[]> consumer,
                              final Map<TaskId, Set<TopicPartition>> tasksToBeCreated) {
        final List<T> createdTasks = new ArrayList<>();
        for (final Map.Entry<TaskId, Set<TopicPartition>> newTaskAndPartitions : tasksToBeCreated.entrySet()) {
            final TaskId taskId = newTaskAndPartitions.getKey();
            final Set<TopicPartition> partitions = newTaskAndPartitions.getValue();
            final T task = createTask(consumer, taskId, partitions);
            if (task != null) {
                log.trace("Created task {} with assigned partitions {}", taskId, partitions);
                createdTasks.add(task);
            }

        }
        return createdTasks;
    }

    abstract T createTask(final Consumer<byte[], byte[]> consumer, final TaskId id, final Set<TopicPartition> partitions);
}
