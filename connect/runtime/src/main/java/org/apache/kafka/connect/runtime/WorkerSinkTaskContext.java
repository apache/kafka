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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WorkerSinkTaskContext implements SinkTaskContext {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private Map<TopicPartition, Long> offsets;
    private long timeoutMs;
    private KafkaConsumer<byte[], byte[]> consumer;
    private final WorkerSinkTask sinkTask;
    private final ClusterConfigState configState;
    private final Set<TopicPartition> pausedPartitions;
    private boolean commitRequested;

    public WorkerSinkTaskContext(KafkaConsumer<byte[], byte[]> consumer,
                                 WorkerSinkTask sinkTask,
                                 ClusterConfigState configState) {
        this.offsets = new HashMap<>();
        this.timeoutMs = -1L;
        this.consumer = consumer;
        this.sinkTask = sinkTask;
        this.configState = configState;
        this.pausedPartitions = new HashSet<>();
    }

    @Override
    public Map<String, String> configs() {
        return configState.taskConfig(sinkTask.id());
    }

    @Override
    public void offset(Map<TopicPartition, Long> offsets) {
        log.debug("{} Setting offsets for topic partitions {}", this, offsets);
        this.offsets.putAll(offsets);
    }

    @Override
    public void offset(TopicPartition tp, long offset) {
        log.debug("{} Setting offset for topic partition {} to {}", this, tp, offset);
        offsets.put(tp, offset);
    }

    public void clearOffsets() {
        offsets.clear();
    }

    /**
     * Get offsets that the SinkTask has submitted to be reset. Used by the Kafka Connect framework.
     * @return the map of offsets
     */
    public Map<TopicPartition, Long> offsets() {
        return offsets;
    }

    @Override
    public void timeout(long timeoutMs) {
        log.debug("{} Setting timeout to {} ms", this, timeoutMs);
        this.timeoutMs = timeoutMs;
    }

    /**
     * Get the timeout in milliseconds set by SinkTasks. Used by the Kafka Connect framework.
     * @return the backoff timeout in milliseconds.
     */
    public long timeout() {
        return timeoutMs;
    }

    @Override
    public Set<TopicPartition> assignment() {
        if (consumer == null) {
            throw new IllegalWorkerStateException("SinkTaskContext may not be used to look up partition assignment until the task is initialized");
        }
        return consumer.assignment();
    }

    @Override
    public void pause(TopicPartition... partitions) {
        if (consumer == null) {
            throw new IllegalWorkerStateException("SinkTaskContext may not be used to pause consumption until the task is initialized");
        }
        try {
            Collections.addAll(pausedPartitions, partitions);
            if (sinkTask.shouldPause()) {
                log.debug("{} Connector is paused, so not pausing consumer's partitions {}", this, partitions);
            } else {
                consumer.pause(Arrays.asList(partitions));
                log.debug("{} Pausing partitions {}. Connector is not paused.", this, partitions);
            }
        } catch (IllegalStateException e) {
            throw new IllegalWorkerStateException("SinkTasks may not pause partitions that are not currently assigned to them.", e);
        }
    }

    @Override
    public void resume(TopicPartition... partitions) {
        if (consumer == null) {
            throw new IllegalWorkerStateException("SinkTaskContext may not be used to resume consumption until the task is initialized");
        }
        try {
            pausedPartitions.removeAll(Arrays.asList(partitions));
            if (sinkTask.shouldPause()) {
                log.debug("{} Connector is paused, so not resuming consumer's partitions {}", this, partitions);
            } else {
                consumer.resume(Arrays.asList(partitions));
                log.debug("{} Resuming partitions: {}", this, partitions);
            }
        } catch (IllegalStateException e) {
            throw new IllegalWorkerStateException("SinkTasks may not resume partitions that are not currently assigned to them.", e);
        }
    }

    public Set<TopicPartition> pausedPartitions() {
        return pausedPartitions;
    }

    @Override
    public void requestCommit() {
        log.debug("{} Requesting commit", this);
        commitRequested = true;
    }

    public boolean isCommitRequested() {
        return commitRequested;
    }

    public void clearCommitRequest() {
        commitRequested = false;
    }

    @Override
    public ErrantRecordReporter errantRecordReporter() {
        return sinkTask.workerErrantRecordReporter();
    }

    @Override
    public String toString() {
        return "WorkerSinkTaskContext{" +
               "id=" + sinkTask.id +
               '}';
    }
}
