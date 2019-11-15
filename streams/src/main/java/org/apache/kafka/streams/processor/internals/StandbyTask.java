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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A StandbyTask
 */
public class StandbyTask extends AbstractTask {
    private boolean updateOffsetLimits;
    private final Sensor closeTaskSensor;
    private final Map<TopicPartition, Long> offsetLimits = new HashMap<>();

    /**
     * Create {@link StandbyTask} with its assigned partitions
     *
     * @param id             the ID of this task
     * @param partitions     the collection of assigned {@link TopicPartition}
     * @param topology       the instance of {@link ProcessorTopology}
     * @param consumer       the instance of {@link Consumer}
     * @param config         the {@link StreamsConfig} specified by the user
     * @param metrics        the {@link StreamsMetrics} created by the thread
     * @param stateDirectory the {@link StateDirectory} created by the thread
     */
    StandbyTask(final TaskId id,
                final Set<TopicPartition> partitions,
                final ProcessorTopology topology,
                final Consumer<byte[], byte[]> consumer,
                final ChangelogReader changelogReader,
                final StreamsConfig config,
                final StreamsMetricsImpl metrics,
                final StateDirectory stateDirectory) {
        super(id, partitions, topology, consumer, changelogReader, true, stateDirectory, config);

        closeTaskSensor = ThreadMetrics.closeTaskSensor(Thread.currentThread().getName(), metrics);
        processorContext = new StandbyContextImpl(id, config, stateMgr, metrics);

        final Set<String> changelogTopicNames = new HashSet<>(topology.storeToChangelogTopic().values());
        partitions.stream()
            .filter(tp -> changelogTopicNames.contains(tp.topic()))
            .forEach(tp -> offsetLimits.put(tp, 0L));
        updateOffsetLimits = true;
    }

    @Override
    public void initializeMetadata() {}

    @Override
    public boolean initializeStateStores() {
        log.trace("Initializing state stores");
        registerStateStores();
        processorContext.initialize();
        taskInitialized = true;
        return true;
    }

    @Override
    public void initializeTopology() {}

    /**
     * <pre>
     * - update offset limits
     * </pre>
     */
    @Override
    public void resume() {
        log.debug("Resuming");
        allowUpdateOfOffsetLimit();
    }

    /**
     * <pre>
     * - flush store
     * - checkpoint store
     * - update offset limits
     * </pre>
     */
    @Override
    public void commit() {
        log.trace("Committing");
        flushAndCheckpointState();
        allowUpdateOfOffsetLimit();
        commitNeeded = false;
    }

    private void flushAndCheckpointState() {
        stateMgr.flush();
        stateMgr.checkpoint(Collections.emptyMap());
    }

    /**
     * <pre>
     * - {@link #commit()}
     * - close state
     * <pre>
     * @param isZombie ignored by {@code StandbyTask} as it can never be a zombie
     */
    @Override
    public void close(final boolean clean,
                      final boolean isZombie) {
        closeTaskSensor.record();
        if (!taskInitialized) {
            return;
        }
        log.debug("Closing");
        try {
            if (clean) {
                commit();
            }
        } finally {
            closeStateManager(true);
        }

        taskClosed = true;
    }

    /**
     * Updates a state store using records from one change log partition
     *
     * @return a list of records not consumed
     */
    public List<ConsumerRecord<byte[], byte[]>> update(final TopicPartition partition,
                                                       final List<ConsumerRecord<byte[], byte[]>> records) {
        if (records.isEmpty()) {
            return Collections.emptyList();
        }

        log.trace("Updating standby replicas of its state store for partition [{}]", partition);
        long limit = offsetLimits.getOrDefault(partition, Long.MAX_VALUE);

        long lastOffset = -1L;
        final List<ConsumerRecord<byte[], byte[]>> restoreRecords = new ArrayList<>(records.size());
        final List<ConsumerRecord<byte[], byte[]>> remainingRecords = new ArrayList<>();

        for (final ConsumerRecord<byte[], byte[]> record : records) {
            // Check if we're unable to process records due to an offset limit (e.g. when our
            // partition is both a source and a changelog). If we're limited then try to refresh
            // the offset limit if possible.
            if (record.offset() >= limit && updateOffsetLimits) {
                limit = updateOffsetLimits(partition);
            }

            if (record.offset() < limit) {
                restoreRecords.add(record);
                lastOffset = record.offset();
            } else {
                remainingRecords.add(record);
            }
        }

        if (!restoreRecords.isEmpty()) {
            stateMgr.updateStandbyStates(partition, restoreRecords, lastOffset);
            commitNeeded = true;
        }

        return remainingRecords;
    }

    Map<TopicPartition, Long> checkpointedOffsets() {
        return Collections.unmodifiableMap(stateMgr.checkpointed());
    }

    private long updateOffsetLimits(final TopicPartition partition) {
        if (!offsetLimits.containsKey(partition)) {
            throw new IllegalArgumentException("Topic is not both a source and a changelog: " + partition);
        }

        final Map<TopicPartition, Long> newLimits = committedOffsetForPartitions(offsetLimits.keySet());

        for (final Map.Entry<TopicPartition, Long> newlimit : newLimits.entrySet()) {
            final Long previousLimit = offsetLimits.get(newlimit.getKey());
            if (previousLimit != null && previousLimit > newlimit.getValue()) {
                throw new IllegalStateException("Offset limit should monotonically increase, but was reduced. " +
                    "New limit: " + newlimit.getValue() + ". Previous limit: " + previousLimit);
            }

        }

        offsetLimits.putAll(newLimits);
        updateOffsetLimits = false;

        return offsetLimits.get(partition);
    }

    private Map<TopicPartition, Long> committedOffsetForPartitions(final Set<TopicPartition> partitions) {
        try {
            // those do not have a committed offset would default to 0
            return consumer.committed(partitions).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() == null ? 0L : e.getValue().offset()));
        } catch (final AuthorizationException e) {
            throw new ProcessorStateException(String.format("task [%s] AuthorizationException when initializing offsets for %s", id, partitions), e);
        } catch (final WakeupException e) {
            throw e;
        } catch (final KafkaException e) {
            throw new ProcessorStateException(String.format("task [%s] Failed to initialize offsets for %s", id, partitions), e);
        }
    }

    void allowUpdateOfOffsetLimit() {
        updateOffsetLimits = true;
    }
}