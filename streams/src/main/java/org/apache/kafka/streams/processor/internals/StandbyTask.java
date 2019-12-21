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

    private final ChangelogReader changelogReader;
    private final Set<TopicPartition> changelogPartitions;

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
                final StreamsConfig config,
                final StreamsMetricsImpl metrics,
                final ProcessorStateManager stateMgr,
                final StateDirectory stateDirectory) {
        super(id, partitions, topology, consumer, true, stateMgr, stateDirectory, config);

        closeTaskSensor = ThreadMetrics.closeTaskSensor(Thread.currentThread().getName(), metrics);
        processorContext = new StandbyContextImpl(id, config, stateMgr, metrics);

        final Set<String> changelogTopics = new HashSet<>(topology.storeToChangelogTopic().values());
        this.changelogPartitions = partitions.stream().filter(tp -> changelogTopics.contains(tp.topic())).collect(Collectors.toSet());
    }

    @Override
    public void initializeMetadata() {}

    @Override
    public boolean initializeStateStores() {
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
        // this could theoretically throw a ProcessorStateException caused by a ProducerFencedException,
        // but in practice this shouldn't happen for standby tasks, since they don't produce to changelog topics
        // or downstream topics.
        stateMgr.flush();
        stateMgr.checkpoint(Collections.emptyMap());
    }

    /**
     * <pre>
     * - {@link #commit()}
     * - close state
     * <pre>
     */
    @Override
    public void close(final boolean clean) {
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

    Map<TopicPartition, Long> checkpointedOffsets() {
        return Collections.unmodifiableMap(stateMgr.changelogOffsets());
    }

    public void update() {
        // we use the changelog reader to do the actual restoration work,
        // and here we only need to update the offset limits when necessary

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