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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A StandbyTask
 */
public class StandbyTask extends AbstractTask {

    private Map<TopicPartition, Long> checkpointedOffsets = new HashMap<>();

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
                final Collection<TopicPartition> partitions,
                final ProcessorTopology topology,
                final Consumer<byte[], byte[]> consumer,
                final ChangelogReader changelogReader,
                final StreamsConfig config,
                final StreamsMetricsImpl metrics,
                final StateDirectory stateDirectory) {
        super(id, partitions, topology, consumer, changelogReader, true, stateDirectory, config);

        // initialize the topology with its own context
        processorContext = new StandbyContextImpl(id, config, stateMgr, metrics);
    }

    @Override
    public boolean initializeStateStores() {
        log.trace("Initializing state stores");
        registerStateStores();
        checkpointedOffsets = Collections.unmodifiableMap(stateMgr.checkpointed());
        processorContext.initialized();
        taskInitialized = true;
        return true;
    }

    @Override
    public void initializeTopology() {
        //no-op
    }

    /**
     * <pre>
     * - update offset limits
     * </pre>
     */
    @Override
    public void resume() {
        log.debug("Resuming");
        updateOffsetLimits();
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
        // reinitialize offset limits
        updateOffsetLimits();
    }

    /**
     * <pre>
     * - flush store
     * - checkpoint store
     * </pre>
     */
    @Override
    public void suspend() {
        log.debug("Suspending");
        flushAndCheckpointState();
    }

    private void flushAndCheckpointState() {
        stateMgr.flush();
        stateMgr.checkpoint(Collections.<TopicPartition, Long>emptyMap());
    }

    /**
     * <pre>
     * - {@link #commit()}
     * - close state
     * <pre>
     * @param clean ignored by {@code StandbyTask} as it can always try to close cleanly
     *              (ie, commit, flush, and write checkpoint file)
     * @param isZombie ignored by {@code StandbyTask} as it can never be a zombie
     */
    @Override
    public void close(final boolean clean,
                      final boolean isZombie) {
        if (!taskInitialized) {
            return;
        }
        log.debug("Closing");
        boolean committedSuccessfully = false;
        try {
            if (clean) {
                commit();
                committedSuccessfully = true;
            }
        } finally {
            closeStateManager(committedSuccessfully);
        }

        taskClosed = true;
    }

    @Override
    public void closeSuspended(final boolean clean,
                               final boolean isZombie,
                               final RuntimeException e) {
        close(clean, isZombie);
    }

    /**
     * Updates a state store using records from one change log partition
     *
     * @return a list of records not consumed
     */
    public List<ConsumerRecord<byte[], byte[]>> update(final TopicPartition partition,
                                                       final List<ConsumerRecord<byte[], byte[]>> records) {
        log.trace("Updating standby replicas of its state store for partition [{}]", partition);
        return stateMgr.updateStandbyStates(partition, records);
    }

    Map<TopicPartition, Long> checkpointedOffsets() {
        return checkpointedOffsets;
    }

}