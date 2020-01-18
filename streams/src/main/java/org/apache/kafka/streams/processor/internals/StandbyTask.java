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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A StandbyTask
 */
public class StandbyTask extends AbstractTask {
    private final Sensor closeTaskSensor;
    private State state = State.CREATED;

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
    }

    @Override
    public State state() {
        return state;
    }

    @Override
    public void transitionTo(final State newState) {
        State.validateTransition(state, newState);
        state = newState;
    }

    @Override
    public void initializeIfNeeded() {
        if (state() == State.CREATED) {
            initializeMetadata();
            initializeStateStores();
            transitionTo(State.RUNNING);
        }
    }

    @Override
    public void initializeMetadata() {}

    @Override
    public void initializeStateStores() {
        registerStateStores();
        processorContext.initialize();
        taskInitialized = true;
    }

    @Override
    public void startRunning() {
        // TODO: add changelog partitions to restore consumer?
    }

    @Override
    public boolean hasChangelogs() {
        return true;
    }

    @Override
    public void initializeTopology() {}

    @Override
    public void suspend() {
        log.debug("No-op suspend.");
    }

    @Override
    public void resume() {
        log.debug("No-op resume");
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
        commitNeeded = false;
    }

    private void flushAndCheckpointState() {
        // this could theoretically throw a ProcessorStateException caused by a ProducerFencedException,
        // but in practice this shouldn't happen for standby tasks, since they don't produce to changelog topics
        // or downstream topics.
        stateMgr.flush();
        stateMgr.checkpoint(Collections.emptyMap());
    }

    @Override
    public void closeClean() {
        close(true);
    }

    @Override
    public void closeDirty() {
        close(false);
    }

    /**
     * <pre>
     * - {@link #commit()}
     * - close state
     * <pre>
     */
    private void close(final boolean clean) {
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
        // TODO K9113: finish this logic with ChangeLogReader
    }
}
