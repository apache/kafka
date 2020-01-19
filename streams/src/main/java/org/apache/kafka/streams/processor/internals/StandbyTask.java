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

        processorContext = new StandbyContextImpl(id, config, stateMgr, metrics);
        closeTaskSensor = ThreadMetrics.closeTaskSensor(Thread.currentThread().getName(), metrics);
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
    public void initializeMetadata() {}

    // TODO K9113: remove from Task interface, only needed for StreamTask
    @Override
    public boolean initializeStateStores() {
        registerStateStores();

        // no topology needs initialized, we can transit to RESTORING
        // right after registered the stores
        transitionTo(State.RESTORING);

        processorContext.initialize();

        return true;
    }

    // TODO K9113: remove from Task interface, only needed for StreamTask
    @Override
    public void initializeTopology() {}

    /**
     * <pre>
     * - flush store
     * - checkpoint store
     * - update offset limits
     * </pre>
     */
    @Override
    public void commit() {
        stateMgr.flush();

        // since there's no written offsets we can checkpoint with empty map,
        // and the state current offset would be used to checkpoint
        stateMgr.checkpoint(Collections.emptyMap());

        log.debug("Committed");
    }

    /**
     * 1. when unclean close, we do not need to commit;
     * 2. when unclean close, we do not throw any exception;
     */
    @Override
    public void close(final boolean clean) {
        switch (state) {
            case CREATED:
                // the task is created and not initialized, do nothing
                break;

            case RESTORING:
                if (clean)
                    commit();

                try {
                    closeStateManager(clean);
                } catch (final RuntimeException error) {
                    if (clean) {
                        throw error;
                    } else {
                        log.warn("Closing standby task " + id + " uncleanly throws an exception " + error);
                    }
                }
                break;

            default:
                throw new IllegalStateException("Illegal state " + state + " while closing standby task " + id);
        }

        closeTaskSensor.record();

        log.debug("Closed");
    }

    Map<TopicPartition, Long> restoredOffsets() {
        return Collections.unmodifiableMap(stateMgr.changelogOffsets());
    }
}
