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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A StandbyTask
 */
public class StandbyTask extends AbstractTask implements Task {
    private final Logger log;
    private final String logPrefix;
    private final Sensor closeTaskSensor;
    private final InternalProcessorContext processorContext;

    private Map<TopicPartition, Long> offsetSnapshotSinceLastCommit;

    /**
     * @param id             the ID of this task
     * @param partitions     input topic partitions, used for thread metadata only
     * @param topology       the instance of {@link ProcessorTopology}
     * @param config         the {@link StreamsConfig} specified by the user
     * @param metrics        the {@link StreamsMetrics} created by the thread
     * @param stateMgr       the {@link ProcessorStateManager} for this task
     * @param stateDirectory the {@link StateDirectory} created by the thread
     */
    StandbyTask(final TaskId id,
                final Set<TopicPartition> partitions,
                final ProcessorTopology topology,
                final StreamsConfig config,
                final StreamsMetricsImpl metrics,
                final ProcessorStateManager stateMgr,
                final StateDirectory stateDirectory) {
        super(id, topology, stateDirectory, stateMgr, partitions);

        final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
        logPrefix = threadIdPrefix + String.format("%s [%s] ", "standby-task", id);
        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());

        processorContext = new StandbyContextImpl(id, config, stateMgr, metrics);
        closeTaskSensor = ThreadMetrics.closeTaskSensor(Thread.currentThread().getName(), metrics);
    }

    @Override
    public boolean isActive() {
        return false;
    }

    /**
     * @throws StreamsException fatal error, should close the thread
     */
    @Override
    public void initializeIfNeeded() {
        if (state() == State.CREATED) {
            StateManagerUtil.registerStateStores(log, logPrefix, topology, stateMgr, stateDirectory, processorContext);

            // no topology needs initialized, we can transit to RUNNING
            // right after registered the stores
            transitionTo(State.RESTORING);
            transitionTo(State.RUNNING);

            processorContext.initialize();

            log.info("Initialized");
        }
    }

    @Override
    public void completeRestoration() {
        throw new IllegalStateException("Standby task " + id + " should never be completing restoration");
    }

    @Override
    public void suspend() {
        log.trace("No-op suspend with state {}", state());
    }

    @Override
    public void resume() {
        log.trace("No-op resume with state {}", state());
    }

    /**
     * 1. flush store
     * 2. write checkpoint file
     *
     * @throws TaskMigratedException all the task has been migrated
     * @throws StreamsException fatal error, should close the thread
     */
    @Override
    public void commit() {
        switch (state()) {
            case RUNNING:
                stateMgr.flush();

                // since there's no written offsets we can checkpoint with empty map,
                // and the state current offset would be used to checkpoint
                stateMgr.checkpoint(Collections.emptyMap());

                offsetSnapshotSinceLastCommit = new HashMap<>(stateMgr.changelogOffsets());

                log.info("Committed");
                break;

            case CLOSING:
                // do nothing and also not throw
                log.trace("Skip committing since task is closing");

                break;

            default:
                throw new IllegalStateException("Illegal state " + state() + " while committing standby task " + id);

        }
    }

    @Override
    public void closeClean() {
        close(true);

        log.info("Closed clean");
    }

    @Override
    public void closeDirty() {
        close(false);

        log.info("Closed dirty");
    }

    /**
     * 1. commit if we are running and clean close;
     * 2. close the state manager.
     *
     * @throws TaskMigratedException all the task has been migrated
     * @throws StreamsException fatal error, should close the thread
     */
    private void close(final boolean clean) {
        if (state() == State.CREATED) {
            // the task is created and not initialized, do nothing
            transitionTo(State.CLOSING);
        } else {
            if (state() == State.RUNNING) {
                if (clean)
                    commit();

                transitionTo(State.CLOSING);
            }

            if (state() == State.CLOSING) {
                StateManagerUtil.closeStateManager(log, logPrefix, clean, stateMgr, stateDirectory);

                // TODO: if EOS is enabled, we should wipe out the state stores like we did for StreamTask too
            } else {
                throw new IllegalStateException("Illegal state " + state() + " while closing standby task " + id);
            }
        }

        closeTaskSensor.record();
        transitionTo(State.CLOSED);
    }

    @Override
    public boolean commitNeeded() {
        // we can commit if the store's offset has changed since last commit
        return offsetSnapshotSinceLastCommit == null || !offsetSnapshotSinceLastCommit.equals(stateMgr.changelogOffsets());
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        return Collections.unmodifiableMap(stateMgr.changelogOffsets());
    }

    @Override
    public void addRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        throw new IllegalStateException("Attempted to add records to task " + id() + " for invalid input partition " + partition);
    }

    /**
     * Produces a string representation containing useful information about a Task.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the StreamTask instance.
     */
    @Override
    public String toString() {
        return toString("");
    }

    /**
     * Produces a string representation containing useful information about a Task starting with the given indent.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the Task instance.
     */
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder();
        sb.append(indent);
        sb.append("TaskId: ");
        sb.append(id);
        sb.append("\n");

        // print topology
        if (topology != null) {
            sb.append(indent).append(topology.toString(indent + "\t"));
        }

        return sb.toString();
    }
}
