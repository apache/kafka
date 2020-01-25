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
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A StandbyTask
 */
public class StandbyTask extends AbstractTask implements Task {
    private final TaskId id;
    private final ProcessorTopology topology;
    private final ProcessorStateManager stateMgr;
    private final String logPrefix;
    private final Logger log;
    private final StateDirectory stateDirectory;
    private final Sensor closeTaskSensor;
    private final InternalProcessorContext processorContext;

    /**
     * @param id             the ID of this task
     * @param topology       the instance of {@link ProcessorTopology}
     * @param config         the {@link StreamsConfig} specified by the user
     * @param metrics        the {@link StreamsMetrics} created by the thread
     * @param stateDirectory the {@link StateDirectory} created by the thread
     */
    StandbyTask(final TaskId id,
                final ProcessorTopology topology,
                final StreamsConfig config,
                final StreamsMetricsImpl metrics,
                final ProcessorStateManager stateMgr,
                final StateDirectory stateDirectory) {
        this.id = id;
        this.topology = topology;
        this.stateDirectory = stateDirectory;

        this.stateMgr = stateMgr;

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

    @Override
    public void initializeIfNeeded() {
        if (state() == State.CREATED) {
            initializeStateStores();

            // no topology needs initialized, we can transit to RUNNING
            // right after registered the stores
            transitionTo(State.RESTORING);
            transitionTo(State.RUNNING);

            processorContext.initialize();

            log.debug("Initialized");
        }
    }

    private void initializeStateStores() {
        TaskUtils.registerStateStores(topology, stateDirectory, id, logPrefix, log, processorContext, stateMgr);
    }

    @Override
    public void completeRestoration() {
        throw new IllegalStateException("Standby task " + id + " should never be completing restoration");
    }

    @Override
    public void suspend() {
        log.trace("No-op suspend.");
    }

    @Override
    public void resume() {
        log.trace("No-op resume");
    }

    /**
     * <pre>
     * - flush store
     * - checkpoint store
     * </pre>
     */
    @Override
    public void commit() {
        if (state() == State.RUNNING) {
            stateMgr.flush();

            // since there's no written offsets we can checkpoint with empty map,
            // and the state current offset would be used to checkpoint
            stateMgr.checkpoint(Collections.emptyMap());

            log.debug("Committed");
        } else {
            throw new IllegalStateException("Illegal state " + state() + " while committing standby task " + id);
        }
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
     * 1. when unclean close, we do not need to commit;
     * 2. when unclean close, we do not throw any exception;
     */
    private void close(final boolean clean) {
        switch (state()) {
            case CREATED:
                transitionTo(State.CLOSING);
                // the task is created and not initialized, do nothing
                break;

            case RUNNING:
                // No resources are destroyed here, so we can do this before transit to CLOSING
                if (clean) {
                    commit();
                }

                transitionTo(State.CLOSING);

                try {
                    TaskUtils.closeStateManager(log, logPrefix, stateMgr, stateDirectory, id);
                } catch (final RuntimeException error) {
                    if (clean) {
                        throw error;
                    } else {
                        log.warn("Closing standby task " + id + " uncleanly throws an exception " + error);
                    }
                }
                break;

            default:
                throw new IllegalStateException("Illegal state " + state() + " while closing standby task " + id);
        }

        closeTaskSensor.record();
        transitionTo(State.CLOSED);

        log.debug("Closed");
    }

    @Override
    public TaskId id() {
        return id;
    }

    @Override
    public Set<TopicPartition> inputPartitions() {
        return Collections.emptySet();
    }

    @Override
    public void addRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        throw new IllegalStateException("Attempted to add records to task " + id() + " for invalid input partition " + partition);
    }

    @Override
    public StateStore getStore(final String name) {
        return stateMgr.getStore(name);
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

    public boolean isClosed() {
        return state() == State.CLOSED;
    }

    public boolean commitNeeded() {
        return false;
    }

    public Collection<TopicPartition> changelogPartitions() {
        return stateMgr.changelogPartitions();
    }

    public Map<TopicPartition, Long> changelogOffsets() {
        return Collections.unmodifiableMap(stateMgr.changelogOffsets());
    }
}
