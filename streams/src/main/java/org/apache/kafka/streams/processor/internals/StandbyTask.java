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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A StandbyTask
 */
public class StandbyTask extends AbstractTask implements Task {
    private final Sensor closeTaskSensor;
    private final boolean eosEnabled;
    private final InternalProcessorContext processorContext;
    private final StreamsMetricsImpl streamsMetrics;

    /**
     * @param id             the ID of this task
     * @param partitions     input topic partitions, used for thread metadata only
     * @param topology       the instance of {@link ProcessorTopology}
     * @param config         the {@link StreamsConfig} specified by the user
     * @param streamsMetrics the {@link StreamsMetrics} created by the thread
     * @param stateMgr       the {@link ProcessorStateManager} for this task
     * @param stateDirectory the {@link StateDirectory} created by the thread
     */
    StandbyTask(final TaskId id,
                final Set<TopicPartition> inputPartitions,
                final ProcessorTopology topology,
                final StreamsConfig config,
                final StreamsMetricsImpl streamsMetrics,
                final ProcessorStateManager stateMgr,
                final StateDirectory stateDirectory,
                final ThreadCache cache,
                final InternalProcessorContext processorContext) {
        super(
            id,
            topology,
            stateDirectory,
            stateMgr,
            inputPartitions,
            config.getLong(StreamsConfig.TASK_TIMEOUT_MS_CONFIG),
            "standby-task",
            StandbyTask.class
        );
        this.processorContext = processorContext;
        this.streamsMetrics = streamsMetrics;
        processorContext.transitionToStandby(cache);

        closeTaskSensor = ThreadMetrics.closeTaskSensor(Thread.currentThread().getName(), streamsMetrics);
        eosEnabled = StreamThread.eosEnabled(config);
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

            // with and without EOS we would check for checkpointing at each commit during running,
            // and the file may be deleted in which case we should checkpoint immediately,
            // therefore we initialize the snapshot as empty
            offsetSnapshotSinceLastFlush = Collections.emptyMap();

            // no topology needs initialized, we can transit to RUNNING
            // right after registered the stores
            transitionTo(State.RESTORING);
            transitionTo(State.RUNNING);

            processorContext.initialize();

            log.info("Initialized");
        } else if (state() == State.RESTORING) {
            throw new IllegalStateException("Illegal state " + state() + " while initializing standby task " + id);
        }
    }

    @Override
    public void completeRestoration() {
        throw new IllegalStateException("Standby task " + id + " should never be completing restoration");
    }

    @Override
    public void suspend() {
        switch (state()) {
            case CREATED:
                log.info("Suspended created");
                transitionTo(State.SUSPENDED);

                break;

            case RUNNING:
                log.info("Suspended running");
                transitionTo(State.SUSPENDED);

                break;

            case SUSPENDED:
                log.info("Skip suspending since state is {}", state());

                break;

            case RESTORING:
            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while suspending standby task " + id);

            default:
                throw new IllegalStateException("Unknown state " + state() + " while suspending standby task " + id);
        }
    }

    @Override
    public void resume() {
        if (state() == State.RESTORING) {
            throw new IllegalStateException("Illegal state " + state() + " while resuming standby task " + id);
        }
        log.trace("No-op resume with state {}", state());
    }

    /**
     * Flush stores before a commit; the following exceptions maybe thrown from the state manager flushing call
     *
     * @throws TaskMigratedException recoverable error sending changelog records that would cause the task to be removed
     * @throws StreamsException fatal error when flushing the state store, for example sending changelog records failed
     *                          or flushing state store get IO errors; such error should cause the thread to die
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> prepareCommit() {
        switch (state()) {
            case CREATED:
                log.debug("Skipped preparing created task for commit");

                break;

            case RUNNING:
            case SUSPENDED:
                // do not need to flush state store caches in pre-commit since nothing would be sent for standby tasks
                log.debug("Prepared {} task for committing", state());

                break;

            default:
                throw new IllegalStateException("Illegal state " + state() + " while preparing standby task " + id + " for committing ");
        }

        return Collections.emptyMap();
    }

    @Override
    public void postCommit(final boolean enforceCheckpoint) {
        switch (state()) {
            case CREATED:
                // We should never write a checkpoint for a CREATED task as we may overwrite an existing checkpoint
                // with empty uninitialized offsets
                log.debug("Skipped writing checkpoint for created task");

                break;

            case RUNNING:
            case SUSPENDED:
                maybeWriteCheckpoint(enforceCheckpoint);

                log.debug("Finalized commit for {} task", state());

                break;

            default:
                throw new IllegalStateException("Illegal state " + state() + " while post committing standby task " + id);
        }
    }

    @Override
    public void closeClean() {
        streamsMetrics.removeAllTaskLevelSensors(Thread.currentThread().getName(), id.toString());
        close(true);
        log.info("Closed clean");
    }

    @Override
    public void closeDirty() {
        streamsMetrics.removeAllTaskLevelSensors(Thread.currentThread().getName(), id.toString());
        close(false);
        log.info("Closed dirty");
    }

    @Override
    public void closeCleanAndRecycleState() {
        streamsMetrics.removeAllTaskLevelSensors(Thread.currentThread().getName(), id.toString());
        if (state() == State.SUSPENDED) {
            stateMgr.recycle();
        } else {
            throw new IllegalStateException("Illegal state " + state() + " while closing standby task " + id);
        }

        closeTaskSensor.record();
        transitionTo(State.CLOSED);

        log.info("Closed clean and recycled state");
    }

    private void close(final boolean clean) {
        switch (state()) {
            case SUSPENDED:
                TaskManager.executeAndMaybeSwallow(
                    clean,
                    () -> StateManagerUtil.closeStateManager(
                        log,
                        logPrefix,
                        clean,
                        eosEnabled,
                        stateMgr,
                        stateDirectory,
                        TaskType.STANDBY
                    ),
                    "state manager close",
                    log
                );

                break;

            case CLOSED:
                log.trace("Skip closing since state is {}", state());
                return;

            case CREATED:
            case RESTORING: // a StandbyTask is never in RESTORING state
            case RUNNING:
                throw new IllegalStateException("Illegal state " + state() + " while closing standby task " + id);

            default:
                throw new IllegalStateException("Unknown state " + state() + " while closing standby task " + id);
        }

        closeTaskSensor.record();
        transitionTo(State.CLOSED);
    }

    @Override
    public boolean commitNeeded() {
        // for standby tasks committing is the same as checkpointing,
        // so we only need to commit if we want to checkpoint
        return StateManagerUtil.checkpointNeeded(false, offsetSnapshotSinceLastFlush, stateMgr.changelogOffsets());
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        return Collections.unmodifiableMap(stateMgr.changelogOffsets());
    }

    @Override
    public void addRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        throw new IllegalStateException("Attempted to add records to task " + id() + " for invalid input partition " + partition);
    }

    @Override
    public void addFetchedMetadata(final TopicPartition partition, final ConsumerRecords.Metadata metadata) {
        throw new IllegalStateException("Attempted to update metadata for standby task " + id());
    }

    InternalProcessorContext processorContext() {
        return processorContext;
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
