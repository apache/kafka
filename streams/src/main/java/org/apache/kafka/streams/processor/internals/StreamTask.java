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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.TopologyConfig.TaskConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.errors.internals.DefaultErrorHandlerContext;
import org.apache.kafka.streams.errors.internals.FailedProcessingException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.AbstractPartitionGroup.RecordInfo;
import org.apache.kafka.streams.processor.internals.metrics.ProcessorNodeMetrics;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeRecordSensor;

/**
 * A StreamTask is associated with a {@link AbstractPartitionGroup}, and is assigned to a StreamThread for processing.
 */
public class StreamTask extends AbstractTask implements ProcessorNodePunctuator, Task {

    private final Time time;
    private final Consumer<byte[], byte[]> mainConsumer;

    // we want to abstract eos logic out of StreamTask, however
    // there's still an optimization that requires this info to be
    // leaked into this class, which is to checkpoint after committing if EOS is not enabled.
    private final boolean eosEnabled;

    private final int maxBufferedSize;
    private final AbstractPartitionGroup partitionGroup;
    private final RecordCollector recordCollector;
    private final AbstractPartitionGroup.RecordInfo recordInfo;
    private final Map<TopicPartition, Long> consumedOffsets;
    private final Map<TopicPartition, OffsetAndMetadata> nextOffsetsAndMetadataToBeConsumed = new HashMap<>();
    private final Map<TopicPartition, Long> committedOffsets;
    private final Map<TopicPartition, Long> highWatermark;
    private final Set<TopicPartition> resetOffsetsForPartitions;
    private final Set<TopicPartition> partitionsToResume;
    private final PunctuationQueue streamTimePunctuationQueue;
    private final PunctuationQueue systemTimePunctuationQueue;
    private final StreamsMetricsImpl streamsMetrics;

    private long processTimeMs = 0L;

    private final Sensor closeTaskSensor;
    private final Sensor processRatioSensor;
    private final Sensor processLatencySensor;
    private final Sensor restoreSensor;
    private final Sensor restoreRemainingSensor;
    private final Sensor punctuateLatencySensor;
    private final Sensor bufferedRecordsSensor;
    private final Sensor droppedRecordsSensor;
    private final Map<String, Sensor> e2eLatencySensors = new HashMap<>();

    private final RecordQueueCreator recordQueueCreator;

    @SuppressWarnings("rawtypes")
    protected final InternalProcessorContext processorContext;
    private final ProcessingExceptionHandler processingExceptionHandler;

    private StampedRecord record;
    private boolean commitNeeded = false;
    private boolean commitRequested = false;
    private boolean hasPendingTxCommit = false;
    private Optional<Long> timeCurrentIdlingStarted;

    @SuppressWarnings({"rawtypes", "this-escape", "checkstyle:ParameterNumber"})
    public StreamTask(final TaskId id,
                      final Set<TopicPartition> inputPartitions,
                      final ProcessorTopology topology,
                      final Consumer<byte[], byte[]> mainConsumer,
                      final TaskConfig config,
                      final StreamsMetricsImpl streamsMetrics,
                      final StateDirectory stateDirectory,
                      final ThreadCache cache,
                      final Time time,
                      final ProcessorStateManager stateMgr,
                      final RecordCollector recordCollector,
                      final InternalProcessorContext processorContext,
                      final LogContext logContext,
                      final boolean processingThreadsEnabled
                      ) {
        super(
            id,
            topology,
            stateDirectory,
            stateMgr,
            inputPartitions,
            config,
            "task",
            StreamTask.class
        );
        this.mainConsumer = mainConsumer;

        this.processorContext = processorContext;
        processorContext.transitionToActive(this, recordCollector, cache);

        this.time = time;
        this.recordCollector = recordCollector;
        this.eosEnabled = config.eosEnabled;

        final String threadId = Thread.currentThread().getName();
        this.streamsMetrics = streamsMetrics;
        closeTaskSensor = ThreadMetrics.closeTaskSensor(threadId, streamsMetrics);
        final String taskId = id.toString();
        restoreSensor = TaskMetrics.restoreSensor(threadId, taskId, streamsMetrics);
        restoreRemainingSensor = TaskMetrics.restoreRemainingRecordsSensor(threadId, taskId, streamsMetrics);
        processRatioSensor = TaskMetrics.activeProcessRatioSensor(threadId, taskId, streamsMetrics);
        processLatencySensor = TaskMetrics.processLatencySensor(threadId, taskId, streamsMetrics);
        punctuateLatencySensor = TaskMetrics.punctuateSensor(threadId, taskId, streamsMetrics);
        bufferedRecordsSensor = TaskMetrics.activeBufferedRecordsSensor(threadId, taskId, streamsMetrics);
        droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(threadId, taskId, streamsMetrics);

        for (final String terminalNodeName : topology.terminalNodes()) {
            e2eLatencySensors.put(
                terminalNodeName,
                ProcessorNodeMetrics.e2ELatencySensor(threadId, taskId, terminalNodeName, streamsMetrics)
            );
        }

        for (final ProcessorNode<?, ?, ?, ?> sourceNode : topology.sources()) {
            final String sourceNodeName = sourceNode.name();
            e2eLatencySensors.put(
                sourceNodeName,
                ProcessorNodeMetrics.e2ELatencySensor(threadId, taskId, sourceNodeName, streamsMetrics)
            );
        }

        streamTimePunctuationQueue = new PunctuationQueue();
        systemTimePunctuationQueue = new PunctuationQueue();
        maxBufferedSize = config.maxBufferedSize;

        // initialize the consumed and committed offset cache
        consumedOffsets = new HashMap<>();
        resetOffsetsForPartitions = new HashSet<>();
        partitionsToResume = new HashSet<>();

        recordQueueCreator = new RecordQueueCreator(this.logContext, config.timestampExtractor, config.deserializationExceptionHandler);

        recordInfo = new RecordInfo();

        final Sensor enforcedProcessingSensor;
        enforcedProcessingSensor = TaskMetrics.enforcedProcessingSensor(threadId, taskId, streamsMetrics);
        final long maxTaskIdleMs = config.maxTaskIdleMs;
        if (processingThreadsEnabled) {
            partitionGroup = new SynchronizedPartitionGroup(new PartitionGroup(
                logContext,
                createPartitionQueues(),
                mainConsumer::currentLag,
                TaskMetrics.recordLatenessSensor(threadId, taskId, streamsMetrics),
                enforcedProcessingSensor,
                maxTaskIdleMs
            ));
        } else {
            partitionGroup = new PartitionGroup(
                logContext,
                createPartitionQueues(),
                mainConsumer::currentLag,
                TaskMetrics.recordLatenessSensor(threadId, taskId, streamsMetrics),
                enforcedProcessingSensor,
                maxTaskIdleMs
            );
        }

        stateMgr.registerGlobalStateStores(topology.globalStateStores());
        committedOffsets = new HashMap<>();
        highWatermark = new HashMap<>();
        for (final TopicPartition topicPartition: inputPartitions) {
            committedOffsets.put(topicPartition, -1L);
            highWatermark.put(topicPartition, -1L);
        }
        timeCurrentIdlingStarted = Optional.empty();
        processingExceptionHandler = config.processingExceptionHandler;
    }

    // create queues for each assigned partition and associate them
    // to corresponding source nodes in the processor topology
    private Map<TopicPartition, RecordQueue> createPartitionQueues() {
        final Map<TopicPartition, RecordQueue> partitionQueues = new HashMap<>();
        for (final TopicPartition partition : inputPartitions()) {
            partitionQueues.put(partition, recordQueueCreator.createQueue(partition));
        }
        return partitionQueues;
    }

    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public void recordRestoration(final Time time, final long numRecords, final boolean initRemaining) {
        if (initRemaining) {
            maybeRecordSensor(numRecords, time, restoreRemainingSensor);
        } else {
            maybeRecordSensor(numRecords, time, restoreSensor);
            maybeRecordSensor(-1 * numRecords, time, restoreRemainingSensor);
        }
    }

    /**
     * @throws TaskCorruptedException if the state cannot be reused (with EOS) and needs to be reset
     * @throws LockException    could happen when multi-threads within the single instance, could retry
     * @throws TimeoutException if initializing record collector timed out
     * @throws StreamsException fatal error, should close the thread
     */
    @Override
    public void initializeIfNeeded() {
        if (state() == State.CREATED) {
            recordCollector.initialize();

            StateManagerUtil.registerStateStores(log, logPrefix, topology, stateMgr, stateDirectory, processorContext);

            // without EOS the checkpoint file would not be deleted after loading, and
            // with EOS we would not checkpoint ever during running state anyways.
            // therefore we can initialize the snapshot as empty so that we would checkpoint right after loading
            offsetSnapshotSinceLastFlush = Collections.emptyMap();

            transitionTo(State.RESTORING);

            log.info("Initialized");
        }
    }

    public void addPartitionsForOffsetReset(final Set<TopicPartition> partitionsForOffsetReset) {
        mainConsumer.pause(partitionsForOffsetReset);
        resetOffsetsForPartitions.addAll(partitionsForOffsetReset);
    }

    /**
     * @throws TimeoutException if fetching committed offsets timed out
     */
    @Override
    public void completeRestoration(final java.util.function.Consumer<Set<TopicPartition>> offsetResetter) {
        switch (state()) {
            case RUNNING:
                return;

            case RESTORING:
                resetOffsetsIfNeededAndInitializeMetadata(offsetResetter);
                initializeTopology();
                processorContext.initialize();
                if (!eosEnabled) {
                    maybeCheckpoint(true);
                }

                transitionTo(State.RUNNING);

                log.info("Restored and ready to run");

                break;

            case CREATED:
            case SUSPENDED:
            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while completing restoration for active task " + id);

            default:
                throw new IllegalStateException("Unknown state " + state() + " while completing restoration for active task " + id);
        }
    }

    @Override
    public void suspend() {
        switch (state()) {
            case CREATED:
                transitToSuspend();
                break;

            case RESTORING:
                transitToSuspend();
                break;

            case RUNNING:
                try {
                    // use try-catch to ensure state transition to SUSPENDED even if user code throws in `Processor#close()`
                    closeTopology();

                    // we must clear the buffered records when suspending because upon resuming the consumer would
                    // re-fetch those records starting from the committed position
                    partitionGroup.clear();
                } finally {
                    transitToSuspend();
                }

                break;

            case SUSPENDED:
                log.info("Skip suspending since state is {}", state());

                break;

            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while suspending active task " + id);

            default:
                throw new IllegalStateException("Unknown state " + state() + " while suspending active task " + id);
        }
    }

    @SuppressWarnings("unchecked")
    private void closeTopology() {
        log.trace("Closing processor topology");

        // close the processors
        // make sure close() is called for each node even when there is a RuntimeException
        RuntimeException exception = null;
        for (final ProcessorNode<?, ?, ?, ?> node : topology.processors()) {
            processorContext.setCurrentNode(node);
            try {
                node.close();
            } catch (final RuntimeException e) {
                exception = e;
            } finally {
                processorContext.setCurrentNode(null);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    /**
     * <pre>
     * - resume the task
     * </pre>
     */
    @Override
    public void resume() {
        switch (state()) {
            case CREATED:
            case RUNNING:
            case RESTORING:
                // no need to do anything, just let them continue running / restoring / closing
                log.trace("Skip resuming since state is {}", state());
                break;

            case SUSPENDED:
                // just transit the state without any logical changes: suspended and restoring states
                // are not actually any different for inner modules

                // Deleting checkpoint file before transition to RESTORING state (KAFKA-10362)
                try {
                    stateMgr.deleteCheckPointFileIfEOSEnabled();
                    log.debug("Deleted check point file upon resuming with EOS enabled");
                } catch (final IOException ioe) {
                    log.error("Encountered error while deleting the checkpoint file due to this exception", ioe);
                }

                transitionTo(State.RESTORING);
                log.info("Resumed to restoring state");

                break;

            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while resuming active task " + id);

            default:
                throw new IllegalStateException("Unknown state " + state() + " while resuming active task " + id);
        }
        timeCurrentIdlingStarted = Optional.empty();
    }


    public void flush() {
        stateMgr.flushCache();
        recordCollector.flush();
    }

    /**
     * @throws StreamsException fatal error that should cause the thread to die
     * @throws TaskMigratedException recoverable error that would cause the task to be removed
     * @return offsets that should be committed for this task
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> prepareCommit() {
        switch (state()) {
            case CREATED:
            case RESTORING:
            case RUNNING:
            case SUSPENDED:
                // the commitNeeded flag just indicates whether we have reached RUNNING and processed any new data,
                // so it only indicates whether the record collector should be flushed or not, whereas the state
                // manager should always be flushed; either there is newly restored data or the flush will be a no-op
                if (commitNeeded) {
                    // we need to flush the store caches before flushing the record collector since it may cause some
                    // cached records to be processed and hence generate more records to be sent out
                    //
                    // TODO: this should be removed after we decouple caching with emitting
                    flush();
                    hasPendingTxCommit = eosEnabled;

                    log.debug("Prepared {} task for committing", state());
                    return committableOffsetsAndMetadata();
                } else {
                    log.debug("Skipped preparing {} task for commit since there is nothing to commit", state());
                    return Collections.emptyMap();
                }

            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while preparing active task " + id + " for committing");

            default:
                throw new IllegalStateException("Unknown state " + state() + " while preparing active task " + id + " for committing");
        }
    }

    private OffsetAndMetadata findOffsetAndMetadata(final TopicPartition partition) {
        Long offset = partitionGroup.headRecordOffset(partition);
        Optional<Integer> leaderEpoch = partitionGroup.headRecordLeaderEpoch(partition);
        final long partitionTime = partitionGroup.partitionTimestamp(partition);
        if (offset == null) {
            try {
                if (nextOffsetsAndMetadataToBeConsumed.containsKey(partition)) {
                    final OffsetAndMetadata offsetAndMetadata = nextOffsetsAndMetadataToBeConsumed.get(partition);
                    offset = offsetAndMetadata.offset();
                    leaderEpoch = offsetAndMetadata.leaderEpoch();
                } else {
                    // This indicates a bug and thus we rethrow it as fatal `IllegalStateException`
                    throw new IllegalStateException("Stream task " + id + " does not know the partition: " + partition);
                }
            } catch (final KafkaException fatal) {
                throw new StreamsException(fatal);
            }
        }
        return new OffsetAndMetadata(offset,
                leaderEpoch,
                new TopicPartitionMetadata(partitionTime, processorContext.processorMetadata()).encode());
    }

    private Map<TopicPartition, OffsetAndMetadata> committableOffsetsAndMetadata() {
        final Map<TopicPartition, OffsetAndMetadata> committableOffsets;

        switch (state()) {
            case CREATED:
            case RESTORING:
                committableOffsets = Collections.emptyMap();

                break;

            case RUNNING:
            case SUSPENDED:

                // If there's processor metadata to be committed. We need to commit them to all
                // input partitions
                final Set<TopicPartition> partitionsNeedCommit = processorContext.processorMetadata().needsCommit() ?
                    inputPartitions() : consumedOffsets.keySet();
                committableOffsets = new HashMap<>(partitionsNeedCommit.size());

                for (final TopicPartition partition : partitionsNeedCommit) {
                    committableOffsets.put(partition, findOffsetAndMetadata(partition));
                }
                break;

            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while getting committable offsets for active task " + id);

            default:
                throw new IllegalStateException("Unknown state " + state() + " while post committing active task " + id);
        }

        return committableOffsets;
    }

    @Override
    public void postCommit(final boolean enforceCheckpoint) {
        switch (state()) {
            case CREATED:
                // We should never write a checkpoint for a CREATED task as we may overwrite an existing checkpoint
                // with empty uninitialized offsets
                log.debug("Skipped writing checkpoint for {} task", state());

                break;

            case RESTORING:
            case SUSPENDED:
                maybeCheckpoint(enforceCheckpoint);
                log.debug("Finalized commit for {} task with enforce checkpoint {}", state(), enforceCheckpoint);

                break;

            case RUNNING:
                if (enforceCheckpoint || !eosEnabled) {
                    maybeCheckpoint(enforceCheckpoint);
                }
                log.debug("Finalized commit for {} task with eos {} enforce checkpoint {}", state(), eosEnabled, enforceCheckpoint);

                break;

            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while post committing active task " + id);

            default:
                throw new IllegalStateException("Unknown state " + state() + " while post committing active task " + id);
        }

        clearCommitStatuses();
    }

    private void clearCommitStatuses() {
        commitNeeded = false;
        commitRequested = false;
        hasPendingTxCommit = false;
        processorContext.processorMetadata().setNeedsCommit(false);
    }


    @Override
    public void closeClean() {
        validateClean();
        removeAllSensors();
        clearCommitStatuses();
        close(true);
        log.info("Closed clean");
    }

    @Override
    public void closeDirty() {
        removeAllSensors();
        clearCommitStatuses();
        close(false);
        log.info("Closed dirty");
    }

    @Override
    public void updateInputPartitions(final Set<TopicPartition> topicPartitions, final Map<String, List<String>> allTopologyNodesToSourceTopics) {
        super.updateInputPartitions(topicPartitions, allTopologyNodesToSourceTopics);
        partitionGroup.updatePartitions(topicPartitions, recordQueueCreator::createQueue);
        processorContext.processorMetadata().setNeedsCommit(true);
    }

    @Override
    public void prepareRecycle() {
        validateClean();
        removeAllSensors();
        clearCommitStatuses();
        switch (state()) {
            case SUSPENDED:
                stateMgr.recycle();
                partitionGroup.close();
                recordCollector.closeClean();

                break;

            case CREATED:
            case RESTORING:
            case RUNNING:
            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while recycling active task " + id);
            default:
                throw new IllegalStateException("Unknown state " + state() + " while recycling active task " + id);
        }

        closeTaskSensor.record();
        transitionTo(State.CLOSED);

        log.info("Closed and recycled state");
    }

    @Override
    public void resumePollingForPartitionsWithAvailableSpace() {
        if (!partitionsToResume.isEmpty()) {
            mainConsumer.resume(partitionsToResume);
            partitionsToResume.clear();
        }
    }

    @Override
    public void updateLags() {
        if (state() == State.RUNNING) {
            partitionGroup.updateLags();
        }
    }

    /**
     * The following exceptions maybe thrown from the state manager flushing call
     *
     * @throws TaskMigratedException recoverable error sending changelog records that would cause the task to be removed
     * @throws StreamsException fatal error when flushing the state store, for example sending changelog records failed
     *                          or flushing state store get IO errors; such error should cause the thread to die
     */
    @Override
    public void maybeCheckpoint(final boolean enforceCheckpoint) {
        // commitNeeded indicates we may have processed some records since last commit
        // and hence we need to refresh checkpointable offsets regardless whether we should checkpoint or not
        if (commitNeeded || enforceCheckpoint) {
            stateMgr.updateChangelogOffsets(checkpointableOffsets());
        }

        super.maybeCheckpoint(enforceCheckpoint);
    }

    private void validateClean() {
        // It may be that we failed to commit a task during handleRevocation, but "forgot" this and tried to
        // closeClean in handleAssignment. We should throw if we detect this to force the TaskManager to closeDirty
        if (commitNeeded) {
            log.debug("Tried to close clean but there was pending uncommitted data, this means we failed to"
                          + " commit and should close as dirty instead");
            throw new TaskMigratedException("Tried to close dirty task as clean");
        }
    }

    private void removeAllSensors() {
        streamsMetrics.removeAllTaskLevelSensors(Thread.currentThread().getName(), id.toString());
        for (final String nodeName : e2eLatencySensors.keySet()) {
            streamsMetrics.removeAllNodeLevelSensors(Thread.currentThread().getName(), id.toString(), nodeName);
        }
    }

    /**
     * You must commit a task and checkpoint the state manager before closing as this will release the state dir lock
     */
    private void close(final boolean clean) {
        switch (state()) {
            case SUSPENDED:
                TaskManager.executeAndMaybeSwallow(
                    clean,
                    partitionGroup::close,
                    "partition group close",
                    log
                );

                // first close state manager (which is idempotent) then close the record collector
                // if the latter throws and we re-close dirty which would close the state manager again.
                TaskManager.executeAndMaybeSwallow(
                    clean,
                    () -> StateManagerUtil.closeStateManager(
                        log,
                        logPrefix,
                        clean,
                        eosEnabled,
                        stateMgr,
                        stateDirectory,
                        TaskType.ACTIVE
                    ),
                    "state manager close",
                    log);

                TaskManager.executeAndMaybeSwallow(
                    clean,
                    clean ? recordCollector::closeClean : recordCollector::closeDirty,
                    "record collector close",
                    log
                );

                break;

            case CLOSED:
                log.trace("Skip closing since state is {}", state());
                return;

            case CREATED:
            case RESTORING:
            case RUNNING:
                throw new IllegalStateException("Illegal state " + state() + " while closing active task " + id);

            default:
                throw new IllegalStateException("Unknown state " + state() + " while closing active task " + id);
        }

        record = null;
        closeTaskSensor.record();
        partitionsToResume.clear();

        transitionTo(State.CLOSED);
    }

    /**
     * An active task is processable if its buffer contains data for all of its input
     * source topic partitions, or if it is enforced to be processable.
     */
    public boolean isProcessable(final long wallClockTime) {
        if (state() == State.CLOSED) {
            // a task is only closing / closed when 1) task manager is closing, 2) a rebalance is undergoing;
            // in either case we can just log it and move on without notifying the thread since the consumer
            // would soon be updated to not return any records for this task anymore.
            log.info("Stream task {} is already in {} state, skip processing it.", id(), state());

            return false;
        }

        if (hasPendingTxCommit) {
            // if the task has a pending TX commit, we should just retry the commit but not process any records
            // thus, the task is not processable, even if there is available data in the record queue
            return false;
        }
        final boolean readyToProcess = partitionGroup.readyToProcess(wallClockTime);
        if (!readyToProcess) {
            if (!timeCurrentIdlingStarted.isPresent()) {
                timeCurrentIdlingStarted = Optional.of(wallClockTime);
            }
        } else {
            timeCurrentIdlingStarted = Optional.empty();
        }
        return readyToProcess;
    }

    /**
     * Process one record.
     *
     * @return true if this method processes a record, false if it does not process a record.
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    @SuppressWarnings("unchecked")
    public boolean process(final long wallClockTime) {
        if (record == null) {
            if (!isProcessable(wallClockTime)) {
                return false;
            }

            // get the next record to process
            record = partitionGroup.nextRecord(recordInfo, wallClockTime);

            // if there is no record to process, return immediately
            if (record == null) {
                return false;
            }
        }

        try {
            final TopicPartition partition = recordInfo.partition();

            if (!(record instanceof CorruptedRecord)) {
                doProcess(wallClockTime);
            }

            // update the consumed offset map after processing is done
            consumedOffsets.put(partition, record.offset());
            commitNeeded = true;

            // after processing this record, if its partition queue's buffered size has been
            // decreased to the threshold, we can then resume the consumption on this partition
            if (recordInfo.queue().size() <= maxBufferedSize) {
                partitionsToResume.add(partition);
            }

            record = null;
        } catch (final TimeoutException timeoutException) {
            if (!eosEnabled) {
                throw timeoutException;
            } else {
                record = null;
                throw new TaskCorruptedException(Collections.singleton(id));
            }
        } catch (final FailedProcessingException failedProcessingException) {
            // Do not keep the failed processing exception in the stack trace
            handleException(
                failedProcessingException.getMessage(),
                failedProcessingException.failedProcessorNodeName(),
                failedProcessingException.getCause()
            );
        } catch (final StreamsException exception) {
            record = null;
            throw exception;
        } catch (final RuntimeException e) {
            handleException(processorContext.currentNode().name(), e);
        } finally {
            processorContext.setCurrentNode(null);
        }

        return true;
    }

    private void handleException(final String failedProcessorNodeName, final Throwable originalException) {
        handleException(
            String.format(
                "Exception caught in process. taskId=%s, processor=%s, topic=%s, partition=%d, offset=%d",
                id(),
                failedProcessorNodeName,
                record.topic(),
                record.partition(),
                record.offset()
            ),
            failedProcessorNodeName,
            originalException);
    }

    private void handleException(final String errorMessage, final String failedProcessorNodeName, final Throwable originalException) {
        if (errorMessage == null) {
            handleException(failedProcessorNodeName, originalException);
        }

        final StreamsException error = new StreamsException(errorMessage, originalException);
        record = null;

        throw error;
    }

    @SuppressWarnings("unchecked")
    private void doProcess(final long wallClockTime) {
        // process the record by passing to the source node of the topology
        final ProcessorNode<Object, Object, Object, Object> currNode = (ProcessorNode<Object, Object, Object, Object>) recordInfo.node();
        log.trace("Start processing one record [{}]", record);

        final ProcessorRecordContext recordContext = new ProcessorRecordContext(
            record.timestamp,
            record.offset(),
            record.partition(),
            record.topic(),
            record.headers()
        );
        updateProcessorContext(currNode, wallClockTime, recordContext);

        maybeRecordE2ELatency(record.timestamp, wallClockTime, currNode.name());
        final Record<Object, Object> toProcess = new Record<>(
            record.key(),
            record.value(),
            processorContext.timestamp(),
            processorContext.headers()
        );
        maybeMeasureLatency(() -> currNode.process(toProcess), time, processLatencySensor);

        log.trace("Completed processing one record [{}]", record);
    }

    @Override
    public void recordProcessBatchTime(final long processBatchTime) {
        processTimeMs += processBatchTime;
    }

    @Override
    public void recordProcessTimeRatioAndBufferSize(final long allTaskProcessMs, final long now) {
        bufferedRecordsSensor.record(partitionGroup.numBuffered());
        processRatioSensor.record((double) processTimeMs / allTaskProcessMs, now);
        processTimeMs = 0L;
    }

    /**
     * @throws IllegalStateException if the current node is not null
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void punctuate(final ProcessorNode<?, ?, ?, ?> node,
                          final long timestamp,
                          final PunctuationType type,
                          final Punctuator punctuator) {
        if (processorContext.currentNode() != null) {
            throw new IllegalStateException(String.format("%sCurrent node is not null", logPrefix));
        }

        // when punctuating, we need to preserve the timestamp (this can be either system time or event time)
        // while other record context are set as dummy: null topic, -1 partition, -1 offset and empty header
        final ProcessorRecordContext recordContext = new ProcessorRecordContext(
            timestamp,
            -1L,
            -1,
            null,
            new RecordHeaders()
        );
        updateProcessorContext(node, time.milliseconds(), recordContext);

        if (log.isTraceEnabled()) {
            log.trace("Punctuating processor {} with timestamp {} and punctuation type {}", node.name(), timestamp, type);
        }

        try {
            maybeMeasureLatency(() -> punctuator.punctuate(timestamp), time, punctuateLatencySensor);
        } catch (final TimeoutException timeoutException) {
            if (!eosEnabled) {
                throw timeoutException;
            } else {
                record = null;
                throw new TaskCorruptedException(Collections.singleton(id));
            }
        } catch (final FailedProcessingException e) {
            throw createStreamsException(node.name(), e.getCause());
        } catch (final TaskCorruptedException | TaskMigratedException e) {
            throw e;
        } catch (final Exception processingException) {
            // while Java distinguishes checked vs unchecked exceptions, other languages
            // like Scala or Kotlin do not, and thus we need to catch `Exception`
            // (instead of `RuntimeException`) to work well with those languages
            final ErrorHandlerContext errorHandlerContext = new DefaultErrorHandlerContext(
                null,
                recordContext.topic(),
                recordContext.partition(),
                recordContext.offset(),
                recordContext.headers(),
                node.name(),
                id(),
                recordContext.timestamp()
            );

            final ProcessingExceptionHandler.ProcessingHandlerResponse response;
            try {
                response = Objects.requireNonNull(
                    processingExceptionHandler.handle(errorHandlerContext, null, processingException),
                    "Invalid ProcessingExceptionHandler response."
                );
            } catch (final Exception fatalUserException) {
                // while Java distinguishes checked vs unchecked exceptions, other languages
                // like Scala or Kotlin do not, and thus we need to catch `Exception`
                // (instead of `RuntimeException`) to work well with those languages
                log.error(
                    "Processing error callback failed after processing error for record: {}",
                    errorHandlerContext,
                    processingException
                );
                throw new FailedProcessingException("Fatal user code error in processing error callback", node.name(), fatalUserException);
            }

            if (response == ProcessingExceptionHandler.ProcessingHandlerResponse.FAIL) {
                log.error("Processing exception handler is set to fail upon" +
                        " a processing error. If you would rather have the streaming pipeline" +
                        " continue after a processing error, please set the " +
                        PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG + " appropriately.");

                throw createStreamsException(node.name(), processingException);
            } else {
                droppedRecordsSensor.record();
            }
        } finally {
            processorContext.setCurrentNode(null);
        }
    }

    private StreamsException createStreamsException(final String processorName, final Throwable cause) {
        return new StreamsException(String.format("%sException caught while punctuating processor '%s'", logPrefix, processorName), cause);
    }

    @SuppressWarnings("unchecked")
    private void updateProcessorContext(final ProcessorNode<?, ?, ?, ?> currNode,
                                        final long wallClockTime,
                                        final ProcessorRecordContext recordContext) {
        processorContext.setRecordContext(recordContext);
        processorContext.setCurrentNode(currNode);
        processorContext.setSystemTimeMs(wallClockTime);
    }

    /**
     * Return all the checkpointable offsets(written + consumed) to the state manager.
     * Currently only changelog topic offsets need to be checkpointed.
     */
    private Map<TopicPartition, Long> checkpointableOffsets() {
        final Map<TopicPartition, Long> checkpointableOffsets = new HashMap<>(recordCollector.offsets());
        for (final Map.Entry<TopicPartition, Long> entry : consumedOffsets.entrySet()) {
            checkpointableOffsets.putIfAbsent(entry.getKey(), entry.getValue());
        }

        log.debug("Checkpointable offsets {}", checkpointableOffsets);

        return checkpointableOffsets;
    }

    private void resetOffsetsIfNeededAndInitializeMetadata(final java.util.function.Consumer<Set<TopicPartition>> offsetResetter) {
        try {
            final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = mainConsumer.committed(inputPartitions());

            for (final Map.Entry<TopicPartition, OffsetAndMetadata> committedEntry : offsetsAndMetadata.entrySet()) {
                if (resetOffsetsForPartitions.contains(committedEntry.getKey())) {
                    final OffsetAndMetadata offsetAndMetadata = committedEntry.getValue();
                    if (offsetAndMetadata != null) {
                        mainConsumer.seek(committedEntry.getKey(), offsetAndMetadata);
                        resetOffsetsForPartitions.remove(committedEntry.getKey());
                    }
                }
            }

            offsetResetter.accept(resetOffsetsForPartitions);
            resetOffsetsForPartitions.clear();

            initializeTaskTimeAndProcessorMetadata(offsetsAndMetadata.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        } catch (final TimeoutException timeoutException) {
            log.warn(
                "Encountered {} while trying to fetch committed offsets, will retry initializing the metadata in the next loop." +
                    "\nConsider overwriting consumer config {} to a larger value to avoid timeout errors",
                time.toString(),
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);

            // re-throw to trigger `task.timeout.ms`
            throw timeoutException;
        } catch (final KafkaException e) {
            throw new StreamsException(String.format("task [%s] Failed to initialize offsets for %s", id, inputPartitions()), e);
        }
    }

    private void initializeTaskTimeAndProcessorMetadata(final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata) {
        final ProcessorMetadata finalProcessMetadata = new ProcessorMetadata();
        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetsAndMetadata.entrySet()) {
            final TopicPartition partition = entry.getKey();
            final OffsetAndMetadata metadata = entry.getValue();

            if (metadata != null) {
                final TopicPartitionMetadata committedTimestampAndMeta = TopicPartitionMetadata.decode(metadata.metadata());
                final long committedTimestamp = committedTimestampAndMeta.partitionTime();
                partitionGroup.setPartitionTime(partition, committedTimestamp);
                log.debug("A committed timestamp was detected: setting the partition time of partition {}"
                    + " to {} in stream task {}", partition, committedTimestamp, id);

                final ProcessorMetadata processorMetadata = committedTimestampAndMeta.processorMetadata();
                finalProcessMetadata.update(processorMetadata);
            } else {
                log.debug("No committed timestamp was found in metadata for partition {}", partition);
            }
        }
        processorContext.setProcessorMetadata(finalProcessMetadata);

        final Set<TopicPartition> nonCommitted = new HashSet<>(inputPartitions());
        nonCommitted.removeAll(offsetsAndMetadata.keySet());
        for (final TopicPartition partition : nonCommitted) {
            log.debug("No committed offset for partition {}, therefore no timestamp can be found for this partition", partition);
        }
    }

    @Override
    public Map<TopicPartition, Long> purgeableOffsets() {
        final Map<TopicPartition, Long> purgeableConsumedOffsets = new HashMap<>();
        for (final Map.Entry<TopicPartition, Long> entry : committedOffsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            if (topology.isRepartitionTopic(tp.topic())) {
                // committedOffsets map is initialized at -1 so no purging until there's a committed offset
                if (entry.getValue() > -1) {
                    purgeableConsumedOffsets.put(tp, entry.getValue());
                }
            }
        }

        return purgeableConsumedOffsets;
    }

    @SuppressWarnings("unchecked")
    private void initializeTopology() {
        // initialize the task by initializing all its processor nodes in the topology
        log.trace("Initializing processor nodes of the topology");
        for (final ProcessorNode<?, ?, ?, ?> node : topology.processors()) {
            processorContext.setCurrentNode(node);
            try {
                node.init(processorContext, processingExceptionHandler);
            } finally {
                processorContext.setCurrentNode(null);
            }
        }
    }

    /**
     * Adds records to queues. If a record has an invalid (i.e., negative) timestamp, the record is skipped
     * and not added to the queue for processing
     *
     * @param partition the partition
     * @param records   the records
     */
    @Override
    public void addRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        final int newQueueSize = partitionGroup.addRawRecords(partition, records);

        if (log.isTraceEnabled()) {
            log.trace("Added records into the buffered queue of partition {}, new queue size is {}", partition, newQueueSize);
        }

        // if after adding these records, its partition queue's buffered size has been
        // increased beyond the threshold, we can then pause the consumption for this partition
        if (newQueueSize > maxBufferedSize) {
            mainConsumer.pause(singleton(partition));
        }
    }

    public void updateNextOffsets(final TopicPartition partition, final OffsetAndMetadata offsetAndMetadata) {
        nextOffsetsAndMetadataToBeConsumed.put(partition, offsetAndMetadata);
    }

    /**
     * Schedules a punctuation for the processor
     *
     * @param interval the interval in milliseconds
     * @param type     the punctuation type
     * @throws IllegalStateException if the current node is not null
     */
    public Cancellable schedule(final long interval, final PunctuationType type, final Punctuator punctuator) {
        switch (type) {
            case STREAM_TIME:
                // align punctuation to 0L, punctuate as soon as we have data
                return schedule(0L, interval, type, punctuator);
            case WALL_CLOCK_TIME:
                // align punctuation to now, punctuate after interval has elapsed
                return schedule(time.milliseconds() + interval, interval, type, punctuator);
            default:
                throw new IllegalArgumentException("Unrecognized PunctuationType: " + type);
        }
    }

    /**
     * Schedules a punctuation for the processor
     *
     * @param startTime time of the first punctuation
     * @param interval  the interval in milliseconds
     * @param type      the punctuation type
     * @throws IllegalStateException if the current node is not null
     */
    private Cancellable schedule(final long startTime, final long interval, final PunctuationType type, final Punctuator punctuator) {
        if (processorContext.currentNode() == null) {
            throw new IllegalStateException(String.format("%sCurrent node is null", logPrefix));
        }

        final PunctuationSchedule schedule = new PunctuationSchedule(processorContext.currentNode(), startTime, interval, punctuator);

        switch (type) {
            case STREAM_TIME:
                // STREAM_TIME punctuation is data driven, will first punctuate as soon as stream-time is known and >= time,
                // stream-time is known when we have received at least one record from each input topic
                return streamTimePunctuationQueue.schedule(schedule);
            case WALL_CLOCK_TIME:
                // WALL_CLOCK_TIME is driven by the wall clock time, will first punctuate when now >= time
                return systemTimePunctuationQueue.schedule(schedule);
            default:
                throw new IllegalArgumentException("Unrecognized PunctuationType: " + type);
        }
    }

    /**
     * Possibly trigger registered stream-time punctuation functions if
     * current partition group timestamp has reached the defined stamp
     * Note, this is only called in the presence of new records
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    public boolean maybePunctuateStreamTime() {
        final long streamTime = partitionGroup.streamTime();

        // if the timestamp is not known yet, meaning there is not enough data accumulated
        // to reason stream partition time, then skip.
        if (streamTime == RecordQueue.UNKNOWN) {
            return false;
        } else {
            final boolean punctuated = streamTimePunctuationQueue.maybePunctuate(streamTime, PunctuationType.STREAM_TIME, this);

            if (punctuated) {
                commitNeeded = true;
            }

            return punctuated;
        }
    }

    public boolean canPunctuateStreamTime() {
        final long streamTime = partitionGroup.streamTime();
        return streamTimePunctuationQueue.canPunctuate(streamTime);
    }

    /**
     * Possibly trigger registered system-time punctuation functions if
     * current system timestamp has reached the defined stamp
     * Note, this is called irrespective of the presence of new records
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    public boolean maybePunctuateSystemTime() {
        final long systemTime = time.milliseconds();

        final boolean punctuated = systemTimePunctuationQueue.maybePunctuate(systemTime, PunctuationType.WALL_CLOCK_TIME, this);

        if (punctuated) {
            commitNeeded = true;
        }

        return punctuated;
    }

    public boolean canPunctuateSystemTime() {
        final long systemTime = time.milliseconds();
        return systemTimePunctuationQueue.canPunctuate(systemTime);
    }

    void maybeRecordE2ELatency(final long recordTimestamp, final long now, final String nodeName) {
        final Sensor e2eLatencySensor = e2eLatencySensors.get(nodeName);
        if (e2eLatencySensor == null) {
            throw new IllegalStateException("Requested to record e2e latency but could not find sensor for node " + nodeName);
        } else if (e2eLatencySensor.shouldRecord() && e2eLatencySensor.hasMetrics()) {
            e2eLatencySensor.record(now - recordTimestamp, now);
        }
    }

    /**
     * Request committing the current task's state
     */
    void requestCommit() {
        commitRequested = true;
    }

    /**
     * Whether or not a request has been made to commit the current state
     */
    @Override
    public boolean commitRequested() {
        return commitRequested;
    }

    @SuppressWarnings("rawtypes")
    public InternalProcessorContext processorContext() {
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

        // print assigned partitions
        final Set<TopicPartition> partitions = inputPartitions();
        if (partitions != null && !partitions.isEmpty()) {
            sb.append(indent).append("Partitions [");
            for (final TopicPartition topicPartition : partitions) {
                sb.append(topicPartition).append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append("]\n");
        }
        return sb.toString();
    }

    @Override
    public boolean commitNeeded() {
        // we need to do an extra check if the flag was false, that
        // if the consumer position has been updated; this is because
        // there may be non data records such as control markers bypassed
        if (commitNeeded) {
            return true;
        } else {
            for (final Map.Entry<TopicPartition, Long> entry : consumedOffsets.entrySet()) {
                final TopicPartition partition = entry.getKey();
                try {
                    final long offset = mainConsumer.position(partition);

                    // note the position in consumer is the "next" record to fetch,
                    // so it should be larger than the consumed offset by 1; if it is
                    // more than 1 it means there are control records, which the consumer skips over silently
                    if (offset > entry.getValue() + 1) {
                        commitNeeded = true;
                        entry.setValue(offset - 1);
                    }
                } catch (final TimeoutException swallow) {
                    log.debug(
                        String.format("Could not get consumer position for partition %s", partition),
                        swallow
                    );
                } catch (final KafkaException fatal) {
                    throw new StreamsException(fatal);
                }
            }

            return commitNeeded;
        }
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        if (state() == State.RUNNING) {
            // if we are in running state, just return the latest offset sentinel indicating
            // we should be at the end of the changelog
            return changelogPartitions().stream()
                                        .collect(Collectors.toMap(Function.identity(), tp -> Task.LATEST_OFFSET));
        } else {
            return Collections.unmodifiableMap(stateMgr.changelogOffsets());
        }
    }

    @Override
    public Map<TopicPartition, Long> committedOffsets() {
        return Collections.unmodifiableMap(committedOffsets);
    }

    @Override
    public Map<TopicPartition, Long> highWaterMark() {
        return Collections.unmodifiableMap(highWatermark);
    }

    private void transitToSuspend() {
        log.info("Suspended from {}", state());
        transitionTo(State.SUSPENDED);
        timeCurrentIdlingStarted = Optional.of(System.currentTimeMillis());
    }

    @Override
    public Optional<Long> timeCurrentIdlingStarted() {
        return timeCurrentIdlingStarted;
    }

    public void updateCommittedOffsets(final TopicPartition topicPartition, final Long offset) {
        committedOffsets.put(topicPartition, offset);
    }

    public void updateEndOffsets(final TopicPartition topicPartition, final Long offset) {
        highWatermark.put(topicPartition, offset);
    }

    public boolean hasRecordsQueued() {
        return numBuffered() > 0;
    }

    RecordCollector recordCollector() {
        return recordCollector;
    }

    // below are visible for testing only
    int numBuffered() {
        return partitionGroup.numBuffered();
    }

    long streamTime() {
        return partitionGroup.streamTime();
    }

    private class RecordQueueCreator {
        private final LogContext logContext;
        private final TimestampExtractor defaultTimestampExtractor;
        private final DeserializationExceptionHandler defaultDeserializationExceptionHandler;

        private RecordQueueCreator(final LogContext logContext,
                                   final TimestampExtractor defaultTimestampExtractor,
                                   final DeserializationExceptionHandler defaultDeserializationExceptionHandler) {
            this.logContext = logContext;
            this.defaultTimestampExtractor = defaultTimestampExtractor;
            this.defaultDeserializationExceptionHandler = defaultDeserializationExceptionHandler;
        }

        public RecordQueue createQueue(final TopicPartition partition) {
            final SourceNode<?, ?> source = topology.source(partition.topic());
            if (source == null) {
                throw new TopologyException(
                        "Topic " + partition.topic() + " is unknown to the topology. " +
                                "This may happen if different KafkaStreams instances of the same application execute different Topologies. " +
                                "Note that Topologies are only identical if all operators are added in the same order."
                );
            }

            final TimestampExtractor sourceTimestampExtractor = source.timestampExtractor();
            final TimestampExtractor timestampExtractor = sourceTimestampExtractor != null ? sourceTimestampExtractor : defaultTimestampExtractor;
            return new RecordQueue(
                    partition,
                    source,
                    timestampExtractor,
                    defaultDeserializationExceptionHandler,
                    processorContext,
                    logContext
            );
        }
    }
}
