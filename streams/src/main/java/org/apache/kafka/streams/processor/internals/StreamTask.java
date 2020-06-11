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
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.metrics.ProcessorNodeMetrics;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

/**
 * A StreamTask is associated with a {@link PartitionGroup}, and is assigned to a StreamThread for processing.
 */
public class StreamTask extends AbstractTask implements ProcessorNodePunctuator, Task {

    private static final ConsumerRecord<Object, Object> DUMMY_RECORD = new ConsumerRecord<>(ProcessorContextImpl.NONEXIST_TOPIC, -1, -1L, null, null);
    // visible for testing
    static final byte LATEST_MAGIC_BYTE = 1;

    private final String logPrefix;
    private final Logger log;

    private final InternalProcessorContext processorContext;
    private final Time time;
    private final Consumer<byte[], byte[]> mainConsumer;
    private final RecordCollector recordCollector;
    private final PartitionGroup.RecordInfo recordInfo;
    private final RecordQueueCreator recordQueueCreator;
    private final PartitionGroup partitionGroup;
    private final PunctuationQueue streamTimePunctuationQueue;
    private final PunctuationQueue systemTimePunctuationQueue;
    private final Map<TopicPartition, Long> consumedOffsets;

    // we want to abstract eos logic out of StreamTask, however
    // there's still an optimization that requires this info to be
    // leaked into this class, which is to checkpoint after committing if EOS is not enabled.
    private final boolean eosEnabled;
    private final long maxTaskIdleMs;
    private final int maxBufferedSize;

    private final Sensor closeTaskSensor;
    private final Sensor processRatioSensor;
    private final Sensor processLatencySensor;
    private final Sensor punctuateLatencySensor;
    private final Sensor bufferedRecordsSensor;
    private final Sensor enforcedProcessingSensor;
    private final Map<String, Sensor> e2eLatencySensors = new HashMap<>();

    private boolean commitNeeded = false;
    private boolean commitRequested = false;
    private long idleStartTimeMs = RecordQueue.UNKNOWN;
    private long processTimeMs = 0L;
    private Map<TopicPartition, Long> checkpoint = null;

    public StreamTask(final TaskId id,
                      final ProcessorTopology topology,
                      final StateDirectory stateDirectory,
                      final ProcessorStateManager stateMgr,
                      final Set<TopicPartition> partitions,
                      final StreamsConfig config,
                      final InternalProcessorContext processorContext,
                      final ThreadCache cache,
                      final StreamsMetricsImpl streamsMetrics,
                      final Time time,
                      final Consumer<byte[], byte[]> mainConsumer,
                      final RecordCollector recordCollector) {
        super(id, topology, stateDirectory, stateMgr, partitions);

        final String taskId = id.toString();
        final String threadId = Thread.currentThread().getName();

        // logging
        final String threadIdPrefix = String.format("stream-thread [%s] ", threadId);
        logPrefix = threadIdPrefix + String.format("%s [%s] ", "task", taskId);
        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());

        // members
        this.processorContext = processorContext;
        processorContext.transitionToActive(this, recordCollector, cache);
        this.time = time;
        this.mainConsumer = mainConsumer;
        this.recordCollector = recordCollector;

        recordInfo = new PartitionGroup.RecordInfo();
        recordQueueCreator = new RecordQueueCreator(logContext, config.defaultTimestampExtractor(), config.defaultDeserializationExceptionHandler());
        partitionGroup = new PartitionGroup(
            createPartitionQueues(),
            TaskMetrics.recordLatenessSensor(threadId, taskId, streamsMetrics)
        );
        streamTimePunctuationQueue = new PunctuationQueue();
        systemTimePunctuationQueue = new PunctuationQueue();
        consumedOffsets = new HashMap<>();

        stateMgr.registerGlobalStateStores(topology.globalStateStores());

        // config
        eosEnabled = StreamThread.eosEnabled(config);
        maxTaskIdleMs = config.getLong(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
        maxBufferedSize = config.getInt(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG);

        // metrics
        closeTaskSensor = ThreadMetrics.closeTaskSensor(threadId, streamsMetrics);
        processRatioSensor = TaskMetrics.activeProcessRatioSensor(threadId, taskId, streamsMetrics);
        processLatencySensor = TaskMetrics.processLatencySensor(threadId, taskId, streamsMetrics);
        punctuateLatencySensor = TaskMetrics.punctuateSensor(threadId, taskId, streamsMetrics);
        bufferedRecordsSensor = TaskMetrics.activeBufferedRecordsSensor(threadId, taskId, streamsMetrics);

        if (streamsMetrics.version() == Version.FROM_0100_TO_24) {
            final Sensor parent = ThreadMetrics.commitOverTasksSensor(threadId, streamsMetrics);
            enforcedProcessingSensor = TaskMetrics.enforcedProcessingSensor(threadId, taskId, streamsMetrics, parent);
        } else {
            enforcedProcessingSensor = TaskMetrics.enforcedProcessingSensor(threadId, taskId, streamsMetrics);
        }

        for (final String terminalNode : topology.terminalNodes()) {
            e2eLatencySensors.put(
                terminalNode,
                ProcessorNodeMetrics.recordE2ELatencySensor(threadId, taskId, terminalNode, RecordingLevel.INFO, streamsMetrics)
            );
        }

        for (final ProcessorNode<?, ?> sourceNode : topology.sources()) {
            final String processorId = sourceNode.name();
            e2eLatencySensors.put(
                processorId,
                ProcessorNodeMetrics.recordE2ELatencySensor(threadId, taskId, processorId, RecordingLevel.INFO, streamsMetrics)
            );
        }
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

    /**
     * @throws LockException    could happen when multi-threads within the single instance, could retry
     * @throws TimeoutException if initializing record collector timed out
     * @throws StreamsException fatal error, should close the thread
     */
    @Override
    public void initializeIfNeeded() {
        if (state() == State.CREATED) {
            recordCollector.initialize();

            StateManagerUtil.registerStateStores(log, logPrefix, topology, stateMgr, stateDirectory, processorContext);

            transitionTo(State.RESTORING);

            log.info("Initialized");
        }
    }

    /**
     * @throws TimeoutException if fetching committed offsets timed out
     */
    @Override
    public void completeRestoration() {
        switch (state()) {
            case RUNNING:
                return;

            case RESTORING:
                initializeMetadata();
                initializeTopology();
                processorContext.initialize();
                idleStartTimeMs = RecordQueue.UNKNOWN;

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

    private void initializeMetadata() {
        try {
            final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = mainConsumer.committed(inputPartitions()).entrySet().stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            initializeTaskTime(offsetsAndMetadata);
        } catch (final TimeoutException e) {
            log.warn("Encountered {} while trying to fetch committed offsets, will retry initializing the metadata in the next loop." +
                    "\nConsider overwriting consumer config {} to a larger value to avoid timeout errors",
                e.toString(),
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);

            throw e;
        } catch (final KafkaException e) {
            throw new StreamsException(String.format("task [%s] Failed to initialize offsets for %s", id, inputPartitions()), e);
        }
    }

    private void initializeTaskTime(final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata) {
        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetsAndMetadata.entrySet()) {
            final TopicPartition partition = entry.getKey();
            final OffsetAndMetadata metadata = entry.getValue();

            if (metadata != null) {
                final long committedTimestamp = decodeTimestamp(metadata.metadata());
                partitionGroup.setPartitionTime(partition, committedTimestamp);
                log.debug("A committed timestamp was detected: setting the partition time of partition {}"
                    + " to {} in stream task {}", partition, committedTimestamp, id);
            } else {
                log.debug("No committed timestamp was found in metadata for partition {}", partition);
            }
        }

        final Set<TopicPartition> nonCommitted = new HashSet<>(inputPartitions());
        nonCommitted.removeAll(offsetsAndMetadata.keySet());
        for (final TopicPartition partition : nonCommitted) {
            log.debug("No committed offset for partition {}, therefore no timestamp can be found for this partition", partition);
        }
    }

    private void initializeTopology() {
        // initialize the task by initializing all its processor nodes in the topology
        log.trace("Initializing processor nodes of the topology");
        for (final ProcessorNode<?, ?> node : topology.processors()) {
            processorContext.setCurrentNode(node);
            try {
                node.init(processorContext);
            } finally {
                processorContext.setCurrentNode(null);
            }
        }
    }

    @Override
    public void suspend() {
        switch (state()) {
            case CREATED:
            case SUSPENDED:
                log.info("Skip suspending since state is {}", state());

                break;

            case RESTORING:
                transitionTo(State.SUSPENDED);
                log.info("Suspended restoring");

                break;

            case RUNNING:
                try {
                    // use try-catch to ensure state transition to SUSPENDED even if user code throws in `Processor#close()`
                    closeTopology();
                } finally {
                    transitionTo(State.SUSPENDED);
                    log.info("Suspended running");
                }

                break;

            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while suspending active task " + id);

            default:
                throw new IllegalStateException("Unknown state " + state() + " while suspending active task " + id);
        }
    }

    private void closeTopology() {
        log.trace("Closing processor topology");

        // close the processors
        // make sure close() is called for each node even when there is a RuntimeException
        RuntimeException exception = null;
        for (final ProcessorNode<?, ?> node : topology.processors()) {
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
                transitionTo(State.RESTORING);
                log.info("Resumed to restoring state");

                break;

            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while resuming active task " + id);

            default:
                throw new IllegalStateException("Unknown state " + state() + " while resuming active task " + id);
        }
    }

    @Override
    public void closeDirty() {
        close(false);
        log.info("Closed dirty");
    }

    @Override
    public void closeClean() {
        close(true);
        log.info("Closed clean");
    }

    /**
     * the following order must be followed:
     *  1. checkpoint the state manager -- even if we crash before this step, EOS is still guaranteed
     *  2. then if we are closing on EOS and dirty, wipe out the state store directory
     *  3. finally release the state manager lock
     */
    private void close(final boolean clean) {
        if (clean) {
            executeAndMaybeSwallow(true, this::writeCheckpointIfNeed, "state manager checkpoint", log);
        }

        switch (state()) {
            case CREATED:
            case RESTORING:
            case SUSPENDED:
                // first close state manager (which is idempotent) then close the record collector
                // if the latter throws and we re-close dirty which would close the state manager again.
                executeAndMaybeSwallow(
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

                executeAndMaybeSwallow(clean, recordCollector::close, "record collector close", log);

                break;

            case CLOSED:
                log.trace("Skip closing since state is {}", state());
                return;

            case RUNNING:
                throw new IllegalStateException("Illegal state " + state() + " while closing active task " + id);

            default:
                throw new IllegalStateException("Unknown state " + state() + " while closing active task " + id);
        }

        partitionGroup.clear();
        closeTaskSensor.record();

        transitionTo(State.CLOSED);
    }

    @Override
    public void closeAndRecycleState() {
        suspend();
        prepareCommit();
        writeCheckpointIfNeed();

        switch (state()) {
            case CREATED:
            case SUSPENDED:
                stateMgr.recycle();
                recordCollector.close();

                break;

            case RESTORING: // we should have transitioned to `SUSPENDED` already
            case RUNNING: // we should have transitioned to `SUSPENDED` already
            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while recycling active task " + id);
            default:
                throw new IllegalStateException("Unknown state " + state() + " while recycling active task " + id);
        }

        partitionGroup.clear();
        closeTaskSensor.record();

        transitionTo(State.CLOSED);

        log.info("Closed clean and recycled state");
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

    /**
     * Process one record.
     *
     * @return true if this method processes a record, false if it does not process a record.
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    @SuppressWarnings("unchecked")
    public boolean process(final long wallClockTime) {
        if (!isProcessable(wallClockTime)) {
            return false;
        }

        // get the next record to process
        final StampedRecord record = partitionGroup.nextRecord(recordInfo, wallClockTime);

        // if there is no record to process, return immediately
        if (record == null) {
            return false;
        }

        try {
            // process the record by passing to the source node of the topology
            final ProcessorNode<Object, Object> currNode = (ProcessorNode<Object, Object>) recordInfo.node();
            final TopicPartition partition = recordInfo.partition();

            log.trace("Start processing one record [{}]", record);

            updateProcessorContext(record, currNode, wallClockTime);
            maybeRecordE2ELatency(record.timestamp, wallClockTime, currNode.name());
            maybeMeasureLatency(() -> currNode.process(record.key(), record.value()), time, processLatencySensor);

            log.trace("Completed processing one record [{}]", record);

            // update the consumed offset map after processing is done
            consumedOffsets.put(partition, record.offset());
            commitNeeded = true;

            // after processing this record, if its partition queue's buffered size has been
            // decreased to the threshold, we can then resume the consumption on this partition
            if (recordInfo.queue().size() == maxBufferedSize) {
                mainConsumer.resume(singleton(partition));
            }
        } catch (final StreamsException e) {
            throw e;
        } catch (final RuntimeException e) {
            final String stackTrace = getStacktraceString(e);
            throw new StreamsException(String.format("Exception caught in process. taskId=%s, " +
                    "processor=%s, topic=%s, partition=%d, offset=%d, stacktrace=%s",
                id(),
                processorContext.currentNode().name(),
                record.topic(),
                record.partition(),
                record.offset(),
                stackTrace
            ), e);
        } finally {
            processorContext.setCurrentNode(null);
        }

        return true;
    }

    private String getStacktraceString(final RuntimeException e) {
        String stacktrace = null;
        try (final StringWriter stringWriter = new StringWriter();
             final PrintWriter printWriter = new PrintWriter(stringWriter)) {
            e.printStackTrace(printWriter);
            stacktrace = stringWriter.toString();
        } catch (final IOException ioe) {
            log.error("Encountered error extracting stacktrace from this exception", ioe);
        }
        return stacktrace;
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
     * Possibly trigger registered stream-time punctuation functions if
     * current partition group timestamp has reached the defined stamp
     * Note, this is only called in the presence of new records
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    @Override
    public boolean maybePunctuateStreamTime() {
        final long streamTime = partitionGroup.streamTime();

        // if the timestamp is not known yet, meaning there is not enough data accumulated
        // to reason stream partition time, then skip.
        if (streamTime == RecordQueue.UNKNOWN) {
            return false;
        } else {
            final boolean punctuated = streamTimePunctuationQueue.mayPunctuate(streamTime, PunctuationType.STREAM_TIME, this);

            if (punctuated) {
                commitNeeded = true;
            }

            return punctuated;
        }
    }

    /**
     * Possibly trigger registered system-time punctuation functions if
     * current system timestamp has reached the defined stamp
     * Note, this is called irrespective of the presence of new records
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    @Override
    public boolean maybePunctuateSystemTime() {
        final long systemTime = time.milliseconds();

        final boolean punctuated = systemTimePunctuationQueue.mayPunctuate(systemTime, PunctuationType.WALL_CLOCK_TIME, this);

        if (punctuated) {
            commitNeeded = true;
        }

        return punctuated;
    }

    /**
     * @throws IllegalStateException if the current node is not null
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    @Override
    public void punctuate(final ProcessorNode<?, ?> node,
                          final long timestamp,
                          final PunctuationType type,
                          final Punctuator punctuator) {
        if (processorContext.currentNode() != null) {
            throw new IllegalStateException(String.format("%sCurrent node is not null", logPrefix));
        }

        updateProcessorContext(new StampedRecord(DUMMY_RECORD, timestamp), node, time.milliseconds());

        if (log.isTraceEnabled()) {
            log.trace("Punctuating processor {} with timestamp {} and punctuation type {}", node.name(), timestamp, type);
        }

        try {
            maybeMeasureLatency(() -> node.punctuate(timestamp, punctuator), time, punctuateLatencySensor);
        } catch (final StreamsException e) {
            throw e;
        } catch (final RuntimeException e) {
            throw new StreamsException(String.format("%sException caught while punctuating processor '%s'", logPrefix, node.name()), e);
        } finally {
            processorContext.setCurrentNode(null);
        }
    }

    @Override
    public boolean commitNeeded() {
        return commitNeeded;
    }

    /**
     * Whether or not a request has been made to commit the current state
     */
    @Override
    public boolean commitRequested() {
        return commitRequested;
    }

    /**
     * @return offsets that should be committed for this task
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> prepareCommit() {
        switch (state()) {
            case RUNNING:
            case RESTORING:
            case SUSPENDED:
                maybeScheduleCheckpoint();
                stateMgr.flush();
                recordCollector.flush();

                log.debug("Prepared task for committing");

                break;

            case CREATED:
            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while preparing active task " + id + " for committing");

            default:
                throw new IllegalStateException("Unknown state " + state() + " while preparing active task " + id + " for committing");
        }

        return committableOffsetsAndMetadata();
    }

    private void maybeScheduleCheckpoint() {
        switch (state()) {
            case RESTORING:
            case SUSPENDED:
                this.checkpoint = checkpointableOffsets();

                break;

            case RUNNING:
                if (!eosEnabled) {
                    this.checkpoint = checkpointableOffsets();
                }

                break;

            case CREATED:
            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while scheduling checkpoint for active task " + id);

            default:
                throw new IllegalStateException("Unknown state " + state() + " while scheduling checkpoint for active task " + id);
        }
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
        return checkpointableOffsets;
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
                final Map<TopicPartition, Long> partitionTimes = extractPartitionTimes();

                committableOffsets = new HashMap<>(consumedOffsets.size());
                for (final Map.Entry<TopicPartition, Long> entry : consumedOffsets.entrySet()) {
                    final TopicPartition partition = entry.getKey();
                    Long offset = partitionGroup.headRecordOffset(partition);
                    if (offset == null) {
                        try {
                            offset = mainConsumer.position(partition);
                        } catch (final TimeoutException error) {
                            // the `consumer.position()` call should never block, because we know that we did process data
                            // for the requested partition and thus the consumer should have a valid local position
                            // that it can return immediately

                            // hence, a `TimeoutException` indicates a bug and thus we rethrow it as fatal `IllegalStateException`
                            throw new IllegalStateException(error);
                        } catch (final KafkaException fatal) {
                            throw new StreamsException(fatal);
                        }
                    }
                    final long partitionTime = partitionTimes.get(partition);
                    committableOffsets.put(partition, new OffsetAndMetadata(offset, encodeTimestamp(partitionTime)));
                }

                break;

            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while getting commitable offsets for active task " + id);

            default:
                throw new IllegalStateException("Unknown state " + state() + " while post committing active task " + id);
        }

        return committableOffsets;
    }

    private Map<TopicPartition, Long> extractPartitionTimes() {
        final Map<TopicPartition, Long> partitionTimes = new HashMap<>();
        for (final TopicPartition partition : partitionGroup.partitions()) {
            partitionTimes.put(partition, partitionGroup.partitionTimestamp(partition));
        }
        return partitionTimes;
    }

    @Override
    public void postCommit() {
        commitRequested = false;
        commitNeeded = false;

        switch (state()) {
            case RESTORING:
                writeCheckpointIfNeed();

                break;

            case RUNNING:
                if (!eosEnabled) { // if RUNNING, checkpoint only for non-eos
                    writeCheckpointIfNeed();
                }

                break;

            case SUSPENDED:
                writeCheckpointIfNeed();
                // we cannot `clear()` the `PartitionGroup` in `suspend()` already, but only after committing,
                // because otherwise we loose the partition-time information
                partitionGroup.clear();

                break;

            case CREATED:
            case CLOSED:
                throw new IllegalStateException("Illegal state " + state() + " while post committing active task " + id);

            default:
                throw new IllegalStateException("Unknown state " + state() + " while post committing active task " + id);
        }

        log.debug("Committed");
    }

    @Override
    public Map<TopicPartition, Long> purgeableOffsets() {
        final Map<TopicPartition, Long> purgeableConsumedOffsets = new HashMap<>();
        for (final Map.Entry<TopicPartition, Long> entry : consumedOffsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            if (topology.isRepartitionTopic(tp.topic())) {
                purgeableConsumedOffsets.put(tp, entry.getValue() + 1);
            }
        }

        return purgeableConsumedOffsets;
    }

    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public void updateInputPartitions(final Set<TopicPartition> topicPartitions, final Map<String, List<String>> nodeToSourceTopics) {
        super.updateInputPartitions(topicPartitions, nodeToSourceTopics);
        partitionGroup.updatePartitions(topicPartitions, recordQueueCreator::createQueue);
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

    private void writeCheckpointIfNeed() {
        if (commitNeeded) {
            throw new IllegalStateException("A checkpoint should only be written if no commit is needed.");
        }
        if (checkpoint != null) {
            stateMgr.checkpoint(checkpoint);
            checkpoint = null;
        }
    }

    /**
     * An active task is processable if its buffer contains data for all of its input
     * source topic partitions, or if it is enforced to be processable
     */
    public boolean isProcessable(final long wallClockTime) {
        if (state() == State.CLOSED) {
            // a task is only closing / closed when 1) task manager is closing, 2) a rebalance is undergoing;
            // in either case we can just log it and move on without notifying the thread since the consumer
            // would soon be updated to not return any records for this task anymore.
            log.info("Stream task {} is already in {} state, skip processing it.", id(), state());

            return false;
        }

        if (partitionGroup.allPartitionsBuffered()) {
            idleStartTimeMs = RecordQueue.UNKNOWN;
            return true;
        } else if (partitionGroup.numBuffered() > 0) {
            if (idleStartTimeMs == RecordQueue.UNKNOWN) {
                idleStartTimeMs = wallClockTime;
            }

            if (wallClockTime - idleStartTimeMs >= maxTaskIdleMs) {
                enforcedProcessingSensor.record(1.0d, wallClockTime);
                return true;
            } else {
                return false;
            }
        } else {
            // there's no data in any of the topics; we should reset the enforced
            // processing timer
            idleStartTimeMs = RecordQueue.UNKNOWN;
            return false;
        }
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

    private void updateProcessorContext(final StampedRecord record, final ProcessorNode<?, ?> currNode, final long wallClockTime) {
        processorContext.setRecordContext(
            new ProcessorRecordContext(
                record.timestamp,
                record.offset(),
                record.partition(),
                record.topic(),
                record.headers()));
        processorContext.setCurrentNode(currNode);
        processorContext.setSystemTimeMs(wallClockTime);
    }

    /**
     * Request committing the current task's state
     */
    void requestCommit() {
        commitRequested = true;
    }

    void maybeRecordE2ELatency(final long recordTimestamp, final String nodeName) {
        maybeRecordE2ELatency(recordTimestamp, time.milliseconds(), nodeName);
    }

    private void maybeRecordE2ELatency(final long recordTimestamp, final long now, final String nodeName) {
        final Sensor e2eLatencySensor = e2eLatencySensors.get(nodeName);
        if (e2eLatencySensor == null) {
            throw new IllegalStateException("Requested to record e2e latency but could not find sensor for node " + nodeName);
        } else if (e2eLatencySensor.shouldRecord() && e2eLatencySensor.hasMetrics()) {
            e2eLatencySensor.record(now - recordTimestamp, now);
        }
    }

    public InternalProcessorContext processorContext() {
        return processorContext;
    }

    public boolean hasRecordsQueued() {
        return numBuffered() > 0;
    }

    // visible for testing
    static String encodeTimestamp(final long partitionTime) {
        final ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.put(LATEST_MAGIC_BYTE);
        buffer.putLong(partitionTime);
        return Base64.getEncoder().encodeToString(buffer.array());
    }

    // visible for testing
    long decodeTimestamp(final String encryptedString) {
        if (encryptedString.isEmpty()) {
            return RecordQueue.UNKNOWN;
        }
        final ByteBuffer buffer = ByteBuffer.wrap(Base64.getDecoder().decode(encryptedString));
        final byte version = buffer.get();
        switch (version) {
            case LATEST_MAGIC_BYTE:
                return buffer.getLong();
            default:
                log.warn("Unsupported offset metadata version found. Supported version {}. Found version {}.",
                    LATEST_MAGIC_BYTE, version);
                return RecordQueue.UNKNOWN;
        }
    }



    // for testing only

    RecordCollector recordCollector() {
        return recordCollector;
    }

    int numBuffered() {
        return partitionGroup.numBuffered();
    }

    long streamTime() {
        return partitionGroup.streamTime();
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
            final TimestampExtractor sourceTimestampExtractor = source.getTimestampExtractor();
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
