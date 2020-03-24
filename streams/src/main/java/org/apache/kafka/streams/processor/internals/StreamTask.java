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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
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
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

/**
 * A StreamTask is associated with a {@link PartitionGroup}, and is assigned to a StreamThread for processing.
 */
public class StreamTask extends AbstractTask implements ProcessorNodePunctuator, Task {

    private static final ConsumerRecord<Object, Object> DUMMY_RECORD = new ConsumerRecord<>(ProcessorContextImpl.NONEXIST_TOPIC, -1, -1L, null, null);
    // visible for testing
    static final byte LATEST_MAGIC_BYTE = 1;

    private final Time time;
    private final Logger log;
    private final String logPrefix;
    private final Consumer<byte[], byte[]> mainConsumer;

    // we want to abstract eos logic out of StreamTask, however
    // there's still an optimization that requires this info to be
    // leaked into this class, which is to checkpoint after committing if EOS is not enabled.
    private final boolean eosEnabled;

    private final long maxTaskIdleMs;
    private final int maxBufferedSize;
    private final PartitionGroup partitionGroup;
    private final RecordCollector recordCollector;
    private final PartitionGroup.RecordInfo recordInfo;
    private final Map<TopicPartition, Long> consumedOffsets;
    private final PunctuationQueue streamTimePunctuationQueue;
    private final PunctuationQueue systemTimePunctuationQueue;

    private final Sensor closeTaskSensor;
    private final Sensor processLatencySensor;
    private final Sensor punctuateLatencySensor;
    private final Sensor enforcedProcessingSensor;
    private final InternalProcessorContext processorContext;

    private long idleStartTime;
    private boolean commitNeeded = false;
    private boolean commitRequested = false;

    public StreamTask(final TaskId id,
                      final Set<TopicPartition> partitions,
                      final ProcessorTopology topology,
                      final Consumer<byte[], byte[]> mainConsumer,
                      final StreamsConfig config,
                      final StreamsMetricsImpl streamsMetrics,
                      final StateDirectory stateDirectory,
                      final ThreadCache cache,
                      final Time time,
                      final ProcessorStateManager stateMgr,
                      final RecordCollector recordCollector) {
        super(id, topology, stateDirectory, stateMgr, partitions);
        this.mainConsumer = mainConsumer;

        final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
        logPrefix = threadIdPrefix + String.format("%s [%s] ", "task", id);
        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());

        this.time = time;
        this.recordCollector = recordCollector;
        eosEnabled = StreamThread.eosEnabled(config);

        final String threadId = Thread.currentThread().getName();
        closeTaskSensor = ThreadMetrics.closeTaskSensor(threadId, streamsMetrics);
        final String taskId = id.toString();
        if (streamsMetrics.version() == Version.FROM_0100_TO_24) {
            final Sensor parent = ThreadMetrics.commitOverTasksSensor(threadId, streamsMetrics);
            enforcedProcessingSensor = TaskMetrics.enforcedProcessingSensor(threadId, taskId, streamsMetrics, parent);
        } else {
            enforcedProcessingSensor = TaskMetrics.enforcedProcessingSensor(threadId, taskId, streamsMetrics);
        }
        processLatencySensor = TaskMetrics.processLatencySensor(threadId, taskId, streamsMetrics);
        punctuateLatencySensor = TaskMetrics.punctuateSensor(threadId, taskId, streamsMetrics);

        streamTimePunctuationQueue = new PunctuationQueue();
        systemTimePunctuationQueue = new PunctuationQueue();
        maxTaskIdleMs = config.getLong(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
        maxBufferedSize = config.getInt(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG);

        // initialize the consumed and committed offset cache
        consumedOffsets = new HashMap<>();

        // create queues for each assigned partition and associate them
        // to corresponding source nodes in the processor topology
        final Map<TopicPartition, RecordQueue> partitionQueues = new HashMap<>();

        // initialize the topology with its own context
        processorContext = new ProcessorContextImpl(id, this, config, this.recordCollector, stateMgr, streamsMetrics, cache);

        final TimestampExtractor defaultTimestampExtractor = config.defaultTimestampExtractor();
        final DeserializationExceptionHandler defaultDeserializationExceptionHandler = config.defaultDeserializationExceptionHandler();
        for (final TopicPartition partition : partitions) {
            final SourceNode<?, ?> source = topology.source(partition.topic());
            final TimestampExtractor sourceTimestampExtractor = source.getTimestampExtractor();
            final TimestampExtractor timestampExtractor = sourceTimestampExtractor != null ? sourceTimestampExtractor : defaultTimestampExtractor;
            final RecordQueue queue = new RecordQueue(
                partition,
                source,
                timestampExtractor,
                defaultDeserializationExceptionHandler,
                processorContext,
                logContext
            );
            partitionQueues.put(partition, queue);
        }

        recordInfo = new PartitionGroup.RecordInfo();
        partitionGroup = new PartitionGroup(partitionQueues,
                                            TaskMetrics.recordLatenessSensor(threadId, taskId, streamsMetrics));

        stateMgr.registerGlobalStateStores(topology.globalStateStores());
    }

    @Override
    public boolean isActive() {
        return true;
    }

    /**
     * @throws LockException could happen when multi-threads within the single instance, could retry
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
        if (state() == State.RESTORING) {
            initializeMetadata();
            initializeTopology();
            processorContext.initialize();
            idleStartTime = RecordQueue.UNKNOWN;
            transitionTo(State.RUNNING);

            log.info("Restored and ready to run");
        } else {
            throw new IllegalStateException("Illegal state " + state() + " while completing restoration for active task " + id);
        }
    }

    /**
     * <pre>
     * the following order must be followed:
     *  1. first close topology to make sure all cached records in the topology are processed
     *  2. then flush the state, send any left changelog records
     *  3. then flush the record collector
     *  4. then commit the record collector -- for EOS this is the synchronization barrier
     *  5. then checkpoint the state manager -- even if we crash before this step, EOS is still guaranteed
     * </pre>
     *
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    @Override
    public void prepareSuspend() {
        if (state() == State.CREATED || state() == State.SUSPENDED) {
            // do nothing
            log.trace("Skip prepare suspending since state is {}", state());
        } else if (state() == State.RUNNING) {
            closeTopology(true);

            stateMgr.flush();
            recordCollector.flush();

            log.info("Prepare suspending running");
        } else if (state() == State.RESTORING) {
            stateMgr.flush();

            log.info("Prepare suspending restoring");
        } else {
            throw new IllegalStateException("Illegal state " + state() + " while suspending active task " + id);
        }
    }

    @Override
    public void suspend() {
        if (state() == State.CREATED || state() == State.SUSPENDED) {
            // do nothing
            log.trace("Skip suspending since state is {}", state());
        } else if (state() == State.RUNNING) {
            stateMgr.checkpoint(checkpointableOffsets());
            partitionGroup.clear();

            transitionTo(State.SUSPENDED);
            log.info("Suspended running");
        } else if (state() == State.RESTORING) {
            // we just checkpoint the position that we've restored up to without
            // going through the commit process
            stateMgr.checkpoint(emptyMap());

            // we should also clear any buffered records of a task when suspending it
            partitionGroup.clear();

            transitionTo(State.SUSPENDED);
            log.info("Suspended restoring");
        } else {
            throw new IllegalStateException("Illegal state " + state() + " while suspending active task " + id);
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

            default:
                throw new IllegalStateException("Illegal state " + state() + " while resuming active task " + id);
        }
    }

    @Override
    public void prepareCommit() {
        switch (state()) {
            case RUNNING:
            case RESTORING:
                stateMgr.flush();
                recordCollector.flush();

                log.info("Prepared task for committing");

                break;

            default:
                throw new IllegalStateException("Illegal state " + state() + " while preparing active task " + id + " for committing");
        }
    }

    @Override
    public void postCommit() {
        switch (state()) {
            case RUNNING:
                commitNeeded = false;
                commitRequested = false;

                if (!eosEnabled) {
                    stateMgr.checkpoint(checkpointableOffsets());
                }

                log.info("Committed");

                break;

            case RESTORING:
                commitNeeded = false;
                commitRequested = false;

                stateMgr.checkpoint(checkpointableOffsets());

                log.info("Committed");

                break;

            default:
                throw new IllegalStateException("Illegal state " + state() + " while post committing active task " + id);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committableOffsetsAndMetadata() {
        if (state() == State.CLOSED) {
            throw new IllegalStateException("Task " + id + " is closed.");
        }

        if (state() != State.RUNNING) {
            return Collections.emptyMap();
        }

        final Map<TopicPartition, Long> partitionTimes = extractPartitionTimes();

        final Map<TopicPartition, OffsetAndMetadata> consumedOffsetsAndMetadata = new HashMap<>(consumedOffsets.size());
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
            consumedOffsetsAndMetadata.put(partition, new OffsetAndMetadata(offset, encodeTimestamp(partitionTime)));
        }

        return consumedOffsetsAndMetadata;
    }

    private Map<TopicPartition, Long> extractPartitionTimes() {
        final Map<TopicPartition, Long> partitionTimes = new HashMap<>();
        for (final TopicPartition partition : partitionGroup.partitions()) {
            partitionTimes.put(partition, partitionGroup.partitionTimestamp(partition));
        }
        return partitionTimes;
    }

    @Override
    public Map<TopicPartition, Long> prepareCloseClean() {
        final Map<TopicPartition, Long> checkpoint = prepareClose(true);

        log.info("Prepared clean close");

        return checkpoint;
    }

    @Override
    public void closeClean(final Map<TopicPartition, Long> checkpoint) {
        close(true, checkpoint);

        log.info("Closed clean");
    }

    @Override
    public void prepareCloseDirty() {
        prepareClose(false);

        log.info("Prepared dirty close");
    }

    @Override
    public void closeDirty() {
        close(false, null);

        log.info("Closed dirty");
    }

    /**
     * <pre>
     * the following order must be followed:
     *  1. first close topology to make sure all cached records in the topology are processed
     *  2. then flush the state, send any left changelog records
     *  3. then flush the record collector
     * </pre>
     *
     * @param clean    shut down cleanly (ie, incl. flush) if {@code true} --
     *                 otherwise, just close open resources
     * @throws TaskMigratedException if the task producer got fenced (EOS)
     */
    private Map<TopicPartition, Long> prepareClose(final boolean clean) {
        final Map<TopicPartition, Long> checkpoint;

        if (state() == State.CREATED) {
            // the task is created and not initialized, just re-write the checkpoint file
            checkpoint = Collections.emptyMap();
        } else if (state() == State.RUNNING) {
            closeTopology(clean);

            if (clean) {
                stateMgr.flush();
                recordCollector.flush();
                checkpoint = checkpointableOffsets();
            } else {
                checkpoint = null; // `null` indicates to not write a checkpoint
                executeAndMaybeSwallow(false, stateMgr::flush, "state manager flush", log);
            }
        } else if (state() == State.RESTORING) {
            executeAndMaybeSwallow(clean, stateMgr::flush, "state manager flush", log);
            checkpoint = Collections.emptyMap();
        } else if (state() == State.SUSPENDED) {
            // if `SUSPENDED` do not need to checkpoint, since when suspending we've already committed the state
            checkpoint = null; // `null` indicates to not write a checkpoint
        } else {
            throw new IllegalStateException("Illegal state " + state() + " while prepare closing active task " + id);
        }

        return checkpoint;
    }

    /**
     * <pre>
     * the following order must be followed:
     *  1. checkpoint the state manager -- even if we crash before this step, EOS is still guaranteed
     *  2. then if we are closing on EOS and dirty, wipe out the state store directory
     *  3. finally release the state manager lock
     * </pre>
     */
    private void close(final boolean clean,
                       final Map<TopicPartition, Long> checkpoint) {
        if (clean && checkpoint != null) {
            executeAndMaybeSwallow(clean, () -> stateMgr.checkpoint(checkpoint), "state manager checkpoint", log);
        }

        switch (state()) {
            case CREATED:
            case RUNNING:
            case RESTORING:
            case SUSPENDED:
                // if EOS is enabled, we wipe out the whole state store for unclean close
                // since they are invalid to use anymore
                final boolean wipeStateStore = !clean && eosEnabled;

                // first close state manager (which is idempotent) then close the record collector (which could throw),
                // if the latter throws and we re-close dirty which would close the state manager again.
                executeAndMaybeSwallow(
                    clean,
                    () -> StateManagerUtil.closeStateManager(
                        log,
                        logPrefix,
                        clean,
                        wipeStateStore,
                        stateMgr,
                        stateDirectory,
                        TaskType.ACTIVE
                    ),
                    "state manager close",
                    log);

                executeAndMaybeSwallow(clean, recordCollector::close, "record collector close", log);

                break;

            default:
                throw new IllegalStateException("Illegal state " + state() + " while closing active task " + id);
        }

        partitionGroup.close();
        closeTaskSensor.record();

        transitionTo(State.CLOSED);
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
            idleStartTime = RecordQueue.UNKNOWN;
            return true;
        } else if (partitionGroup.numBuffered() > 0) {
            if (idleStartTime == RecordQueue.UNKNOWN) {
                idleStartTime = wallClockTime;
            }

            if (wallClockTime - idleStartTime >= maxTaskIdleMs) {
                enforcedProcessingSensor.record();
                return true;
            } else {
                return false;
            }
        } else {
            // there's no data in any of the topics; we should reset the enforced
            // processing timer
            idleStartTime = RecordQueue.UNKNOWN;
            return false;
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
        final StampedRecord record = partitionGroup.nextRecord(recordInfo);

        // if there is no record to process, return immediately
        if (record == null) {
            return false;
        }

        try {
            // process the record by passing to the source node of the topology
            final ProcessorNode<Object, Object> currNode = (ProcessorNode<Object, Object>) recordInfo.node();
            final TopicPartition partition = recordInfo.partition();

            log.trace("Start processing one record [{}]", record);

            updateProcessorContext(record, currNode);
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

        updateProcessorContext(new StampedRecord(DUMMY_RECORD, timestamp), node);

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

    private void updateProcessorContext(final StampedRecord record, final ProcessorNode<?, ?> currNode) {
        processorContext.setRecordContext(
            new ProcessorRecordContext(
                record.timestamp,
                record.offset(),
                record.partition(),
                record.topic(),
                record.headers()));
        processorContext.setCurrentNode(currNode);
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

    private void initializeMetadata() {
        try {
            final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = mainConsumer.committed(partitions).entrySet().stream()
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
            throw new StreamsException(String.format("task [%s] Failed to initialize offsets for %s", id, partitions), e);
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

        final Set<TopicPartition> nonCommitted = new HashSet<>(partitions);
        nonCommitted.removeAll(offsetsAndMetadata.keySet());
        for (final TopicPartition partition : nonCommitted) {
            log.debug("No committed offset for partition {}, therefore no timestamp can be found for this partition", partition);
        }
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

    private void closeTopology(final boolean clean) {
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

        if (exception != null && clean) {
            throw exception;
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
    public boolean maybePunctuateSystemTime() {
        final long systemTime = time.milliseconds();

        final boolean punctuated = systemTimePunctuationQueue.mayPunctuate(systemTime, PunctuationType.WALL_CLOCK_TIME, this);

        if (punctuated) {
            commitNeeded = true;
        }

        return punctuated;
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

    static String encodeTimestamp(final long partitionTime) {
        final ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.put(LATEST_MAGIC_BYTE);
        buffer.putLong(partitionTime);
        return Base64.getEncoder().encodeToString(buffer.array());
    }

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

    public ProcessorContext context() {
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
        return commitNeeded;
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

    public boolean hasRecordsQueued() {
        return numBuffered() > 0;
    }

    // below are visible for testing only
    RecordCollector recordCollector() {
        return recordCollector;
    }

    InternalProcessorContext processorContext() {
        return processorContext;
    }

    int numBuffered() {
        return partitionGroup.numBuffered();
    }

    long streamTime() {
        return partitionGroup.streamTime();
    }
}
