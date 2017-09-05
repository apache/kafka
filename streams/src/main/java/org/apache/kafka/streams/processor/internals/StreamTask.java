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

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Collections.singleton;

/**
 * A StreamTask is associated with a {@link PartitionGroup}, and is assigned to a StreamThread for processing.
 */
public class StreamTask extends AbstractTask implements ProcessorNodePunctuator {

    private static final Logger log = LoggerFactory.getLogger(StreamTask.class);

    private static final ConsumerRecord<Object, Object> DUMMY_RECORD = new ConsumerRecord<>(ProcessorContextImpl.NONEXIST_TOPIC, -1, -1L, null, null);

    private final PartitionGroup partitionGroup;
    private final PartitionGroup.RecordInfo recordInfo = new PartitionGroup.RecordInfo();
    private final PunctuationQueue streamTimePunctuationQueue;
    private final PunctuationQueue systemTimePunctuationQueue;

    private final Map<TopicPartition, Long> consumedOffsets;
    private final RecordCollector recordCollector;
    private final Producer<byte[], byte[]> producer;
    private final int maxBufferedSize;

    private boolean commitRequested = false;
    private boolean commitOffsetNeeded = false;
    private boolean transactionInFlight = false;
    private final Time time;
    private final TaskMetrics metrics;

    protected class TaskMetrics  {
        final StreamsMetricsImpl metrics;
        final Sensor taskCommitTimeSensor;


        TaskMetrics(final StreamsMetrics metrics) {
            final String name = id().toString();
            this.metrics = (StreamsMetricsImpl) metrics;
            taskCommitTimeSensor = metrics.addLatencyAndThroughputSensor("task", name, "commit",
                    Sensor.RecordingLevel.DEBUG);
        }

        void removeAllSensors() {
            metrics.removeSensor(taskCommitTimeSensor);
        }
    }

    /**
     * Create {@link StreamTask} with its assigned partitions
     * @param id                    the ID of this task
     * @param applicationId         the ID of the stream processing application
     * @param partitions            the collection of assigned {@link TopicPartition}
     * @param topology              the instance of {@link ProcessorTopology}
     * @param consumer              the instance of {@link Consumer}
     * @param changelogReader       the instance of {@link ChangelogReader} used for restoring state
     * @param config                the {@link StreamsConfig} specified by the user
     * @param metrics               the {@link StreamsMetrics} created by the thread
     * @param stateDirectory        the {@link StateDirectory} created by the thread
     * @param producer              the instance of {@link Producer} used to produce records
     */
    public StreamTask(final TaskId id,
                      final String applicationId,
                      final Collection<TopicPartition> partitions,
                      final ProcessorTopology topology,
                      final Consumer<byte[], byte[]> consumer,
                      final ChangelogReader changelogReader,
                      final StreamsConfig config,
                      final StreamsMetrics metrics,
                      final StateDirectory stateDirectory,
                      final ThreadCache cache,
                      final Time time,
                      final Producer<byte[], byte[]> producer) {
        super(id, applicationId, partitions, topology, consumer, changelogReader, false, stateDirectory, config);
        streamTimePunctuationQueue = new PunctuationQueue();
        systemTimePunctuationQueue = new PunctuationQueue();
        maxBufferedSize = config.getInt(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG);
        this.metrics = new TaskMetrics(metrics);

        // create queues for each assigned partition and associate them
        // to corresponding source nodes in the processor topology
        final Map<TopicPartition, RecordQueue> partitionQueues = new HashMap<>();

        // initialize the consumed offset cache
        consumedOffsets = new HashMap<>();

        this.producer = producer;
        recordCollector = createRecordCollector();

        // initialize the topology with its own context
        processorContext = new ProcessorContextImpl(id, this, config, recordCollector, stateMgr, metrics, cache);

        final TimestampExtractor defaultTimestampExtractor  = config.defaultTimestampExtractor();
        final DeserializationExceptionHandler defaultDeserializationExceptionHandler = config.defaultDeserializationExceptionHandler();
        for (final TopicPartition partition : partitions) {
            final SourceNode source = topology.source(partition.topic());
            final TimestampExtractor sourceTimestampExtractor = source.getTimestampExtractor() != null ? source.getTimestampExtractor() : defaultTimestampExtractor;
            final RecordQueue queue = new RecordQueue(partition, source, sourceTimestampExtractor, defaultDeserializationExceptionHandler, processorContext);
            partitionQueues.put(partition, queue);
        }

        partitionGroup = new PartitionGroup(partitionQueues);
        this.time = time;

        stateMgr.registerGlobalStateStores(topology.globalStateStores());
        if (eosEnabled) {
            this.producer.initTransactions();
            this.producer.beginTransaction();
            transactionInFlight = true;
        }
    }

    /**
     * <pre>
     * - re-initialize the task
     * - if (eos) begin new transaction
     * </pre>
     */
    @Override
    public void resume() {
        log.debug("{} Resuming", logPrefix);
        if (eosEnabled) {
            producer.beginTransaction();
            transactionInFlight = true;
        }
        initTopology();
    }

    /**
     * Process one record.
     *
     * @return true if this method processes a record, false if it does not process a record.
     */
    @SuppressWarnings("unchecked")
    public boolean process() {
        // get the next record to process
        final StampedRecord record = partitionGroup.nextRecord(recordInfo);

        // if there is no record to process, return immediately
        if (record == null) {
            return false;
        }

        try {
            // process the record by passing to the source node of the topology
            final ProcessorNode currNode = recordInfo.node();
            final TopicPartition partition = recordInfo.partition();

            log.trace("{} Start processing one record [{}]", logPrefix, record);

            updateProcessorContext(record, currNode);
            currNode.process(record.key(), record.value());

            log.trace("{} Completed processing one record [{}]", logPrefix, record);

            // update the consumed offset map after processing is done
            consumedOffsets.put(partition, record.offset());
            commitOffsetNeeded = true;

            // after processing this record, if its partition queue's buffered size has been
            // decreased to the threshold, we can then resume the consumption on this partition
            if (recordInfo.queue().size() == maxBufferedSize) {
                consumer.resume(singleton(partition));
            }
        } catch (final KafkaException e) {
            throw new StreamsException(format("Exception caught in process. taskId=%s, processor=%s, topic=%s, partition=%d, offset=%d",
                id(),
                processorContext.currentNode().name(),
                record.topic(),
                record.partition(),
                record.offset()
            ), e);
        } finally {
            processorContext.setCurrentNode(null);
        }

        return true;
    }

    /**
     * @throws IllegalStateException if the current node is not null
     */
    @Override
    public void punctuate(final ProcessorNode node, final long timestamp, final PunctuationType type, final Punctuator punctuator) {
        if (processorContext.currentNode() != null) {
            throw new IllegalStateException(String.format("%s Current node is not null", logPrefix));
        }

        updateProcessorContext(new StampedRecord(DUMMY_RECORD, timestamp), node);

        if (log.isTraceEnabled()) {
            log.trace("{} Punctuating processor {} with timestamp {} and punctuation type {}", logPrefix, node.name(), timestamp, type);
        }

        try {
            node.punctuate(timestamp, punctuator);
        } catch (final KafkaException e) {
            throw new StreamsException(String.format("%s Exception caught while punctuating processor '%s'", logPrefix,  node.name()), e);
        } finally {
            processorContext.setCurrentNode(null);
        }
    }

    private void updateProcessorContext(final StampedRecord record, final ProcessorNode currNode) {
        processorContext.setRecordContext(new ProcessorRecordContext(record.timestamp, record.offset(), record.partition(), record.topic()));
        processorContext.setCurrentNode(currNode);
    }

    /**
     * <pre>
     * - flush state and producer
     * - if(!eos) write checkpoint
     * - commit offsets and start new transaction
     * </pre>
     */
    @Override
    public void commit() {
        commit(true);
    }

    // visible for testing
    void commit(final boolean startNewTransaction) {
        log.debug("{} Committing", logPrefix);
        metrics.metrics.measureLatencyNs(
            time,
            new Runnable() {
                @Override
                public void run() {
                    flushState();
                    if (!eosEnabled) {
                        stateMgr.checkpoint(recordCollectorOffsets());
                    }
                    commitOffsets(startNewTransaction);
                }
            },
            metrics.taskCommitTimeSensor);

        commitRequested = false;
    }

    @Override
    protected Map<TopicPartition, Long> recordCollectorOffsets() {
        return recordCollector.offsets();
    }

    @Override
    protected void flushState() {
        log.trace("{} Flushing state and producer", logPrefix);
        super.flushState();
        recordCollector.flush();
    }

    private void commitOffsets(final boolean startNewTransaction) {
        if (commitOffsetNeeded) {
            log.trace("{} Committing offsets", logPrefix);
            final Map<TopicPartition, OffsetAndMetadata> consumedOffsetsAndMetadata = new HashMap<>(consumedOffsets.size());
            for (final Map.Entry<TopicPartition, Long> entry : consumedOffsets.entrySet()) {
                final TopicPartition partition = entry.getKey();
                final long offset = entry.getValue() + 1;
                consumedOffsetsAndMetadata.put(partition, new OffsetAndMetadata(offset));
                stateMgr.putOffsetLimit(partition, offset);
            }

            if (eosEnabled) {
                producer.sendOffsetsToTransaction(consumedOffsetsAndMetadata, applicationId);
                producer.commitTransaction();
                transactionInFlight = false;
                if (startNewTransaction) {
                    transactionInFlight = true;
                    producer.beginTransaction();
                }
            } else {
                try {
                    consumer.commitSync(consumedOffsetsAndMetadata);
                } catch (final CommitFailedException e) {
                    log.warn("{} Failed offset commits {} due to CommitFailedException", logPrefix, consumedOffsetsAndMetadata);
                    throw e;
                }
            }
            commitOffsetNeeded = false;
        } else if (eosEnabled && !startNewTransaction && transactionInFlight) { // need to make sure to commit txn for suspend case
            producer.commitTransaction();
            transactionInFlight = false;
        }
    }

    void initTopology() {
        // initialize the task by initializing all its processor nodes in the topology
        log.trace("{} Initializing processor nodes of the topology", logPrefix);
        for (final ProcessorNode node : topology.processors()) {
            processorContext.setCurrentNode(node);
            try {
                node.init(processorContext);
            } finally {
                processorContext.setCurrentNode(null);
            }
        }
    }

    /**
     * <pre>
     * - close topology
     * - {@link #commit()}
     *   - flush state and producer
     *   - if (!eos) write checkpoint
     *   - commit offsets
     * </pre>
     */
    @Override
    public void suspend() {
        log.debug("{} Suspending", logPrefix);
        suspend(true);
    }

    /**
     * <pre>
     * - close topology
     * - if (clean) {@link #commit()}
     *   - flush state and producer
     *   - if (!eos) write checkpoint
     *   - commit offsets
     * </pre>
     */
    // visible for testing
    void suspend(final boolean clean) {
        closeTopology(); // should we call this only on clean suspend?
        if (clean) {
            commit(false);
        }
    }

    private void closeTopology() {
        log.trace("{} Closing processor topology", logPrefix);

        partitionGroup.clear();

        // close the processors
        // make sure close() is called for each node even when there is a RuntimeException
        RuntimeException exception = null;
        if (taskInitialized) {
            for (final ProcessorNode node : topology.processors()) {
                processorContext.setCurrentNode(node);
                try {
                    node.close();
                } catch (final RuntimeException e) {
                    exception = e;
                } finally {
                    processorContext.setCurrentNode(null);
                }
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    // helper to avoid calling suspend() twice if a suspended task is not reassigned and closed
    @Override
    public void closeSuspended(boolean clean,
                               final boolean isZombie,
                               RuntimeException firstException) {
        try {
            closeStateManager(clean);
        } catch (final RuntimeException e) {
            clean = false;
            if (firstException == null) {
                firstException = e;
            }
            log.error("{} Could not close state manager due to the following error:", logPrefix, e);
        }

        try {
            partitionGroup.close();
            metrics.removeAllSensors();
        } finally {
            if (eosEnabled) {
                if (!clean) {
                    try {
                        if (!isZombie) {
                            producer.abortTransaction();
                        }
                        transactionInFlight = false;
                    } catch (final ProducerFencedException e) {
                        // can be ignored: transaction got already aborted by brokers/transactional-coordinator if this happens
                    }
                }
                try {
                    if (!isZombie) {
                        recordCollector.close();
                    }
                } catch (final Throwable e) {
                    log.error("{} Failed to close producer due to the following error:", logPrefix, e);
                }
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    @Override
    public Map<TopicPartition, Long> checkpointedOffsets() {
        throw new UnsupportedOperationException("checkpointedOffsets is not supported by StreamTasks");
    }

    /**
     * <pre>
     * - {@link #suspend(boolean) suspend(clean)}
     *   - close topology
     *   - if (clean) {@link #commit()}
     *     - flush state and producer
     *     - commit offsets
     * - close state
     *   - if (clean) write checkpoint
     * - if (eos) close producer
     * </pre>
     * @param clean shut down cleanly (ie, incl. flush and commit) if {@code true} --
     *              otherwise, just close open resources
     * @param isZombie {@code true} is this task is a zombie or not
     */
    @Override
    public void close(boolean clean,
                      final boolean isZombie) {
        log.debug("{} Closing", logPrefix);

        RuntimeException firstException = null;
        try {
            suspend(clean);
        } catch (final RuntimeException e) {
            clean = false;
            firstException = e;
            log.error("{} Could not close task due to the following error:", logPrefix, e);
        }

        closeSuspended(clean, isZombie, firstException);
    }

    /**
     * Adds records to queues. If a record has an invalid (i.e., negative) timestamp, the record is skipped
     * and not added to the queue for processing
     *
     * @param partition the partition
     * @param records  the records
     * @return the number of added records
     */
    @SuppressWarnings("unchecked")
    public int addRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        final int oldQueueSize = partitionGroup.numBuffered(partition);
        final int newQueueSize = partitionGroup.addRawRecords(partition, records);

        if (log.isTraceEnabled()) {
            log.trace("{} Added records into the buffered queue of partition {}, new queue size is {}", logPrefix, partition, newQueueSize);
        }

        // if after adding these records, its partition queue's buffered size has been
        // increased beyond the threshold, we can then pause the consumption for this partition
        if (newQueueSize > maxBufferedSize) {
            consumer.pause(singleton(partition));
        }

        return newQueueSize - oldQueueSize;
    }

    /**
     * Schedules a punctuation for the processor
     *
     * @param interval  the interval in milliseconds
     * @param type
     * @throws IllegalStateException if the current node is not null
     */
    public Cancellable schedule(final long interval, final PunctuationType type, final Punctuator punctuator) {
        if (processorContext.currentNode() == null) {
            throw new IllegalStateException(String.format("%s Current node is null", logPrefix));
        }

        final PunctuationSchedule schedule = new PunctuationSchedule(processorContext.currentNode(), interval, punctuator);

        switch (type) {
            case STREAM_TIME:
                return streamTimePunctuationQueue.schedule(schedule);
            case SYSTEM_TIME:
                return systemTimePunctuationQueue.schedule(schedule);
            default:
                throw new IllegalArgumentException("Unrecognized PunctuationType: " + type);
        }
    }

    /**
     * @return The number of records left in the buffer of this task's partition group
     */
    int numBuffered() {
        return partitionGroup.numBuffered();
    }

    /**
     * Possibly trigger registered stream-time punctuation functions if
     * current partition group timestamp has reached the defined stamp
     * Note, this is only called in the presence of new records
     */
    public boolean maybePunctuateStreamTime() {
        final long timestamp = partitionGroup.timestamp();

        // if the timestamp is not known yet, meaning there is not enough data accumulated
        // to reason stream partition time, then skip.
        if (timestamp == TimestampTracker.NOT_KNOWN) {
            return false;
        } else {
            return streamTimePunctuationQueue.mayPunctuate(timestamp, PunctuationType.STREAM_TIME, this);
        }
    }

    /**
     * Possibly trigger registered system-time punctuation functions if
     * current system timestamp has reached the defined stamp
     * Note, this is called irrespective of the presence of new records
     */
    public boolean maybePunctuateSystemTime() {
        final long timestamp = time.milliseconds();

        return systemTimePunctuationQueue.mayPunctuate(timestamp, PunctuationType.SYSTEM_TIME, this);
    }

    @Override
    public List<ConsumerRecord<byte[], byte[]>> update(final TopicPartition partition, final List<ConsumerRecord<byte[], byte[]>> remaining) {
        throw new UnsupportedOperationException("update is not implemented");
    }

    /**
     * Request committing the current task's state
     */
    void needCommit() {
        commitRequested = true;
    }

    /**
     * Whether or not a request has been made to commit the current state
     */
    public boolean commitNeeded() {
        return commitRequested;
    }

    // visible for testing only
    ProcessorContext processorContext() {
        return processorContext;
    }

    // visible for testing only
    RecordCollector recordCollector() {
        return recordCollector;
    }

    // visible for testing only
    RecordCollector createRecordCollector() {
        return new RecordCollectorImpl(producer, id.toString());
    }

    public boolean initialize() {
        log.debug("{} Initializing", logPrefix);
        initializeStateStores();
        initTopology();
        processorContext.initialized();
        taskInitialized = true;
        return topology.stateStores().isEmpty();
    }

}
