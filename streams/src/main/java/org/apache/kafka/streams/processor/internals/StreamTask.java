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
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Collections.singleton;

/**
 * A StreamTask is associated with a {@link PartitionGroup}, and is assigned to a StreamThread for processing.
 */
public class StreamTask extends AbstractTask implements ProcessorNodePunctuator {

    private static final ConsumerRecord<Object, Object> DUMMY_RECORD = new ConsumerRecord<>(ProcessorContextImpl.NONEXIST_TOPIC, -1, -1L, null, null);

    private final PartitionGroup partitionGroup;
    private final PartitionGroup.RecordInfo recordInfo;
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
    private final TaskMetrics taskMetrics;

    protected static final class TaskMetrics {
        final StreamsMetricsImpl metrics;
        final Sensor taskCommitTimeSensor;
        private final String taskName;


        TaskMetrics(final TaskId id, final StreamsMetricsImpl metrics) {
            taskName = id.toString();
            this.metrics = metrics;
            final String group = "stream-task-metrics";

            // first add the global operation metrics if not yet, with the global tags only
            final Map<String, String> allTagMap = metrics.tagMap("task-id", "all");
            final Sensor parent = metrics.threadLevelSensor("commit", Sensor.RecordingLevel.DEBUG);
            parent.add(
                new MetricName("commit-latency-avg", group, "The average latency of commit operation.", allTagMap),
                new Avg()
            );
            parent.add(
                new MetricName("commit-latency-max", group, "The max latency of commit operation.", allTagMap),
                new Max()
            );
            parent.add(
                new MetricName("commit-rate", group, "The average number of occurrence of commit operation per second.", allTagMap),
                new Rate(TimeUnit.SECONDS, new Count())
            );
            parent.add(
                new MetricName("commit-total", group, "The total number of occurrence of commit operations.", allTagMap),
                new Count()
            );

            // add the operation metrics with additional tags
            final Map<String, String> tagMap = metrics.tagMap("task-id", taskName);
            taskCommitTimeSensor = metrics.taskLevelSensor("commit", taskName, Sensor.RecordingLevel.DEBUG, parent);
            taskCommitTimeSensor.add(
                new MetricName("commit-latency-avg", group, "The average latency of commit operation.", tagMap),
                new Avg()
            );
            taskCommitTimeSensor.add(
                new MetricName("commit-latency-max", group, "The max latency of commit operation.", tagMap),
                new Max()
            );
            taskCommitTimeSensor.add(
                new MetricName("commit-rate", group, "The average number of occurrence of commit operation per second.", tagMap),
                new Rate(TimeUnit.SECONDS, new Count())
            );
            taskCommitTimeSensor.add(
                new MetricName("commit-total", group, "The total number of occurrence of commit operations.", tagMap),
                new Count()
            );
        }

        void removeAllSensors() {
            metrics.removeAllTaskLevelSensors(taskName);
        }
    }

    public StreamTask(final TaskId id,
                      final Collection<TopicPartition> partitions,
                      final ProcessorTopology topology,
                      final Consumer<byte[], byte[]> consumer,
                      final ChangelogReader changelogReader,
                      final StreamsConfig config,
                      final StreamsMetricsImpl metrics,
                      final StateDirectory stateDirectory,
                      final ThreadCache cache,
                      final Time time,
                      final Producer<byte[], byte[]> producer) {
        this(id, partitions, topology, consumer, changelogReader, config, metrics, stateDirectory, cache, time, producer, null);
    }

    public StreamTask(final TaskId id,
                      final Collection<TopicPartition> partitions,
                      final ProcessorTopology topology,
                      final Consumer<byte[], byte[]> consumer,
                      final ChangelogReader changelogReader,
                      final StreamsConfig config,
                      final StreamsMetricsImpl metrics,
                      final StateDirectory stateDirectory,
                      final ThreadCache cache,
                      final Time time,
                      final Producer<byte[], byte[]> producer,
                      final RecordCollector recordCollector) {
        super(id, partitions, topology, consumer, changelogReader, false, stateDirectory, config);

        this.time = time;
        this.producer = producer;
        this.taskMetrics = new TaskMetrics(id, metrics);

        final ProductionExceptionHandler productionExceptionHandler = config.defaultProductionExceptionHandler();

        if (recordCollector == null) {
            this.recordCollector = new RecordCollectorImpl(
                producer,
                id.toString(),
                logContext,
                productionExceptionHandler,
                metrics.skippedRecordsSensor()
            );
        } else {
            this.recordCollector = recordCollector;
        }
        streamTimePunctuationQueue = new PunctuationQueue();
        systemTimePunctuationQueue = new PunctuationQueue();
        maxBufferedSize = config.getInt(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG);

        // initialize the consumed and committed offset cache
        consumedOffsets = new HashMap<>();

        // create queues for each assigned partition and associate them
        // to corresponding source nodes in the processor topology
        final Map<TopicPartition, RecordQueue> partitionQueues = new HashMap<>();

        // initialize the topology with its own context
        processorContext = new ProcessorContextImpl(id, this, config, this.recordCollector, stateMgr, metrics, cache);

        final TimestampExtractor defaultTimestampExtractor = config.defaultTimestampExtractor();
        final DeserializationExceptionHandler defaultDeserializationExceptionHandler = config.defaultDeserializationExceptionHandler();
        for (final TopicPartition partition : partitions) {
            final SourceNode source = topology.source(partition.topic());
            final TimestampExtractor sourceTimestampExtractor = source.getTimestampExtractor() != null ? source.getTimestampExtractor() : defaultTimestampExtractor;
            final RecordQueue queue = new RecordQueue(
                partition,
                source,
                sourceTimestampExtractor,
                defaultDeserializationExceptionHandler,
                processorContext,
                logContext
            );
            partitionQueues.put(partition, queue);
        }

        recordInfo = new PartitionGroup.RecordInfo();
        partitionGroup = new PartitionGroup(partitionQueues);

        stateMgr.registerGlobalStateStores(topology.globalStateStores());

        // initialize transactions if eos is turned on, which will block if the previous transaction has not
        // completed yet; do not start the first transaction until the topology has been initialized later
        if (eosEnabled) {
            this.producer.initTransactions();
        }
    }

    @Override
    public boolean initializeStateStores() {
        log.trace("Initializing state stores");
        registerStateStores();
        return changelogPartitions().isEmpty();
    }

    /**
     * <pre>
     * - (re-)initialize the topology of the task
     * </pre>
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    @Override
    public void initializeTopology() {
        initTopology();

        if (eosEnabled) {
            try {
                this.producer.beginTransaction();
            } catch (final ProducerFencedException fatal) {
                throw new TaskMigratedException(this, fatal);
            }
            transactionInFlight = true;
        }

        processorContext.initialized();
        taskInitialized = true;
    }

    /**
     * <pre>
     * - resume the task
     * </pre>
     */
    @Override
    public void resume() {
        // nothing to do; new transaction will be started only after topology is initialized
        log.debug("Resuming");
    }

    /**
     * Process one record.
     *
     * @return true if this method processes a record, false if it does not process a record.
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
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

            log.trace("Start processing one record [{}]", record);

            updateProcessorContext(record, currNode);
            currNode.process(record.key(), record.value());

            log.trace("Completed processing one record [{}]", record);

            // update the consumed offset map after processing is done
            consumedOffsets.put(partition, record.offset());
            commitOffsetNeeded = true;

            // after processing this record, if its partition queue's buffered size has been
            // decreased to the threshold, we can then resume the consumption on this partition
            if (recordInfo.queue().size() == maxBufferedSize) {
                consumer.resume(singleton(partition));
            }
        } catch (final ProducerFencedException fatal) {
            throw new TaskMigratedException(this, fatal);
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
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    @Override
    public void punctuate(final ProcessorNode node, final long timestamp, final PunctuationType type, final Punctuator punctuator) {
        if (processorContext.currentNode() != null) {
            throw new IllegalStateException(String.format("%sCurrent node is not null", logPrefix));
        }

        updateProcessorContext(new StampedRecord(DUMMY_RECORD, timestamp), node);

        if (log.isTraceEnabled()) {
            log.trace("Punctuating processor {} with timestamp {} and punctuation type {}", node.name(), timestamp, type);
        }

        try {
            node.punctuate(timestamp, punctuator);
        } catch (final ProducerFencedException fatal) {
            throw new TaskMigratedException(this, fatal);
        } catch (final KafkaException e) {
            throw new StreamsException(String.format("%sException caught while punctuating processor '%s'", logPrefix, node.name()), e);
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
     *
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    @Override
    public void commit() {
        commit(true);
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    // visible for testing
    void commit(final boolean startNewTransaction) {
        final long startNs = time.nanoseconds();
        log.debug("Committing");

        flushState();

        if (!eosEnabled) {
            stateMgr.checkpoint(recordCollectorOffsets());
        }

        commitOffsets(startNewTransaction);

        commitRequested = false;

        taskMetrics.taskCommitTimeSensor.record(time.nanoseconds() - startNs);
    }

    @Override
    protected Map<TopicPartition, Long> recordCollectorOffsets() {
        return recordCollector.offsets();
    }

    @Override
    protected void flushState() {
        log.trace("Flushing state and producer");
        super.flushState();
        try {
            recordCollector.flush();
        } catch (final ProducerFencedException fatal) {
            throw new TaskMigratedException(this, fatal);
        }
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    private void commitOffsets(final boolean startNewTransaction) {
        try {
            if (commitOffsetNeeded) {
                log.trace("Committing offsets");
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
                        producer.beginTransaction();
                        transactionInFlight = true;
                    }
                } else {
                    consumer.commitSync(consumedOffsetsAndMetadata);
                }
                commitOffsetNeeded = false;
            } else if (eosEnabled && !startNewTransaction && transactionInFlight) { // need to make sure to commit txn for suspend case
                producer.commitTransaction();
                transactionInFlight = false;
            }
        } catch (final CommitFailedException | ProducerFencedException fatal) {
            throw new TaskMigratedException(this, fatal);
        }
    }

    Map<TopicPartition, Long> purgableOffsets() {
        final Map<TopicPartition, Long> purgableConsumedOffsets = new HashMap<>();
        for (final Map.Entry<TopicPartition, Long> entry : consumedOffsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            if (topology.isRepartitionTopic(tp.topic())) {
                purgableConsumedOffsets.put(tp, entry.getValue() + 1);
            }
        }

        return purgableConsumedOffsets;
    }

    private void initTopology() {
        // initialize the task by initializing all its processor nodes in the topology
        log.trace("Initializing processor nodes of the topology");
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
     *
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    @Override
    public void suspend() {
        log.debug("Suspending");
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
     *
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    // visible for testing
    void suspend(final boolean clean) {
        closeTopology(); // should we call this only on clean suspend?
        if (clean) {
            commit(false);
        }
    }

    private void closeTopology() {
        log.trace("Closing processor topology");

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
            log.error("Could not close state manager due to the following error:", e);
        }

        try {
            partitionGroup.close();
            taskMetrics.removeAllSensors();
        } finally {
            if (eosEnabled) {
                if (!clean) {
                    try {
                        if (!isZombie && transactionInFlight) {
                            producer.abortTransaction();
                        }
                        transactionInFlight = false;
                    } catch (final ProducerFencedException ignore) {
                        /* TODO
                         * this should actually never happen atm as we guard the call to #abortTransaction
                         * -> the reason for the guard is a "bug" in the Producer -- it throws IllegalStateException
                         * instead of ProducerFencedException atm. We can remove the isZombie flag after KAFKA-5604 got
                         * fixed and fall-back to this catch-and-swallow code
                         */

                        // can be ignored: transaction got already aborted by brokers/transactional-coordinator if this happens
                    }
                }
                try {
                    if (!isZombie) {
                        recordCollector.close();
                    }
                } catch (final Throwable e) {
                    log.error("Failed to close producer due to the following error:", e);
                }
            }
        }

        if (firstException != null) {
            throw firstException;
        }
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
     *
     * @param clean    shut down cleanly (ie, incl. flush and commit) if {@code true} --
     *                 otherwise, just close open resources
     * @param isZombie {@code true} is this task is a zombie or not (this will repress {@link TaskMigratedException}
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    @Override
    public void close(boolean clean,
                      final boolean isZombie) {
        log.debug("Closing");

        RuntimeException firstException = null;
        try {
            suspend(clean);
        } catch (final RuntimeException e) {
            clean = false;
            firstException = e;
            log.error("Could not close task due to the following error:", e);
        }

        closeSuspended(clean, isZombie, firstException);

        taskClosed = true;
    }

    /**
     * Adds records to queues. If a record has an invalid (i.e., negative) timestamp, the record is skipped
     * and not added to the queue for processing
     *
     * @param partition the partition
     * @param records   the records
     */
    @SuppressWarnings("unchecked")
    public void addRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        final int newQueueSize = partitionGroup.addRawRecords(partition, records);

        if (log.isTraceEnabled()) {
            log.trace("Added records into the buffered queue of partition {}, new queue size is {}", partition, newQueueSize);
        }

        // if after adding these records, its partition queue's buffered size has been
        // increased beyond the threshold, we can then pause the consumption for this partition
        if (newQueueSize > maxBufferedSize) {
            consumer.pause(singleton(partition));
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
    Cancellable schedule(final long startTime, final long interval, final PunctuationType type, final Punctuator punctuator) {
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
     * @return The number of records left in the buffer of this task's partition group
     */
    int numBuffered() {
        return partitionGroup.numBuffered();
    }

    /**
     * Possibly trigger registered stream-time punctuation functions if
     * current partition group timestamp has reached the defined stamp
     * Note, this is only called in the presence of new records
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
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
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    public boolean maybePunctuateSystemTime() {
        final long timestamp = time.milliseconds();

        return systemTimePunctuationQueue.mayPunctuate(timestamp, PunctuationType.WALL_CLOCK_TIME, this);
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
    boolean commitNeeded() {
        return commitRequested;
    }

    // visible for testing only
    RecordCollector recordCollector() {
        return recordCollector;
    }

    Producer<byte[], byte[]> getProducer() {
        return producer;
    }
}
