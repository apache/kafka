/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Collections.singleton;

/**
 * A StreamTask is associated with a {@link PartitionGroup}, and is assigned to a StreamThread for processing.
 */
public class StreamTask extends AbstractTask implements Punctuator {

    private static final Logger log = LoggerFactory.getLogger(StreamTask.class);

    private static final ConsumerRecord<Object, Object> DUMMY_RECORD = new ConsumerRecord<>(ProcessorContextImpl.NONEXIST_TOPIC, -1, -1L, null, null);

    private final String logPrefix;
    private final PartitionGroup partitionGroup;
    private final PartitionGroup.RecordInfo recordInfo = new PartitionGroup.RecordInfo();
    private final PunctuationQueue punctuationQueue;
    private final Map<TopicPartition, RecordQueue> partitionQueues;

    private final Map<TopicPartition, Long> consumedOffsets;
    private final RecordCollector recordCollector;
    private final int maxBufferedSize;

    private boolean commitRequested = false;
    private boolean commitOffsetNeeded = false;
    private boolean requiresPoll = true;
    private final Time time;
    private final TaskMetrics metrics;
    private Runnable commitDelegate = new Runnable() {
        @Override
        public void run() {
            log.debug("{} Committing its state", logPrefix);
            // 1) flush local state
            stateMgr.flush(processorContext);

            log.trace("{} Start flushing its producer's sent records upon committing its state", logPrefix);
            // 2) flush produced records in the downstream and change logs of local states
            recordCollector.flush();

            // 3) commit consumed offsets if it is dirty already
            commitOffsets();
        }
    };

    /**
     * Create {@link StreamTask} with its assigned partitions
     * @param id                    the ID of this task
     * @param applicationId         the ID of the stream processing application
     * @param partitions            the collection of assigned {@link TopicPartition}
     * @param topology              the instance of {@link ProcessorTopology}
     * @param consumer              the instance of {@link Consumer}
     * @param restoreConsumer       the instance of {@link Consumer} used when restoring state
     * @param config                the {@link StreamsConfig} specified by the user
     * @param metrics               the {@link StreamsMetrics} created by the thread
     * @param stateDirectory        the {@link StateDirectory} created by the thread
     * @param recordCollector       the instance of {@link RecordCollector} used to produce records
     */
    public StreamTask(TaskId id,
                      String applicationId,
                      Collection<TopicPartition> partitions,
                      ProcessorTopology topology,
                      Consumer<byte[], byte[]> consumer,
                      Consumer<byte[], byte[]> restoreConsumer,
                      StreamsConfig config,
                      StreamsMetrics metrics,
                      StateDirectory stateDirectory,
                      ThreadCache cache,
                      Time time,
                      final RecordCollector recordCollector) {
        super(id, applicationId, partitions, topology, consumer, restoreConsumer, false, stateDirectory, cache);
        this.punctuationQueue = new PunctuationQueue();
        this.maxBufferedSize = config.getInt(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG);
        this.metrics = new TaskMetrics(metrics);

        // create queues for each assigned partition and associate them
        // to corresponding source nodes in the processor topology
        partitionQueues = new HashMap<>();

        TimestampExtractor timestampExtractor = config.getConfiguredInstance(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractor.class);

        for (TopicPartition partition : partitions) {
            SourceNode source = topology.source(partition.topic());
            RecordQueue queue = createRecordQueue(partition, source, timestampExtractor);
            partitionQueues.put(partition, queue);
        }

        this.logPrefix = String.format("task [%s]", id);


        this.partitionGroup = new PartitionGroup(partitionQueues, timestampExtractor);

        // initialize the consumed offset cache
        this.consumedOffsets = new HashMap<>();

        // create the record recordCollector that maintains the produced offsets
        this.recordCollector = recordCollector;

        // initialize the topology with its own context
        this.processorContext = new ProcessorContextImpl(id, this, config, this.recordCollector, stateMgr, metrics, cache);
        this.time = time;
        // initialize the state stores
        log.info("{} Initializing state stores", logPrefix);
        initializeStateStores();
        stateMgr.registerGlobalStateStores(topology.globalStateStores());
        initTopology();
        this.processorContext.initialized();
    }

    /**
     * Adds records to queues. If a record has an invalid (i.e., negative) timestamp, the record is skipped
     * and not added to the queue for processing
     *
     * @param partition the partition
     * @param records  the records
     * @returns the number of added records
     */
    @SuppressWarnings("unchecked")
    public int addRecords(TopicPartition partition, Iterable<ConsumerRecord<byte[], byte[]>> records) {
        final int oldQueueSize = partitionGroup.numBuffered();
        final int newQueueSize = partitionGroup.addRawRecords(partition, records);

        log.trace("{} Added records into the buffered queue of partition {}, new queue size is {}", logPrefix, partition, newQueueSize);

        // if after adding these records, its partition queue's buffered size has been
        // increased beyond the threshold, we can then pause the consumption for this partition
        if (newQueueSize > this.maxBufferedSize) {
            consumer.pause(singleton(partition));
        }

        return newQueueSize - oldQueueSize;
    }

    /**
     * Process one record
     *
     * @return number of records left in the buffer of this task's partition group after the processing is done
     */
    @SuppressWarnings("unchecked")
    public int process() {
        // get the next record to process
        StampedRecord record = partitionGroup.nextRecord(recordInfo);

        // if there is no record to process, return immediately
        if (record == null) {
            requiresPoll = true;
            return 0;
        }

        requiresPoll = false;

        try {
            // process the record by passing to the source node of the topology
            final ProcessorNode currNode = recordInfo.node();
            TopicPartition partition = recordInfo.partition();

            log.trace("{} Start processing one record [{}]", logPrefix, record);
            final ProcessorRecordContext recordContext = createRecordContext(record);
            updateProcessorContext(recordContext, currNode);
            currNode.process(record.key(), record.value());

            log.trace("{} Completed processing one record [{}]", logPrefix, record);

            // update the consumed offset map after processing is done
            consumedOffsets.put(partition, record.offset());
            commitOffsetNeeded = true;

            // after processing this record, if its partition queue's buffered size has been
            // decreased to the threshold, we can then resume the consumption on this partition
            if (recordInfo.queue().size() == this.maxBufferedSize) {
                consumer.resume(singleton(partition));
                requiresPoll = true;
            }

            if (partitionGroup.topQueueSize() <= this.maxBufferedSize) {
                requiresPoll = true;
            }
        } catch (KafkaException ke) {
            throw new StreamsException(format("Exception caught in process. taskId=%s, processor=%s, topic=%s, partition=%d, offset=%d",
                                              id.toString(),
                                              processorContext.currentNode().name(),
                                              record.topic(),
                                              record.partition(),
                                              record.offset()
                                              ), ke);
        } finally {
            processorContext.setCurrentNode(null);
        }

        return partitionGroup.numBuffered();
    }

    private void updateProcessorContext(final ProcessorRecordContext recordContext, final ProcessorNode currNode) {
        processorContext.setRecordContext(recordContext);
        processorContext.setCurrentNode(currNode);
    }

    public boolean requiresPoll() {
        return requiresPoll;
    }

    /**
     * Possibly trigger registered punctuation functions if
     * current partition group timestamp has reached the defined stamp
     */
    public boolean maybePunctuate() {
        long timestamp = partitionGroup.timestamp();

        // if the timestamp is not known yet, meaning there is not enough data accumulated
        // to reason stream partition time, then skip.
        if (timestamp == TimestampTracker.NOT_KNOWN)
            return false;
        else
            return punctuationQueue.mayPunctuate(timestamp, this);
    }

    /**
     * @throws IllegalStateException if the current node is not null
     */
    @Override
    public void punctuate(ProcessorNode node, long timestamp) {
        if (processorContext.currentNode() != null)
            throw new IllegalStateException(String.format("%s Current node is not null", logPrefix));

        final StampedRecord stampedRecord = new StampedRecord(DUMMY_RECORD, timestamp);
        updateProcessorContext(createRecordContext(stampedRecord), node);

        log.trace("{} Punctuating processor {} with timestamp {}", logPrefix, node.name(), timestamp);

        try {
            node.punctuate(timestamp);
        } catch (KafkaException ke) {
            throw new StreamsException(String.format("Exception caught in punctuate. taskId=%s processor=%s", id,  node.name()), ke);
        } finally {
            processorContext.setCurrentNode(null);
        }
    }

    /**
     * Commit the current task state
     */
    public void commit() {
        metrics.metrics.measureLatencyNs(time, commitDelegate, metrics.taskCommitTimeSensor);
    }

    /**
     * commit consumed offsets if needed
     */
    @Override
    public void commitOffsets() {
        if (commitOffsetNeeded) {
            Map<TopicPartition, OffsetAndMetadata> consumedOffsetsAndMetadata = new HashMap<>(consumedOffsets.size());
            for (Map.Entry<TopicPartition, Long> entry : consumedOffsets.entrySet()) {
                TopicPartition partition = entry.getKey();
                long offset = entry.getValue() + 1;
                consumedOffsetsAndMetadata.put(partition, new OffsetAndMetadata(offset));
                stateMgr.putOffsetLimit(partition, offset);
            }
            consumer.commitSync(consumedOffsetsAndMetadata);
            commitOffsetNeeded = false;
        }

        commitRequested = false;
    }

    /**
     * Whether or not a request has been made to commit the current state
     */
    public boolean commitNeeded() {
        return this.commitRequested;
    }

    /**
     * Request committing the current task's state
     */
    public void needCommit() {
        this.commitRequested = true;
    }

    /**
     * Schedules a punctuation for the processor
     *
     * @param interval  the interval in milliseconds
     * @throws IllegalStateException if the current node is not null
     */
    public void schedule(long interval) {
        if (processorContext.currentNode() == null)
            throw new IllegalStateException(String.format("%s Current node is null", logPrefix));

        punctuationQueue.schedule(new PunctuationSchedule(processorContext.currentNode(), interval));
    }

    @Override
    public void initTopology() {
        // initialize the task by initializing all its processor nodes in the topology
        log.info("{} Initializing processor nodes of the topology", logPrefix);
        for (ProcessorNode node : this.topology.processors()) {
            processorContext.setCurrentNode(node);
            try {
                node.init(processorContext);
            } finally {
                processorContext.setCurrentNode(null);
            }
        }
    }

    @Override
    public void closeTopology() {

        this.partitionGroup.clear();

        // close the processors
        // make sure close() is called for each node even when there is a RuntimeException
        RuntimeException exception = null;
        for (ProcessorNode node : this.topology.processors()) {
            processorContext.setCurrentNode(node);
            try {
                node.close();
            } catch (RuntimeException e) {
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
     * @throws RuntimeException if an error happens during closing of processor nodes
     */
    @Override
    public void close() {
        log.debug("{} Closing processor topology", logPrefix);

        this.partitionGroup.close();
        closeTopology();
        metrics.removeAllSensors();
    }

    @Override
    protected Map<TopicPartition, Long> recordCollectorOffsets() {
        return recordCollector.offsets();
    }

    @SuppressWarnings("unchecked")
    private RecordQueue createRecordQueue(TopicPartition partition, SourceNode source, final TimestampExtractor timestampExtractor) {
        return new RecordQueue(partition, source, timestampExtractor);
    }

    private ProcessorRecordContext createRecordContext(final StampedRecord currRecord) {
        return new ProcessorRecordContext(currRecord.timestamp, currRecord.offset(), currRecord.partition(), currRecord.topic());
    }

    /**
     * Produces a string representation contain useful information about a StreamTask.
     * This is useful in debugging scenarios.
     * @return A string representation of the StreamTask instance.
     */
    public String toString() {
        return super.toString();
    }

    protected class TaskMetrics  {
        final StreamsMetricsImpl metrics;
        final Sensor taskCommitTimeSensor;


        public TaskMetrics(StreamsMetrics metrics) {
            String name = id.toString();
            this.metrics = (StreamsMetricsImpl) metrics;
            this.taskCommitTimeSensor = metrics.addLatencyAndThroughputSensor("task", name, "commit", Sensor.RecordingLevel.DEBUG, "streams-task-id", name);
        }

        public void removeAllSensors() {
            metrics.removeSensor(taskCommitTimeSensor);
        }
    }

    @Override
    public void flushState() {
        super.flushState();
        recordCollector.flush();
    }

}
