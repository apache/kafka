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

package org.apache.kafka.streaming.processor.internals;

import org.apache.kafka.clients.consumer.CommitType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streaming.StreamingConfig;
import org.apache.kafka.streaming.processor.Processor;
import org.apache.kafka.streaming.processor.ProcessorContext;
import org.apache.kafka.streaming.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A StreamTask is associated with a {@link PartitionGroup}, and is assigned to a StreamThread for processing.
 */
public class StreamTask {

    private static final Logger log = LoggerFactory.getLogger(StreamTask.class);

    private final int id;
    private final int maxBufferedSize;

    private final Consumer consumer;
    private final PartitionGroup partitionGroup;
    private final PunctuationQueue punctuationQueue;
    private final ProcessorContext processorContext;
    private final TimestampExtractor timestampExtractor;

    private final Map<TopicPartition, Long> consumedOffsets;
    private final Map<TopicPartition, Long> producedoffsets;
    private final Callback producerCallback;

    private boolean commitRequested = false;
    private StampedRecord currRecord = null;
    private ProcessorNode currNode = null;

    /**
     * Create {@link StreamTask} with its assigned partitions
     *
     * @param id                    the ID of this task
     * @param consumer              the instance of {@link Consumer}
     * @param topology              the instance of {@link ProcessorTopology}
     * @param partitions            the collection of assigned {@link TopicPartition}
     * @param config                the {@link StreamingConfig} specified by the user
     */
    public StreamTask(int id,
                      Consumer consumer,
                      ProcessorTopology topology,
                      Collection<TopicPartition> partitions,
                      RecordCollector collector,
                      StreamingConfig config) {

        this.id = id;
        this.consumer = consumer;
        this.punctuationQueue = new PunctuationQueue();
        this.maxBufferedSize = config.getInt(StreamingConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG);
        this.timestampExtractor = config.getConfiguredInstance(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractor.class);

        // create queues for each assigned partition and associate them
        // to corresponding source nodes in the processor topology
        Map<TopicPartition, RecordQueue> partitionQueues = new HashMap<>();

        for (TopicPartition partition : partitions) {
            SourceNode source = topology.source(partition.topic());
            RecordQueue queue = createRecordQueue(partition, source);
            partitionQueues.put(partition, queue);
        }

        this.partitionGroup = new PartitionGroup(partitionQueues);

        // initialize the topology with its own context
        try {
            this.processorContext = new ProcessorContextImpl(id, this, config, collector, new Metrics());
        } catch (IOException e) {
            throw new KafkaException("Error while creating the state manager in processor context.");
        }

        topology.init(this.processorContext);

        // initialize the consumed and produced offset cache
        this.consumedOffsets = new HashMap<>();
        this.producedoffsets = new HashMap<>();

        this.producerCallback = new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    TopicPartition partition = new TopicPartition(metadata.topic(), metadata.partition());
                    producedoffsets.put(partition, metadata.offset());
                } else {
                    log.error("Error sending record: ", exception);
                }
            }
        };
    }

    public int id() {
        return id;
    }

    public Set<TopicPartition> partitions() {
        return this.partitionGroup.partitions();
    }

    /**
     * Adds records to queues
     *
     * @param partition the partition
     * @param iterator  the iterator of records
     */
    @SuppressWarnings("unchecked")
    public void addRecords(TopicPartition partition, Iterator<ConsumerRecord<byte[], byte[]>> iterator) {

        // get deserializers for this partition
        Deserializer<?> keyDeserializer = partitionGroup.keyDeserializer(partition);
        Deserializer<?> valDeserializer = partitionGroup.valDeserializer(partition);

        while (iterator.hasNext()) {

            ConsumerRecord<byte[], byte[]> rawRecord = iterator.next();

            // deserialize the raw record, extract the timestamp and put into the queue
            Object key = keyDeserializer.deserialize(rawRecord.topic(), rawRecord.key());
            Object value = valDeserializer.deserialize(rawRecord.topic(), rawRecord.value());
            long timestamp = timestampExtractor.extract(rawRecord.topic(), key, value);

            StampedRecord stampedRecord = new StampedRecord(new ConsumerRecord<>(rawRecord.topic(), rawRecord.partition(), rawRecord.offset(), key, value), timestamp);

            partitionGroup.putRecord(stampedRecord, partition);
        }
    }

    /**
     * Schedules a punctuation for the processor
     *
     * @param processor the processor requesting scheduler
     * @param interval  the interval in milliseconds
     */
    public void schedule(Processor processor, long interval) {
        punctuationQueue.schedule(new PunctuationSchedule(processor, interval));
    }

    /**
     * Processes one record
     */
    @SuppressWarnings("unchecked")
    public boolean process() {
        synchronized (this) {
            boolean readyForNextExecution = false;

            // get the next record queue to process
            RecordQueue queue = partitionGroup.nextQueue();

            // get a record from the queue and process it
            // by passing to the source node of the topology
            this.currRecord = partitionGroup.getRecord(queue);
            this.currNode = queue.source();
            this.currNode.process(currRecord.key(), currRecord.value());

            // update the consumed offset map after processing is done
            consumedOffsets.put(queue.partition(), currRecord.offset());

            // commit the current task state if requested during the processing
            if (commitRequested) {
                // 1) flush local state
                ((ProcessorContextImpl) processorContext).stateManager().flush();

                // 2) commit consumed offsets
                consumer.commit(consumedOffsets, CommitType.SYNC);

                // 3) flush produced records in the downstream
                ((ProcessorContextImpl) processorContext).recordCollector().flush();
            }

            // we can continue processing this task as long as its
            // partition group still have buffered records
            if (partitionGroup.numbuffered() > 0) {
                readyForNextExecution = true;
            }

            // if after processing this record, its partition queue's buffered size has been
            // decreased to the threshold, we can then resume the consumption on this partition
            if (partitionGroup.numbuffered(queue.partition()) == this.maxBufferedSize) {
                consumer.resume(queue.partition());
            }

            // possibly trigger registered punctuation functions if
            // partition group's time has reached the defined stamp
            long timestamp = partitionGroup.timestamp();
            punctuationQueue.mayPunctuate(timestamp);

            return readyForNextExecution;
        }
    }

    public StampedRecord record() {
        return this.currRecord;
    }

    public ProcessorNode node() {
        return this.currNode;
    }

    public void node(ProcessorNode node) {
        this.currNode = node;
    }

    /**
     * Request committing the current task's state
     */
    public void commit() {
        this.commitRequested = true;
    }

    public void close() {
        this.partitionGroup.close();
        this.consumedOffsets.clear();
    }

    protected RecordQueue createRecordQueue(TopicPartition partition, SourceNode source) {
        return new RecordQueue(partition, source);
    }
}
