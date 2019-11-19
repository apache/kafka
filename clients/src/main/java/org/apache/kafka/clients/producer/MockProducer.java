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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata;
import org.apache.kafka.clients.producer.internals.ProduceRequestResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;

/**
 * A mock of the producer interface you can use for testing code that uses Kafka.
 * <p>
 * By default this mock will synchronously complete each send call successfully. However it can be configured to allow
 * the user to control the completion of the call and supply an optional error for the producer to throw.
 */
public class MockProducer<K, V> implements Producer<K, V> {

    private final Cluster cluster;
    private final Partitioner partitioner;
    private final List<ProducerRecord<K, V>> sent;
    private final List<ProducerRecord<K, V>> uncommittedSends;
    private final Deque<Completion> completions;
    private final Map<TopicPartition, Long> offsets;
    private final List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> consumerGroupOffsets;
    private Map<String, Map<TopicPartition, OffsetAndMetadata>> uncommittedConsumerGroupOffsets;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private boolean autoComplete;
    private boolean closed;
    private boolean transactionInitialized;
    private boolean transactionInFlight;
    private boolean transactionCommitted;
    private boolean transactionAborted;
    private boolean producerFenced;
    private boolean producerFencedOnClose;
    private boolean sentOffsets;
    private long commitCount = 0L;
    private Map<MetricName, Metric> mockMetrics;

    /**
     * Create a mock producer
     *
     * @param cluster The cluster holding metadata for this producer
     * @param autoComplete If true automatically complete all requests successfully and execute the callback. Otherwise
     *        the user must call {@link #completeNext()} or {@link #errorNext(RuntimeException)} after
     *        {@link #send(ProducerRecord) send()} to complete the call and unblock the {@link
     *        java.util.concurrent.Future Future&lt;RecordMetadata&gt;} that is returned.
     * @param partitioner The partition strategy
     * @param keySerializer The serializer for key that implements {@link Serializer}.
     * @param valueSerializer The serializer for value that implements {@link Serializer}.
     */
    public MockProducer(final Cluster cluster,
                        final boolean autoComplete,
                        final Partitioner partitioner,
                        final Serializer<K> keySerializer,
                        final Serializer<V> valueSerializer) {
        this.cluster = cluster;
        this.autoComplete = autoComplete;
        this.partitioner = partitioner;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.offsets = new HashMap<>();
        this.sent = new ArrayList<>();
        this.uncommittedSends = new ArrayList<>();
        this.consumerGroupOffsets = new ArrayList<>();
        this.uncommittedConsumerGroupOffsets = new HashMap<>();
        this.completions = new ArrayDeque<>();
        this.mockMetrics = new HashMap<>();
    }

    /**
     * Create a new mock producer with invented metadata the given autoComplete setting and key\value serializers.
     *
     * Equivalent to {@link #MockProducer(Cluster, boolean, Partitioner, Serializer, Serializer)} new MockProducer(Cluster.empty(), autoComplete, new DefaultPartitioner(), keySerializer, valueSerializer)}
     */
    public MockProducer(final boolean autoComplete,
                        final Serializer<K> keySerializer,
                        final Serializer<V> valueSerializer) {
        this(Cluster.empty(), autoComplete, new DefaultPartitioner(), keySerializer, valueSerializer);
    }

    /**
     * Create a new mock producer with invented metadata the given autoComplete setting, partitioner and key\value serializers.
     *
     * Equivalent to {@link #MockProducer(Cluster, boolean, Partitioner, Serializer, Serializer)} new MockProducer(Cluster.empty(), autoComplete, partitioner, keySerializer, valueSerializer)}
     */
    public MockProducer(final boolean autoComplete,
                        final Partitioner partitioner,
                        final Serializer<K> keySerializer,
                        final Serializer<V> valueSerializer) {
        this(Cluster.empty(), autoComplete, partitioner, keySerializer, valueSerializer);
    }

    /**
     * Create a new mock producer with invented metadata.
     *
     * Equivalent to {@link #MockProducer(Cluster, boolean, Partitioner, Serializer, Serializer)} new MockProducer(Cluster.empty(), false, null, null, null)}
     */
    public MockProducer() {
        this(Cluster.empty(), false, null, null, null);
    }

    @Override
    public void initTransactions() {
        verifyProducerState();
        if (this.transactionInitialized) {
            throw new IllegalStateException("MockProducer has already been initialized for transactions.");
        }
        this.transactionInitialized = true;
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        verifyProducerState();
        verifyTransactionsInitialized();
        this.transactionInFlight = true;
        this.transactionCommitted = false;
        this.transactionAborted = false;
        this.sentOffsets = false;
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) throws ProducerFencedException {
        verifyProducerState();
        verifyTransactionsInitialized();
        verifyNoTransactionInFlight();
        Objects.requireNonNull(consumerGroupId);
        if (offsets.size() == 0) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> uncommittedOffsets = this.uncommittedConsumerGroupOffsets.get(consumerGroupId);
        if (uncommittedOffsets == null) {
            uncommittedOffsets = new HashMap<>();
            this.uncommittedConsumerGroupOffsets.put(consumerGroupId, uncommittedOffsets);
        }
        uncommittedOffsets.putAll(offsets);
        this.sentOffsets = true;
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        verifyProducerState();
        verifyTransactionsInitialized();
        verifyNoTransactionInFlight();

        flush();

        this.sent.addAll(this.uncommittedSends);
        if (!this.uncommittedConsumerGroupOffsets.isEmpty())
            this.consumerGroupOffsets.add(this.uncommittedConsumerGroupOffsets);

        this.uncommittedSends.clear();
        this.uncommittedConsumerGroupOffsets = new HashMap<>();
        this.transactionCommitted = true;
        this.transactionAborted = false;
        this.transactionInFlight = false;

        ++this.commitCount;
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        verifyProducerState();
        verifyTransactionsInitialized();
        verifyNoTransactionInFlight();
        flush();
        this.uncommittedSends.clear();
        this.uncommittedConsumerGroupOffsets.clear();
        this.transactionCommitted = false;
        this.transactionAborted = true;
        this.transactionInFlight = false;
    }

    private synchronized void verifyProducerState() {
        if (this.closed) {
            throw new IllegalStateException("MockProducer is already closed.");
        }
        if (this.producerFenced) {
            throw new ProducerFencedException("MockProducer is fenced.");
        }
    }

    private void verifyTransactionsInitialized() {
        if (!this.transactionInitialized) {
            throw new IllegalStateException("MockProducer hasn't been initialized for transactions.");
        }
    }

    private void verifyNoTransactionInFlight() {
        if (!this.transactionInFlight) {
            throw new IllegalStateException("There is no open transaction.");
        }
    }

    /**
     * Adds the record to the list of sent records. The {@link RecordMetadata} returned will be immediately satisfied.
     * 
     * @see #history()
     */
    @Override
    public synchronized Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    /**
     * Adds the record to the list of sent records.
     *
     * @see #history()
     */
    @Override
    public synchronized Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        if (this.closed) {
            throw new IllegalStateException("MockProducer is already closed.");
        }
        if (this.producerFenced) {
            throw new KafkaException("MockProducer is fenced.", new ProducerFencedException("Fenced"));
        }
        int partition = 0;
        if (!this.cluster.partitionsForTopic(record.topic()).isEmpty())
            partition = partition(record, this.cluster);
        TopicPartition topicPartition = new TopicPartition(record.topic(), partition);
        ProduceRequestResult result = new ProduceRequestResult(topicPartition);
        FutureRecordMetadata future = new FutureRecordMetadata(result, 0, RecordBatch.NO_TIMESTAMP,
                0L, 0, 0, Time.SYSTEM);
        long offset = nextOffset(topicPartition);
        Completion completion = new Completion(offset, new RecordMetadata(topicPartition, 0, offset,
                RecordBatch.NO_TIMESTAMP, Long.valueOf(0L), 0, 0), result, callback);

        if (!this.transactionInFlight)
            this.sent.add(record);
        else
            this.uncommittedSends.add(record);

        if (autoComplete)
            completion.complete(null);
        else
            this.completions.addLast(completion);

        return future;
    }

    /**
     * Get the next offset for this topic/partition
     */
    private long nextOffset(TopicPartition tp) {
        Long offset = this.offsets.get(tp);
        if (offset == null) {
            this.offsets.put(tp, 1L);
            return 0L;
        } else {
            Long next = offset + 1;
            this.offsets.put(tp, next);
            return offset;
        }
    }

    public synchronized void flush() {
        verifyProducerState();
        while (!this.completions.isEmpty())
            completeNext();
    }

    public List<PartitionInfo> partitionsFor(String topic) {
        return this.cluster.partitionsForTopic(topic);
    }

    public Map<MetricName, Metric> metrics() {
        return mockMetrics;
    }

    /**
     * Set a mock metric for testing purpose
     */
    public void setMockMetrics(MetricName name, Metric metric) {
        mockMetrics.put(name, metric);
    }

    @Override
    public void close() {
        close(Duration.ofMillis(0));
    }

    @Override
    public void close(Duration timeout) {
        if (producerFencedOnClose) {
            throw new ProducerFencedException("MockProducer is fenced.");
        }
        this.closed = true;
    }

    public boolean closed() {
        return this.closed;
    }

    public synchronized void fenceProducer() {
        verifyProducerState();
        verifyTransactionsInitialized();
        this.producerFenced = true;
    }

    public void fenceProducerOnClose() {
        verifyProducerState();
        verifyTransactionsInitialized();
        this.producerFencedOnClose = true;
    }

    public boolean transactionInitialized() {
        return this.transactionInitialized;
    }

    public boolean transactionInFlight() {
        return this.transactionInFlight;
    }

    public boolean transactionCommitted() {
        return this.transactionCommitted;
    }

    public boolean transactionAborted() {
        return this.transactionAborted;
    }

    public boolean flushed() {
        return this.completions.isEmpty();
    }

    public boolean sentOffsets() {
        return this.sentOffsets;
    }

    public long commitCount() {
        return this.commitCount;
    }

    /**
     * Get the list of sent records since the last call to {@link #clear()}
     */
    public synchronized List<ProducerRecord<K, V>> history() {
        return new ArrayList<>(this.sent);
    }

    /**
     * Get the list of committed consumer group offsets since the last call to {@link #clear()}
     */
    public synchronized List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> consumerGroupOffsetsHistory() {
        return new ArrayList<>(this.consumerGroupOffsets);
    }
    /**
     *
     * Clear the stored history of sent records, consumer group offsets, and transactional state
     */
    public synchronized void clear() {
        this.sent.clear();
        this.uncommittedSends.clear();
        this.completions.clear();
        this.consumerGroupOffsets.clear();
        this.uncommittedConsumerGroupOffsets.clear();
        this.transactionInitialized = false;
        this.transactionInFlight = false;
        this.transactionCommitted = false;
        this.transactionAborted = false;
        this.producerFenced = false;
    }

    /**
     * Complete the earliest uncompleted call successfully.
     *
     * @return true if there was an uncompleted call to complete
     */
    public synchronized boolean completeNext() {
        return errorNext(null);
    }

    /**
     * Complete the earliest uncompleted call with the given error.
     *
     * @return true if there was an uncompleted call to complete
     */
    public synchronized boolean errorNext(RuntimeException e) {
        Completion completion = this.completions.pollFirst();
        if (completion != null) {
            completion.complete(e);
            return true;
        } else {
            return false;
        }
    }

    /**
     * computes partition for given record.
     */
    private int partition(ProducerRecord<K, V> record, Cluster cluster) {
        Integer partition = record.partition();
        String topic = record.topic();
        if (partition != null) {
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            int numPartitions = partitions.size();
            // they have given us a partition, use it
            if (partition < 0 || partition >= numPartitions)
                throw new IllegalArgumentException("Invalid partition given with record: " + partition
                                                   + " is not in the range [0..."
                                                   + numPartitions
                                                   + "].");
            return partition;
        }
        byte[] keyBytes = keySerializer.serialize(topic, record.headers(), record.key());
        byte[] valueBytes = valueSerializer.serialize(topic, record.headers(), record.value());
        return this.partitioner.partition(topic, record.key(), keyBytes, record.value(), valueBytes, cluster);
    }

    private static class Completion {
        private final long offset;
        private final RecordMetadata metadata;
        private final ProduceRequestResult result;
        private final Callback callback;

        public Completion(long offset,
                          RecordMetadata metadata,
                          ProduceRequestResult result,
                          Callback callback) {
            this.metadata = metadata;
            this.offset = offset;
            this.result = result;
            this.callback = callback;
        }

        public void complete(RuntimeException e) {
            result.set(e == null ? offset : -1L, RecordBatch.NO_TIMESTAMP, e);
            if (callback != null) {
                if (e == null)
                    callback.onCompletion(metadata, null);
                else
                    callback.onCompletion(null, e);
            }
            result.done();
        }
    }

}
