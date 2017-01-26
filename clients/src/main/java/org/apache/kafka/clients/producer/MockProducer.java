/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.internals.FutureRecordMetadata;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.clients.producer.internals.ProduceRequestResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Serializer;


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
    private final Deque<Completion> completions;
    private boolean autoComplete;
    private Map<TopicPartition, Long> offsets;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    /**
     * Create a mock producer
     *
     * @param cluster The cluster holding metadata for this producer
     * @param autoComplete If true automatically complete all requests successfully and execute the callback. Otherwise
     *        the user must call {@link #completeNext()} or {@link #errorNext(RuntimeException)} after
     *        {@link #send(ProducerRecord) send()} to complete the call and unblock the @{link
     *        java.util.concurrent.Future Future&lt;RecordMetadata&gt;} that is returned.
     * @param partitioner The partition strategy
     * @param keySerializer The serializer for key that implements {@link Serializer}.
     * @param valueSerializer The serializer for value that implements {@link Serializer}.
     */
    public MockProducer(Cluster cluster, boolean autoComplete, Partitioner partitioner, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.cluster = cluster;
        this.autoComplete = autoComplete;
        this.partitioner = partitioner;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.offsets = new HashMap<TopicPartition, Long>();
        this.sent = new ArrayList<ProducerRecord<K, V>>();
        this.completions = new ArrayDeque<Completion>();
    }

    /**
     * Create a new mock producer with invented metadata the given autoComplete setting and key\value serializers
     *
     * Equivalent to {@link #MockProducer(Cluster, boolean, Partitioner, Serializer, Serializer)} new MockProducer(Cluster.empty(), autoComplete, new DefaultPartitioner(), keySerializer, valueSerializer)}
     */
    public MockProducer(boolean autoComplete, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(Cluster.empty(), autoComplete, new DefaultPartitioner(), keySerializer, valueSerializer);
    }

    /**
     * Create a new mock producer with invented metadata the given autoComplete setting, partitioner and key\value serializers
     *
     * Equivalent to {@link #MockProducer(Cluster, boolean, Partitioner, Serializer, Serializer)} new MockProducer(Cluster.empty(), autoComplete, partitioner, keySerializer, valueSerializer)}
     */
    public MockProducer(boolean autoComplete, Partitioner partitioner, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(Cluster.empty(), autoComplete, partitioner, keySerializer, valueSerializer);
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
        int partition = 0;
        if (this.cluster.partitionsForTopic(record.topic()) != null)
            partition = partition(record, this.cluster);
        TopicPartition topicPartition = new TopicPartition(record.topic(), partition);
        ProduceRequestResult result = new ProduceRequestResult(topicPartition);
        FutureRecordMetadata future = new FutureRecordMetadata(result, 0, Record.NO_TIMESTAMP, 0, 0, 0);
        long offset = nextOffset(topicPartition);
        Completion completion = new Completion(offset,
                                               new RecordMetadata(topicPartition, 0, offset, Record.NO_TIMESTAMP, 0, 0, 0),
                                               result, callback);
        this.sent.add(record);
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
        while (!this.completions.isEmpty())
            completeNext();
    }

    public List<PartitionInfo> partitionsFor(String topic) {
        return this.cluster.partitionsForTopic(topic);
    }

    public Map<MetricName, Metric> metrics() {
        return Collections.emptyMap();
    }

    @Override
    public void close() {
    }

    @Override
    public void close(long timeout, TimeUnit timeUnit) {
    }

    /**
     * Get the list of sent records since the last call to {@link #clear()}
     */
    public synchronized List<ProducerRecord<K, V>> history() {
        return new ArrayList<ProducerRecord<K, V>>(this.sent);
    }

    /**
     * Clear the stored history of sent records
     */
    public synchronized void clear() {
        this.sent.clear();
        this.completions.clear();
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
        byte[] keyBytes = keySerializer.serialize(topic, record.key());
        byte[] valueBytes = valueSerializer.serialize(topic, record.value());
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
            result.set(e == null ? offset : -1L, Record.NO_TIMESTAMP, e);
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
