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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordCollectorImpl implements RecordCollector {
    private static final int MAX_SEND_ATTEMPTS = 3;
    private static final long SEND_RETRY_BACKOFF = 100L;

    private static final Logger log = LoggerFactory.getLogger(RecordCollectorImpl.class);
    
    private final Producer<byte[], byte[]> producer;
    private final Map<TopicPartition, Long> offsets;
    private final String logPrefix;
    private volatile Exception sendException;


    public RecordCollectorImpl(Producer<byte[], byte[]> producer, String streamTaskId) {
        this.producer = producer;
        this.offsets = new HashMap<>();
        this.logPrefix = String.format("task [%s]", streamTaskId);
    }

    @Override
    public <K, V> void send(final String topic,
                            K key,
                            V value,
                            Integer partition,
                            Long timestamp,
                            Serializer<K> keySerializer,
                            Serializer<V> valueSerializer) {
        send(topic, key, value, partition, timestamp, keySerializer, valueSerializer, null);
    }

    @Override
    public <K, V> void  send(final String topic,
                             K key,
                             V value,
                             Integer partition,
                             Long timestamp,
                             Serializer<K> keySerializer,
                             Serializer<V> valueSerializer,
                             StreamPartitioner<? super K, ? super V> partitioner) {
        checkForException();
        byte[] keyBytes = keySerializer.serialize(topic, key);
        byte[] valBytes = valueSerializer.serialize(topic, value);
        if (partition == null && partitioner != null) {
            List<PartitionInfo> partitions = this.producer.partitionsFor(topic);
            if (partitions != null && partitions.size() > 0)
                partition = partitioner.partition(key, value, partitions.size());
        }

        ProducerRecord<byte[], byte[]> serializedRecord =
                new ProducerRecord<>(topic, partition, timestamp, keyBytes, valBytes);

        for (int attempt = 1; attempt <= MAX_SEND_ATTEMPTS; attempt++) {
            try {
                this.producer.send(serializedRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            if (sendException != null) {
                                return;
                            }
                            TopicPartition tp = new TopicPartition(metadata.topic(), metadata.partition());
                            offsets.put(tp, metadata.offset());
                        } else {
                            sendException = exception;
                            log.error("{} Error sending record to topic {}. No more offsets will be recorded for this task and the exception will eventually be thrown", logPrefix, topic, exception);
                        }
                    }
                });
                return;
            } catch (TimeoutException e) {
                if (attempt == MAX_SEND_ATTEMPTS) {
                    throw new StreamsException(String.format("%s Failed to send record to topic %s after %d attempts", logPrefix, topic, attempt));
                }
                log.warn("{} Timeout exception caught when sending record to topic {} attempt {}", logPrefix, topic, attempt);
                Utils.sleep(SEND_RETRY_BACKOFF);
            }

        }
    }

    private void checkForException() {
        if (sendException != null) {
            throw new StreamsException(String.format("%s exception caught when producing", logPrefix), sendException);
        }
    }

    @Override
    public void flush() {
        log.debug("{} Flushing producer", logPrefix);
        this.producer.flush();
        checkForException();
    }

    /**
     * Closes this RecordCollector
     */
    @Override
    public void close() {
        producer.close();
        checkForException();
    }

    /**
     * The last ack'd offset from the producer
     *
     * @return the map from TopicPartition to offset
     */
    @Override
    public Map<TopicPartition, Long> offsets() {
        return this.offsets;
    }
}
