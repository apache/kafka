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

public class RecordCollector {
    private static final int MAX_SEND_ATTEMPTS = 3;
    private static final long SEND_RETRY_BACKOFF = 100L;

    /**
     * A supplier of a {@link RecordCollector} instance.
     */
    public interface Supplier {
        /**
         * Get the record collector.
         * @return the record collector
         */
        RecordCollector recordCollector();
    }

    private static final Logger log = LoggerFactory.getLogger(RecordCollector.class);

    private final Producer<byte[], byte[]> producer;
    private final Map<TopicPartition, Long> offsets;
    private final String logPrefix;


    public RecordCollector(Producer<byte[], byte[]> producer, String streamTaskId) {
        this.producer = producer;
        this.offsets = new HashMap<>();
        this.logPrefix = String.format("task [%s]", streamTaskId);
    }

    public <K, V> void send(ProducerRecord<K, V> record, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        send(record, keySerializer, valueSerializer, null);
    }

    public <K, V> void send(ProducerRecord<K, V> record, Serializer<K> keySerializer, Serializer<V> valueSerializer,
                            StreamPartitioner<K, V> partitioner) {
        byte[] keyBytes = keySerializer.serialize(record.topic(), record.key());
        byte[] valBytes = valueSerializer.serialize(record.topic(), record.value());
        Integer partition = record.partition();
        if (partition == null && partitioner != null) {
            List<PartitionInfo> partitions = this.producer.partitionsFor(record.topic());
            if (partitions != null && partitions.size() > 0)
                partition = partitioner.partition(record.key(), record.value(), partitions.size());
        }

        ProducerRecord<byte[], byte[]> serializedRecord =
                new ProducerRecord<>(record.topic(), partition, record.timestamp(), keyBytes, valBytes);
        final String topic = serializedRecord.topic();

        for (int attempt = 1; attempt <= MAX_SEND_ATTEMPTS; attempt++) {
            try {
                this.producer.send(serializedRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            TopicPartition tp = new TopicPartition(metadata.topic(), metadata.partition());
                            offsets.put(tp, metadata.offset());
                        } else {
                            log.error("{} Error sending record to topic {}", logPrefix, topic, exception);
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

    public void flush() {
        log.debug("{} Flushing producer", logPrefix);
        this.producer.flush();
    }

    /**
     * Closes this RecordCollector
     */
    public void close() {
        producer.close();
    }

    /**
     * The last ack'd offset from the producer
     *
     * @return the map from TopicPartition to offset
     */
    Map<TopicPartition, Long> offsets() {
        return this.offsets;
    }
}
