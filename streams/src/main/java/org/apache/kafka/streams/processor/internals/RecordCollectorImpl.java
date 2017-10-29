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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordCollectorImpl implements RecordCollector {
    private static final int MAX_SEND_ATTEMPTS = 3;
    private static final long SEND_RETRY_BACKOFF = 100L;


    private final Logger log;
    private final Producer<byte[], byte[]> producer;
    private final Map<TopicPartition, Long> offsets;
    private final String logPrefix;

    private volatile KafkaException sendException;

    public RecordCollectorImpl(final Producer<byte[], byte[]> producer, final String streamTaskId, final LogContext logContext) {
        this.producer = producer;
        this.offsets = new HashMap<>();
        this.logPrefix = String.format("task [%s] ", streamTaskId);
        this.log = logContext.logger(getClass());
    }

    @Override
    public <K, V> void send(final String topic,
                            final K key,
                            final V value,
                            final Long timestamp,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer,
                            final StreamPartitioner<? super K, ? super V> partitioner) {
        Integer partition = null;

        if (partitioner != null) {
            final List<PartitionInfo> partitions = producer.partitionsFor(topic);
            if (partitions.size() > 0) {
                partition = partitioner.partition(key, value, partitions.size());
            } else {
                throw new StreamsException("Could not get partition information for topic '" + topic + "'." +
                    " This can happen if the topic does not exist.");
            }
        }

        send(topic, key, value, partition, timestamp, keySerializer, valueSerializer);
    }

    @Override
    public <K, V> void  send(final String topic,
                             final K key,
                             final V value,
                             final Integer partition,
                             final Long timestamp,
                             final Serializer<K> keySerializer,
                             final Serializer<V> valueSerializer) {
        checkForException();
        final byte[] keyBytes = keySerializer.serialize(topic, key);
        final byte[] valBytes = valueSerializer.serialize(topic, value);

        final ProducerRecord<byte[], byte[]> serializedRecord =
                new ProducerRecord<>(topic, partition, timestamp, keyBytes, valBytes);

        // counting from 1 to make check further down more natural
        // -> `if (attempt == MAX_SEND_ATTEMPTS)`
        for (int attempt = 1; attempt <= MAX_SEND_ATTEMPTS; ++attempt) {
            try {
                producer.send(serializedRecord, new Callback() {
                    @Override
                    public void onCompletion(final RecordMetadata metadata, final Exception exception) {
                        if (exception == null) {
                            if (sendException != null) {
                                return;
                            }
                            final TopicPartition tp = new TopicPartition(metadata.topic(), metadata.partition());
                            offsets.put(tp, metadata.offset());
                        } else {
                            if (sendException == null) {
                                log.error("Error sending record (key {} value {} timestamp {}) to topic {} due to {}; " +
                                                "No more records will be sent and no more offsets will be recorded for this task.",
                                        key, value, timestamp, topic, exception);
                                if (exception instanceof ProducerFencedException) {
                                    sendException = new ProducerFencedException(String.format("%sAbort sending since producer got fenced with a previous record (key %s value %s timestamp %d) to topic %s, error message: %s",
                                            logPrefix, key, value, timestamp, topic, exception.getMessage()));
                                } else {
                                    sendException = new StreamsException(String.format("%sAbort sending since an error caught with a previous record (key %s value %s timestamp %d) to topic %s due to %s.",
                                            logPrefix, key, value, timestamp, topic, exception), exception);
                                }
                            }
                        }
                    }
                });
                return;
            } catch (final TimeoutException e) {
                if (attempt == MAX_SEND_ATTEMPTS) {
                    throw new StreamsException(String.format("%sFailed to send record to topic %s due to timeout after %d attempts", logPrefix, topic, attempt));
                }
                log.warn("Timeout exception caught when sending record to topic {}; retrying with {} attempt", topic, attempt);
                Utils.sleep(SEND_RETRY_BACKOFF);
            }
        }
    }

    private void checkForException()  {
        if (sendException != null) {
            throw sendException;
        }
    }

    @Override
    public void flush() {
        log.debug("Flushing producer");
        producer.flush();
        checkForException();
    }

    @Override
    public void close() {
        log.debug("Closing producer");
        producer.close();
        checkForException();
    }

    @Override
    public Map<TopicPartition, Long> offsets() {
        return offsets;
    }

    // for testing only
    Producer<byte[], byte[]> producer() {
        return producer;
    }

}
