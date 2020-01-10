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

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The interface for the {@link KafkaProducer}
 * @see KafkaProducer
 * @see MockProducer
 */
public interface Producer<K, V> extends Closeable {

    /**
     * See {@link KafkaProducer#initTransactions()}
     */
    void initTransactions();

    /**
     * See {@link KafkaProducer#beginTransaction()}
     */
    void beginTransaction() throws ProducerFencedException;

    /**
     * See {@link KafkaProducer#sendOffsetsToTransaction(Map, String)}
     */
    void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                  String consumerGroupId) throws ProducerFencedException;

    /**
     * See {@link KafkaProducer#sendOffsetsToTransaction(Map, ConsumerGroupMetadata)}
     */
    void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                  ConsumerGroupMetadata groupMetadata) throws ProducerFencedException;

    /**
     * See {@link KafkaProducer#commitTransaction()}
     */
    void commitTransaction() throws ProducerFencedException;

    /**
     * See {@link KafkaProducer#abortTransaction()}
     */
    void abortTransaction() throws ProducerFencedException;

    /**
     * See {@link KafkaProducer#send(ProducerRecord)}
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record);

    /**
     * See {@link KafkaProducer#send(ProducerRecord, Callback)}
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);

    /**
     * See {@link KafkaProducer#flush()}
     */
    void flush();

    /**
     * See {@link KafkaProducer#partitionsFor(String)}
     */
    List<PartitionInfo> partitionsFor(String topic);

    /**
     * See {@link KafkaProducer#metrics()}
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * See {@link KafkaProducer#close()}
     */
    void close();

    @Deprecated
    default void close(long timeout, TimeUnit unit) {
        close(Duration.ofMillis(unit.toMillis(timeout)));
    }

    /**
     * See {@link KafkaProducer#close(Duration)}
     */
    void close(Duration timeout);
}
