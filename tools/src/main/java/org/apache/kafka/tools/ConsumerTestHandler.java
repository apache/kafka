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
package org.apache.kafka.tools;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A consumer based EOS test handler that will poll data from input topic and
 * buffer them in an in-memory queue.
 */
class ConsumerTestHandler<K, V> implements ClientTestHandler<K, V> {

    private final Consumer<K, V> consumer;
    private final String consumerGroupId;
    private Deque<ProducerRecord<K, V>> recordsToBeSent;
    private final long pollTimeout;
    private final String inputTopic;
    private final String outputTopic;
    private final Producer<K, V> producer;

    ConsumerTestHandler(Namespace res,
                        String consumerGroupId,
                        Producer<K, V> producer) throws IOException {
        this.consumerGroupId = consumerGroupId;
        List<String> consumerProps = res.getList("consumerConfig");
        String consumerConfigFile = res.getString("consumerConfigFile");
        pollTimeout = res.getLong("pollTimeout");
        inputTopic = res.getString("inputTopic");
        outputTopic = res.getString("topic");

        Properties consumerProperties = KafkaClientPerformance.generateProps(consumerProps, consumerConfigFile);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.consumer.subscribe(Collections.singletonList(inputTopic));

        this.producer = producer;
        recordsToBeSent = new ArrayDeque<>();
    }

    @Override
    public ProducerRecord<K, V> getRecord() {
        System.out.println("Consumer polls new data with timeout " + pollTimeout);
        while (recordsToBeSent.isEmpty()) {
            ConsumerRecords<K, V> consumedRecords = consumer.poll(Duration.ofMillis(pollTimeout));
            for (ConsumerRecord<K, V> consumerRecord : consumedRecords) {
                recordsToBeSent.add(new ProducerRecord<>(outputTopic, consumerRecord.value()));
            }
        }
        return recordsToBeSent.remove();
    }

    @Override
    public void onFlush() {
        producer.flush();
        consumer.commitSync();
    }

    @Override
    public void onTxnCommit() {
        producer.sendOffsetsToTransaction(offsets(consumer), consumerGroupId);
        producer.commitTransaction();
    }

    private Map<TopicPartition, OffsetAndMetadata> offsets(Consumer<K, V> consumer) {
        Map<TopicPartition, OffsetAndMetadata> positions = new HashMap<>();
        for (TopicPartition topicPartition : consumer.assignment()) {
            positions.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition), null));
        }
        return positions;
    }

    @Override
    public void close() {
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(inputTopic);
        Set<TopicPartition> partitions = new HashSet<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }

        consumer.seekToBeginning(partitions);
        consumer.unsubscribe();
        consumer.close();
    }
}
