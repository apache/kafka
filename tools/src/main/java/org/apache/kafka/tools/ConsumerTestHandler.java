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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

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
    private final Deque<ProducerRecord<K, V>> recordsToBeSent;
    private final long pollTimeout;
    private final String inputTopic;
    private final String outputTopic;
    private final Producer<K, V> producer;
    private final int pollRetries;

    ConsumerTestHandler(Namespace res,
                        String consumerGroupId,
                        Producer<K, V> producer,
                        String outputTopic) throws IOException {
        this.consumerGroupId = consumerGroupId;
        List<String> consumerProps = res.getList("consumerConfig");
        String consumerConfigFile = res.getString("consumerConfigFile");
        this.pollTimeout = res.getLong("pollTimeout");
        this.inputTopic = res.getString("inputTopic");
        this.outputTopic = outputTopic;

        Properties consumerProperties = KafkaClientPerformance.generateProps(consumerProps, consumerConfigFile);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.consumer.subscribe(Collections.singletonList(inputTopic));

        this.producer = producer;
        recordsToBeSent = new ArrayDeque<>();
        this.pollRetries = 10;
    }

    @Override
    public ProducerRecord<K, V> getRecord() {
        System.out.println("Consumer polls new data with timeout " + pollTimeout);

        int numPolls = 0;
        while (recordsToBeSent.isEmpty() && numPolls < pollRetries) {
            ConsumerRecords<K, V> consumedRecords = consumer.poll(Duration.ofMillis(pollTimeout));
            for (ConsumerRecord<K, V> consumerRecord : consumedRecords) {
                recordsToBeSent.add(new ProducerRecord<>(outputTopic, consumerRecord.value()));
            }
            numPolls += 1;
        }
        // No records are being fetched, fail the application.
        if (recordsToBeSent.isEmpty()) {
            throw new IllegalStateException("Fetch fails after " + numPolls + " polls. " +
                "There could be some network error or not enough records within the input topic");
        }
        return recordsToBeSent.remove();
    }

    @Override
    public void flush() {
        producer.flush();
        consumer.commitSync();
    }

    @Override
    public void commitTransaction() {
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
