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
package org.apache.kafka.server.log;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogAppendTimeTest {
    private static final String TOPIC = "log-append-time-topic";
    private static final int NUM_PARTITION = 1;
    private static final short NUM_REPLICAS = 1;

    @ClusterTest(
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = "log.message.timestamp.type", value = "LogAppendTime"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        }
    )
    public void testProduceConsumeWithConfigOnBroker(ClusterInstance clusterInstance) throws InterruptedException {
        clusterInstance.createTopic(TOPIC, NUM_PARTITION, NUM_REPLICAS);

        testProduceConsume(clusterInstance);
    }

    @ClusterTest(
        types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        }
    )
    public void testProduceConsumeWithConfigOnTopic(ClusterInstance clusterInstance) throws InterruptedException {
        try (Admin admin = clusterInstance.admin()) {
            admin.createTopics(List.of(
                new NewTopic(TOPIC, NUM_PARTITION, NUM_REPLICAS).
                    configs(Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime"))));
            clusterInstance.waitTopicCreation(TOPIC, NUM_PARTITION);
        }

        testProduceConsume(clusterInstance);
    }

    public void testProduceConsume(ClusterInstance clusterInstance) throws InterruptedException {
        long now = System.currentTimeMillis();
        long createTime = now - TimeUnit.DAYS.toMillis(1);
        int recordCount = 10;
        List<ProducerRecord<byte[], byte[]>> producerRecords = IntStream.range(0, recordCount)
            .mapToObj(i -> new ProducerRecord<>(TOPIC, null, createTime, "key".getBytes(), "value".getBytes()))
            .toList();

        List<RecordMetadata> recordMetadatas = new ArrayList<>();
        try (Producer<byte[], byte[]> producer = clusterInstance.producer()) {
            producerRecords.stream()
                .map(record ->
                    assertDoesNotThrow(() -> producer.send(record).get(10, TimeUnit.SECONDS)))
                .forEach(recordMetadatas::add);
        }
        assertEquals(recordCount, recordMetadatas.size());
        recordMetadatas.forEach(recordMetadata -> {
            assertTrue(recordMetadata.timestamp() >= now);
            assertTrue(recordMetadata.timestamp() < now + TimeUnit.SECONDS.toMillis(60));
        });

        for (GroupProtocol groupProtocol : clusterInstance.supportedGroupProtocols()) {
            try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(
                Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name())
            )) {
                consumer.subscribe(Set.of(TOPIC));
                ArrayList<ConsumerRecord<byte[], byte[]>> consumerRecords = new ArrayList<>();
                TestUtils.waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                    records.forEach(consumerRecords::add);
                    return consumerRecords.size() == producerRecords.size();
                }, "Consumer with protocol " + groupProtocol.name + " cannot consume all records");

                consumerRecords.forEach(consumerRecord -> {
                    int index = consumerRecords.indexOf(consumerRecord);
                    ProducerRecord<byte[], byte[]> producerRecord = producerRecords.get(index);
                    RecordMetadata recordMetadata = recordMetadatas.get(index);
                    assertArrayEquals(producerRecord.key(), consumerRecord.key(), "Key mismatch for consumer with protocol " + groupProtocol.name);
                    assertArrayEquals(producerRecord.value(), consumerRecord.value(), "Key mismatch for consumer with protocol " + groupProtocol.name);
                    assertNotEquals(producerRecord.timestamp(), consumerRecord.timestamp(), "Timestamp mismatch with producer record for consumer with protocol " + groupProtocol.name);
                    assertEquals(recordMetadata.timestamp(), consumerRecord.timestamp(), "Timestamp mismatch with record metadata for consumer with protocol " + groupProtocol.name);
                    assertEquals(TimestampType.LOG_APPEND_TIME, consumerRecord.timestampType(), "Timestamp type mismatch for consumer with protocol " + groupProtocol.name);
                });
            }
        }
    }
}
