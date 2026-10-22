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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@ClusterTestDefaults(
    types = {Type.KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
        @ClusterConfigProperty(key = "group.share.max.partition.max.record.locks", value = "10000"),
        @ClusterConfigProperty(key = "group.share.partition.max.record.locks", value = "10000"),
        @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "60000"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "1")
    }
)
/**
 * Verifies that a share fetch spreads its record budget across all the partitions with data and does not
 * keep serving the same partition while the others are starved.
 */
public class ShareConsumerFairnessTest extends ShareConsumerTestBase {

    private static final int RECORDS_PER_PARTITION = 8000;
    private static final int MAX_POLL_RECORDS = 100;
    private static final int POLLS = 60;

    public ShareConsumerFairnessTest(ClusterInstance cluster) {
        super(cluster);
    }

    @ClusterTest
    public void testPartitionsWithDeepBacklogAreServedFairly() throws Exception {
        String topic = "fairness";
        String groupId = "fairness-group";
        int partitions = 2;
        createTopic(topic, partitions, 1);
        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = 0; i < RECORDS_PER_PARTITION; i++) {
                for (int p = 0; p < partitions; p++) {
                    producer.send(new ProducerRecord<>(topic, p, null, null, "value".getBytes()));
                }
            }
            producer.flush();
        }
        alterShareAutoOffsetReset(groupId, "earliest");

        Map<Integer, Integer> recordsByPartition = new TreeMap<>();
        try (ShareConsumer<byte[], byte[]> consumer = createShareConsumer(groupId, Map.of(
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(MAX_POLL_RECORDS),
            ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit"))) {
            consumer.subscribe(Set.of(topic));
            for (int poll = 0; poll < POLLS; poll++) {
                consumer.poll(Duration.ofSeconds(1))
                    .forEach(record -> recordsByPartition.merge(record.partition(), 1, Integer::sum));
            }
        }

        int total = recordsByPartition.values().stream().mapToInt(Integer::intValue).sum();
        for (int p = 0; p < partitions; p++) {
            // Both partitions have a deep backlog, hence each should get a meaningful share of the records.
            assertTrue(recordsByPartition.getOrDefault(p, 0) * 5 >= total,
                "Partition " + p + " was starved, records by partition: " + recordsByPartition);
        }
    }
}
