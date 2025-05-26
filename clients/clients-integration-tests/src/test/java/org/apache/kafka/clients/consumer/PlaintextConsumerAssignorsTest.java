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


import org.apache.kafka.clients.ClientsTestUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ClusterTestDefaults(
        types = {Type.KRAFT},
        brokers = PlaintextConsumerAssignorsTest.BROKER_COUNT,
        serverProperties = {
                @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
                @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
                @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "10"),
        }
)

public class PlaintextConsumerAssignorsTest {
    public static final int BROKER_COUNT = 3;

    int numPartitions = 3;
    short numReplica  = 3;

    private final ClusterInstance clusterInstance;
    String topic0 = "topic0";
    int partition0 = 0;
    TopicPartition tp0 = new TopicPartition(topic0, numPartitions);
    String topic1 = "topic1";
    TopicPartition tp1 = new TopicPartition(topic1, numPartitions);
    String topic2= "topic2";
    TopicPartition tp2 = new TopicPartition(topic2, numPartitions);


    PlaintextConsumerAssignorsTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        clusterInstance.createTopic(topic0, numPartitions, numReplica);
        clusterInstance.createTopic(topic1, numPartitions, numReplica);
    }


    @ClusterTest
    void testClassicMultiConsumerRoundRobinAssignor() throws Exception {
        testMultiConsumerRoundRobinAssignor(GroupProtocol.CLASSIC.name);
    }

    void testMultiConsumerRoundRobinAssignor(String groupProtocol) throws InterruptedException {
        int numRecords = 10000;

        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, "roundrobin-group",
                PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName()))) {
            ClientsTestUtils.sendRecords(clusterInstance, tp0, numRecords);
            ClientsTestUtils.sendRecords(clusterInstance, tp1, numRecords);
            assertEquals(0, consumer.assignment().size());
            
            consumer.subscribe(List.of(topic0, topic1));
            ClientsTestUtils.awaitAssignment(consumer, Set.of(tp0, tp1));

            ClientsTestUtils.pollUntilTrue(consumer, () -> cb.successCount >= 1 || cb.lastError.isPresent(),
                    10000, "Failed to observe commit callback before timeout");
            Map<TopicPartition, OffsetAndMetadata> committedOffset = consumer.committed(Set.of(tp));
            assertNotNull(committedOffset);
            // No valid fetch position due to the absence of consumer.poll; and therefore no offset was committed to
            // tp. The committed offset should be null. This is intentional.
            assertNull(committedOffset.get(tp));
            assertTrue(consumer.assignment().contains(tp));
        }
    }

}
