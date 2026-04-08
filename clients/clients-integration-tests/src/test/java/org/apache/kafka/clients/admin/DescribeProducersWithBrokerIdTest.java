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
package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@ClusterTestDefaults(
    brokers = 3
)
class DescribeProducersWithBrokerIdTest {
    private static final String TOPIC_NAME = "test-topic";
    private static final int NUM_PARTITIONS = 1;
    private static final short REPLICATION_FACTOR = 3;

    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC_NAME, 0);
    
    private final ClusterInstance clusterInstance;

    public DescribeProducersWithBrokerIdTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    private static void sendTestRecords(Producer<byte[], byte[]> producer) {
        producer.send(new ProducerRecord<>(TOPIC_NAME, TOPIC_PARTITION.partition(), "key-0".getBytes(), "value-0".getBytes()));
        producer.flush();
    }
    
    @BeforeEach
    void setUp() throws InterruptedException {
        clusterInstance.createTopic(TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR);
    }

    private List<Integer> getReplicaBrokerIds(Admin admin) throws Exception {
        var topicDescription = admin.describeTopics(List.of(TOPIC_PARTITION.topic())).allTopicNames().get().get(TOPIC_PARTITION.topic());
        return topicDescription.partitions().get(TOPIC_PARTITION.partition()).replicas().stream()
            .map(Node::id)
            .toList();
    }
    
    private int getNonReplicaBrokerId(Admin admin) throws Exception {
        var replicaBrokerIds = getReplicaBrokerIds(admin);
        return clusterInstance.brokerIds().stream()
            .filter(id -> !replicaBrokerIds.contains(id))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No non-replica broker found"));
    }
    
    private int getFollowerBrokerId(Admin admin) throws Exception {
        var replicaBrokerIds = getReplicaBrokerIds(admin);
        var leaderBrokerId = clusterInstance.getLeaderBrokerId(TOPIC_PARTITION);
        return replicaBrokerIds.stream()
            .filter(id -> id != leaderBrokerId)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No follower found for partition " + TOPIC_PARTITION));
    }

    @ClusterTest
    void testDescribeProducersDefaultRoutesToLeader() throws Exception {
        try (Producer<byte[], byte[]> producer = clusterInstance.producer();
             var admin = clusterInstance.admin()) {
            sendTestRecords(producer);
            
            var stateWithExplicitLeader = admin.describeProducers(
                List.of(TOPIC_PARTITION), 
                new DescribeProducersOptions().brokerId(clusterInstance.getLeaderBrokerId(TOPIC_PARTITION))
            ).partitionResult(TOPIC_PARTITION).get();
            
            var stateWithDefaultRouting = admin.describeProducers(
                List.of(TOPIC_PARTITION)
            ).partitionResult(TOPIC_PARTITION).get();
            
            assertNotNull(stateWithDefaultRouting);
            assertFalse(stateWithDefaultRouting.activeProducers().isEmpty());
            assertEquals(stateWithExplicitLeader.activeProducers(), stateWithDefaultRouting.activeProducers());
        }
    }

    @ClusterTest
    void testDescribeProducersFromFollower() throws Exception {
        try (Producer<byte[], byte[]> producer = clusterInstance.producer();
             var admin = clusterInstance.admin()) {
            sendTestRecords(producer);

            var followerState = admin.describeProducers(
                List.of(TOPIC_PARTITION), 
                new DescribeProducersOptions().brokerId(getFollowerBrokerId(admin))
            ).partitionResult(TOPIC_PARTITION).get();
            
            var leaderState = admin.describeProducers(
                List.of(TOPIC_PARTITION)
            ).partitionResult(TOPIC_PARTITION).get();

            assertNotNull(followerState);
            assertFalse(followerState.activeProducers().isEmpty());
            assertEquals(leaderState.activeProducers(), followerState.activeProducers());
        }
    }

    @ClusterTest(brokers = 4)
    void testDescribeProducersWithInvalidBrokerId() throws Exception {
        try (Producer<byte[], byte[]> producer = clusterInstance.producer();
             var admin = clusterInstance.admin()) {
            sendTestRecords(producer);

            TestUtils.assertFutureThrows(NotLeaderOrFollowerException.class, 
                admin.describeProducers(
                    List.of(TOPIC_PARTITION), 
                    new DescribeProducersOptions().brokerId(getNonReplicaBrokerId(admin))
                ).partitionResult(TOPIC_PARTITION)); 
        }
    }
}