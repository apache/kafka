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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.test.TestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class ShareConsumerRackAwareTest {
    @ClusterTest(
        types = {Type.KRAFT},
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(id = 0, key = "broker.rack", value = "rack0"),
            @ClusterConfigProperty(id = 1, key = "broker.rack", value = "rack1"),
            @ClusterConfigProperty(id = 2, key = "broker.rack", value = "rack2"),
            @ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic, share"),
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNORS_CONFIG, value = "org.apache.kafka.clients.consumer.RackAwareAssignor")
        }
    )
    void testShareConsumerWithRackAwareAssignor(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        String groupId = "group0";
        String topic = "test-topic";
        try (Admin admin = clusterInstance.admin();
             Producer<byte[], byte[]> producer = clusterInstance.producer();
             ShareConsumer<byte[], byte[]> consumer0 = clusterInstance.shareConsumer(Map.of(
                 CommonClientConfigs.GROUP_ID_CONFIG, groupId,
                 CommonClientConfigs.CLIENT_ID_CONFIG, "client0",
                 CommonClientConfigs.CLIENT_RACK_CONFIG, "rack0"
             ));
             ShareConsumer<byte[], byte[]> consumer1 = clusterInstance.shareConsumer(Map.of(
                 CommonClientConfigs.GROUP_ID_CONFIG, groupId,
                 CommonClientConfigs.CLIENT_ID_CONFIG, "client1",
                 CommonClientConfigs.CLIENT_RACK_CONFIG, "rack1"
             ));
             ShareConsumer<byte[], byte[]> consumer2 = clusterInstance.shareConsumer(Map.of(
                 CommonClientConfigs.GROUP_ID_CONFIG, groupId,
                 CommonClientConfigs.CLIENT_ID_CONFIG, "client2",
                 CommonClientConfigs.CLIENT_RACK_CONFIG, "rack2"
             ))
        ) {
            // Create a new topic with 1 partition on broker 0.
            admin.createTopics(List.of(new NewTopic(topic, Map.of(0, List.of(0)))));
            clusterInstance.waitTopicCreation(topic, 1);

            producer.send(new ProducerRecord<>(topic, "key".getBytes(), "value".getBytes()));
            producer.flush();

            consumer0.subscribe(List.of(topic));
            consumer1.subscribe(List.of(topic));
            consumer2.subscribe(List.of(topic));

            TestUtils.waitForCondition(() -> {
                consumer0.poll(Duration.ofMillis(1000));
                consumer1.poll(Duration.ofMillis(1000));
                consumer2.poll(Duration.ofMillis(1000));
                Map<String, ShareGroupDescription> groups = assertDoesNotThrow(() -> admin.describeShareGroups(Set.of("group0")).all().get());
                ShareGroupDescription groupDescription = groups.get(groupId);
                return isExpectedAssignment(groupDescription, 3, Map.of(
                    "client0", Set.of(new TopicPartition(topic, 0)),
                    "client1", Set.of(),
                    "client2", Set.of()
                ));
            }, "Consumer 0 should be assigned to topic partition 0");

            // Add a new partition 1 and 2 to broker 1.
            admin.createPartitions(
                Map.of(
                    topic,
                    NewPartitions.increaseTo(3, List.of(List.of(1), List.of(1)))
                )
            );
            clusterInstance.waitTopicCreation(topic, 3);

            TestUtils.waitForCondition(() -> {
                consumer0.poll(Duration.ofMillis(1000));
                consumer1.poll(Duration.ofMillis(1000));
                consumer2.poll(Duration.ofMillis(1000));
                Map<String, ShareGroupDescription> groups = assertDoesNotThrow(() -> admin.describeShareGroups(Set.of("group0")).all().get());
                ShareGroupDescription groupDescription = groups.get(groupId);
                return isExpectedAssignment(groupDescription, 3, Map.of(
                    "client0", Set.of(new TopicPartition(topic, 0)),
                    "client1", Set.of(new TopicPartition(topic, 1), new TopicPartition(topic, 2)),
                    "client2", Set.of()
                ));
            }, "Consumer 1 should be assigned to topic partition 1 and 2");

            // Add a new partition 3, 4, and 5 to broker 2.
            admin.createPartitions(
                Map.of(
                    topic,
                    NewPartitions.increaseTo(6, List.of(List.of(2), List.of(2), List.of(2)))
                )
            );
            TestUtils.waitForCondition(() -> {
                consumer0.poll(Duration.ofMillis(1000));
                consumer1.poll(Duration.ofMillis(1000));
                consumer2.poll(Duration.ofMillis(1000));
                Map<String, ShareGroupDescription> groups = assertDoesNotThrow(() -> admin.describeShareGroups(Set.of("group0")).all().get());
                ShareGroupDescription groupDescription = groups.get(groupId);
                return isExpectedAssignment(groupDescription, 3, Map.of(
                    "client0", Set.of(new TopicPartition(topic, 0)),
                    "client1", Set.of(new TopicPartition(topic, 1), new TopicPartition(topic, 2)),
                    "client2", Set.of(new TopicPartition(topic, 3), new TopicPartition(topic, 4), new TopicPartition(topic, 5))
                ));
            }, "Consumer 2 should be assigned to topic partition 3, 4, and 5");

            // Change partitions to different brokers.
            // partition 0 -> broker 2
            // partition 1 -> broker 2
            // partition 2 -> broker 2
            // partition 3 -> broker 1
            // partition 4 -> broker 1
            // partition 5 -> broker 0
            admin.alterPartitionReassignments(Map.of(
                new TopicPartition(topic, 0), Optional.of(new NewPartitionReassignment(List.of(2))),
                new TopicPartition(topic, 1), Optional.of(new NewPartitionReassignment(List.of(2))),
                new TopicPartition(topic, 2), Optional.of(new NewPartitionReassignment(List.of(2))),
                new TopicPartition(topic, 3), Optional.of(new NewPartitionReassignment(List.of(1))),
                new TopicPartition(topic, 4), Optional.of(new NewPartitionReassignment(List.of(1))),
                new TopicPartition(topic, 5), Optional.of(new NewPartitionReassignment(List.of(0)))
            )).all().get();
            TestUtils.waitForCondition(() -> {
                consumer0.poll(Duration.ofMillis(1000));
                consumer1.poll(Duration.ofMillis(1000));
                consumer2.poll(Duration.ofMillis(1000));
                Map<String, ShareGroupDescription> groups = assertDoesNotThrow(() -> admin.describeShareGroups(Set.of("group0")).all().get());
                ShareGroupDescription groupDescription = groups.get(groupId);
                return isExpectedAssignment(groupDescription, 3, Map.of(
                    "client0", Set.of(new TopicPartition(topic, 5)),
                    "client1", Set.of(new TopicPartition(topic, 3), new TopicPartition(topic, 4)),
                    "client2", Set.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 2))
                ));
            }, "Consumer with topic partition mapping should be 0 -> 5 | 1 -> 3, 4 | 2 -> 0, 1, 2");
        }
    }

    boolean isExpectedAssignment(
        ShareGroupDescription groupDescription,
        int memberCount,
        Map<String, Set<TopicPartition>> expectedAssignments
    ) {
        return groupDescription != null &&
            groupDescription.members().size() == memberCount &&
            groupDescription.members().stream().allMatch(
                member -> {
                    String clientId = member.clientId();
                    Set<TopicPartition> expectedPartitions = expectedAssignments.get(clientId);
                    return member.assignment().topicPartitions().equals(expectedPartitions);
                }
            );
    }
}
