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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.test.AdminUtils;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(types = {Type.KRAFT}, brokers = 4)
public class AddPartitionsTest {

    @ClusterTest
    public void testWrongReplicaCount(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            cluster.createTopicWithAssignment("topic1", Map.of(0, List.of(0, 1)));

            ExecutionException exception = assertThrows(ExecutionException.class, () ->
                admin.createPartitions(Map.of("topic1",
                    NewPartitions.increaseTo(2, List.of(List.of(0, 1, 2))))).all().get());
            assertInstanceOf(InvalidReplicaAssignmentException.class, exception.getCause());
        }
    }

    @ClusterTest
    public void testMissingPartitionsInCreateTopics(ClusterInstance cluster) {
        try (Admin admin = cluster.admin()) {
            Map<Integer, List<Integer>> topic6Placements = Map.of(
                1, List.of(0, 1),
                2, List.of(1, 0));
            Map<Integer, List<Integer>> topic7Placements = Map.of(
                2, List.of(0, 1),
                3, List.of(1, 0));

            CreateTopicsResult result = admin.createTopics(List.of(
                new NewTopic("new-topic6", topic6Placements),
                new NewTopic("new-topic7", topic7Placements)));

            Throwable topic6Cause = assertThrows(ExecutionException.class,
                () -> result.values().get("new-topic6").get()).getCause();
            assertInstanceOf(InvalidReplicaAssignmentException.class, topic6Cause);
            assertTrue(topic6Cause.getMessage().contains("partitions should be a consecutive 0-based integer sequence"),
                "Unexpected error message: " + topic6Cause.getMessage());

            Throwable topic7Cause = assertThrows(ExecutionException.class,
                () -> result.values().get("new-topic7").get()).getCause();
            assertInstanceOf(InvalidReplicaAssignmentException.class, topic7Cause);
            assertTrue(topic7Cause.getMessage().contains("partitions should be a consecutive 0-based integer sequence"),
                "Unexpected error message: " + topic7Cause.getMessage());
        }
    }

    @ClusterTest
    public void testMissingPartitionsInCreatePartitions(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            cluster.createTopicWithAssignment("topic1", Map.of(0, List.of(0, 1)));

            Throwable cause = assertThrows(ExecutionException.class, () ->
                admin.createPartitions(Map.of("topic1",
                    NewPartitions.increaseTo(3, List.of(List.of(0, 1, 2))))).all().get()).getCause();
            assertInstanceOf(InvalidReplicaAssignmentException.class, cause);
            assertTrue(cause.getMessage().contains(
                "Attempted to add 2 additional partition(s), but only 1 assignment(s) were specified."),
                "Unexpected error message: " + cause.getMessage());
        }
    }

    @ClusterTest
    public void testIncrementPartitions(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            cluster.createTopicWithAssignment("topic1", Map.of(0, List.of(0, 1)));

            admin.createPartitions(Map.of("topic1", NewPartitions.increaseTo(3))).all().get();

            AdminUtils.fetchOrWaitForLeader(admin, "topic1", 1, 30000);
            AdminUtils.fetchOrWaitForLeader(admin, "topic1", 2, 30000);

            cluster.waitTopicCreation("topic1", 3);

            Map<String, TopicDescription> descriptions = admin.describeTopics(List.of("topic1")).allTopicNames().get();
            TopicDescription topicDescription = descriptions.get("topic1");
            assertEquals(3, topicDescription.partitions().size());

            for (TopicPartitionInfo partition : topicDescription.partitions()) {
                assertEquals(2, partition.replicas().size());
                assertNotNull(partition.leader());
                assertTrue(partition.replicas().contains(partition.leader()));
            }
        }
    }

    @ClusterTest
    public void testManualAssignmentOfReplicas(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            cluster.createTopicWithAssignment("topic2", Map.of(0, List.of(1, 2)));

            admin.createPartitions(Map.of("topic2", NewPartitions.increaseTo(3,
                List.of(List.of(0, 1), List.of(2, 3))))).all().get();

            AdminUtils.fetchOrWaitForLeader(admin, "topic2", 1, 30000);
            AdminUtils.fetchOrWaitForLeader(admin, "topic2", 2, 30000);

            cluster.waitTopicCreation("topic2", 3);

            Map<String, TopicDescription> descriptions = admin.describeTopics(List.of("topic2")).allTopicNames().get();
            TopicDescription topicDescription = descriptions.get("topic2");
            assertEquals(3, topicDescription.partitions().size());

            List<TopicPartitionInfo> partitions = topicDescription.partitions().stream()
                .sorted(Comparator.comparingInt(TopicPartitionInfo::partition))
                .toList();

            assertEquals(0, partitions.get(0).partition());
            assertEquals(1, partitions.get(1).partition());
            assertEquals(2, partitions.get(2).partition());

            Set<Integer> partition1Replicas = partitions.get(1).replicas().stream()
                .map(Node::id)
                .collect(Collectors.toSet());
            assertEquals(2, partition1Replicas.size());
            assertEquals(Set.of(0, 1), partition1Replicas);
        }
    }

    @ClusterTest
    public void testReplicaPlacementAllServers(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            cluster.createTopicWithAssignment("topic3", Map.of(0, List.of(2, 3, 0, 1)));

            admin.createPartitions(Map.of("topic3", NewPartitions.increaseTo(7))).all().get();

            cluster.waitTopicCreation("topic3", 7);

            Map<String, TopicDescription> descriptions = admin.describeTopics(List.of("topic3")).allTopicNames().get();
            TopicDescription topicDescription = descriptions.get("topic3");
            assertEquals(7, topicDescription.partitions().size());

            for (TopicPartitionInfo partition : topicDescription.partitions()) {
                Set<Integer> replicaIds = partition.replicas().stream()
                    .map(Node::id)
                    .collect(Collectors.toSet());
                assertEquals(4, replicaIds.size(),
                    "Partition " + partition.partition() + " should have 4 replicas");
                assertTrue(replicaIds.stream().allMatch(id -> id >= 0 && id <= 3),
                    "Replicas should only include brokers 0-3");
                assertNotNull(partition.leader(),
                    "Partition " + partition.partition() + " should have a leader");
                assertTrue(replicaIds.contains(partition.leader().id()),
                    "Leader should be one of the replicas");
            }
        }
    }

    @ClusterTest
    public void testReplicaPlacementPartialServers(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            cluster.createTopicWithAssignment("topic2", Map.of(0, List.of(1, 2)));

            admin.createPartitions(Map.of("topic2", NewPartitions.increaseTo(3))).all().get();

            cluster.waitTopicCreation("topic2", 3);

            Map<String, TopicDescription> descriptions = admin.describeTopics(List.of("topic2")).allTopicNames().get();
            TopicDescription topicDescription = descriptions.get("topic2");
            assertEquals(3, topicDescription.partitions().size());

            for (TopicPartitionInfo partition : topicDescription.partitions()) {
                Set<Integer> replicaIds = partition.replicas().stream()
                    .map(Node::id)
                    .collect(Collectors.toSet());
                assertEquals(2, replicaIds.size(),
                    "Partition " + partition.partition() + " should have 2 replicas");
                assertTrue(replicaIds.stream().allMatch(id -> id >= 0 && id <= 3),
                    "Replicas should only include brokers 0-3");
                assertNotNull(partition.leader(),
                    "Partition " + partition.partition() + " should have a leader");
                assertTrue(replicaIds.contains(partition.leader().id()),
                    "Leader should be one of the replicas");
            }
        }
    }

    @ClusterTest(brokers = 3)
    public void testCreatePartitions(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            String topic1 = "create-partitions-topic-1";
            String topic2 = "create-partitions-topic-2";
            admin.createTopics(List.of(
                    new NewTopic(topic1, 1, (short) 1),
                    new NewTopic(topic2, 1, (short) 2))).all().get();
            cluster.waitTopicCreation(topic1, 1);
            cluster.waitTopicCreation(topic2, 1);
            assertEquals(1, numPartitions(admin, topic1));
            assertEquals(1, numPartitions(admin, topic2));

            CreatePartitionsOptions validateOnly = new CreatePartitionsOptions().validateOnly(true);
            CreatePartitionsOptions actuallyDoIt = new CreatePartitionsOptions().validateOnly(false);

            admin.createPartitions(Map.of(topic1, NewPartitions.increaseTo(3)), validateOnly)
                    .values().get(topic1).get();
            cluster.waitTopicCreation(topic1, 1);

            admin.createPartitions(Map.of(topic1, NewPartitions.increaseTo(3)), actuallyDoIt)
                    .values().get(topic1).get();
            cluster.waitTopicCreation(topic1, 3);

            List<Integer> brokerIds = cluster.brokerIds().stream().sorted().toList();
            List<List<Integer>> newPartition2Assignments = List.of(
                    List.of(brokerIds.get(0), brokerIds.get(1)),
                    List.of(brokerIds.get(1), brokerIds.get(2)));
            admin.createPartitions(Map.of(topic2,
                    NewPartitions.increaseTo(3, newPartition2Assignments)), validateOnly)
                    .values().get(topic2).get();
            cluster.waitTopicCreation(topic2, 1);

            admin.createPartitions(Map.of(topic2,
                    NewPartitions.increaseTo(3, newPartition2Assignments)), actuallyDoIt)
                    .values().get(topic2).get();
            cluster.waitTopicCreation(topic2, 3);

            List<TopicPartitionInfo> actualPartitions2 = partitions(admin, topic2);
            assertEquals(3, actualPartitions2.size());
            assertEquals(newPartition2Assignments.get(0), replicaIds(actualPartitions2.get(1)));
            assertEquals(newPartition2Assignments.get(1), replicaIds(actualPartitions2.get(2)));

            int firstBrokerId = brokerIds.get(0);
            int secondBrokerId = brokerIds.get(1);
            int thirdBrokerId = brokerIds.get(2);
            int unknownBrokerId = brokerIds.get(brokerIds.size() - 1) + 1;
            for (CreatePartitionsOptions option : List.of(validateOnly, actuallyDoIt)) {
                assertCreatePartitionsFailure(admin, option, topic1,
                        NewPartitions.increaseTo(1),
                        InvalidPartitionsException.class,
                        "The topic create-partitions-topic-1 currently has 3 partition(s); " +
                                "1 would not be an increase.",
                        3);
                assertCreatePartitionsFailure(admin, option, topic2,
                        NewPartitions.increaseTo(3),
                        InvalidPartitionsException.class,
                        "Topic already has 3 partition(s).",
                        3);
                assertCreatePartitionsFailure(admin, option, topic2,
                        NewPartitions.increaseTo(3, newPartition2Assignments),
                        InvalidPartitionsException.class,
                        "Topic already has 3 partition(s).",
                        3);
                assertCreatePartitionsFailure(admin, option, topic2,
                        NewPartitions.increaseTo(3, List.of(
                                newPartition2Assignments.get(1), newPartition2Assignments.get(0))),
                        InvalidPartitionsException.class,
                        "Topic already has 3 partition(s).",
                        3);
                assertCreatePartitionsFailure(admin, option, "an-unknown-topic",
                        NewPartitions.increaseTo(2),
                        UnknownTopicOrPartitionException.class,
                        "This server does not host this topic-partition.",
                        null);
                assertCreatePartitionsFailure(admin, option, topic1,
                        NewPartitions.increaseTo(-22),
                        InvalidPartitionsException.class,
                        "The topic create-partitions-topic-1 currently has 3 partition(s); " +
                                "-22 would not be an increase.",
                        3);
                assertCreatePartitionsFailure(admin, option, topic1,
                        NewPartitions.increaseTo(4, List.of(List.of(secondBrokerId, thirdBrokerId))),
                        InvalidReplicaAssignmentException.class,
                        "The manual partition assignment includes a partition with 2 replica(s), " +
                                "but this is not consistent with previous partitions, which have 1 replica(s).",
                        3);
                assertCreatePartitionsFailure(admin, option, topic1,
                        NewPartitions.increaseTo(6, List.of(List.of(secondBrokerId))),
                        InvalidReplicaAssignmentException.class,
                        "Attempted to add 3 additional partition(s), but only 1 assignment(s) were specified.",
                        3);
                assertCreatePartitionsFailure(admin, option, topic1,
                        NewPartitions.increaseTo(4, List.of(
                                List.of(secondBrokerId), List.of(thirdBrokerId))),
                        InvalidReplicaAssignmentException.class,
                        "Attempted to add 1 additional partition(s), but only 2 assignment(s) were specified.",
                        3);
                assertCreatePartitionsFailure(admin, option, topic1,
                        NewPartitions.increaseTo(4, List.of(List.of(secondBrokerId, secondBrokerId))),
                        InvalidReplicaAssignmentException.class,
                        "The manual partition assignment includes the broker " + secondBrokerId + " more than once.",
                        3);
                assertCreatePartitionsFailure(admin, option, topic1,
                        NewPartitions.increaseTo(5, List.of(
                                List.of(secondBrokerId), List.of(secondBrokerId, firstBrokerId))),
                        InvalidReplicaAssignmentException.class,
                        "The manual partition assignment includes a partition with 2 replica(s), " +
                                "but this is not consistent with previous partitions, which have 1 replica(s).",
                        3);
                assertCreatePartitionsFailure(admin, option, topic1,
                        NewPartitions.increaseTo(4, List.of(List.of(unknownBrokerId))),
                        InvalidReplicaAssignmentException.class,
                        "The manual partition assignment includes broker " + unknownBrokerId +
                                ", but no such broker is registered.",
                        3);
                assertCreatePartitionsFailure(admin, option, topic1,
                        NewPartitions.increaseTo(4, List.of()),
                        InvalidReplicaAssignmentException.class,
                        "Attempted to add 1 additional partition(s), but only 0 assignment(s) were specified.",
                        3);
            }

            CreatePartitionsResult mixedResult = admin.createPartitions(Map.of(
                    topic1, NewPartitions.increaseTo(4),
                    topic2, NewPartitions.increaseTo(2)), actuallyDoIt);
            mixedResult.values().get(topic1).get();
            cluster.waitTopicCreation(topic1, 4);
            Throwable cause = assertThrows(ExecutionException.class,
                    () -> mixedResult.values().get(topic2).get()).getCause();
            assertInstanceOf(InvalidPartitionsException.class, cause);
            assertEquals("The topic create-partitions-topic-2 currently has 3 partition(s); " +
                    "2 would not be an increase.", cause.getMessage());
            cluster.waitTopicCreation(topic2, 3);

            admin.deleteTopics(List.of(topic1)).topicNameValues().get(topic1).get();
            CreatePartitionsResult deletedTopicResult = admin.createPartitions(
                    Map.of(topic1, NewPartitions.increaseTo(4)), validateOnly);
            cause = assertThrows(ExecutionException.class,
                    () -> deletedTopicResult.values().get(topic1).get()).getCause();
            assertInstanceOf(UnknownTopicOrPartitionException.class, cause);
            assertEquals("This server does not host this topic-partition.", cause.getMessage());
        }
    }

    private static void assertCreatePartitionsFailure(
            Admin admin,
            CreatePartitionsOptions option,
            String topic,
            NewPartitions newPartitions,
            Class<? extends Throwable> expectedException,
            String expectedMessage,
            Integer expectedPartitionCount
    ) throws Exception {
        String description = "validateOnly=" + option.validateOnly();
        CreatePartitionsResult result = admin.createPartitions(Map.of(topic, newPartitions), option);
        Throwable cause = assertThrows(ExecutionException.class,
                () -> result.values().get(topic).get(), description).getCause();
        assertInstanceOf(expectedException, cause, description);
        assertEquals(expectedMessage, cause.getMessage(), description);
        if (expectedPartitionCount != null) {
            assertEquals(expectedPartitionCount, numPartitions(admin, topic), description);
        }
    }

    private static int numPartitions(Admin admin, String topic) throws Exception {
        return partitions(admin, topic).size();
    }

    private static List<TopicPartitionInfo> partitions(Admin admin, String topic) throws Exception {
        return admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).partitions();
    }

    private static List<Integer> replicaIds(TopicPartitionInfo partition) {
        return partition.replicas().stream().map(Node::id).toList();
    }
}
