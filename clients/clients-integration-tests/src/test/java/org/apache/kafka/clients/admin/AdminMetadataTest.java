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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(brokers = 3, types = {Type.KRAFT})
public class AdminMetadataTest {

    // Incorrect broker port which can used by kafka clients in tests. This port should not be used
    // by any other service and hence we use a reserved port.
    private static final int INCORRECT_BROKER_PORT = 225;

    private final ClusterInstance clusterInstance;

    AdminMetadataTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @ClusterTest
    public void testListNodes() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            List<String> brokerStrs = Arrays.stream(clusterInstance.bootstrapServers().split(","))
                    .sorted()
                    .toList();
            TestUtils.waitForCondition(
                    () -> admin.describeCluster().nodes().get().size() >= brokerStrs.size(),
                    "Timed out waiting for all brokers to be discovered");
            List<String> nodeStrs = admin.describeCluster().nodes().get().stream()
                    .map(node -> node.host() + ":" + node.port())
                    .sorted()
                    .toList();
            assertEquals(String.join(",", brokerStrs), String.join(",", nodeStrs));
        }
    }

    @ClusterTest
    public void testListNodesWithFencedBroker() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            int fencedBrokerId = Collections.max(clusterInstance.brokerIds());
            int totalBrokers = clusterInstance.brokerIds().size();
            clusterInstance.shutdownBroker(fencedBrokerId);

            // It takes a few seconds for a broker to get fenced after being killed,
            // so we retry until only the non-fenced brokers are returned.
            TestUtils.waitForCondition(
                    () -> admin.describeCluster().nodes().get().size() == totalBrokers - 1,
                    20_000,
                    "Timed out waiting for broker " + fencedBrokerId + " to be fenced");

            // List nodes again but this time include the fenced broker.
            Collection<Node> nodes = admin.describeCluster(
                    new DescribeClusterOptions().includeFencedBrokers(true)).nodes().get();
            assertEquals(totalBrokers, nodes.size());
            for (Node node : nodes) {
                if (node.id() == fencedBrokerId) {
                    assertTrue(node.isFenced());
                } else {
                    assertFalse(node.isFenced());
                }
            }
        }
    }

    @ClusterTest
    public void testDescribeCluster() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            DescribeClusterResult result = admin.describeCluster();
            Collection<Node> nodes = result.nodes().get();
            assertEquals(clusterInstance.clusterId(), result.clusterId().get());

            // In KRaft, we return a random brokerId as the current controller.
            Node controller = result.controller().get();
            assertTrue(clusterInstance.brokerIds().contains(controller.id()));

            Set<String> brokerEndpoints = Set.of(clusterInstance.bootstrapServers().split(","));
            assertEquals(brokerEndpoints.size(), nodes.size());
            for (Node node : nodes) {
                String hostStr = node.host() + ":" + node.port();
                assertTrue(brokerEndpoints.contains(hostStr),
                        "Unknown host:port pair " + hostStr + " in brokerVersionInfos");
            }
        }
    }

    @ClusterTest
    public void testListTopicsWithOptionTimeoutMs() {
        Admin admin = createInvalidAdminClient();
        try {
            ListTopicsOptions timeoutOption = new ListTopicsOptions().timeoutMs(0);
            ExecutionException exception = assertThrows(ExecutionException.class,
                    () -> admin.listTopics(timeoutOption).names().get());
            assertInstanceOf(TimeoutException.class, exception.getCause());
        } finally {
            admin.close(Duration.ZERO);
        }
    }

    @ClusterTest
    public void testListTopicsWithOptionListInternal() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            admin.createTopics(List.of(new NewTopic(Topic.GROUP_METADATA_TOPIC_NAME, 1, (short) 1))).all().get();
            clusterInstance.waitTopicCreation(Topic.GROUP_METADATA_TOPIC_NAME, 1);

            Set<String> topicNames = admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get();
            assertTrue(topicNames.contains(Topic.GROUP_METADATA_TOPIC_NAME),
                "Expected to see internal topic " + Topic.GROUP_METADATA_TOPIC_NAME);
        }
    }


    @ClusterTest
    public void testDescribeTopicsWithOptionPartitionSizeLimitPerResponse() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            String testTopic = "test-topic";
            admin.createTopics(List.of(new NewTopic(testTopic, 3, (short) 1))).all().get();
            clusterInstance.waitTopicCreation(testTopic, 3);

            Map<String, TopicDescription> topics = admin.describeTopics(List.of(testTopic),
                    new DescribeTopicsOptions().partitionSizeLimitPerResponse(1)).allTopicNames().get();
            assertEquals(1, topics.size());
            assertEquals(3, topics.get(testTopic).partitions().size());
        }
    }

    @ClusterTest
    public void testDescribeTopicsWithOptionTimeoutMs() {
        Admin admin = createInvalidAdminClient();
        try {
            DescribeTopicsOptions timeoutOption = new DescribeTopicsOptions().timeoutMs(0);
            ExecutionException exception = assertThrows(ExecutionException.class,
                    () -> admin.describeTopics(List.of("test-topic"), timeoutOption).allTopicNames().get());
            assertInstanceOf(TimeoutException.class, exception.getCause());
        } finally {
            admin.close(Duration.ZERO);
        }
    }

    /**
     * describe should not auto create topics.
     */
    @ClusterTest
    public void testDescribeNonExistingTopic() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            String existingTopic = "existing-topic";
            admin.createTopics(List.of(new NewTopic(existingTopic, 1, (short) 1))).all().get();
            clusterInstance.waitTopicCreation(existingTopic, 1);

            String nonExistingTopic = "non-existing";
            Map<String, KafkaFuture<TopicDescription>> results =
                    admin.describeTopics(List.of(nonExistingTopic, existingTopic)).topicNameValues();
            assertEquals(existingTopic, results.get(existingTopic).get().name());
            assertFutureThrows(UnknownTopicOrPartitionException.class, results.get(nonExistingTopic));
        }
    }

    @ClusterTest
    public void testDescribeTopicsWithIds() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            String existingTopic = "existing-topic";
            CreateTopicsResult createResult = admin.createTopics(List.of(new NewTopic(existingTopic, 1, (short) 1)));
            createResult.all().get();
            clusterInstance.waitTopicCreation(existingTopic, 1);
            clusterInstance.ensureConsistentMetadata();

            Uuid existingTopicId = createResult.topicId(existingTopic).get();
            Uuid nonExistingTopicId = Uuid.randomUuid();

            Map<Uuid, KafkaFuture<TopicDescription>> results = admin.describeTopics(
                    TopicCollection.ofTopicIds(List.of(existingTopicId, nonExistingTopicId))).topicIdValues();
            assertEquals(existingTopicId, results.get(existingTopicId).get().topicId());
            assertFutureThrows(UnknownTopicIdException.class, results.get(nonExistingTopicId));
        }
    }

    @ClusterTest
    public void testDescribeTopicsWithNames() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            String existingTopic = "existing-topic";
            CreateTopicsResult createResult = admin.createTopics(List.of(new NewTopic(existingTopic, 1, (short) 1)));
            createResult.all().get();
            clusterInstance.waitTopicCreation(existingTopic, 1);
            clusterInstance.ensureConsistentMetadata();

            Uuid existingTopicId = createResult.topicId(existingTopic).get();
            Map<String, KafkaFuture<TopicDescription>> results = admin.describeTopics(
                    TopicCollection.ofTopicNames(List.of(existingTopic))).topicNameValues();
            assertEquals(existingTopicId, results.get(existingTopic).get().topicId());
        }
    }

    private Admin createInvalidAdminClient() {
        Map<String, Object> config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + INCORRECT_BROKER_PORT
        );
        return Admin.create(config);
    }
}
