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
package org.apache.kafka.connect.util;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.AdminClientUnitTestEnv;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TopicAdminTest {

    /**
     * 0.11.0.0 clients can talk with older brokers, but the CREATE_TOPIC API was added in 0.10.1.0. That means,
     * if our TopicAdmin talks to a pre 0.10.1 broker, it should receive an UnsupportedVersionException, should
     * create no topics, and return false.
     */
    @Test
    public void returnNullWithApiVersionMismatch() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(createTopicResponseWithUnsupportedVersion(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            boolean created = admin.createTopic(newTopic);
            assertFalse(created);
        }
    }

    @Test
    public void returnNullWithClusterAuthorizationFailure() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().prepareResponse(createTopicResponseWithClusterAuthorizationException(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            boolean created = admin.createTopic(newTopic);
            assertFalse(created);
        }
    }

    @Test
    public void returnNullWithTopicAuthorizationFailure() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().prepareResponse(createTopicResponseWithTopicAuthorizationException(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            boolean created = admin.createTopic(newTopic);
            assertFalse(created);
        }
    }

    @Test
    public void shouldNotCreateTopicWhenItAlreadyExists() {
        NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.<Node>emptyList());
            mockAdminClient.addTopic(false, "myTopic", Collections.singletonList(topicPartitionInfo), null);
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            assertFalse(admin.createTopic(newTopic));
        }
    }

    @Test
    public void shouldCreateTopicWithPartitionsWhenItDoesNotExist() {
        for (int numBrokers = 1; numBrokers < 10; ++numBrokers) {
            int expectedReplicas = Math.min(3, numBrokers);
            int maxDefaultRf = Math.min(numBrokers, 5);
            for (int numPartitions = 1; numPartitions < 30; ++numPartitions) {
                NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(numPartitions).compacted().build();

                // Try clusters with no default replication factor or default partitions
                assertTopicCreation(numBrokers, newTopic, null, null, expectedReplicas, numPartitions);

                // Try clusters with different default partitions
                for (int defaultPartitions = 1; defaultPartitions < 20; ++defaultPartitions) {
                    assertTopicCreation(numBrokers, newTopic, defaultPartitions, null, expectedReplicas, numPartitions);
                }

                // Try clusters with different default replication factors
                for (int defaultRF = 1; defaultRF < maxDefaultRf; ++defaultRF) {
                    assertTopicCreation(numBrokers, newTopic, null, defaultRF, defaultRF, numPartitions);
                }
            }
        }
    }

    @Test
    public void shouldCreateTopicWithReplicationFactorWhenItDoesNotExist() {
        for (int numBrokers = 1; numBrokers < 10; ++numBrokers) {
            int maxRf = Math.min(numBrokers, 5);
            int maxDefaultRf = Math.min(numBrokers, 5);
            for (short rf = 1; rf < maxRf; ++rf) {
                NewTopic newTopic = TopicAdmin.defineTopic("myTopic").replicationFactor(rf).compacted().build();

                // Try clusters with no default replication factor or default partitions
                assertTopicCreation(numBrokers, newTopic, null, null, rf, 1);

                // Try clusters with different default partitions
                for (int numPartitions = 1; numPartitions < 30; ++numPartitions) {
                    assertTopicCreation(numBrokers, newTopic, numPartitions, null, rf, numPartitions);
                }

                // Try clusters with different default replication factors
                for (int defaultRF = 1; defaultRF < maxDefaultRf; ++defaultRF) {
                    assertTopicCreation(numBrokers, newTopic, null, defaultRF, rf, 1);
                }
            }
        }
    }

    @Test
    public void shouldCreateTopicWithDefaultPartitionsAndReplicationFactorWhenItDoesNotExist() {
        NewTopic newTopic = TopicAdmin.defineTopic("my-topic")
                                      .defaultPartitions()
                                      .defaultReplicationFactor()
                                      .compacted()
                                      .build();

        for (int numBrokers = 1; numBrokers < 10; ++numBrokers) {
            int expectedReplicas = Math.min(3, numBrokers);
            assertTopicCreation(numBrokers, newTopic, null, null, expectedReplicas, 1);
            assertTopicCreation(numBrokers, newTopic, 30, null, expectedReplicas, 30);
        }
    }

    @Test
    public void shouldCreateOneTopicWhenProvidedMultipleDefinitionsWithSameTopicName() {
        NewTopic newTopic1 = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        NewTopic newTopic2 = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            Set<String> newTopicNames = admin.createTopics(newTopic1, newTopic2);
            assertEquals(1, newTopicNames.size());
            assertEquals(newTopic2.name(), newTopicNames.iterator().next());
        }
    }

    @Test
    public void shouldReturnFalseWhenSuppliedNullTopicDescription() {
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            boolean created = admin.createTopic(null);
            assertFalse(created);
        }
    }

    private Cluster createCluster(int numNodes) {
        HashMap<Integer, Node> nodes = new HashMap<>();
        for (int i = 0; i < numNodes; ++i) {
            nodes.put(i, new Node(i, "localhost", 8121 + i));
        }
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                Collections.<PartitionInfo>emptySet(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));
        return cluster;
    }

    private CreateTopicsResponse createTopicResponseWithUnsupportedVersion(NewTopic... topics) {
        return createTopicResponse(new ApiError(Errors.UNSUPPORTED_VERSION, "This version of the API is not supported"), topics);
    }

    private CreateTopicsResponse createTopicResponseWithClusterAuthorizationException(NewTopic... topics) {
        return createTopicResponse(new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, "Not authorized to create topic(s)"), topics);
    }

    private CreateTopicsResponse createTopicResponseWithTopicAuthorizationException(NewTopic... topics) {
        return createTopicResponse(new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, "Not authorized to create topic(s)"), topics);
    }

    private CreateTopicsResponse createTopicResponse(ApiError error, NewTopic... topics) {
        if (error == null) error = new ApiError(Errors.NONE, "");
        CreateTopicsResponseData response = new CreateTopicsResponseData();
        for (NewTopic topic : topics) {
            response.topics().add(new CreatableTopicResult().
                setName(topic.name()).
                setErrorCode(error.error().code()).
                setErrorMessage(error.message()));
        }
        return new CreateTopicsResponse(response);
    }

    protected void assertTopicCreation(
            int brokers,
            NewTopic newTopic,
            Integer defaultPartitions,
            Integer defaultReplicationFactor,
            int expectedReplicas,
            int expectedPartitions
    ) {
        Cluster cluster = createCluster(brokers);
        MockAdminClient.Builder clientBuilder = MockAdminClient.create();
        if (defaultPartitions != null) {
            clientBuilder.defaultPartitions(defaultPartitions.shortValue());
        }
        if (defaultReplicationFactor != null) {
            clientBuilder.defaultReplicationFactor(defaultReplicationFactor.intValue());
        }
        clientBuilder.brokers(cluster.nodes());
        clientBuilder.controller(0);
        try (MockAdminClient admin = clientBuilder.build()) {
            TopicAdmin topicClient = new TopicAdmin(null, admin, false);
            assertTrue(topicClient.createTopic(newTopic));
            assertTopic(admin, newTopic.name(), expectedPartitions, expectedReplicas);
        }
    }

    protected void assertTopic(MockAdminClient admin, String topicName, int expectedPartitions, int expectedReplicas) {
        TopicDescription desc = null;
        try {
            desc = topicDescription(admin, topicName);
        } catch (Throwable t) {
            fail("Failed to find topic description for topic '" + topicName + "'");
        }
        assertEquals(expectedPartitions, desc.partitions().size());
        for (TopicPartitionInfo tp : desc.partitions()) {
            assertEquals(expectedReplicas, tp.replicas().size());
        }
    }

    protected TopicDescription topicDescription(MockAdminClient admin, String topicName)
            throws ExecutionException, InterruptedException {
        DescribeTopicsResult result = admin.describeTopics(Collections.singleton(topicName));
        Map<String, KafkaFuture<TopicDescription>> byName = result.values();
        return byName.get(topicName).get();
    }
}
