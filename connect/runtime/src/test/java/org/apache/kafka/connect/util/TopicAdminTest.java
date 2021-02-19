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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientUnitTestEnv;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    public void shouldCreateTopicWhenItDoesNotExist() {
        NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            assertTrue(admin.createTopic(newTopic));
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

    @SuppressWarnings("unchecked")
    private <T> KafkaFuture<T> mockFuture() {
        return (KafkaFuture<T>) mock(KafkaFuture.class);
    }

    private Admin expectAdminListOffsetsFailure(Throwable expected) throws Exception {
        // When the admin client lists offsets
        Admin mockAdmin = mock(Admin.class);
        ListOffsetsResult results = mock(ListOffsetsResult.class);
        when(mockAdmin.listOffsets(anyMap())).thenReturn(results);
        // and throws an exception via the future.get()
        ExecutionException execException = new ExecutionException(expected);
        KafkaFuture<ListOffsetsResultInfo> future = mockFuture();
        when(future.get()).thenThrow(execException);
        when(results.partitionResult(any(TopicPartition.class))).thenReturn(future);
        return mockAdmin;
    }

    @Test(expected = ConnectException.class)
    public void endOffsetsShouldFailWithNonRetriableWhenAuthorizationFailureOccurs() throws Exception {
        String topicName = "myTopic";
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        Set<TopicPartition> tps = Collections.singleton(tp1);

        // When the admin client lists offsets throws an exception
        Admin mockAdmin = expectAdminListOffsetsFailure(new AuthorizationException("failed"));

        // Then the topic admin should throw exception
        TopicAdmin admin = new TopicAdmin(null, mockAdmin);
        admin.endOffsets(tps);
    }

    @Test(expected = UnsupportedVersionException.class)
    public void endOffsetsShouldFailWithUnsupportedVersionWhenVersionUnsupportedErrorOccurs() throws Exception {
        String topicName = "myTopic";
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        Set<TopicPartition> tps = Collections.singleton(tp1);

        // When the admin client lists offsets throws an exception
        Admin mockAdmin = expectAdminListOffsetsFailure(new UnsupportedVersionException("failed"));

        // Then the topic admin should throw exception
        TopicAdmin admin = new TopicAdmin(null, mockAdmin);
        admin.endOffsets(tps);
    }

    @Test(expected = TimeoutException.class)
    public void endOffsetsShouldFailWithTimeoutExceptionWhenTimeoutErrorOccurs() throws Exception {
        String topicName = "myTopic";
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        Set<TopicPartition> tps = Collections.singleton(tp1);

        // When the admin client lists offsets throws an exception
        Admin mockAdmin = expectAdminListOffsetsFailure(new TimeoutException("failed"));

        // Then the topic admin should throw exception
        TopicAdmin admin = new TopicAdmin(null, mockAdmin);
        admin.endOffsets(tps);
    }

    @Test(expected = ConnectException.class)
    public void endOffsetsShouldFailWithNonRetriableWhenUnknownErrorOccurs() throws Exception {
        String topicName = "myTopic";
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        Set<TopicPartition> tps = Collections.singleton(tp1);

        // When the admin client lists offsets throws an exception
        Admin mockAdmin = expectAdminListOffsetsFailure(new RuntimeException("failed"));

        // Then the topic admin should throw exception
        TopicAdmin admin = new TopicAdmin(null, mockAdmin);
        admin.endOffsets(tps);
    }

    @Test
    public void endOffsetsShouldReturnEmptyMapWhenPartitionsSetIsNull() throws Exception {
        String topicName = "myTopic";
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        Set<TopicPartition> tps = Collections.singleton(tp1);

        // Then the topic admin should return immediately
        Admin mockAdmin = mock(Admin.class);
        TopicAdmin admin = new TopicAdmin(null, mockAdmin);
        Map<TopicPartition, Long> offsets = admin.endOffsets(Collections.emptySet());
        assertTrue(offsets.isEmpty());
    }

    @Test(expected = ConnectException.class)
    public void endOffsetsShouldFailWhenAnyTopicPartitionHasError() throws Exception {
        String topicName = "myTopic";
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        Set<TopicPartition> tps = Collections.singleton(tp1);

        // When the admin client lists offsets throws an exception
        Admin mockAdmin = expectAdminListOffsetsFailure(new AuthorizationException("failed"));

        // Then the topic admin should throw exception
        TopicAdmin admin = new TopicAdmin(null, mockAdmin);
        admin.endOffsets(tps);
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
}
