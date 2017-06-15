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
import org.apache.kafka.clients.admin.MockKafkaAdminClientEnv;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TopicAdminTest {

    /**
     * 0.10.x clients can't talk with 0.9.x brokers, and 0.10.0.0 introduced the new protocol with API versions.
     * That means we can simulate an API version mismatch.
     *
     * @throws Exception
     */
    @Test
    public void returnNullWithApiVersionMismatch() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        boolean internal = false;
        Cluster cluster = createCluster(1);
        try (MockKafkaAdminClientEnv env = new MockKafkaAdminClientEnv(cluster)) {
            env.kafkaClient().setNode(cluster.controller());
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareMetadataUpdate(env.cluster(), Collections.<String>emptySet());
            env.kafkaClient().prepareResponse(createTopicResponseWithUnsupportedVersion(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            admin.createTopic(newTopic);
            fail();
        } catch (UnsupportedVersionException e) {
            // expected
        }
    }

    @Test
    public void shouldNotCreateTopicWhenItAlreadyExists() {
        NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (MockKafkaAdminClientEnv env = new MockKafkaAdminClientEnv(cluster)) {
            env.kafkaClient().setNode(cluster.controller());
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareMetadataUpdate(env.cluster(), Collections.<String>emptySet());
            env.kafkaClient().prepareResponse(createTopicResponseWithAlreadyExists(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            boolean created = admin.createTopic(newTopic);
            assertFalse(created);
        }
    }

    @Test
    public void shouldCreateTopicWhenItDoesNotExist() {
        NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (MockKafkaAdminClientEnv env = new MockKafkaAdminClientEnv(cluster)) {
            env.kafkaClient().setNode(cluster.controller());
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareMetadataUpdate(env.cluster(), Collections.<String>emptySet());
            env.kafkaClient().prepareResponse(createTopicResponse(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            boolean created = admin.createTopic(newTopic);
            assertTrue(created);
        }
    }

    @Test
    public void shouldCreateOneTopicWhenProvidedMultipleDefinitionsWithSameTopicName() {
        NewTopic newTopic1 = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        NewTopic newTopic2 = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (MockKafkaAdminClientEnv env = new MockKafkaAdminClientEnv(cluster)) {
            env.kafkaClient().setNode(cluster.controller());
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareMetadataUpdate(env.cluster(), Collections.<String>emptySet());
            env.kafkaClient().prepareResponse(createTopicResponse(newTopic1));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            Set<String> newTopicNames = admin.createTopics(newTopic1, newTopic2);
            assertEquals(1, newTopicNames.size());
            assertEquals(newTopic2.name(), newTopicNames.iterator().next());
        }
    }

    @Test
    public void shouldReturnFalseWhenSuppliedNullTopicDescription() {
        Cluster cluster = createCluster(1);
        try (MockKafkaAdminClientEnv env = new MockKafkaAdminClientEnv(cluster)) {
            env.kafkaClient().setNode(cluster.controller());
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareMetadataUpdate(env.cluster(), Collections.<String>emptySet());
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            boolean created = admin.createTopic(null);
            assertFalse(created);
        }
    }

    private Cluster createCluster(int numNodes) {
        HashMap<Integer, Node> nodes = new HashMap<>();
        for (int i = 0; i != numNodes; ++i) {
            nodes.put(i, new Node(i, "localhost", 8121 + i));
        }
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                Collections.<PartitionInfo>emptySet(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));
        return cluster;
    }

    private CreateTopicsResponse createTopicResponse(NewTopic... topics) {
        return createTopicResponse(new ApiError(Errors.NONE, ""), topics);
    }

    private CreateTopicsResponse createTopicResponseWithAlreadyExists(NewTopic... topics) {
        return createTopicResponse(new ApiError(Errors.TOPIC_ALREADY_EXISTS, "Topic already exists"), topics);
    }

    private CreateTopicsResponse createTopicResponseWithUnsupportedVersion(NewTopic... topics) {
        return createTopicResponse(new ApiError(Errors.UNSUPPORTED_VERSION, "This version of the API is not supported"), topics);
    }

    private CreateTopicsResponse createTopicResponse(ApiError error, NewTopic... topics) {
        if (error == null) error = new ApiError(Errors.NONE, "");
        Map<String, ApiError> topicResults = new HashMap<>();
        for (NewTopic topic : topics) {
            topicResults.put(topic.name(), error);
        }
        return new CreateTopicsResponse(topicResults);
    }
}
