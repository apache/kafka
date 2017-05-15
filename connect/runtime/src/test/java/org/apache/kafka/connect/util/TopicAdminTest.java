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
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TopicAdminTest {

    /**
     * 0.10.x clients can't talk with 0.9.x brokers, and 0.10.0.0 introduced the new protocol with API versions.
     * That means we can simulate an API version mismatch.
     *
     * @throws Exception
     */
    @Test
    public void returnNullWithApiVersionMismatch() throws Exception {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        boolean internal = false;
        Cluster cluster = createCluster(1);
        try (MockKafkaAdminClientEnv env = new MockKafkaAdminClientEnv(cluster)) {
            env.kafkaClient().setNode(cluster.controller());
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareMetadataUpdate(env.cluster(), Collections.<String>emptySet());
            env.kafkaClient().prepareResponse(metadataResponseFor(cluster).response());
            env.kafkaClient().prepareResponse(createTopicResponseWithUnsupportedVersion(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            TopicDescription desc = admin.createTopicIfMissing(newTopic);
            assertNull(desc);
        }
    }

    @Test
    public void returnTopicDescriptionForExistingTopic() throws Exception {
        NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        boolean internal = false;
        Cluster cluster = createCluster(1);
        try (MockKafkaAdminClientEnv env = new MockKafkaAdminClientEnv(cluster)) {
            env.kafkaClient().setNode(cluster.controller());
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareMetadataUpdate(env.cluster(), Collections.<String>emptySet());
            env.kafkaClient().prepareResponse(metadataResponseFor(cluster).withTopic(newTopic.name(), newTopic.partitions(), internal).response());
            env.kafkaClient().prepareResponse(metadataResponseFor(cluster).withTopic(newTopic.name(), newTopic.partitions(), internal).response());
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            TopicDescription desc = admin.createTopicIfMissing(newTopic);
            assertNotNull(desc);
            assertEquals(desc.name(), newTopic.name());
            assertEquals(desc.internal(), internal);
        }
    }

    @Test
    public void returnTopicDescriptionForExistingInternalTopic() throws Exception {
        NewTopic newTopic = TopicAdmin.defineTopic("_myTopic").partitions(1).compacted().build();
        boolean internal = true;
        Cluster cluster = createCluster(1);
        try (MockKafkaAdminClientEnv env = new MockKafkaAdminClientEnv(cluster)) {
            env.kafkaClient().setNode(cluster.controller());
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareMetadataUpdate(env.cluster(), Collections.<String>emptySet());
            env.kafkaClient().prepareResponse(metadataResponseFor(cluster).withTopic(newTopic.name(), newTopic.partitions(), internal).response());
            env.kafkaClient().prepareResponse(metadataResponseFor(cluster).withTopic(newTopic.name(), newTopic.partitions(), internal).response());
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            TopicDescription desc = admin.createTopicIfMissing(newTopic);
            assertNotNull(desc);
            assertEquals(desc.name(), newTopic.name());
            assertEquals(desc.internal(), internal);
        }
    }

    @Test
    public void returnTopicDescriptionForNewTopic() throws Exception {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        boolean internal = false;
        Cluster cluster = createCluster(1);
        try (MockKafkaAdminClientEnv env = new MockKafkaAdminClientEnv(cluster)) {
            env.kafkaClient().setNode(cluster.controller());
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareMetadataUpdate(env.cluster(), Collections.<String>emptySet());
            // Three calls are made ...
            env.kafkaClient().prepareResponse(metadataResponseFor(cluster).response());
            env.kafkaClient().prepareResponse(createTopicResponse(newTopic));
            env.kafkaClient().prepareResponse(metadataResponseFor(cluster).withTopic(newTopic.name(), newTopic.partitions(), internal).response());
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            TopicDescription desc = admin.createTopicIfMissing(newTopic);
            assertNotNull(desc);
            assertEquals(desc.name(), newTopic.name());
            assertEquals(desc.internal(), internal);
        }
    }

    @Test
    public void returnTopicDescriptionsForMultipleNewTopics() throws Exception {
        final NewTopic newTopic1 = TopicAdmin.defineTopic("myTopic1").partitions(1).compacted().build();
        final NewTopic newTopic2 = TopicAdmin.defineTopic("myTopic2").partitions(1).compacted().build();
        boolean internal = false;
        Cluster cluster = createCluster(1);
        try (MockKafkaAdminClientEnv env = new MockKafkaAdminClientEnv(cluster)) {
            env.kafkaClient().setNode(cluster.controller());
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareMetadataUpdate(env.cluster(), Collections.<String>emptySet());
            // Three calls are made ...
            env.kafkaClient().prepareResponse(metadataResponseFor(cluster).response());
            env.kafkaClient().prepareResponse(createTopicResponse(newTopic1, newTopic2));
            env.kafkaClient().prepareResponse(metadataResponseFor(cluster).
                    withTopic(newTopic1.name(), newTopic1.partitions(), internal).
                    withTopic(newTopic2.name(), newTopic2.partitions(), internal).
                    response());
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            Map<String, TopicDescription> desc = admin.createTopicsIfMissing(newTopic1, newTopic2);
            assertNotNull(desc);
            assertEquals(desc.size(), 2);
            TopicDescription desc1 = desc.get(newTopic1.name());
            TopicDescription desc2 = desc.get(newTopic2.name());
            assertEquals(desc1.name(), newTopic1.name());
            assertEquals(desc1.internal(), internal);
            assertEquals(desc2.name(), newTopic2.name());
            assertEquals(desc2.internal(), internal);
        }
    }

    @Test
    public void returnTopicDescriptionsForNewAndExistingTopics() throws Exception {
        final NewTopic existingTopic = TopicAdmin.defineTopic("myTopic1").partitions(1).compacted().build();
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic2").partitions(1).compacted().build();
        boolean internal = false;
        Cluster cluster = createCluster(1);
        try (MockKafkaAdminClientEnv env = new MockKafkaAdminClientEnv(cluster)) {
            env.kafkaClient().setNode(cluster.controller());
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareMetadataUpdate(env.cluster(), Collections.<String>emptySet());
            // First expect to find existing topic ...
            env.kafkaClient().prepareResponse(metadataResponseFor(cluster).withTopic(existingTopic.name(), existingTopic.partitions(), internal).response());
            // Then expect to create the new topic ...
            env.kafkaClient().prepareResponse(createTopicResponse(newTopic));
            // Finally return information for new and existing topics ...
            env.kafkaClient().prepareResponse(metadataResponseFor(cluster).
                    withTopic(newTopic.name(), newTopic.partitions(), internal).
                    withTopic(existingTopic.name(), existingTopic.partitions(), internal).
                    response());
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            Map<String, TopicDescription> desc = admin.createTopicsIfMissing(existingTopic, newTopic);
            assertNotNull(desc);
            assertEquals(desc.size(), 2);
            TopicDescription desc1 = desc.get(existingTopic.name());
            TopicDescription desc2 = desc.get(newTopic.name());
            assertEquals(desc1.name(), existingTopic.name());
            assertEquals(desc1.internal(), internal);
            assertEquals(desc2.name(), newTopic.name());
            assertEquals(desc2.internal(), internal);
        }
    }

    @Test
    public void returnTopicDescriptionsForMultipleTopicsCreatedByConcurrentClient() throws Exception {
        final NewTopic existingTopic = TopicAdmin.defineTopic("myTopic1").partitions(1).compacted().build();
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic2").partitions(1).compacted().build();
        boolean internal = false;
        Cluster cluster = createCluster(1);
        try (MockKafkaAdminClientEnv env = new MockKafkaAdminClientEnv(cluster)) {
            env.kafkaClient().setNode(cluster.controller());
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareMetadataUpdate(env.cluster(), Collections.<String>emptySet());
            // First expect to find only the existing topic ...
            env.kafkaClient().prepareResponse(metadataResponseFor(cluster).withTopic(existingTopic.name(), existingTopic.partitions(), internal).response());
            // Then expect to create the new topic, except error because another client just created it ...
            env.kafkaClient().prepareResponse(createTopicResponseWithAlreadyExists(newTopic));
            // Finally return information for new and existing topics ...
            env.kafkaClient().prepareResponse(metadataResponseFor(cluster).
                    withTopic(newTopic.name(), newTopic.partitions(), internal).
                    withTopic(existingTopic.name(), existingTopic.partitions(), internal).
                    response());
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            Map<String, TopicDescription> desc = admin.createTopicsIfMissing(existingTopic, newTopic);
            assertNotNull(desc);
            assertEquals(desc.size(), 2);
            TopicDescription desc1 = desc.get(existingTopic.name());
            TopicDescription desc2 = desc.get(newTopic.name());
            assertEquals(desc1.name(), existingTopic.name());
            assertEquals(desc1.internal(), internal);
            assertEquals(desc2.name(), newTopic.name());
            assertEquals(desc2.internal(), internal);
        }
    }

    private static class MetadataResponseBuilder {
        private final Cluster cluster;
        private final List<TopicMetadata> topicMetadata = new ArrayList<>();

        private MetadataResponseBuilder(Cluster cluster) {
            this.cluster = cluster;
        }

        public MetadataResponseBuilder withTopic(String topicName) {
            return withTopic(topicName, 1);
        }

        public MetadataResponseBuilder withTopic(String topicName, int numPartitions) {
            return withTopic(topicName, numPartitions, false);
        }

        public MetadataResponseBuilder withInternalTopic(String topicName) {
            return withInternalTopic(topicName, 1);
        }

        public MetadataResponseBuilder withInternalTopic(String topicName, int numPartitions) {
            return withTopic(topicName, numPartitions, true);
        }

        public MetadataResponseBuilder withTopic(String topicName, int numPartitions, boolean internalTopic) {
            List<PartitionMetadata> partitionMetadataList = new ArrayList<>();
            for (int i = 0; i != numPartitions; ++i) {
                PartitionMetadata pm = new PartitionMetadata(Errors.NONE, i + 1, cluster.nodeById(i), cluster.nodes(), cluster.nodes());
                partitionMetadataList.add(pm);
            }
            topicMetadata.add(new TopicMetadata(Errors.NONE, topicName, internalTopic, partitionMetadataList));
            return this;
        }

        public MetadataResponse response() {
            return new MetadataResponse(cluster.nodes(), cluster.clusterResource().clusterId(), cluster.controller().id(), topicMetadata);
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

    private MetadataResponseBuilder metadataResponseFor(Cluster cluster) {
        return new MetadataResponseBuilder(cluster);
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
