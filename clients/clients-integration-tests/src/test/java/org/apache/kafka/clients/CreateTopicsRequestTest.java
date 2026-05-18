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

package org.apache.kafka.clients;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignmentCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.IntegrationTestUtils;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
    }
)
public class CreateTopicsRequestTest {

    @ClusterTest
    public void testValidCreateTopicsRequests(ClusterInstance cluster) throws Exception {
        // Generated assignments
        validateValidCreateTopicsRequests(cluster, topicsReq(topicReq("topic1")));
        validateValidCreateTopicsRequests(cluster, topicsReq(topicReq("topic2", null, 3)));
        validateValidCreateTopicsRequests(cluster, topicsReq(
            topicReq("topic3", 5, 2, Map.of("min.insync.replicas", "2"), null)));

        // Manual assignments
        validateValidCreateTopicsRequests(cluster, topicsReq(
            topicReq("topic4", null, null, null, Map.of(0, List.of(0)))));
        validateValidCreateTopicsRequests(cluster, topicsReq(
            topicReq("topic5", null, null, Map.of("min.insync.replicas", "2"),
                Map.of(0, List.of(0, 1), 1, List.of(1, 0), 2, List.of(1, 2)))));

        // Mixed
        validateValidCreateTopicsRequests(cluster, topicsReq(
            topicReq("topic6"),
            topicReq("topic7", 5, 2),
            topicReq("topic8", null, null, null,
                Map.of(0, List.of(0, 1), 1, List.of(1, 0), 2, List.of(1, 2)))));
        validateValidCreateTopicsRequests(cluster, topicsReq(true,
            topicReq("topic9"),
            topicReq("topic10", 5, 2),
            topicReq("topic11", null, null, null,
                Map.of(0, List.of(0, 1), 1, List.of(1, 0), 2, List.of(1, 2)))));

        // Defaults
        validateValidCreateTopicsRequests(cluster, topicsReq(topicReq("topic12", -1, -1)));
        validateValidCreateTopicsRequests(cluster, topicsReq(topicReq("topic13", -1, 2)));
        validateValidCreateTopicsRequests(cluster, topicsReq(topicReq("topic14", 2, -1)));
    }

    @ClusterTest
    public void testErrorCreateTopicsRequests(ClusterInstance cluster) throws Exception {
        String existingTopic = "existing-topic";
        cluster.createTopic(existingTopic, 1, (short) 1);

        int brokerCount = cluster.brokers().size();

        // Basic
        validateErrorCreateTopicsRequests(cluster,
            topicsReq(topicReq(existingTopic)),
            Map.of(existingTopic, new ApiError(Errors.TOPIC_ALREADY_EXISTS, "Topic 'existing-topic' already exists.")), true);
        validateErrorCreateTopicsRequests(cluster,
            topicsReq(topicReq("error-partitions", -2, null)),
            Map.of("error-partitions", new ApiError(Errors.INVALID_PARTITIONS)), false);
        validateErrorCreateTopicsRequests(cluster,
            topicsReq(topicReq("error-replication", null, brokerCount + 1)),
            Map.of("error-replication", new ApiError(Errors.INVALID_REPLICATION_FACTOR)), false);
        validateErrorCreateTopicsRequests(cluster,
            topicsReq(topicReq("error-config", null, null, Map.of("not.a.property", "error"), null)),
            Map.of("error-config", new ApiError(Errors.INVALID_CONFIG)), false);
        validateErrorCreateTopicsRequests(cluster,
            topicsReq(topicReq("error-assignment", null, null, null,
                Map.of(0, List.of(0, 1), 1, List.of(0)))),
            Map.of("error-assignment", new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT)), false);

        // Partial
        validateErrorCreateTopicsRequests(cluster,
            topicsReq(
                topicReq(existingTopic),
                topicReq("partial-partitions", -2, null),
                topicReq("partial-replication", null, brokerCount + 1),
                topicReq("partial-assignment", null, null, null,
                    Map.of(0, List.of(0, 1), 1, List.of(0))),
                topicReq("partial-none")),
            Map.of(
                existingTopic, new ApiError(Errors.TOPIC_ALREADY_EXISTS),
                "partial-partitions", new ApiError(Errors.INVALID_PARTITIONS),
                "partial-replication", new ApiError(Errors.INVALID_REPLICATION_FACTOR),
                "partial-assignment", new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT),
                "partial-none", new ApiError(Errors.NONE)),
            false);
        validateTopicExists(cluster, "partial-none", 1);
    }

    @ClusterTest
    public void testInvalidCreateTopicsRequests(ClusterInstance cluster) throws Exception {
        // Partitions/ReplicationFactor and ReplicaAssignment should not both be specified
        validateErrorCreateTopicsRequests(cluster,
            topicsReq(topicReq("bad-args-topic", 10, 3, null, Map.of(0, List.of(0)))),
            Map.of("bad-args-topic", new ApiError(Errors.INVALID_REQUEST)), false);

        validateErrorCreateTopicsRequests(cluster,
            topicsReq(true, topicReq("bad-args-topic", 10, 3, null, Map.of(0, List.of(0)))),
            Map.of("bad-args-topic", new ApiError(Errors.INVALID_REQUEST)), false);
    }

    @ClusterTest
    public void testCreateTopicsRequestVersions(ClusterInstance cluster) throws Exception {
        for (short version = ApiKeys.CREATE_TOPICS.oldestVersion(); version <= ApiKeys.CREATE_TOPICS.latestVersion(); version++) {
            String topic = "topic_" + version;
            CreateTopicsRequestData data = new CreateTopicsRequestData()
                .setTimeoutMs(10000)
                .setValidateOnly(false)
                .setTopics(new CreatableTopicCollection(List.of(
                    topicReq(topic, 1, 1, Map.of("min.insync.replicas", "2"), null)
                ).iterator()));

            CreateTopicsRequest request = new CreateTopicsRequest.Builder(data).build(version);
            CreateTopicsResponse response = sendCreateTopicRequest(cluster, request);

            CreatableTopicResult topicResponse = response.data().topics().find(topic);
            assertNotNull(topicResponse);
            assertEquals(topic, topicResponse.name());
            assertEquals(Errors.NONE.code(), topicResponse.errorCode());

            if (version >= 5) {
                assertEquals(1, topicResponse.numPartitions());
                assertEquals(1, topicResponse.replicationFactor());
                var config = topicResponse.configs().stream()
                    .filter(c -> "min.insync.replicas".equals(c.name())).findFirst();
                assertTrue(config.isPresent());
                assertEquals("2", config.get().value());
            } else {
                assertEquals(-1, topicResponse.numPartitions());
                assertEquals(-1, topicResponse.replicationFactor());
                assertTrue(topicResponse.configs().isEmpty());
            }

            if (version >= 7) {
                assertNotEquals(Uuid.ZERO_UUID, topicResponse.topicId());
            } else {
                assertEquals(Uuid.ZERO_UUID, topicResponse.topicId());
            }
        }
    }

    @ClusterTest
    public void testCreateClusterMetadataTopic(ClusterInstance cluster) throws Exception {
        validateErrorCreateTopicsRequests(cluster,
            topicsReq(topicReq(Topic.CLUSTER_METADATA_TOPIC_NAME)),
            Map.of(Topic.CLUSTER_METADATA_TOPIC_NAME,
                new ApiError(Errors.INVALID_REQUEST,
                    "Creation of internal topic " + Topic.CLUSTER_METADATA_TOPIC_NAME + " is prohibited.")),
            true);
    }

    private static CreateTopicsRequest topicsReq(CreatableTopic... topics) {
        return topicsReq(false, topics);
    }

    private static CreateTopicsRequest topicsReq(boolean validateOnly, CreatableTopic... topics) {
        var req = new CreateTopicsRequestData()
            .setTimeoutMs(10000)
            .setTopics(new CreatableTopicCollection(List.of(topics).iterator()))
            .setValidateOnly(validateOnly);
        return new CreateTopicsRequest.Builder(req).build();
    }

    private static CreatableTopic topicReq(String name) {
        return topicReq(name, null, null, null, null);
    }

    private static CreatableTopic topicReq(String name, Integer numPartitions, Integer replicationFactor) {
        return topicReq(name, numPartitions, replicationFactor, null, null);
    }

    private static CreatableTopic topicReq(
        String name,
        Integer numPartitions,
        Integer replicationFactor,
        Map<String, String> config,
        Map<Integer, List<Integer>> assignment
    ) {
        CreatableTopic topic = new CreatableTopic();
        topic.setName(name);
        if (numPartitions != null) {
            topic.setNumPartitions(numPartitions);
        } else if (assignment != null) {
            topic.setNumPartitions(-1);
        } else {
            topic.setNumPartitions(1);
        }
        if (replicationFactor != null) {
            topic.setReplicationFactor(replicationFactor.shortValue());
        } else if (assignment != null) {
            topic.setReplicationFactor((short) -1);
        } else {
            topic.setReplicationFactor((short) 1);
        }
        if (config != null) {
            var effectiveConfigs = new CreatableTopicConfigCollection();
            config.forEach((configName, configValue) ->
                effectiveConfigs.add(new CreatableTopicConfig()
                    .setName(configName)
                    .setValue(configValue)));
            topic.setConfigs(effectiveConfigs);
        }
        if (assignment != null) {
            var effectiveAssignments = new CreatableReplicaAssignmentCollection();
            assignment.forEach((partitionIndex, brokerIdList) ->
                effectiveAssignments.add(new CreatableReplicaAssignment()
                    .setPartitionIndex(partitionIndex)
                    .setBrokerIds(new ArrayList<>(brokerIdList))));
            topic.setAssignments(effectiveAssignments);
        }
        return topic;
    }

    private static void validateValidCreateTopicsRequests(ClusterInstance cluster, CreateTopicsRequest request) throws Exception {
        CreateTopicsResponse response = sendCreateTopicRequest(cluster, request);

        assertFalse(response.errorCounts().keySet().stream().anyMatch(e -> e.code() > 0),
            "There should be no errors, found " + response.errorCounts().keySet());

        for (CreatableTopic topic : request.data().topics()) {
            if (!request.data().validateOnly()) {
                int partitions = !topic.assignments().isEmpty()
                    ? topic.assignments().size()
                    : (topic.numPartitions() == -1 ? defaultNumPartitions(cluster) : topic.numPartitions());
                cluster.waitTopicCreation(topic.name(), partitions);
            }
            verifyMetadata(cluster, topic, request.data().validateOnly());
        }
    }

    private static void verifyMetadata(ClusterInstance cluster, CreatableTopic topic,
                                       boolean validateOnly) throws Exception {
        MetadataResponse metadataResponse = sendMetadataRequest(cluster,
            new MetadataRequest.Builder(List.of(topic.name()), false).build());
        MetadataResponse.TopicMetadata metadataForTopic = metadataResponse.topicMetadata().stream()
            .filter(t -> topic.name().equals(t.topic())).findFirst().orElse(null);

        int partitions = !topic.assignments().isEmpty() ? topic.assignments().size() : topic.numPartitions();
        int replication = !topic.assignments().isEmpty() ? topic.assignments().iterator().next().brokerIds().size() : topic.replicationFactor();

        if (validateOnly) {
            assertNotNull(metadataForTopic);
            assertNotEquals(Errors.NONE, metadataForTopic.error(), "Topic " + topic + " should not be created");
            assertTrue(metadataForTopic.partitionMetadata().isEmpty(), "The topic should have no partitions");
        } else {
            assertNotNull(metadataForTopic, "The topic should be created");
            assertEquals(Errors.NONE, metadataForTopic.error());
            if (partitions == -1) {
                assertEquals(defaultNumPartitions(cluster), metadataForTopic.partitionMetadata().size(),
                    "The topic should have the default number of partitions");
            } else {
                assertEquals(partitions, metadataForTopic.partitionMetadata().size(),
                    "The topic should have the correct number of partitions");
            }

            if (replication == -1) {
                assertEquals(defaultReplicationFactor(cluster), metadataForTopic.partitionMetadata().get(0).replicaIds.size(),
                    "The topic should have the default replication factor");
            } else {
                assertEquals(replication, metadataForTopic.partitionMetadata().get(0).replicaIds.size(),
                    "The topic should have the correct replication factor");
            }
        }
    }

    private static void validateErrorCreateTopicsRequests(
        ClusterInstance cluster,
        CreateTopicsRequest request,
        Map<String, ApiError> expectedResponse,
        boolean checkErrorMessage
    ) throws Exception {
        CreateTopicsResponse response = sendCreateTopicRequest(cluster, request);
        assertEquals(expectedResponse.size(), response.data().topics().size(), "The response size should match");

        for (var entry : expectedResponse.entrySet()) {
            String topicName = entry.getKey();
            ApiError expectedError = entry.getValue();

            CreatableTopicResult actual = response.data().topics().find(topicName);
            if (actual == null) {
                throw new RuntimeException("No response data found for topic " + topicName);
            }
            assertEquals(expectedError.error().code(), actual.errorCode(), "The response error code should match");
            if (checkErrorMessage) {
                assertEquals(expectedError.message(), actual.errorMessage(), "The response error message should match");
            }
            // If no error, validate topic exists
            if (expectedError.isSuccess() && !request.data().validateOnly()) {
                CreatableTopic topic = request.data().topics().find(topicName);
                int partitions = !topic.assignments().isEmpty()
                    ? topic.assignments().size()
                    : (topic.numPartitions() == -1 ? defaultNumPartitions(cluster) : topic.numPartitions());
                validateTopicExists(cluster, topicName, partitions);
            }
        }
    }

    private static void validateTopicExists(ClusterInstance cluster, String topic, int partitions) throws Exception {
        cluster.waitTopicCreation(topic, partitions);
        MetadataResponse metadataResponse = sendMetadataRequest(cluster,
            new MetadataRequest.Builder(List.of(topic), true).build());
        assertTrue(metadataResponse.topicMetadata().stream().anyMatch(p -> topic.equals(p.topic()) && p.error() == Errors.NONE),
            "The topic should be created");
    }

    private static CreateTopicsResponse sendCreateTopicRequest(ClusterInstance cluster, CreateTopicsRequest request) throws Exception {
        return IntegrationTestUtils.connectAndReceive(request, cluster.brokerBoundPorts().get(0));
    }

    private static MetadataResponse sendMetadataRequest(ClusterInstance cluster, MetadataRequest request) throws Exception {
        return IntegrationTestUtils.connectAndReceive(request, cluster.brokerBoundPorts().get(0));
    }

    private static int defaultNumPartitions(ClusterInstance cluster) {
        return Integer.parseInt(cluster.config().serverProperties()
            .getOrDefault(ServerLogConfigs.NUM_PARTITIONS_CONFIG, "1"));
    }

    private static int defaultReplicationFactor(ClusterInstance cluster) {
        return Integer.parseInt(cluster.config().serverProperties()
            .getOrDefault(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "1"));
    }
}
