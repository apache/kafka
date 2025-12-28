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
package org.apache.kafka.server;

import kafka.network.SocketServer;
import kafka.server.KafkaBroker;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.test.TestUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "5"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(id = 0, key = "broker.rack", value = "rack/0"),
        @ClusterConfigProperty(id = 1, key = "broker.rack", value = "rack/1"),
        @ClusterConfigProperty(id = 2, key = "broker.rack", value = "rack/2")
    }
)

public class MetadataRequestTest {

    private final ClusterInstance clusterInstance;

    MetadataRequestTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    private List<KafkaBroker> brokers() {
        return clusterInstance.brokers().values().stream().toList();
    }

    private SocketServer anySocketServer() throws IllegalStateException {
        Map<Integer, KafkaBroker> aliveBrokers = clusterInstance.aliveBrokers();
        if (aliveBrokers.isEmpty()) {
            throw new IllegalStateException("No alive brokers is available");
        }
        return aliveBrokers.values().stream().map(KafkaBroker::socketServer).iterator().next();
    }

    private MetadataRequestData requestData(List<String> topics, boolean allowAutoTopicCreation) {
        MetadataRequestData data = new MetadataRequestData();
        if (topics == null) {
            data.setTopics(null);
        } else {
            List<MetadataRequestData.MetadataRequestTopic> requestTopics = topics.stream().map(topic -> new MetadataRequestData.MetadataRequestTopic().setName(topic)).toList();
            data.setTopics(requestTopics);
            data.setAllowAutoTopicCreation(allowAutoTopicCreation);
        }
        return data;
    }

    private MetadataResponse sendMetadataRequest(MetadataRequest request) throws IOException {
        return sendMetadataRequest(request, anySocketServer());
    }

    private MetadataResponse sendMetadataRequest(MetadataRequest request, SocketServer destination) throws IOException {
        ListenerName listener = clusterInstance.clientListener();
        int port = destination.boundPort(listener);
        return IntegrationTestUtils.connectAndReceive(request, port);
    }

    protected void checkAutoCreatedTopic(String autoCreatedTopic, MetadataResponse response) throws InterruptedException {
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.errors().get(autoCreatedTopic));
        int numPartitions = brokers().get(0).config().numPartitions();

        for (int i = 0; i < numPartitions; i++) {
            final int partitionId = i;
            TestUtils.waitForCondition(() -> {
                for (KafkaBroker broker : brokers()) {
                    Optional<LeaderAndIsr> leaderOpt = broker.metadataCache().getLeaderAndIsr(autoCreatedTopic, partitionId);
                    if (leaderOpt.isEmpty()) {
                        return false;
                    }
                    if (!FetchRequest.isValidBrokerId(leaderOpt.get().leader())) {
                        return false;
                    }
                }
                return true;
            }, "Partition [" + autoCreatedTopic + "," + partitionId + "] metadata not propagated");
        }
    }

    @ClusterTest
    public void testClusterIdWithRequestVersion1() throws IOException {
        MetadataResponse v1MetadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics().build((short) 1));
        String v1ClusterId = v1MetadataResponse.clusterId();
        assertNull(v1ClusterId, "v1 clusterId should be null");
    }

    @ClusterTest
    public void testClusterIdIsValid() throws IOException {
        MetadataResponse metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics().build((short) 4));
        TestUtils.isValidClusterId(metadataResponse.clusterId());
    }

    @ClusterTest
    public void testRack() throws IOException {
        MetadataResponse metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics().build((short) 4));
        // Validate rack matches what's set in generateConfigs() above
        metadataResponse.brokers().forEach(broker -> {
            assertEquals(String.format("rack/%d", broker.id()), broker.rack(), "Rack information should match config");
        });
    }

    @ClusterTest
    public void testIsInternal() throws InterruptedException, IOException {
        String internalTopic = Topic.GROUP_METADATA_TOPIC_NAME;
        String notInternalTopic = "notInternal";
        // create the topics
        clusterInstance.createTopic(internalTopic, 3, (short) 2);
        clusterInstance.createTopic(notInternalTopic, 3, (short) 2);

        MetadataResponse metadataResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics().build((short) 4));
        assertTrue(metadataResponse.errors().isEmpty(), "Response should have no errors");

        Collection<MetadataResponse.TopicMetadata> topicMetadata = metadataResponse.topicMetadata();
        MetadataResponse.TopicMetadata internalTopicMetadata = topicMetadata.stream().filter(metadata -> metadata.topic().equals(internalTopic)).findFirst().get();
        MetadataResponse.TopicMetadata notInternalTopicMetadata = topicMetadata.stream().filter(metadata -> metadata.topic().equals(notInternalTopic)).findFirst().get();

        assertTrue(internalTopicMetadata.isInternal(), "internalTopic should show isInternal");
        assertFalse(notInternalTopicMetadata.isInternal(), "notInternalTopic topic not should show isInternal");

        assertEquals(Set.of(internalTopic), metadataResponse.buildCluster().internalTopics());
    }

    @ClusterTest
    public void testNoTopicsRequest() throws InterruptedException, IOException {
        // create some topics
        clusterInstance.createTopic("t1", 3, (short) 2);
        clusterInstance.createTopic("t2", 3, (short) 2);

        MetadataResponse metadataResponse = sendMetadataRequest(new MetadataRequest.Builder(Collections.emptyList(), true, (short) 4).build());
        assertTrue(metadataResponse.errors().isEmpty(), "Response should have no errors");
        assertTrue(metadataResponse.topicMetadata().isEmpty(), "Response should have no topics");
    }

    @ClusterTest
    public void testAutoTopicCreation() throws InterruptedException, IOException {
        String topic1 = "t1";
        String topic2 = "t2";
        String topic3 = "t3";
        String topic4 = "t4";
        String topic5 = "t5";
        clusterInstance.createTopic(topic1, 1, (short) 1);

        MetadataResponse response1 = sendMetadataRequest(new MetadataRequest.Builder(List.of(topic1, topic2), true).build());
        assertNull(response1.errors().get(topic1));
        checkAutoCreatedTopic(topic2, response1);

        // The default behavior in old versions of the metadata API is to allow topic creation, so
        // protocol downgrades should happen gracefully when auto-creation is explicitly requested.
        MetadataResponse response2 = sendMetadataRequest(new MetadataRequest.Builder(List.of(topic3), true).build((short) 1));
        checkAutoCreatedTopic(topic3, response2);

        // V3 doesn't support a configurable allowAutoTopicCreation, so disabling auto-creation is not supported
        assertThrows(UnsupportedVersionException.class, () -> sendMetadataRequest(new MetadataRequest.Builder(List.of(topic4), false, (short) 3).build()));

        // V4 and higher support a configurable allowAutoTopicCreation
        MetadataResponse response3 = sendMetadataRequest(new MetadataRequest.Builder(List.of(topic4, topic5), false, (short) 4).build());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response3.errors().get(topic4));
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response3.errors().get(topic5));
    }

    @ClusterTest(brokers = 3, serverProperties = {@ClusterConfigProperty(key = ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, value = "3")})
    public void testAutoCreateTopicWithInvalidReplicationFactor() throws IOException {
        // Shutdown all but one broker so that the number of brokers is less than the default replication factor
        for (int i = 1; i < brokers().size(); i++) {
            KafkaBroker broker = brokers().get(i);
            broker.shutdown();
            broker.awaitShutdown();
        }

        String topic1 = "testAutoCreateTopic";
        MetadataResponse response1 = sendMetadataRequest(new MetadataRequest.Builder(List.of(topic1), true).build());
        assertEquals(1, response1.topicMetadata().size());
        MetadataResponse.TopicMetadata topicMetadata = response1.topicMetadata().stream().toList().get(0);
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicMetadata.error());
        assertEquals(topic1, topicMetadata.topic());
        assertEquals(0, topicMetadata.partitionMetadata().size());
    }

    @ClusterTest
    public void testAllTopicsRequest() throws InterruptedException, IOException {
        // create some topics
        clusterInstance.createTopic("t1", 3, (short) 2);
        clusterInstance.createTopic("t2", 3, (short) 2);

        // v0, Empty list represents all topics
        MetadataResponse metadataResponseV0 = sendMetadataRequest(new MetadataRequest(requestData(Collections.emptyList(), true), (short) 0));
        assertTrue(metadataResponseV0.errors().isEmpty(), "V0 Response should have no errors");
        assertEquals(2, metadataResponseV0.topicMetadata().size(), "V0 Response should have 2 (all) topics");

        // v1, Null represents all topics
        MetadataResponse metadataResponseV1 = sendMetadataRequest(MetadataRequest.Builder.allTopics().build((short) 1));
        assertTrue(metadataResponseV1.errors().isEmpty(), "V1 Response should have no errors");
        assertEquals(2, metadataResponseV1.topicMetadata().size(), "V1 Response should have 2 (all) topics");
    }

    @ClusterTest
    public void testTopicIdsInResponse() throws ExecutionException, InterruptedException, IOException {
        Map<Integer, List<Integer>> replicaAssignment = Map.of(0, List.of(1, 2, 0), 1, List.of(2, 0, 1));
        String topic1 = "topic1";
        String topic2 = "topic2";
        clusterInstance.createTopicWithAssignment(topic1, replicaAssignment);
        clusterInstance.createTopicWithAssignment(topic2, replicaAssignment);

        // if version < 9, return ZERO_UUID in MetadataResponse
        MetadataResponse resp1 = sendMetadataRequest(new MetadataRequest.Builder(List.of(topic1, topic2), true, (short) 0, (short) 9).build());
        assertEquals(2, resp1.topicMetadata().size());
        resp1.topicMetadata().forEach(topicMetadata -> {
            assertEquals(Errors.NONE, topicMetadata.error());
            assertEquals(Uuid.ZERO_UUID, topicMetadata.topicId());
        });

        // from version 10, UUID will be included in MetadataResponse
        MetadataResponse resp2 = sendMetadataRequest(new MetadataRequest.Builder(List.of(topic1, topic2), true, (short) 10, (short) 10).build());
        assertEquals(2, resp2.topicMetadata().size());
        resp2.topicMetadata().forEach(topicMetadata -> {
            assertEquals(Errors.NONE, topicMetadata.error());
            assertNotEquals(Uuid.ZERO_UUID, topicMetadata.topicId());
            assertNotNull(topicMetadata.topicId());
        });
    }

    /**
     * Preferred replica should be the first item in the replicas list
     */
    @ClusterTest
    public void testPreferredReplica() throws ExecutionException, InterruptedException, IOException {
        Map<Integer, List<Integer>> replicaAssignment = Map.of(0, List.of(1, 2, 0), 1, List.of(2, 0, 1));
        clusterInstance.createTopicWithAssignment("t1", replicaAssignment);
        // Test metadata on two different brokers to ensure that metadata propagation works correctly
        List<MetadataResponse> responses = List.of(
            sendMetadataRequest(new MetadataRequest.Builder(List.of("t1"), true).build(), brokers().get(0).socketServer()),
            sendMetadataRequest(new MetadataRequest.Builder(List.of("t1"), true).build(), brokers().get(1).socketServer())
        );
        responses.forEach(response -> {
            assertEquals(1, response.topicMetadata().size());
            MetadataResponse.TopicMetadata topicMetadata = response.topicMetadata().iterator().next();
            assertEquals(Errors.NONE, topicMetadata.error());
            assertEquals("t1", topicMetadata.topic());
            assertEquals(Set.of(0, 1), topicMetadata.partitionMetadata().stream().map(MetadataResponse.PartitionMetadata::partition).collect(Collectors.toSet()));
            topicMetadata.partitionMetadata().forEach(partitionMetadata -> {
                List<Integer> assignment = replicaAssignment.get(partitionMetadata.partition());
                assertEquals(assignment, partitionMetadata.replicaIds);
                assertEquals(assignment, partitionMetadata.inSyncReplicaIds);
                assertEquals(Optional.of(assignment.get(0)), partitionMetadata.leaderId);
            });
        });
    }

    @ClusterTest
    public void testPartitionInfoPreferredReplica() throws ExecutionException, InterruptedException, IOException {
        Map<Integer, List<Integer>> replicaAssignment = Map.of(0, List.of(1, 2, 0));
        String topic = "testPartitionInfoPreferredReplicaTopic";
        clusterInstance.createTopicWithAssignment(topic, replicaAssignment);

        MetadataResponse response = sendMetadataRequest(new MetadataRequest.Builder(List.of(topic), true).build());
        Cluster snapshot = response.buildCluster();
        List<PartitionInfo> partitionInfos = snapshot.partitionsForTopic(topic);
        assertEquals(1, partitionInfos.size());

        PartitionInfo partitionInfo = partitionInfos.get(0);
        Integer preferredReplicaId = replicaAssignment.get(partitionInfo.partition()).get(0);
        assertEquals(preferredReplicaId, partitionInfo.replicas()[0].id());
    }

    @ClusterTest
    public void testReplicaDownResponse() throws InterruptedException, IOException {
        String replicaDownTopic = "replicaDown";
        short replicaCount = 3;

        // create a topic with 3 replicas
        clusterInstance.createTopic(replicaDownTopic, 1, replicaCount);

        // Kill a replica node that is not the leader
        MetadataResponse metadataResponse = sendMetadataRequest(new MetadataRequest.Builder(List.of(replicaDownTopic), true).build());
        MetadataResponse.PartitionMetadata partitionMetadata = metadataResponse.topicMetadata().iterator().next().partitionMetadata().get(0);
        KafkaBroker downNode = brokers().stream().filter(broker -> {
            int serverId = broker.dataPlaneRequestProcessor().brokerId();
            Optional<Integer> leaderId = partitionMetadata.leaderId;
            List<Integer> replicaIds = partitionMetadata.replicaIds;
            return leaderId.isPresent() && leaderId.get() != serverId && replicaIds.contains(serverId);
        }).findFirst().orElse(null);
        assertNotNull(downNode);
        downNode.shutdown();

        TestUtils.waitForCondition(() -> {
            MetadataResponse response = sendMetadataRequest(new MetadataRequest.Builder(List.of(replicaDownTopic), true).build());
            return response.brokers().stream().noneMatch(broker -> broker.id() == downNode.dataPlaneRequestProcessor().brokerId());
        }, 50000, "Replica was not found down");

        // Validate version 0 still filters unavailable replicas and contains error
        MetadataResponse v0MetadataResponse = sendMetadataRequest(new MetadataRequest(requestData(List.of(replicaDownTopic), true), (short) 0));
        List<Integer> v0BrokerIds = v0MetadataResponse.brokers().stream().map(Node::id).toList();
        assertTrue(v0MetadataResponse.errors().isEmpty(), "Response should have no errors");
        assertFalse(v0BrokerIds.contains(downNode.config().brokerId()), "The downed broker should not be in the brokers list");
        assertEquals(1, v0MetadataResponse.topicMetadata().size(), "Response should have one topic");
        MetadataResponse.PartitionMetadata v0PartitionMetadata = v0MetadataResponse.topicMetadata().iterator().next().partitionMetadata().get(0);
        assertEquals(Errors.REPLICA_NOT_AVAILABLE, v0PartitionMetadata.error, "PartitionMetadata should have an error");
        assertEquals(replicaCount - 1, v0PartitionMetadata.replicaIds.size(), "Response should have %d replicas".formatted(replicaCount - 1));

        // Validate version 1 returns unavailable replicas with no error
        MetadataResponse v1MetadataResponse = sendMetadataRequest(new MetadataRequest.Builder(List.of(replicaDownTopic), true).build((short) 1));
        List<Integer> v1BrokerIds = v1MetadataResponse.brokers().stream().map(Node::id).toList();
        assertTrue(v1MetadataResponse.errors().isEmpty(), "Response should have no errors");
        assertFalse(v1BrokerIds.contains(downNode.config().brokerId()), "The downed broker should not be in the brokers list");
        assertEquals(1, v1MetadataResponse.topicMetadata().size(), "Response should have one topic");
        MetadataResponse.PartitionMetadata v1PartitionMetadata = v1MetadataResponse.topicMetadata().iterator().next().partitionMetadata().get(0);
        assertEquals(Errors.NONE, v1PartitionMetadata.error, "PartitionMetadata should have no errors");
        assertEquals(replicaCount, v1PartitionMetadata.replicaIds.size(), "Response should have %d replicas".formatted(replicaCount));
    }

    private void checkIsr(List<KafkaBroker> brokers, String topic) {
        List<KafkaBroker> activeBrokers = brokers.stream().filter(broker -> broker.brokerState() != BrokerState.NOT_RUNNING).toList();
        Set<Integer> expectedIsr = activeBrokers.stream().map(broker -> broker.config().brokerId()).collect(Collectors.toSet());

        // Assert that topic metadata at new brokers is updated correctly
        activeBrokers.forEach(broker -> {
            try {
                TestUtils.waitForCondition(() -> {
                    MetadataResponse response = sendMetadataRequest(new MetadataRequest.Builder(List.of(topic), false).build(), broker.socketServer());
                    MetadataResponse.PartitionMetadata firstPartitionMetadata = response.topicMetadata().iterator().next().partitionMetadata().get(0);
                    Set<Integer> actualIsr = new HashSet<Integer>(firstPartitionMetadata.inSyncReplicaIds);
                    return actualIsr.equals(expectedIsr);
                }, "Topic metadata not updated correctly in broker %s\n".formatted(broker));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @ClusterTest
    public void testIsrAfterBrokerShutDownAndJoinsBack() throws InterruptedException {
        String topic = "isr-after-broker-shutdown";
        short replicaCount = 3;
        clusterInstance.createTopic(topic, 1, replicaCount);

        brokers().get(0).shutdown();
        brokers().get(0).awaitShutdown();
        brokers().get(0).startup();
        checkIsr(brokers(), topic);
    }

    private void checkMetadata(List<KafkaBroker> brokers, int expectedBrokersCount) throws InterruptedException, IOException {
        TestUtils.waitForCondition(() -> {
            MetadataResponse response = sendMetadataRequest(MetadataRequest.Builder.allTopics().build());
            return response.brokers().size() == expectedBrokersCount;
        }, "Expected " + expectedBrokersCount + " brokers");

        MetadataResponse balancedResponse = sendMetadataRequest(MetadataRequest.Builder.allTopics().build());
        Set<Integer> expectedBrokerIds = balancedResponse.brokers().stream().map(Node::id).collect(Collectors.toSet());

        // Assert that metadata is propagated correctly
        brokers.stream().filter(broker -> broker.brokerState() == BrokerState.RUNNING).forEach(broker -> {
            try {
                TestUtils.waitForCondition(() -> {
                    MetadataResponse response = sendMetadataRequest(MetadataRequest.Builder.allTopics().build());
                    Set<Integer> actualBrokerIds = response.brokers().stream().map(Node::id).collect(Collectors.toSet());
                    return actualBrokerIds.equals(expectedBrokerIds);
                }, "Topic metadata not updated correctly");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @ClusterTest
    public void testAliveBrokersWithNoTopics() throws IOException, InterruptedException {
        brokers().get(0).shutdown();
        brokers().get(0).awaitShutdown();
        checkMetadata(brokers(), brokers().size() - 1);

        brokers().get(0).startup();
        checkMetadata(brokers(), brokers().size());
    }
}