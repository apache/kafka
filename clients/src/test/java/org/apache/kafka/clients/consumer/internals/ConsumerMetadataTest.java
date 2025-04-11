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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ConsumerMetadataTest {

    private final Node node = new Node(1, "localhost", 9092);
    private final SubscriptionState subscription = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.EARLIEST);

    private final Time time = new MockTime();

    @Test
    public void testPatternSubscriptionNoInternalTopics() {
        testPatternSubscription(false);
    }

    @Test
    public void testPatternSubscriptionIncludeInternalTopics() {
        testPatternSubscription(true);
    }

    private void testPatternSubscription(boolean includeInternalTopics) {
        subscription.subscribe(Pattern.compile("__.*"), Optional.empty());
        ConsumerMetadata metadata = newConsumerMetadata(includeInternalTopics);

        MetadataRequest.Builder builder = metadata.newMetadataRequestBuilder();
        assertTrue(builder.isAllTopics());

        List<MetadataResponse.TopicMetadata> topics = new ArrayList<>();
        topics.add(topicMetadata("__consumer_offsets", true));
        topics.add(topicMetadata("__matching_topic", false));
        topics.add(topicMetadata("non_matching_topic", false));

        MetadataResponse response = RequestTestUtils.metadataResponse(singletonList(node),
            "clusterId", node.id(), topics);
        metadata.updateWithCurrentRequestVersion(response, false, time.milliseconds());

        if (includeInternalTopics)
            assertEquals(Set.of("__matching_topic", "__consumer_offsets"), metadata.fetch().topics());
        else
            assertEquals(Collections.singleton("__matching_topic"), metadata.fetch().topics());
    }

    @Test
    public void testUserAssignment() {
        subscription.assignFromUser(Set.of(
                new TopicPartition("foo", 0),
                new TopicPartition("bar", 0),
                new TopicPartition("__consumer_offsets", 0)));
        testBasicSubscription(Set.of("foo", "bar"), Set.of("__consumer_offsets"));

        subscription.assignFromUser(Set.of(
                new TopicPartition("baz", 0),
                new TopicPartition("__consumer_offsets", 0)));
        testBasicSubscription(Set.of("baz"), Set.of("__consumer_offsets"));
    }

    @Test
    public void testNormalSubscription() {
        subscription.subscribe(Set.of("foo", "bar", "__consumer_offsets"), Optional.empty());
        subscription.groupSubscribe(Set.of("baz", "foo", "bar", "__consumer_offsets"));
        testBasicSubscription(Set.of("foo", "bar", "baz"), Set.of("__consumer_offsets"));

        subscription.resetGroupSubscription();
        testBasicSubscription(Set.of("foo", "bar"), Set.of("__consumer_offsets"));
    }

    @Test
    public void testTransientTopics() {
        Map<String, Uuid> topicIds = new HashMap<>();
        topicIds.put("foo", Uuid.randomUuid());
        subscription.subscribe(singleton("foo"), Optional.empty());
        ConsumerMetadata metadata = newConsumerMetadata(false);
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds(1, singletonMap("foo", 1), topicIds), false, time.milliseconds());
        assertEquals(topicIds.get("foo"), metadata.topicIds().get("foo"));
        assertFalse(metadata.updateRequested());

        metadata.addTransientTopics(singleton("foo"));
        assertFalse(metadata.updateRequested());

        metadata.addTransientTopics(singleton("bar"));
        assertTrue(metadata.updateRequested());

        Map<String, Integer> topicPartitionCounts = new HashMap<>();
        topicPartitionCounts.put("foo", 1);
        topicPartitionCounts.put("bar", 1);
        topicIds.put("bar", Uuid.randomUuid());
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds(1, topicPartitionCounts, topicIds), false, time.milliseconds());
        Map<String, Uuid> metadataTopicIds = metadata.topicIds();
        topicIds.forEach((topicName, topicId) -> assertEquals(topicId, metadataTopicIds.get(topicName)));
        assertFalse(metadata.updateRequested());

        assertEquals(Set.of("foo", "bar"), new HashSet<>(metadata.fetch().topics()));

        metadata.clearTransientTopics();
        topicIds.remove("bar");
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds(1, topicPartitionCounts, topicIds), false, time.milliseconds());
        assertEquals(singleton("foo"), new HashSet<>(metadata.fetch().topics()));
        assertEquals(topicIds.get("foo"), metadata.topicIds().get("foo"));
        assertNull(topicIds.get("bar"));
    }

    private void testBasicSubscription(Set<String> expectedTopics, Set<String> expectedInternalTopics) {
        Set<String> allTopics = new HashSet<>();
        allTopics.addAll(expectedTopics);
        allTopics.addAll(expectedInternalTopics);

        ConsumerMetadata metadata = newConsumerMetadata(false);

        MetadataRequest.Builder builder = metadata.newMetadataRequestBuilder();
        assertEquals(allTopics, new HashSet<>(builder.topics()));

        List<MetadataResponse.TopicMetadata> topics = new ArrayList<>();
        for (String expectedTopic : expectedTopics)
            topics.add(topicMetadata(expectedTopic, false));
        for (String expectedInternalTopic : expectedInternalTopics)
            topics.add(topicMetadata(expectedInternalTopic, true));

        MetadataResponse response = RequestTestUtils.metadataResponse(singletonList(node),
            "clusterId", node.id(), topics);
        metadata.updateWithCurrentRequestVersion(response, false, time.milliseconds());

        assertEquals(allTopics, metadata.fetch().topics());
    }

    private MetadataResponse.TopicMetadata topicMetadata(String topic, boolean isInternal) {
        MetadataResponse.PartitionMetadata partitionMetadata = new MetadataResponse.PartitionMetadata(Errors.NONE,
                new TopicPartition(topic, 0), Optional.of(node.id()), Optional.of(5),
                singletonList(node.id()), singletonList(node.id()), singletonList(node.id()));
        return new MetadataResponse.TopicMetadata(Errors.NONE, topic, isInternal, singletonList(partitionMetadata));
    }

    private ConsumerMetadata newConsumerMetadata(boolean includeInternalTopics) {
        long refreshBackoffMs = 50;
        long expireMs = 50000;
        return new ConsumerMetadata(refreshBackoffMs, refreshBackoffMs, expireMs, includeInternalTopics, false,
                subscription, new LogContext(), new ClusterResourceListeners());
    }

    @Test
    public void testInvalidPartitionLeadershipUpdates() {
        Metadata metadata = initializeMetadata();
        List<Node> originalNodes = initializeNodes(metadata);
        ClusterResourceListener mockListener = initializeMockListener(metadata);

        // Ensure invalid updates get ignored
        Map<TopicPartition, Metadata.LeaderIdAndEpoch> invalidUpdates = new HashMap<>();
        // incomplete information
        invalidUpdates.put(new TopicPartition("topic1", 999), new Metadata.LeaderIdAndEpoch(Optional.empty(), Optional.empty()));
        // non-existing leader ID
        invalidUpdates.put(new TopicPartition("topic2", 0), new Metadata.LeaderIdAndEpoch(Optional.of(99999), Optional.of(99999)));
        // non-existing topicPartition
        invalidUpdates.put(new TopicPartition("topic_missing_from_existing_metadata", 1), new Metadata.LeaderIdAndEpoch(Optional.of(0), Optional.of(99999)));
        // stale epoch
        invalidUpdates.put(new TopicPartition("topic1", 0), new Metadata.LeaderIdAndEpoch(Optional.of(1), Optional.of(99)));

        Set<TopicPartition> updatedTps = metadata.updatePartitionLeadership(invalidUpdates, originalNodes);
        assertTrue(updatedTps.isEmpty(), "Invalid updates should be ignored");

        Cluster updatedCluster = metadata.fetch();
        assertEquals(new HashSet<>(originalNodes), new HashSet<>(updatedCluster.nodes()));
        verify(mockListener, never()).onUpdate(any());
        validateForUpdatePartitionLeadership(metadata,
                metadataSupplier(Errors.NONE, new TopicPartition("topic1", 0), Optional.of(1), Optional.of(100), Arrays.asList(1, 2), Arrays.asList(1, 2), Collections.singletonList(3)),
                metadataSupplier(Errors.NONE, new TopicPartition("topic2", 0), Optional.of(2), Optional.of(200), Arrays.asList(2, 3), Arrays.asList(2, 3), Collections.singletonList(1)),
                metadataSupplier(Errors.NONE, new TopicPartition("topic1", 1), Optional.of(2), Optional.of(200), Arrays.asList(2, 3), Arrays.asList(2, 3), Collections.singletonList(1)),
                metadataSupplier(Errors.NONE, new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0), Optional.of(2), Optional.of(300), Arrays.asList(2, 3), Arrays.asList(2, 3), Collections.singletonList(1)),
                originalNodes,
                "kafka-cluster",
                Collections.singleton("topic4"),
                Collections.singleton("topic3"),
                Collections.singleton(Topic.GROUP_METADATA_TOPIC_NAME),
                updatedCluster.controller(),
                metadata.topicIds());
    }


    @Test
    public void testValidPartitionLeadershipUpdate() {
        Metadata metadata = initializeMetadata();
        List<Node> originalNodes = initializeNodes(metadata);
        ClusterResourceListener mockListener = initializeMockListener(metadata);

        // Ensure valid update to tp11 is applied
        Map<TopicPartition, Metadata.LeaderIdAndEpoch> validUpdates = new HashMap<>();
        TopicPartition tp11 = new TopicPartition("topic1", 0);

        Integer newLeaderId = 2; // New leader ID
        Integer newLeaderEpoch = 101; // New leader epoch that is newer than existing

        validUpdates.put(tp11, new Metadata.LeaderIdAndEpoch(Optional.of(newLeaderId), Optional.of(newLeaderEpoch)));

        Set<TopicPartition> updatedTps = metadata.updatePartitionLeadership(validUpdates, originalNodes);

        assertEquals(1, updatedTps.size());
        assertEquals(tp11, updatedTps.iterator().next(), "tp11 should be updated");

        Cluster updatedCluster = metadata.fetch();
        assertEquals(new HashSet<>(originalNodes), new HashSet<>(updatedCluster.nodes()));
        verify(mockListener, times(1)).onUpdate(any());
        validateForUpdatePartitionLeadership(metadata,
                new MetadataResponse.PartitionMetadata(Errors.NONE, tp11, Optional.of(newLeaderId), Optional.of(newLeaderEpoch), Arrays.asList(1, 2), Arrays.asList(1, 2), Collections.singletonList(3)),
                metadataSupplier(Errors.NONE, new TopicPartition("topic2", 0), Optional.of(2), Optional.of(200), Arrays.asList(2, 3), Arrays.asList(2, 3), Collections.singletonList(1)),
                metadataSupplier(Errors.NONE, new TopicPartition("topic1", 1), Optional.of(2), Optional.of(200), Arrays.asList(2, 3), Arrays.asList(2, 3), Collections.singletonList(1)),
                metadataSupplier(Errors.NONE, new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0), Optional.of(2), Optional.of(300), Arrays.asList(2, 3), Arrays.asList(2, 3), Collections.singletonList(1)),
                originalNodes,
                "kafka-cluster",
                Collections.singleton("topic4"),
                Collections.singleton("topic3"),
                Collections.singleton(Topic.GROUP_METADATA_TOPIC_NAME),
                updatedCluster.controller(),
                metadata.topicIds());
    }

    private Metadata initializeMetadata() {
        Metadata metadata = new Metadata(100, 1000, 1000, new LogContext(), new ClusterResourceListeners());

        String topic1 = "topic1";
        String topic2 = "topic2";
        String topic3 = "topic3";
        String topic4 = "topic4";
        String clusterId = "kafka-cluster";
        Uuid topic1Id = Uuid.randomUuid();
        Uuid topic2Id = Uuid.randomUuid();
        Uuid internalTopicId = Uuid.randomUuid();

        Map<String, Uuid> topicIds = new HashMap<>();
        topicIds.put(topic1, topic1Id);
        topicIds.put(topic2, topic2Id);
        topicIds.put(Topic.GROUP_METADATA_TOPIC_NAME, internalTopicId);

        Map<String, Errors> errorCounts = new HashMap<>();
        errorCounts.put(topic3, Errors.INVALID_TOPIC_EXCEPTION);
        errorCounts.put(topic4, Errors.TOPIC_AUTHORIZATION_FAILED);

        Map<String, Integer> topicPartitionCounts = new HashMap<>();
        topicPartitionCounts.put(topic1, 2);
        topicPartitionCounts.put(topic2, 1);
        topicPartitionCounts.put(Topic.GROUP_METADATA_TOPIC_NAME, 1);

        metadata.requestUpdate(true);
        Metadata.MetadataRequestAndVersion versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds());
        metadata.update(versionAndBuilder.requestVersion,
                RequestTestUtils.metadataUpdateWith(clusterId,
                        5,
                        errorCounts,
                        topicPartitionCounts,
                        tp -> null,
                        this::metadataSupplier,
                        ApiKeys.METADATA.latestVersion(),
                        topicIds),
                false,
                time.milliseconds());

        return metadata;
    }

    private List<Node> initializeNodes(Metadata metadata) {
        return new ArrayList<>(metadata.fetch().nodes());
    }

    private ClusterResourceListener initializeMockListener(Metadata metadata) {
        ClusterResourceListener mockListener = Mockito.mock(ClusterResourceListener.class);
        metadata.addClusterUpdateListener(mockListener);
        return mockListener;
    }

    private MetadataResponse.PartitionMetadata metadataSupplier(Errors error, TopicPartition partition, Optional<Integer> leaderId, Optional<Integer> leaderEpoch, List<Integer> replicas, List<Integer> isr, List<Integer> offlineReplicas) {
        if ("topic1".equals(partition.topic()) && partition.partition() == 0)
            return new MetadataResponse.PartitionMetadata(Errors.NONE, partition, Optional.of(1), Optional.of(100), Arrays.asList(1, 2), Arrays.asList(1, 2), Collections.singletonList(3));
        else if ("topic1".equals(partition.topic()) && partition.partition() == 1)
            return new MetadataResponse.PartitionMetadata(Errors.NONE, partition, Optional.of(2), Optional.of(200), Arrays.asList(2, 3), Arrays.asList(2, 3), Collections.singletonList(1));
        else if ("topic2".equals(partition.topic()) && partition.partition() == 0)
            return new MetadataResponse.PartitionMetadata(Errors.NONE, partition, Optional.of(2), Optional.of(200), Arrays.asList(2, 3), Arrays.asList(2, 3), Collections.singletonList(1));
        else if (Topic.GROUP_METADATA_TOPIC_NAME.equals(partition.topic()) && partition.partition() == 0)
            return new MetadataResponse.PartitionMetadata(Errors.NONE, partition, Optional.of(2), Optional.of(300), Arrays.asList(2, 3), Arrays.asList(2, 3), Collections.singletonList(1));
        else throw new RuntimeException("Unexpected partition " + partition);
    }

    private void validateForUpdatePartitionLeadership(Metadata updatedMetadata,
                                                      MetadataResponse.PartitionMetadata part1Metadata,
                                                      MetadataResponse.PartitionMetadata part2Metadata,
                                                      MetadataResponse.PartitionMetadata part12Metadata,
                                                      MetadataResponse.PartitionMetadata internalPartMetadata,
                                                      List<Node> expectedNodes,
                                                      String expectedClusterId,
                                                      Set<String> expectedUnauthorisedTopics,
                                                      Set<String> expectedInvalidTopics,
                                                      Set<String> expectedInternalTopics,
                                                      Node expectedController,
                                                      Map<String, Uuid> expectedTopicIds) {
        Cluster updatedCluster = updatedMetadata.fetch();
        assertEquals(updatedCluster.clusterResource().clusterId(), expectedClusterId);
        assertEquals(new HashSet<>(expectedNodes), new HashSet<>(updatedCluster.nodes()));
        assertEquals(3, updatedCluster.topics().size());
        assertEquals(expectedInternalTopics, updatedCluster.internalTopics());
        assertEquals(expectedInvalidTopics, updatedCluster.invalidTopics());
        assertEquals(expectedUnauthorisedTopics, updatedCluster.unauthorizedTopics());
        assertEquals(expectedController, updatedCluster.controller());
        assertEquals(expectedTopicIds, updatedMetadata.topicIds());

        Map<Integer, Node> nodeMap = expectedNodes.stream().collect(Collectors.toMap(Node::id, Function.identity()));
        for (MetadataResponse.PartitionMetadata partitionMetadata : Arrays.asList(part1Metadata, part2Metadata, part12Metadata, internalPartMetadata)) {
            TopicPartition tp = new TopicPartition(partitionMetadata.topic(), partitionMetadata.partition());

            Metadata.LeaderAndEpoch expectedLeaderInfo = new Metadata.LeaderAndEpoch(Optional.of(nodeMap.get(partitionMetadata.leaderId.get())), partitionMetadata.leaderEpoch);
            assertEquals(expectedLeaderInfo, updatedMetadata.currentLeader(tp));

            Optional<MetadataResponse.PartitionMetadata> optionalUpdatedMetadata = updatedMetadata.partitionMetadataIfCurrent(tp);
            assertTrue(optionalUpdatedMetadata.isPresent());
            MetadataResponse.PartitionMetadata updatedPartMetadata = optionalUpdatedMetadata.get();
            assertEquals(partitionMetadata.topicPartition, updatedPartMetadata.topicPartition);
            assertEquals(partitionMetadata.error, updatedPartMetadata.error);
            assertEquals(partitionMetadata.leaderId, updatedPartMetadata.leaderId);
            assertEquals(partitionMetadata.leaderEpoch, updatedPartMetadata.leaderEpoch);
            assertEquals(partitionMetadata.replicaIds, updatedPartMetadata.replicaIds);
            assertEquals(partitionMetadata.inSyncReplicaIds, updatedPartMetadata.inSyncReplicaIds);
            assertEquals(partitionMetadata.offlineReplicaIds, partitionMetadata.offlineReplicaIds);
        }
    }
}
