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
package org.apache.kafka.server.requests;

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.apache.kafka.server.IntegrationTestUtils;
import org.apache.kafka.server.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.server.TestUtils.consumeRecords;
import static org.apache.kafka.server.config.ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.BROKER_RACK_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.NUM_PARTITIONS_CONFIG;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = FetchRequestTest.BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(id = 0, key = BROKER_RACK_CONFIG, value = "0"),
        @ClusterConfigProperty(id = 1, key = BROKER_RACK_CONFIG, value = "1"),
        @ClusterConfigProperty(key = REPLICA_SELECTOR_CLASS_CONFIG,
            value = "org.apache.kafka.common.replica.RackAwareReplicaSelector"),
        @ClusterConfigProperty(key = CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = NUM_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "2"),
    }
)
public class FetchRequestTest {

    public static final int BROKER_COUNT = 2;
    public static final String TOPIC = "test-fetch-from-follower";
    public static final int LEADER_BROKER_ID = 0;
    public static final int FOLLOWER_BROKER_ID = 1;

    @ClusterTest
    public void testFollowerCompleteDelayedFetchesOnReplication(ClusterInstance cluster) throws Exception {
        cluster.createTopicWithAssignment(TOPIC, Map.of(0, List.of(LEADER_BROKER_ID, FOLLOWER_BROKER_ID)));
        int leaderId = waitUntilLeaderIsKnown(cluster.brokers().values(), new TopicPartition(TOPIC, 0));

        var topicPartition = new TopicPartition(TOPIC, 0);
        assertEquals(LEADER_BROKER_ID, leaderId);

        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetchData = createPartitionMap(cluster, 1000, List.of(topicPartition), Map.of(topicPartition, 0L));
        FetchRequest fetchRequest = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion(), 20000, 1, fetchData)
                .setMaxBytes(1000)
                .rackId("")
                .build();

        int leaderPort = cluster.brokers().get(LEADER_BROKER_ID).boundPort(cluster.clientListener());
        int followerPort = cluster.brokers().get(FOLLOWER_BROKER_ID).boundPort(cluster.clientListener());
        byte[] serializedFetchRequest = Utils.toArray(fetchRequest.serializeWithHeader(
                IntegrationTestUtils.nextRequestHeader(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion())));
        try (Socket leaderSocket = IntegrationTestUtils.connect(leaderPort);
             Socket followerSocket = IntegrationTestUtils.connect(followerPort)) {
            IntegrationTestUtils.sendRequest(leaderSocket, serializedFetchRequest);
            IntegrationTestUtils.sendRequest(followerSocket, serializedFetchRequest);

            try (Producer<byte[], byte[]> producer = cluster.producer(Map.of())) {
                producer.send(new ProducerRecord<>(TOPIC, "key".getBytes(), "value".getBytes())).get();
            }

            FetchResponse leaderResponse = IntegrationTestUtils.receive(leaderSocket, ApiKeys.FETCH, ApiKeys.FETCH.latestVersion());
            assertEquals(Errors.NONE, leaderResponse.error());
            assertEquals(Map.of(Errors.NONE, 2), leaderResponse.errorCounts());

            FetchResponse followerResponse = IntegrationTestUtils.receive(followerSocket, ApiKeys.FETCH, ApiKeys.FETCH.latestVersion());
            assertEquals(Errors.NONE, followerResponse.error());
            assertEquals(Map.of(Errors.NONE, 2), followerResponse.errorCounts());
        }
    }

    @ClusterTest
    public void testFetchFromLeaderWhilePreferredReadReplicaIsUnavailable(ClusterInstance cluster) throws Exception {
        cluster.createTopicWithAssignment(TOPIC, Map.of(0, List.of(LEADER_BROKER_ID, FOLLOWER_BROKER_ID)));
        waitUntilLeaderIsKnown(cluster.brokers().values(), new TopicPartition(TOPIC, 0));

        produceMessages(cluster, 10);
        assertEquals(1, getPreferredReplica(cluster));

        cluster.brokers().get(FOLLOWER_BROKER_ID).shutdown();
        TopicPartition topicPartition = new TopicPartition(TOPIC, 0);

        waitForCondition(
                () -> {
                    KafkaBroker leaderBroker = cluster.brokers().get(LEADER_BROKER_ID);
                    Map<Integer, Node> endpoints = leaderBroker.metadataCache()
                            .getPartitionReplicaEndpoints(topicPartition, cluster.clientListener());
                    return !endpoints.containsKey(FOLLOWER_BROKER_ID);
                },
                "follower is still reachable."
        );

        assertEquals(-1, getPreferredReplica(cluster));
    }

    @ClusterTest
    public void testFetchFromFollowerWithRoll(ClusterInstance cluster) throws Exception {
        cluster.createTopicWithAssignment(TOPIC, Map.of(0, List.of(LEADER_BROKER_ID, FOLLOWER_BROKER_ID)));
        waitUntilLeaderIsKnown(cluster.brokers().values(), new TopicPartition(TOPIC, 0));

        var followerConsumerProps = new HashMap<String, Object>();
        followerConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        followerConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-follower");
        followerConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        followerConsumerProps.put(ConsumerConfig.CLIENT_RACK_CONFIG, String.valueOf(FOLLOWER_BROKER_ID));

        var leaderConsumerProps = new HashMap<String, Object>();
        leaderConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        leaderConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-leader");
        leaderConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (Consumer<byte[], byte[]> followerConsumer = cluster.consumer(followerConsumerProps);
             Consumer<byte[], byte[]> leaderConsumer = cluster.consumer(leaderConsumerProps)) {
            followerConsumer.subscribe(List.of(TOPIC));
            leaderConsumer.subscribe(List.of(TOPIC));

            // Wait until preferred replica is set to follower.
            waitForCondition(
                    () -> getPreferredReplica(cluster) == FOLLOWER_BROKER_ID,
                    "Preferred replica is not set to follower"
            );

            // Produce and consume from both consumers.
            produceMessages(cluster, 1);
            consumeRecords(followerConsumer, 1);
            consumeRecords(leaderConsumer, 1);

            // Shutdown follower, produce and consume should work for both consumers.
            cluster.shutdownBroker(FOLLOWER_BROKER_ID);
            produceMessages(cluster, 1);
            consumeRecords(followerConsumer, 1);
            consumeRecords(leaderConsumer, 1);

            // Start the follower and wait until preferred replica is set to follower.
            cluster.startBroker(FOLLOWER_BROKER_ID);
            waitForCondition(
                    () -> getPreferredReplica(cluster) == FOLLOWER_BROKER_ID,
                    "Preferred replica is not set to follower after restart"
            );

            // Produce and consume should still work for both consumers.
            produceMessages(cluster, 1);
            consumeRecords(followerConsumer, 1);
            consumeRecords(leaderConsumer, 1);
        }
    }

    @ClusterTest
    public void testRackAwareRangeAssignor(ClusterInstance cluster) throws Exception {
        List<Integer> partitionList = cluster.brokers().keySet().stream()
                .sorted()
                .toList();

        String topicWithAllPartitionsOnAllRacks = "topicWithAllPartitionsOnAllRacks";
        cluster.createTopic(topicWithAllPartitionsOnAllRacks,
                cluster.brokers().size(),
                (short) cluster.brokers().size());

        // Racks are in order of broker ids, assign leaders in reverse order
        String topicWithSingleRackPartitions = "topicWithSingleRackPartitions";
        var replicaAssignment = new LinkedHashMap<Integer, List<Integer>>();
        for (int i : partitionList) {
            replicaAssignment.put(i, List.of(cluster.brokers().size() - i - 1));
        }
        cluster.createTopicWithAssignment(topicWithSingleRackPartitions, replicaAssignment);

        List<HashMap<String, Object>> consumerConfigs = cluster.brokers().keySet().stream()
                .sorted()
                .map(brokerId -> {
                    var config = new HashMap<String, Object>();
                    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
                    config.put(ConsumerConfig.GROUP_ID_CONFIG, "rack-aware-group");
                    config.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
                    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                    config.put(ConsumerConfig.CLIENT_RACK_CONFIG, String.valueOf(cluster.brokers().get(brokerId).config().rack().orElse(null)));
                    config.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance-" + brokerId);
                    config.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
                    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                    return config;
                })
                .toList();

        List<Consumer<byte[], byte[]>> consumers = consumerConfigs.stream()
                .map(cluster::<byte[], byte[]>consumer)
                .toList();

        try (Producer<byte[], byte[]> producer = cluster.producer(Map.of())) {
            ExecutorService executor = Executors.newFixedThreadPool(consumers.size());
            try {
                // Rack-based assignment results in partitions assigned in reverse order since partition racks are in the reverse order.
                verifyRackAwareAssignments(executor, consumers, producer, partitionList, topicWithSingleRackPartitions, reverseList(partitionList), List.of());
                // Non-rack-aware assignment results in ordered partitions.
                verifyRackAwareAssignments(executor, consumers, producer, partitionList, topicWithAllPartitionsOnAllRacks, partitionList, List.of());
                // Rack-aware assignment with co-partitioning results in reverse assignment for both topics.
                verifyRackAwareAssignments(executor, consumers, producer, partitionList, topicWithAllPartitionsOnAllRacks, reverseList(partitionList), List.of(topicWithSingleRackPartitions));

                // Perform reassignment for topicWithSingleRackPartitions to reverse the replica racks and
                // verify that change in replica racks results in re-assignment based on new racks.
                try (Admin admin = cluster.admin()) {
                    var reassignments = new HashMap<TopicPartition, Optional<NewPartitionReassignment>>();
                    for (int p : partitionList) {
                        var newAssignment = new NewPartitionReassignment(List.of(p));
                        reassignments.put(new TopicPartition(topicWithSingleRackPartitions, p), Optional.of(newAssignment));
                    }
                    admin.alterPartitionReassignments(reassignments).all().get(30, TimeUnit.SECONDS);
                }

                verifyRackAwareAssignments(executor, consumers, producer, partitionList, topicWithAllPartitionsOnAllRacks, partitionList, List.of(topicWithSingleRackPartitions));

            } finally {
                executor.shutdownNow();
                consumers.forEach(Consumer::close);
            }
        }
    }

    private Map<String, Uuid> getTopicIds(ClusterInstance cluster) throws Exception {
        Map<String, Uuid> topicIds = new HashMap<>();
        try (Admin admin = cluster.admin()) {
            Map<String, TopicDescription> descriptions = admin.describeTopics(List.of(TOPIC))
                    .allTopicNames()
                    .get();
            descriptions.forEach((name, desc) -> topicIds.put(name, desc.topicId()));
            return topicIds;
        }
    }

    private LinkedHashMap<TopicPartition, FetchRequest.PartitionData> createPartitionMap(
            ClusterInstance cluster,
            int maxPartitionBytes,
            List<TopicPartition> topicPartitions,
            Map<TopicPartition, Long> offsetMap) throws Exception {

        Map<String, Uuid> topicIds = getTopicIds(cluster);
        var partitionMap = new LinkedHashMap<TopicPartition, FetchRequest.PartitionData>();
        for (TopicPartition tp : topicPartitions) {
            Uuid topicId = topicIds.getOrDefault(tp.topic(), Uuid.ZERO_UUID);
            long fetchOffset = offsetMap.getOrDefault(tp, 0L);
            partitionMap.put(tp, new FetchRequest.PartitionData(
                    topicId,
                    fetchOffset,
                    0L,
                    maxPartitionBytes,
                    Optional.empty()
            ));
        }
        return partitionMap;
    }

    private int getPreferredReplica(ClusterInstance cluster) throws Exception {
        var topicPartition = new TopicPartition(TOPIC, 0);
        Map<TopicPartition, Long> offsetMap = Map.of(topicPartition, 0L);
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetchData = createPartitionMap(cluster, 1000, List.of(topicPartition), offsetMap);
        FetchRequest fetchRequest = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion(), 500, 1, fetchData)
                .setMaxBytes(1000)
                .rackId(String.valueOf(FOLLOWER_BROKER_ID))
                .build();

        int leaderPort = cluster.brokers().get(LEADER_BROKER_ID).boundPort(cluster.clientListener());
        FetchResponse response = IntegrationTestUtils.connectAndReceive(fetchRequest, leaderPort);
        assertEquals(Errors.NONE, response.error());
        assertEquals(Map.of(Errors.NONE, 2), response.errorCounts());
        assertEquals(1, response.data().responses().size());
        FetchResponseData.FetchableTopicResponse topicResponse = response.data().responses().get(0);
        assertEquals(1, topicResponse.partitions().size());
        return topicResponse.partitions().get(0).preferredReadReplica();
    }

    private void produceMessages(ClusterInstance cluster, int numMessages) throws Exception {
        try (Producer<byte[], byte[]> producer = cluster.producer(Map.of())) {
            for (int i = 0; i < numMessages; i++) {
                producer.send(new ProducerRecord<>(TOPIC, ("key-" + i).getBytes(), ("value-" + i).getBytes())).get();
            }
        }
    }

    private void verifyRackAwareAssignments(
            ExecutorService executor,
            List<Consumer<byte[], byte[]>> consumers,
            Producer<byte[], byte[]> producer,
            List<Integer> partitionList,
            String topic,
            List<Integer> expectedPartitionOrder,
            List<String> additionalTopics) throws Exception {

        List<String> topics = new ArrayList<>();
        topics.add(topic);
        topics.addAll(additionalTopics);

        consumers.forEach(consumer -> consumer.subscribe(topics));

        awaitConsumerAssignments(executor, consumers, topics, expectedPartitionOrder);

        for (int p : partitionList) {
            for (String t : topics) {
                producer.send(new ProducerRecord<>(t, p, ("key-" + t + "-" + p).getBytes(), ("value-" + t + "-" + p).getBytes())).get();
            }
        }

        List<Future<Void>> recordFutures = new ArrayList<>();
        for (Consumer<byte[], byte[]> consumer : consumers) {
            recordFutures.add(executor.submit(() -> {
                try {
                    consumeRecords(consumer, topics.size());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            }));
        }

        for (Future<Void> future : recordFutures) {
            future.get(30, TimeUnit.SECONDS);
        }

        for (Consumer<byte[], byte[]> consumer : consumers) {
            consumer.commitSync();
        }
    }

    private void awaitConsumerAssignments(
            ExecutorService executor,
            List<Consumer<byte[], byte[]>> consumers,
            List<String> topics,
            List<Integer> expectedPartitionOrder) throws Exception {

        List<Future<Void>> assignmentFutures = new ArrayList<>();
        for (int i = 0; i < consumers.size(); i++) {
            final int consumerIndex = i;
            int partition = expectedPartitionOrder.get(i);
            Set<TopicPartition> expectedAssignment = new HashSet<>();
            for (String t : topics) {
                expectedAssignment.add(new TopicPartition(t, partition));
            }

            Consumer<byte[], byte[]> consumer = consumers.get(i);
            assignmentFutures.add(executor.submit(() -> {
                try {
                    waitForCondition(
                            () -> {
                                consumer.poll(Duration.ofMillis(100));
                                return consumer.assignment().equals(expectedAssignment);
                            },
                            "Timed out while awaiting expected assignment for consumer " + consumerIndex
                    );
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            }));
        }

        for (Future<Void> future : assignmentFutures) {
            future.get(30, TimeUnit.SECONDS);
        }
    }

    private static <T> List<T> reverseList(List<T> list) {
        var reversed = new ArrayList<>(list);
        Collections.reverse(reversed);
        return reversed;
    }

    private static Stream<Arguments> fetchVersions() {
        return ApiKeys.FETCH.allVersions().stream().map(Arguments::of);
    }

    private static int waitUntilLeaderIsKnown(
            Collection<KafkaBroker> brokers,
            TopicPartition tp) throws InterruptedException {
        return TestUtils.awaitLeaderChange(brokers, tp, Optional.empty(), Optional.empty(), DEFAULT_MAX_WAIT_MS);
    }

    @ParameterizedTest
    @MethodSource("fetchVersions")
    public void testToReplaceWithDifferentVersions(short version) {
        boolean fetchRequestUsesTopicIds = version >= 13;
        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition tp = new TopicIdPartition(topicId, 0, "topic");

        Map<TopicPartition, FetchRequest.PartitionData> partitionData = Map.of(tp.topicPartition(),
                new FetchRequest.PartitionData(topicId, 0, 0, 0, Optional.empty()));
        List<TopicIdPartition> toReplace = List.of(tp);

        FetchRequest fetchRequest = FetchRequest.Builder
                .forReplica(version, 0, 1, 1, 1, partitionData)
                .removed(List.of())
                .replaced(toReplace)
                .metadata(FetchMetadata.newIncremental(123)).build(version);

        // If version < 13, we should not see any partitions in forgottenTopics. This is because we can not
        // distinguish different topic IDs on versions earlier than 13.
        assertEquals(fetchRequestUsesTopicIds, !fetchRequest.data().forgottenTopicsData().isEmpty());
        fetchRequest.data().forgottenTopicsData().forEach(forgottenTopic -> {
            // Since we didn't serialize, we should see the topic name and ID regardless of the version.
            assertEquals(tp.topic(), forgottenTopic.topic());
            assertEquals(topicId, forgottenTopic.topicId());
        });

        assertEquals(1, fetchRequest.data().topics().size());
        fetchRequest.data().topics().forEach(topic -> {
            // Since we didn't serialize, we should see the topic name and ID regardless of the version.
            assertEquals(tp.topic(), topic.topic());
            assertEquals(topicId, topic.topicId());
        });
    }

    @ParameterizedTest
    @MethodSource("fetchVersions")
    public void testFetchData(short version) {
        TopicPartition topicPartition0 = new TopicPartition("topic", 0);
        TopicPartition topicPartition1 = new TopicPartition("unknownIdTopic", 0);
        Uuid topicId0 = Uuid.randomUuid();
        Uuid topicId1 = Uuid.randomUuid();

        // Only include topic IDs for the first topic partition.
        Map<Uuid, String> topicNames = Map.of(topicId0, topicPartition0.topic());
        List<TopicIdPartition> topicIdPartitions = new LinkedList<>();
        topicIdPartitions.add(new TopicIdPartition(topicId0, topicPartition0));
        topicIdPartitions.add(new TopicIdPartition(topicId1, topicPartition1));

        // Include one topic with topic IDs in the topic names map and one without.
        Map<TopicPartition, FetchRequest.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(topicPartition0, new FetchRequest.PartitionData(topicId0, 0, 0, 0, Optional.empty()));
        partitionData.put(topicPartition1, new FetchRequest.PartitionData(topicId1, 0, 0, 0, Optional.empty()));
        boolean fetchRequestUsesTopicIds = version >= 13;

        FetchRequest fetchRequest = FetchRequest.parse(FetchRequest.Builder
                .forReplica(version, 0, 1, 1, 1, partitionData)
                .removed(List.of())
                .replaced(List.of())
                .metadata(FetchMetadata.newIncremental(123)).build(version).serialize(), version);

        if (version >= 15) {
            assertEquals(1, fetchRequest.data().replicaState().replicaEpoch());
        }
        // For versions < 13, we will be provided a topic name and a zero UUID in FetchRequestData.
        // Versions 13+ will contain a valid topic ID but an empty topic name.
        List<TopicIdPartition> expectedData = new LinkedList<>();
        topicIdPartitions.forEach(tidp -> {
            String expectedName = fetchRequestUsesTopicIds ? "" : tidp.topic();
            Uuid expectedTopicId = fetchRequestUsesTopicIds ? tidp.topicId() : Uuid.ZERO_UUID;
            expectedData.add(new TopicIdPartition(expectedTopicId, tidp.partition(), expectedName));
        });

        // Build the list of TopicIdPartitions based on the FetchRequestData that was serialized and parsed.
        List<TopicIdPartition> convertedFetchData = new LinkedList<>();
        fetchRequest.data().topics().forEach(topic ->
                topic.partitions().forEach(partition ->
                        convertedFetchData.add(new TopicIdPartition(topic.topicId(), partition.partition(), topic.topic()))
                )
        );
        // The TopicIdPartitions built from the request data should match what we expect.
        assertEquals(expectedData, convertedFetchData);

        // For fetch request version 13+ we expect topic names to be filled in for all topics in the topicNames map.
        // Otherwise, the topic name should be null.
        // For earlier request versions, we expect topic names and zero Uuids.
        Map<TopicIdPartition, FetchRequest.PartitionData> expectedFetchData = new LinkedHashMap<>();
        // Build the expected map based on fetchRequestUsesTopicIds.
        expectedData.forEach(tidp -> {
            String expectedName = fetchRequestUsesTopicIds ? topicNames.get(tidp.topicId()) : tidp.topic();
            TopicIdPartition tpKey = new TopicIdPartition(tidp.topicId(), new TopicPartition(expectedName, tidp.partition()));
            // logStartOffset was not a valid field in versions 4 and earlier.
            int logStartOffset = version > 4 ? 0 : -1;
            expectedFetchData.put(tpKey, new FetchRequest.PartitionData(tidp.topicId(), 0, logStartOffset, 0, Optional.empty()));
        });
        assertEquals(expectedFetchData, fetchRequest.fetchData(topicNames));
    }

    @ParameterizedTest
    @MethodSource("fetchVersions")
    public void testForgottenTopics(short version) {
        // Forgotten topics are not allowed prior to version 7
        if (version >= 7) {
            TopicPartition topicPartition0 = new TopicPartition("topic", 0);
            TopicPartition topicPartition1 = new TopicPartition("unknownIdTopic", 0);
            Uuid topicId0 = Uuid.randomUuid();
            Uuid topicId1 = Uuid.randomUuid();
            // Only include topic IDs for the first topic partition.
            Map<Uuid, String> topicNames = Map.of(topicId0, topicPartition0.topic());

            // Include one topic with topic IDs in the topic names map and one without.
            List<TopicIdPartition> toForgetTopics = new LinkedList<>();
            toForgetTopics.add(new TopicIdPartition(topicId0, topicPartition0));
            toForgetTopics.add(new TopicIdPartition(topicId1, topicPartition1));

            boolean fetchRequestUsesTopicIds = version >= 13;

            FetchRequest fetchRequest = FetchRequest.parse(FetchRequest.Builder
                    .forReplica(version, 0, 1, 1, 1, Map.of())
                    .removed(toForgetTopics)
                    .replaced(List.of())
                    .metadata(FetchMetadata.newIncremental(123)).build(version).serialize(), version);

            // For versions < 13, we will be provided a topic name and a zero Uuid in FetchRequestData.
            // Versions 13+ will contain a valid topic ID but an empty topic name.
            List<TopicIdPartition> expectedForgottenTopicData = new LinkedList<>();
            toForgetTopics.forEach(tidp -> {
                String expectedName = fetchRequestUsesTopicIds ? "" : tidp.topic();
                Uuid expectedTopicId = fetchRequestUsesTopicIds ? tidp.topicId() : Uuid.ZERO_UUID;
                expectedForgottenTopicData.add(new TopicIdPartition(expectedTopicId, tidp.partition(), expectedName));
            });

            // Build the list of TopicIdPartitions based on the FetchRequestData that was serialized and parsed.
            List<TopicIdPartition> convertedForgottenTopicData = new LinkedList<>();
            fetchRequest.data().forgottenTopicsData().forEach(forgottenTopic ->
                    forgottenTopic.partitions().forEach(partition ->
                            convertedForgottenTopicData.add(new TopicIdPartition(forgottenTopic.topicId(), partition, forgottenTopic.topic()))
                    )
            );
            // The TopicIdPartitions built from the request data should match what we expect.
            assertEquals(expectedForgottenTopicData, convertedForgottenTopicData);

            // Get the forgottenTopics from the request data.
            List<TopicIdPartition> forgottenTopics = fetchRequest.forgottenTopics(topicNames);

            // For fetch request version 13+ we expect topic names to be filled in for all topics in the topicNames map.
            // Otherwise, the topic name should be null.
            // For earlier request versions, we expect topic names and zero Uuids.
            // Build the list of expected TopicIdPartitions. These are different from the earlier expected topicIdPartitions
            // as empty strings are converted to nulls.
            assertEquals(expectedForgottenTopicData.size(), forgottenTopics.size());
            List<TopicIdPartition> expectedForgottenTopics = new LinkedList<>();
            expectedForgottenTopicData.forEach(tidp -> {
                String expectedName = fetchRequestUsesTopicIds ? topicNames.get(tidp.topicId()) : tidp.topic();
                expectedForgottenTopics.add(new TopicIdPartition(tidp.topicId(), new TopicPartition(expectedName, tidp.partition())));
            });
            assertEquals(expectedForgottenTopics, forgottenTopics);
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.FETCH)
    public void testFetchRequestSimpleBuilderReplicaStateDowngrade(short version) {
        FetchRequestData fetchRequestData = new FetchRequestData();
        fetchRequestData.setReplicaState(new FetchRequestData.ReplicaState().setReplicaId(1));
        FetchRequest.SimpleBuilder builder = new FetchRequest.SimpleBuilder(fetchRequestData);
        fetchRequestData = builder.build(version).data();

        assertEquals(1, FetchRequest.replicaId(fetchRequestData));

        if (version < 15) {
            assertEquals(1, fetchRequestData.replicaId());
            assertEquals(-1, fetchRequestData.replicaState().replicaId());
        } else {
            assertEquals(-1, fetchRequestData.replicaId());
            assertEquals(1, fetchRequestData.replicaState().replicaId());
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.FETCH)
    public void testFetchRequestSimpleBuilderReplicaIdNotSupported(short version) {
        FetchRequestData fetchRequestData = new FetchRequestData().setReplicaId(1);
        FetchRequest.SimpleBuilder builder = new FetchRequest.SimpleBuilder(fetchRequestData);
        assertThrows(IllegalStateException.class, () ->
            builder.build(version)
        );
    }

    @Test
    public void testPartitionDataEquals() {
        assertEquals(new FetchRequest.PartitionData(Uuid.ZERO_UUID, 300, 0L, 300, Optional.of(300)),
                new FetchRequest.PartitionData(Uuid.ZERO_UUID, 300, 0L, 300, Optional.of(300)));

        assertNotEquals(new FetchRequest.PartitionData(Uuid.randomUuid(), 300, 0L, 300, Optional.of(300)),
            new FetchRequest.PartitionData(Uuid.randomUuid(), 300, 0L, 300, Optional.of(300)));
    }

    @ParameterizedTest
    @MethodSource("fetchVersions")
    public void testFetchRequestNoCacheData(short version) {
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;
        TopicIdPartition tp = new TopicIdPartition(topicId, partition, "topic");

        FetchRequest fetchRequest = createFetchRequestByVersion(version, topicId, tp);

        Map<Uuid, String> topicNames = Map.of(topicId, tp.topic());
        List<TopicIdPartition> requestsWithTopicsName = fetchRequest.forgottenTopics(topicNames);
        assertEquals(topicNames.size(), requestsWithTopicsName.size());
        requestsWithTopicsName.forEach(request -> {
            assertEquals(tp.topic(), request.topic());
            assertEquals(topicId, request.topicId());
            assertEquals(tp.partition(), request.partition());
            assertEquals(tp.topicPartition(), request.topicPartition());
        });

        String expectedTopic = version >= 13 ? null : tp.topic();
        List<TopicIdPartition> requestData = fetchRequest.forgottenTopics(Map.of());
        assertEquals(1, requestData.size());
        requestData.forEach(request -> {
            assertEquals(expectedTopic, request.topic());
            assertEquals(topicId, request.topicId());
            assertEquals(tp.partition(), request.partition());
            assertEquals(new TopicPartition(expectedTopic, partition), request.topicPartition());
        });

    }

    private FetchRequest createFetchRequestByVersion(short version, Uuid topicId, TopicIdPartition tp) {
        return createFetchRequestByVersion(version, tp, new FetchRequest.PartitionData(topicId, 0, 0, 0, Optional.empty()));
    }

    private FetchRequest createFetchRequestByVersion(short version, TopicIdPartition tp,
                                                     FetchRequest.PartitionData partitionData) {
        Map<TopicPartition, FetchRequest.PartitionData> partitionDataMap = Map.of(tp.topicPartition(), partitionData);
        if (version >= 13) {
            return FetchRequest.Builder
                .forReplica(version, 0, 1, 1, 1, partitionDataMap)
                .replaced(List.of(tp))
                .metadata(FetchMetadata.newIncremental(123)).build(version);
        } else {
            return FetchRequest.Builder
                .forReplica(version, 0, 1, 1, 1, partitionDataMap)
                .removed(List.of(tp))
                .metadata(FetchMetadata.newIncremental(123)).build(version);
        }
    }

    @ParameterizedTest
    @MethodSource("fetchVersions")
    public void testFetchDataNoCacheData(short version) {
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;
        TopicIdPartition tp = new TopicIdPartition(topicId, partition, "topic1");
        long fetchOffset = 118L;
        long logStartOffset = 119L;
        int maxBytes = 120;
        Optional<Integer> currentLeaderEpoch = Optional.of(121);
        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(topicId, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch);
        FetchRequest fetchRequest = createFetchRequestByVersion(version, tp, partitionData);
        Map<Uuid, String> topicNames = Map.of(topicId, tp.topic());
        Map<TopicIdPartition, FetchRequest.PartitionData> topicIdPartitionMap = fetchRequest.fetchData(topicNames);

        assertEquals(topicNames.size(), topicIdPartitionMap.size());
        topicIdPartitionMap.forEach((topicIdPartition, partitionDataTmp) -> {
            assertEquals(tp.topic(), topicIdPartition.topic());
            assertEquals(topicId, topicIdPartition.topicId());
            assertEquals(tp.partition(), topicIdPartition.partition());
            assertEquals(tp.topicPartition(), topicIdPartition.topicPartition());
            assertEquals(fetchOffset, partitionDataTmp.fetchOffset);
            assertEquals(logStartOffset, partitionDataTmp.logStartOffset);
            assertEquals(maxBytes, partitionDataTmp.maxBytes);
            assertEquals(currentLeaderEpoch, partitionDataTmp.currentLeaderEpoch);
        });

        String expectedTopic = version >= 13 ? null : tp.topic();
        topicIdPartitionMap = fetchRequest.fetchData(Map.of());
        assertEquals(1, topicIdPartitionMap.size());
        topicIdPartitionMap.forEach((topicIdPartition, partitionDataTmp) -> {
            assertEquals(expectedTopic, topicIdPartition.topic());
            assertEquals(topicId, topicIdPartition.topicId());
            assertEquals(tp.partition(), topicIdPartition.partition());
            assertEquals(new TopicPartition(expectedTopic, partition), topicIdPartition.topicPartition());
            assertEquals(fetchOffset, partitionDataTmp.fetchOffset);
            assertEquals(logStartOffset, partitionDataTmp.logStartOffset);
            assertEquals(maxBytes, partitionDataTmp.maxBytes);
            assertEquals(currentLeaderEpoch, partitionDataTmp.currentLeaderEpoch);
        });
    }
}
