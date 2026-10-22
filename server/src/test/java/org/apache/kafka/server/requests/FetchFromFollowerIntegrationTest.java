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
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.IntegrationTestUtils;
import org.apache.kafka.server.TestUtils;
import org.junit.jupiter.api.Timeout;

import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.server.TestUtils.consumeRecords;
import static org.apache.kafka.server.config.ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.BROKER_RACK_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.NUM_PARTITIONS_CONFIG;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = FetchFromFollowerIntegrationTest.BROKER_COUNT,
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
public class FetchFromFollowerIntegrationTest {

    public static final int BROKER_COUNT = 2;
    public static final String TOPIC = "test-fetch-from-follower";
    public static final int LEADER_BROKER_ID = 0;
    public static final int FOLLOWER_BROKER_ID = 1;

    @ClusterTest
    @Timeout(15)
    public void testFollowerCompleteDelayedFetchesOnReplication(ClusterInstance cluster) throws Exception {
        cluster.createTopicWithAssignment(TOPIC, Map.of(0, List.of(LEADER_BROKER_ID, FOLLOWER_BROKER_ID)));
        int leaderId = waitUntilLeaderIsKnown(cluster.brokers().values(), new TopicPartition(TOPIC, 0));

        var topicPartition = new TopicPartition(TOPIC, 0);
        assertEquals(LEADER_BROKER_ID, leaderId);

        // Set fetch.max.wait.ms to a value (20 seconds) greater than the timeout (15 seconds).
        // Send a fetch request before the record is replicated to ensure that the replication
        // triggers purgatory completion.
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

        produceMessages(cluster, TOPIC, 10);
        assertEquals(1, getPreferredReplica(cluster, TOPIC));

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

        assertEquals(-1, getPreferredReplica(cluster, TOPIC));
    }

    @ClusterTest
    public void testFetchFromFollowerWithRoll(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            verifyFetchFromFollowerWithRoll(cluster, groupProtocol);
        }
    }

    private void verifyFetchFromFollowerWithRoll(ClusterInstance cluster, GroupProtocol groupProtocol) throws Exception {
        // Each protocol gets its own topic and group ids so that the iterations of
        // testFetchFromFollowerWithRoll, which share a single cluster, stay independent.
        String protocolName = groupProtocol.name().toLowerCase(Locale.ROOT);
        String topic = TOPIC + "-" + protocolName;
        cluster.createTopicWithAssignment(topic, Map.of(0, List.of(LEADER_BROKER_ID, FOLLOWER_BROKER_ID)));
        waitUntilLeaderIsKnown(cluster.brokers().values(), new TopicPartition(topic, 0));

        var followerConsumerProps = new HashMap<String, Object>();
        followerConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        followerConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-follower-" + protocolName);
        followerConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        followerConsumerProps.put(ConsumerConfig.CLIENT_RACK_CONFIG, String.valueOf(FOLLOWER_BROKER_ID));
        followerConsumerProps.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name());

        var leaderConsumerProps = new HashMap<String, Object>();
        leaderConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        leaderConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-leader-" + protocolName);
        leaderConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        leaderConsumerProps.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name());

        try (Consumer<byte[], byte[]> followerConsumer = cluster.consumer(followerConsumerProps);
             Consumer<byte[], byte[]> leaderConsumer = cluster.consumer(leaderConsumerProps)) {
            followerConsumer.subscribe(List.of(topic));
            leaderConsumer.subscribe(List.of(topic));

            // Wait until preferred replica is set to follower.
            waitForCondition(
                    () -> getPreferredReplica(cluster, topic) == FOLLOWER_BROKER_ID,
                    "Preferred replica is not set to follower for " + protocolName
            );

            // Produce and consume from both consumers.
            produceMessages(cluster, topic, 1);
            consumeRecords(followerConsumer, 1);
            consumeRecords(leaderConsumer, 1);

            // Shutdown follower, produce and consume should work for both consumers.
            cluster.shutdownBroker(FOLLOWER_BROKER_ID);
            produceMessages(cluster, topic, 1);
            consumeRecords(followerConsumer, 1);
            consumeRecords(leaderConsumer, 1);

            // Start the follower and wait until preferred replica is set to follower.
            cluster.startBroker(FOLLOWER_BROKER_ID);
            waitForCondition(
                    () -> getPreferredReplica(cluster, topic) == FOLLOWER_BROKER_ID,
                    "Preferred replica is not set to follower after restart for " + protocolName
            );

            // Produce and consume should still work for both consumers.
            produceMessages(cluster, topic, 1);
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

    private Map<String, Uuid> getTopicIds(ClusterInstance cluster, Collection<String> topics) throws Exception {
        Map<String, Uuid> topicIds = new HashMap<>();
        try (Admin admin = cluster.admin()) {
            Map<String, TopicDescription> descriptions = admin.describeTopics(topics)
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

        Map<String, Uuid> topicIds = getTopicIds(cluster, topicPartitions.stream()
                .map(TopicPartition::topic)
                .collect(Collectors.toSet()));
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

    private int getPreferredReplica(ClusterInstance cluster, String topic) throws Exception {
        var topicPartition = new TopicPartition(topic, 0);
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

    private void produceMessages(ClusterInstance cluster, String topic, int numMessages) throws Exception {
        try (Producer<byte[], byte[]> producer = cluster.producer(Map.of())) {
            for (int i = 0; i < numMessages; i++) {
                producer.send(new ProducerRecord<>(topic, ("key-" + i).getBytes(), ("value-" + i).getBytes())).get();
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
        for (int i = 0; i < consumers.size(); i++) {
            Consumer<byte[], byte[]> consumer = consumers.get(i);
            int consumerIndex = i;
            Set<TopicPartition> expectedAssignment = expectedAssignment(topics, expectedPartitionOrder.get(i));
            recordFutures.add(executor.submit(() -> {
                try {
                    // Verify that the records came from the rack-aware assigned partitions, and not
                    // merely that the expected number of records arrived. The assignment can change
                    // between awaitConsumerAssignments and this consume, so re-check it here.
                    Set<TopicPartition> consumedPartitions = consumeRecords(consumer, topics.size()).stream()
                            .map(record -> new TopicPartition(record.topic(), record.partition()))
                            .collect(Collectors.toSet());
                    assertEquals(expectedAssignment, consumedPartitions,
                            "Consumer " + consumerIndex + " consumed records from unexpected partitions");
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
            Set<TopicPartition> expectedAssignment = expectedAssignment(topics, expectedPartitionOrder.get(i));

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

    private static Set<TopicPartition> expectedAssignment(List<String> topics, int partition) {
        return topics.stream()
                .map(topic -> new TopicPartition(topic, partition))
                .collect(Collectors.toSet());
    }

    private static <T> List<T> reverseList(List<T> list) {
        var reversed = new ArrayList<>(list);
        Collections.reverse(reversed);
        return reversed;
    }

    private static int waitUntilLeaderIsKnown(
            Collection<KafkaBroker> brokers,
            TopicPartition tp) throws InterruptedException {
        return TestUtils.awaitLeaderChange(brokers, tp, Optional.empty(), Optional.empty(), DEFAULT_MAX_WAIT_MS);
    }
}
