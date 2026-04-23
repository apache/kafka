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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.server.TestUtils.consumeRecords;
import static org.apache.kafka.server.config.ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.BROKER_RACK_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.NUM_PARTITIONS_CONFIG;
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

    private final ClusterInstance cluster;
    Map<String, Uuid> topicIds;


    public FetchFromFollowerIntegrationTest(ClusterInstance clusterInstance) {
        this.cluster = clusterInstance;
    }

    @ClusterTest
    @Timeout(15)
    public void testFollowerCompleteDelayedFetchesOnReplication() throws Exception {
        try (Admin admin = cluster.admin()) {
            cluster.createTopicWithAssignment(TOPIC, Map.of(0, List.of(LEADER_BROKER_ID, FOLLOWER_BROKER_ID)));
            int leaderId = TestUtils.waitUntilLeaderIsKnown(cluster.brokers().values(), new TopicPartition(TOPIC, 0));

            var topicPartition = new TopicPartition(TOPIC, 0);
            assertEquals(LEADER_BROKER_ID, leaderId);

            var fetchData = createPartitionMap(1000, List.of(topicPartition), Map.of(topicPartition, 0L));
            var fetchRequest = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion(), 20000, 1, fetchData)
                    .setMaxBytes(1000)
                    .rackId("")
                    .build();

            int followerPort = cluster.brokers().get(FOLLOWER_BROKER_ID).boundPort(cluster.clientListener());
            try (var socket = IntegrationTestUtils.connect(followerPort)) {
                IntegrationTestUtils.sendRequest(socket,
                                        Utils.toArray(fetchRequest.serializeWithHeader(
                                            IntegrationTestUtils.nextRequestHeader(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion()))));

                try (Producer<byte[], byte[]> producer = cluster.producer(Map.of())) {
                    producer.send(new ProducerRecord<>(TOPIC, "key".getBytes(), "value".getBytes())).get();
                }

                FetchResponse response = IntegrationTestUtils.receive(socket, ApiKeys.FETCH, ApiKeys.FETCH.latestVersion());
                assertEquals(Errors.NONE, response.error());
                assertEquals(Map.of(Errors.NONE, 2), response.errorCounts());
            }
        }

    }

    @ClusterTest
    @Timeout(15)
    public void testFetchFromLeaderWhilePreferredReadReplicaIsUnavailable() throws Exception {
        cluster.createTopicWithAssignment(TOPIC, Map.of(0, List.of(LEADER_BROKER_ID, FOLLOWER_BROKER_ID)));
        TestUtils.waitUntilLeaderIsKnown(cluster.brokers().values(), new TopicPartition(TOPIC, 0));

        produceMessages(10);
        assertEquals(1, getPreferredReplica());

        cluster.brokers().get(FOLLOWER_BROKER_ID).shutdown();
        TopicPartition topicPartition = new TopicPartition(TOPIC, 0);

        waitForCondition(
                () -> {
                    var leaderBroker = cluster.brokers().get(LEADER_BROKER_ID);
                    var endpoints = leaderBroker.metadataCache()
                            .getPartitionReplicaEndpoints(topicPartition, cluster.clientListener());
                    return !endpoints.containsKey(FOLLOWER_BROKER_ID);
                },
                "follower is still reachable."
        );

        assertEquals(-1, getPreferredReplica());
    }

    @ClusterTest
    @Timeout(60)
    public void testFetchFromFollowerWithRoll() throws Exception {
        cluster.createTopicWithAssignment(TOPIC, Map.of(0, List.of(LEADER_BROKER_ID, FOLLOWER_BROKER_ID)));
        TestUtils.waitUntilLeaderIsKnown(cluster.brokers().values(), new TopicPartition(TOPIC, 0));

        var consumerProps = new HashMap<String, Object>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.CLIENT_RACK_CONFIG, String.valueOf(FOLLOWER_BROKER_ID));

        try (var consumer = cluster.consumer(consumerProps)) {
            consumer.subscribe(List.of(TOPIC));

            // Wait until preferred replica is set to follower.
            waitForCondition(
                    () -> getPreferredReplica() == FOLLOWER_BROKER_ID,
                    "Preferred replica is not set to follower"
            );

            // Produce and consume.
            produceMessages(1);
            consumeRecords(consumer, 1);

            // Shutdown follower, produce and consume should work.
            cluster.shutdownBroker(FOLLOWER_BROKER_ID);
            produceMessages(1);
            consumeRecords(consumer, 1);

            // Start the follower and wait until preferred replica is set to follower.
            cluster.startBroker(FOLLOWER_BROKER_ID);
            waitForCondition(
                    () -> getPreferredReplica() == FOLLOWER_BROKER_ID,
                    "Preferred replica is not set to follower after restart"
            );

            // Produce and consume should still work.
            produceMessages(1);
            consumeRecords(consumer, 1);
        }
    }

    @ClusterTest
    @Timeout(60)
    public void testRackAwareRangeAssignor() throws Exception {
        var partitionList = cluster.brokers().keySet().stream()
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

        var consumerConfigs = cluster.brokers().keySet().stream()
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

        var consumers = consumerConfigs.stream()
                .map(cluster::consumer)
                .toList();

        try (Producer<byte[], byte[]> producer = cluster.producer(Map.of())) {
            ExecutorService executor = Executors.newFixedThreadPool(consumers.size());
            try {
                // Rack-based assignment results in partitions assigned in reverse order since partition racks are in the reverse order.
                verifyRackAwareAssignments(executor, consumers, producer, partitionList, topicWithSingleRackPartitions, reverseList(partitionList));
                // Non-rack-aware assignment results in ordered partitions.
                verifyRackAwareAssignments(executor, consumers, producer, partitionList, topicWithAllPartitionsOnAllRacks, partitionList);
                // Rack-aware assignment with co-partitioning results in reverse assignment for both topics.
                verifyRackAwareAssignments(executor, consumers, producer, partitionList, topicWithAllPartitionsOnAllRacks, reverseList(partitionList), topicWithSingleRackPartitions);

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

                verifyRackAwareAssignments(executor, consumers, producer, partitionList, topicWithAllPartitionsOnAllRacks, partitionList, topicWithSingleRackPartitions);

            } finally {
                executor.shutdown();
                for (Object c : consumers) {
                    @SuppressWarnings("unchecked")
                    KafkaConsumer<byte[], byte[]> consumer = (KafkaConsumer<byte[], byte[]>) c;
                    consumer.close();
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Map<String, Uuid> getTopicIds() throws Exception {
        if (topicIds != null) {
            return topicIds;
        }
        this.topicIds = new HashMap<>();
        try (Admin admin = cluster.admin()) {
            var descriptions = admin.describeTopics(List.of(TOPIC))
                    .allTopicNames()
                    .get();
            descriptions.forEach((name, desc) -> topicIds.put(name, desc.topicId()));
            return topicIds;
        }
    }

    private LinkedHashMap<TopicPartition, FetchRequest.PartitionData> createPartitionMap(
            int maxPartitionBytes,
            List<TopicPartition> topicPartitions,
            Map<TopicPartition, Long> offsetMap) throws Exception {

        var partitionMap = new LinkedHashMap<TopicPartition, FetchRequest.PartitionData>();
        for (TopicPartition tp : topicPartitions) {
            Uuid topicId = getTopicIds().getOrDefault(tp.topic(), Uuid.ZERO_UUID);
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

    private int getPreferredReplica() throws Exception {
        var topicPartition = new TopicPartition(TOPIC, 0);
        var offsetMap = Map.of(topicPartition, 0L);
        var fetchData = createPartitionMap(1000, List.of(topicPartition), offsetMap);
        var fetchRequest = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion(), 500, 1, fetchData)
                .setMaxBytes(1000)
                .rackId(String.valueOf(FOLLOWER_BROKER_ID))
                .build();

        int leaderPort = cluster.brokers().get(LEADER_BROKER_ID).boundPort(cluster.clientListener());
        try (var socket = IntegrationTestUtils.connect(leaderPort)) {
            IntegrationTestUtils.sendRequest(socket,
                    Utils.toArray(
                            fetchRequest.serializeWithHeader(
                                    IntegrationTestUtils.nextRequestHeader(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion()))));

            FetchResponse response = IntegrationTestUtils.receive(socket, ApiKeys.FETCH, ApiKeys.FETCH.latestVersion());
            assertEquals(Errors.NONE, response.error());
            assertEquals(Map.of(Errors.NONE, 2), response.errorCounts());
            assertEquals(1, response.data().responses().size());
            var topicResponse = response.data().responses().get(0);
            assertEquals(1, topicResponse.partitions().size());
            return topicResponse.partitions().get(0).preferredReadReplica();
        }
    }

    private void produceMessages(int numMessages) throws Exception {
        try (Producer<byte[], byte[]> producer = cluster.producer(Map.of())) {
            for (int i = 0; i < numMessages; i++) {
                producer.send(new ProducerRecord<>(TOPIC, ("key-" + i).getBytes(), ("value-" + i).getBytes())).get();
            }
        }
    }

    private void verifyRackAwareAssignments(
            ExecutorService executor,
            List<?> consumers,
            Producer<byte[], byte[]> producer,
            List<Integer> partitionList,
            String topic,
            List<Integer> expectedPartitionOrder,
            String... additionalTopics) throws Exception {

        List<String> topics = new ArrayList<>();
        topics.add(topic);
        topics.addAll(List.of(additionalTopics));

        for (Object c : consumers) {
            @SuppressWarnings("unchecked")
            KafkaConsumer<byte[], byte[]> consumer = (KafkaConsumer<byte[], byte[]>) c;
            consumer.subscribe(topics);
        }

        awaitConsumerAssignments(executor, consumers, topics, expectedPartitionOrder);

        for (int p : partitionList) {
            for (String t : topics) {
                producer.send(new ProducerRecord<>(t, p, ("key-" + t + "-" + p).getBytes(), ("value-" + t + "-" + p).getBytes())).get();
            }
        }

        List<Future<?>> recordFutures = new ArrayList<>();
        for (Object c : consumers) {
            recordFutures.add(executor.submit(() -> {
                try {
                    @SuppressWarnings("unchecked")
                    KafkaConsumer<byte[], byte[]> consumer = (KafkaConsumer<byte[], byte[]>) c;
                    consumeRecords(consumer, topics.size());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (Future<?> future : recordFutures) {
            future.get(30, TimeUnit.SECONDS);
        }

        for (Object c : consumers) {
            @SuppressWarnings("unchecked")
            KafkaConsumer<byte[], byte[]> consumer = (KafkaConsumer<byte[], byte[]>) c;
            consumer.commitSync();
        }
    }

    private void awaitConsumerAssignments(
            ExecutorService executor,
            List<?> consumers,
            List<String> topics,
            List<Integer> expectedPartitionOrder) throws Exception {

        List<Future<?>> assignmentFutures = new ArrayList<>();
        for (int i = 0; i < consumers.size(); i++) {
            final int consumerIndex = i;
            int partition = expectedPartitionOrder.get(i);
            Set<TopicPartition> expectedAssignment = new HashSet<>();
            for (String t : topics) {
                expectedAssignment.add(new TopicPartition(t, partition));
            }

            @SuppressWarnings("unchecked")
            KafkaConsumer<byte[], byte[]> consumer = (KafkaConsumer<byte[], byte[]>) consumers.get(i);
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
            }));
        }

        for (Future<?> future : assignmentFutures) {
            future.get(30, TimeUnit.SECONDS);
        }
    }

    private static List<Integer> reverseList(List<Integer> list) {
        var reversed = new ArrayList<>(list);
        Collections.reverse(reversed);
        return reversed;
    }
}
