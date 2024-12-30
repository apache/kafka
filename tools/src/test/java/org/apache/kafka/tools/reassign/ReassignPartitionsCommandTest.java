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
package org.apache.kafka.tools.reassign;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.ClusterTests;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.TerseException;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_3_IV0;
import static org.apache.kafka.server.config.QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG;
import static org.apache.kafka.server.config.QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.REPLICA_FETCH_BACKOFF_MS_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_CONFIG;
import static org.apache.kafka.tools.ToolsTestUtils.assignThrottledPartitionReplicas;
import static org.apache.kafka.tools.ToolsTestUtils.throttleAllBrokersReplication;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.BROKER_LEVEL_THROTTLES;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.cancelAssignment;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.executeAssignment;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.generateAssignment;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.verifyAssignment;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(brokers = 5, disksPerBroker = 3, serverProperties = {
    // shorter backoff to reduce test durations when no active partitions are eligible for fetching due to throttling
    @ClusterConfigProperty(key = REPLICA_FETCH_BACKOFF_MS_CONFIG, value = "100"),
    // Don't move partition leaders automatically.
    @ClusterConfigProperty(key = AUTO_LEADER_REBALANCE_ENABLE_CONFIG, value = "false"),
    @ClusterConfigProperty(key = REPLICA_LAG_TIME_MAX_MS_CONFIG, value = "1000"),
    @ClusterConfigProperty(id = 0, key = "broker.rack", value = "rack0"),
    @ClusterConfigProperty(id = 1, key = "broker.rack", value = "rack0"),
    @ClusterConfigProperty(id = 2, key = "broker.rack", value = "rack1"),
    @ClusterConfigProperty(id = 3, key = "broker.rack", value = "rack1"),
    @ClusterConfigProperty(id = 4, key = "broker.rack", value = "rack1"),
})
@ExtendWith(ClusterTestExtensions.class)
public class ReassignPartitionsCommandTest {
    private final ClusterInstance clusterInstance;
    private final Map<Integer, Map<String, Long>> unthrottledBrokerConfigs = IntStream
            .range(0, 4)
            .boxed()
            .collect(Collectors.toMap(Function.identity(), i ->
        BROKER_LEVEL_THROTTLES.stream().collect(Collectors.toMap(Function.identity(), t -> -1L))
    ));

    ReassignPartitionsCommandTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @ClusterTest
    public void testReassignment() throws Exception {
        createTopics();
        executeAndVerifyReassignment();
    }

    @ClusterTests({
        @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT}, metadataVersion = IBP_3_3_IV0)
    })
    public void testReassignmentWithAlterPartitionDisabled() throws Exception {
        // Test reassignment when the IBP is on an older version which does not use
        // the `AlterPartition` API. In this case, the controller will register individual
        // watches for each reassigning partition so that the reassignment can be
        // completed as soon as the ISR is expanded.
        createTopics();
        executeAndVerifyReassignment();
    }

    @ClusterTests({
        @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT}, serverProperties = {
            @ClusterConfigProperty(id = 1, key = INTER_BROKER_PROTOCOL_VERSION_CONFIG, value = "3.3-IV0"),
            @ClusterConfigProperty(id = 2, key = INTER_BROKER_PROTOCOL_VERSION_CONFIG, value = "3.3-IV0"),
            @ClusterConfigProperty(id = 3, key = INTER_BROKER_PROTOCOL_VERSION_CONFIG, value = "3.3-IV0"),
        })
    })
    public void testReassignmentCompletionDuringPartialUpgrade() throws Exception {
        // Test reassignment during a partial upgrade when some brokers are relying on
        // `AlterPartition` and some rely on the old notification logic through Zookeeper.
        // In this test case, broker 0 starts up first on the latest IBP and is typically
        // elected as controller. The three remaining brokers start up on the older IBP.
        // We want to ensure that reassignment can still complete through the ISR change
        // notification path even though the controller expects `AlterPartition`.

        // Override change notification settings so that test is not delayed by ISR
        // change notification delay
        // ZkAlterPartitionManager.DefaultIsrPropagationConfig_$eq(new IsrChangePropagationConfig(500, 100, 500));

        createTopics();
        executeAndVerifyReassignment();
    }

    @ClusterTest
    public void testHighWaterMarkAfterPartitionReassignment() throws Exception {
        createTopics();
        TopicPartition foo0 = new TopicPartition("foo", 0);
        produceMessages(foo0.topic(), foo0.partition(), 100);

        // Execute the assignment
        String assignment = "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[3,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                "]}";
        runExecuteAssignment(false, assignment, -1L, -1L);

        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            Map<TopicPartition, PartitionReassignmentState> finalAssignment = singletonMap(foo0,
                    new PartitionReassignmentState(asList(3, 1, 2), asList(3, 1, 2), true));
            // Wait for the assignment to complete
            waitForVerifyAssignment(admin, assignment, false,
                    new VerifyAssignmentResult(finalAssignment));
            TestUtils.waitForCondition(() -> {
                ListOffsetsResultInfo result = admin.listOffsets(Collections.singletonMap(foo0, new OffsetSpec.LatestSpec())).partitionResult(foo0).get();
                return result.offset() == 100;
            }, "Timeout for waiting offset");
        }
    }

    @ClusterTest
    public void testGenerateAssignmentWithBootstrapServer() throws Exception {
        createTopics();
        TopicPartition foo0 = new TopicPartition("foo", 0);
        produceMessages(foo0.topic(), foo0.partition(), 100);

        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            String assignment = "{\"version\":1,\"partitions\":" +
                    "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[3,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                    "]}";
            generateAssignment(admin, assignment, "1,2,3", false);
            Map<TopicPartition, PartitionReassignmentState> finalAssignment = singletonMap(foo0,
                    new PartitionReassignmentState(asList(0, 1, 2), asList(3, 1, 2), true));
            waitForVerifyAssignment(admin, assignment, false,
                    new VerifyAssignmentResult(finalAssignment));
        }
    }

    @ClusterTest
    public void testAlterReassignmentThrottle() throws Exception {
        createTopics();
        produceMessages("foo", 0, 50);
        produceMessages("baz", 2, 60);
        String assignment = "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,3,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}," +
                "{\"topic\":\"baz\",\"partition\":2,\"replicas\":[3,2,1],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                "]}";

        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            // Execute the assignment with a low throttle
            long initialThrottle = 1L;
            runExecuteAssignment(false, assignment, initialThrottle, -1L);
            waitForInterBrokerThrottle(admin, asList(0, 1, 2, 3), initialThrottle);

            // Now update the throttle and verify the reassignment completes
            long updatedThrottle = 300000L;
            runExecuteAssignment(true, assignment, updatedThrottle, -1L);
            waitForInterBrokerThrottle(admin, asList(0, 1, 2, 3), updatedThrottle);

            Map<TopicPartition, PartitionReassignmentState> finalAssignment = new HashMap<>();
            finalAssignment.put(new TopicPartition("foo", 0),
                    new PartitionReassignmentState(asList(0, 3, 2), asList(0, 3, 2), true));
            finalAssignment.put(new TopicPartition("baz", 2),
                    new PartitionReassignmentState(asList(3, 2, 1), asList(3, 2, 1), true));

            // Now remove the throttles.
            waitForVerifyAssignment(admin, assignment, false,
                    new VerifyAssignmentResult(finalAssignment));
            waitForBrokerLevelThrottles(admin, unthrottledBrokerConfigs);
        }
    }

    /**
     * Test running a reassignment with the interBrokerThrottle set.
     */
    @ClusterTest
    public void testThrottledReassignment() throws Exception {
        createTopics();
        produceMessages("foo", 0, 50);
        produceMessages("baz", 2, 60);
        String assignment = "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,3,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}," +
                "{\"topic\":\"baz\",\"partition\":2,\"replicas\":[3,2,1],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                "]}";

        // Check that the assignment has not yet been started yet.
        Map<TopicPartition, PartitionReassignmentState> initialAssignment = new HashMap<>();
        initialAssignment.put(new TopicPartition("foo", 0),
                new PartitionReassignmentState(asList(0, 1, 2), asList(0, 3, 2), true));
        initialAssignment.put(new TopicPartition("baz", 2),
                new PartitionReassignmentState(asList(0, 2, 1), asList(3, 2, 1), true));
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            assertEquals(new VerifyAssignmentResult(initialAssignment), runVerifyAssignment(admin, assignment, false));
            assertEquals(unthrottledBrokerConfigs, describeBrokerLevelThrottles(admin, unthrottledBrokerConfigs.keySet()));

            // Execute the assignment
            long interBrokerThrottle = 300000L;
            runExecuteAssignment(false, assignment, interBrokerThrottle, -1L);
            waitForInterBrokerThrottle(admin, asList(0, 1, 2, 3), interBrokerThrottle);

            Map<TopicPartition, PartitionReassignmentState> finalAssignment = new HashMap<>();
            finalAssignment.put(new TopicPartition("foo", 0),
                    new PartitionReassignmentState(asList(0, 3, 2), asList(0, 3, 2), true));
            finalAssignment.put(new TopicPartition("baz", 2),
                    new PartitionReassignmentState(asList(3, 2, 1), asList(3, 2, 1), true));

            // Wait for the assignment to complete
            TestUtils.waitForCondition(() -> {
                // Check the reassignment status.
                VerifyAssignmentResult result = runVerifyAssignment(admin, assignment, true);

                if (!result.partsOngoing) {
                    return true;
                } else {
                    assertFalse(
                            result.partStates.values().stream().allMatch(state -> state.done),
                            "Expected at least one partition reassignment to be ongoing when result = " + result
                    );
                    assertEquals(asList(0, 3, 2), result.partStates.get(new TopicPartition("foo", 0)).targetReplicas);
                    assertEquals(asList(3, 2, 1), result.partStates.get(new TopicPartition("baz", 2)).targetReplicas);
                    waitForInterBrokerThrottle(admin, asList(0, 1, 2, 3), interBrokerThrottle);
                    return false;
                }
            }, "Expected reassignment to complete.");

            waitForVerifyAssignment(admin, assignment, true,
                    new VerifyAssignmentResult(finalAssignment));
            // The throttles should still have been preserved, since we ran with --preserve-throttles
            waitForInterBrokerThrottle(admin, asList(0, 1, 2, 3), interBrokerThrottle);
            // Now remove the throttles.
            waitForVerifyAssignment(admin, assignment, false,
                    new VerifyAssignmentResult(finalAssignment));
            waitForBrokerLevelThrottles(admin, unthrottledBrokerConfigs);
        }
    }

    @ClusterTest
    public void testProduceAndConsumeWithReassignmentInProgress() throws Exception {
        createTopics();
        produceMessages("baz", 2, 60);
        String assignment = "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"baz\",\"partition\":2,\"replicas\":[3,2,1],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                "]}";
        runExecuteAssignment(false, assignment, 300L, -1L);
        produceMessages("baz", 2, 100);

        Properties consumerProps = new Properties();
        consumerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        TopicPartition part = new TopicPartition("baz", 2);
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            consumer.assign(singleton(part));
            List<ConsumerRecord<byte[], byte[]>> allRecords = new ArrayList<>();
            TestUtils.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100L));
                records.records("baz").forEach(allRecords::add);
                return allRecords.size() >= 100;
            }, "Timeout for waiting enough records");
        }
        removeReplicationThrottleForPartitions(part);
        Map<TopicPartition, PartitionReassignmentState> finalAssignment = singletonMap(part,
                new PartitionReassignmentState(asList(3, 2, 1), asList(3, 2, 1), true));
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers())))  {
            waitForVerifyAssignment(admin, assignment, false,
                    new VerifyAssignmentResult(finalAssignment));
        }
    }

    /**
     * Test running a reassignment and then cancelling it.
     */
    @ClusterTest
    public void testCancellationWithBootstrapServer() throws Exception {
        testCancellationAction(true);
    }

    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT})
    public void testCancellationWithBootstrapController() throws Exception {
        testCancellationAction(false);
    }

    @ClusterTest
    public void testCancellationWithAddingReplicaInIsr() throws Exception {
        createTopics();
        TopicPartition foo0 = new TopicPartition("foo", 0);
        produceMessages(foo0.topic(), foo0.partition(), 200);

        // The reassignment will bring replicas 3 and 4 into the replica set and remove 1 and 2.
        String assignment = "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,3,4],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                "]}";

        // We will throttle replica 4 so that only replica 3 joins the ISR
        setReplicationThrottleForPartitions(foo0);

        // Execute the assignment and wait for replica 3 (only) to join the ISR
        runExecuteAssignment(false, assignment, -1L, -1L);
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            TestUtils.waitForCondition(
                    () -> {
                        Set<Integer> isr = admin.describeTopics(Collections.singleton(foo0.topic()))
                                .allTopicNames().get().get(foo0.topic()).partitions().stream()
                                .filter(p -> p.partition() == foo0.partition())
                                .flatMap(p -> p.isr().stream())
                                .map(Node::id).collect(Collectors.toSet());
                        return isr.containsAll(Arrays.asList(0, 1, 2, 3));
                    },
                    "Timed out while waiting for replica 3 to join the ISR"
            );
        }

        // Now cancel the assignment and verify that the partition is removed from cancelled replicas
        assertEquals(new AbstractMap.SimpleImmutableEntry<>(singleton(foo0), Collections.emptySet()), runCancelAssignment(assignment, true, true));
        verifyReplicaDeleted(new TopicPartitionReplica(foo0.topic(), foo0.partition(), 3));
        verifyReplicaDeleted(new TopicPartitionReplica(foo0.topic(), foo0.partition(), 4));
    }

    /**
     * Test moving partitions between directories.
     */
    @ClusterTest(types = {Type.KRAFT})
    public void testLogDirReassignment() throws Exception {
        createTopics();
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        produceMessages(topicPartition.topic(), topicPartition.partition(), 700);

        int targetBrokerId = 0;
        List<Integer> replicas = asList(0, 1, 2);
        LogDirReassignment reassignment = buildLogDirReassignment(topicPartition, targetBrokerId, replicas);

        // Start the replica move, but throttle it to be very slow so that it can't complete
        // before our next checks happen.
        long logDirThrottle = 1L;
        runExecuteAssignment(false, reassignment.json, -1L, logDirThrottle);

        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            // Check the output of --verify
            waitForVerifyAssignment(admin, reassignment.json, true,
                    new VerifyAssignmentResult(singletonMap(
                            topicPartition, new PartitionReassignmentState(asList(0, 1, 2), asList(0, 1, 2), true)
                    ), false, singletonMap(
                            new TopicPartitionReplica(topicPartition.topic(), topicPartition.partition(), 0),
                            new ActiveMoveState(reassignment.currentDir, reassignment.targetDir, reassignment.targetDir)
                    ), true));
            waitForLogDirThrottle(admin, singleton(0), logDirThrottle);

            // Remove the throttle
            admin.incrementalAlterConfigs(singletonMap(
                            new ConfigResource(ConfigResource.Type.BROKER, "0"),
                            singletonList(new AlterConfigOp(
                                    new ConfigEntry(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, ""), AlterConfigOp.OpType.DELETE))))
                    .all().get();
            waitForBrokerLevelThrottles(admin, unthrottledBrokerConfigs);

            // Wait for the directory movement to complete.
            waitForVerifyAssignment(admin, reassignment.json, true,
                    new VerifyAssignmentResult(singletonMap(
                            topicPartition, new PartitionReassignmentState(asList(0, 1, 2), asList(0, 1, 2), true)
                    ), false, singletonMap(
                            new TopicPartitionReplica(topicPartition.topic(), topicPartition.partition(), 0),
                            new CompletedMoveState(reassignment.targetDir)
                    ), false));

            BrokerDirs info1 = new BrokerDirs(admin.describeLogDirs(IntStream.range(0, 4).boxed().collect(Collectors.toList())), 0);
            assertEquals(reassignment.targetDir, info1.curLogDirs.getOrDefault(topicPartition, ""));
        }
    }

    @ClusterTest(types = {Type.KRAFT})
    public void testAlterLogDirReassignmentThrottle() throws Exception {
        createTopics();
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        produceMessages(topicPartition.topic(), topicPartition.partition(), 700);

        int targetBrokerId = 0;
        List<Integer> replicas = asList(0, 1, 2);
        LogDirReassignment reassignment = buildLogDirReassignment(topicPartition, targetBrokerId, replicas);

        // Start the replica move with a low throttle so it does not complete
        long initialLogDirThrottle = 1L;
        runExecuteAssignment(false, reassignment.json, -1L, initialLogDirThrottle);
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            waitForLogDirThrottle(admin, new HashSet<>(singletonList(0)), initialLogDirThrottle);

            // Now increase the throttle and verify that the log dir movement completes
            long updatedLogDirThrottle = 3000000L;
            runExecuteAssignment(true, reassignment.json, -1L, updatedLogDirThrottle);
            waitForLogDirThrottle(admin, singleton(0), updatedLogDirThrottle);

            waitForVerifyAssignment(admin, reassignment.json, true,
                    new VerifyAssignmentResult(singletonMap(
                            topicPartition, new PartitionReassignmentState(asList(0, 1, 2), asList(0, 1, 2), true)
                    ), false, singletonMap(
                            new TopicPartitionReplica(topicPartition.topic(), topicPartition.partition(), targetBrokerId),
                            new CompletedMoveState(reassignment.targetDir)
                    ), false));
        }
    }

    private void createTopics() {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            Map<Integer, List<Integer>> fooReplicasAssignments = new HashMap<>();
            fooReplicasAssignments.put(0, asList(0, 1, 2));
            fooReplicasAssignments.put(1, asList(1, 2, 3));
            Assertions.assertDoesNotThrow(() -> admin.createTopics(Collections.singletonList(new NewTopic("foo", fooReplicasAssignments))).topicId("foo").get());
            Assertions.assertDoesNotThrow(() -> clusterInstance.waitForTopic("foo", fooReplicasAssignments.size()));

            Map<Integer, List<Integer>> barReplicasAssignments = new HashMap<>();
            barReplicasAssignments.put(0, asList(3, 2, 1));
            Assertions.assertDoesNotThrow(() -> admin.createTopics(Collections.singletonList(new NewTopic("bar", barReplicasAssignments))).topicId("bar").get());
            Assertions.assertDoesNotThrow(() -> clusterInstance.waitForTopic("bar", barReplicasAssignments.size()));

            Map<Integer, List<Integer>> bazReplicasAssignments = new HashMap<>();
            bazReplicasAssignments.put(0, asList(1, 0, 2));
            bazReplicasAssignments.put(1, asList(2, 0, 1));
            bazReplicasAssignments.put(2, asList(0, 2, 1));
            Assertions.assertDoesNotThrow(() -> admin.createTopics(Collections.singletonList(new NewTopic("baz", bazReplicasAssignments))).topicId("baz").get());
            Assertions.assertDoesNotThrow(() -> clusterInstance.waitForTopic("baz", bazReplicasAssignments.size()));
        }
    }

    private void produceMessages(String topic, int partition, int numMessages) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties, new ByteArraySerializer(), new ByteArraySerializer())) {
            IntStream.range(0, numMessages).forEach(i -> {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, partition, null, new byte[10000]);
                assertDoesNotThrow(() -> producer.send(record).get());
            });
        }
    }

    private void executeAndVerifyReassignment() throws InterruptedException {
        String assignment = "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,1,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}," +
                "{\"topic\":\"bar\",\"partition\":0,\"replicas\":[3,2,0],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                "]}";

        TopicPartition foo0 = new TopicPartition("foo", 0);
        TopicPartition bar0 = new TopicPartition("bar", 0);

        // Check that the assignment has not yet been started yet.
        Map<TopicPartition, PartitionReassignmentState> initialAssignment = new HashMap<>();

        initialAssignment.put(foo0, new PartitionReassignmentState(asList(0, 1, 2), asList(0, 1, 3), true));
        initialAssignment.put(bar0, new PartitionReassignmentState(asList(3, 2, 1), asList(3, 2, 0), true));

        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            waitForVerifyAssignment(admin, assignment, false,
                    new VerifyAssignmentResult(initialAssignment));

            // Execute the assignment
            runExecuteAssignment(false, assignment, -1L, -1L);
            assertEquals(unthrottledBrokerConfigs, describeBrokerLevelThrottles(admin, unthrottledBrokerConfigs.keySet()));
            Map<TopicPartition, PartitionReassignmentState> finalAssignment = new HashMap<>();
            finalAssignment.put(foo0, new PartitionReassignmentState(asList(0, 1, 3), asList(0, 1, 3), true));
            finalAssignment.put(bar0, new PartitionReassignmentState(asList(3, 2, 0), asList(3, 2, 0), true));

            VerifyAssignmentResult verifyAssignmentResult = runVerifyAssignment(admin, assignment, false);
            assertFalse(verifyAssignmentResult.movesOngoing);

            // Wait for the assignment to complete
            waitForVerifyAssignment(admin, assignment, false,
                    new VerifyAssignmentResult(finalAssignment));

            assertEquals(unthrottledBrokerConfigs,
                    describeBrokerLevelThrottles(admin, unthrottledBrokerConfigs.keySet()));
        }

        // Verify that partitions are removed from brokers no longer assigned
        verifyReplicaDeleted(new TopicPartitionReplica(foo0.topic(), foo0.partition(), 2));
        verifyReplicaDeleted(new TopicPartitionReplica(bar0.topic(), bar0.partition(), 1));
    }

    private void verifyReplicaDeleted(TopicPartitionReplica topicPartitionReplica) throws InterruptedException {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            TestUtils.waitForCondition(
                    () -> {
                        TopicDescription topicDescription = assertDoesNotThrow(() -> admin.describeTopics(singleton(topicPartitionReplica.topic())).topicNameValues().get(topicPartitionReplica.topic()).get());
                        return topicDescription.partitions().stream().noneMatch(topicPartitionInfo -> {
                            if (topicPartitionInfo.partition() != topicPartitionReplica.partition()) {
                                return false;
                            }
                            return topicPartitionInfo.replicas().stream().anyMatch(node -> node.id() == topicPartitionReplica.brokerId());
                        });
                    }, "Timed out waiting for replica " + topicPartitionReplica.brokerId() + " of " + topicPartitionReplica + " to be deleted"
            );
        }
    }

    private void waitForLogDirThrottle(Admin admin, Set<Integer> throttledBrokers, Long logDirThrottle) {
        Map<String, Long> throttledConfigMap = new HashMap<>();
        throttledConfigMap.put(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, -1L);
        throttledConfigMap.put(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, -1L);
        throttledConfigMap.put(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, logDirThrottle);
        waitForBrokerThrottles(admin, throttledBrokers, throttledConfigMap);
    }

    private void waitForInterBrokerThrottle(Admin admin, List<Integer> throttledBrokers, Long interBrokerThrottle) {
        Map<String, Long> throttledConfigMap = new HashMap<>();
        throttledConfigMap.put(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, interBrokerThrottle);
        throttledConfigMap.put(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, interBrokerThrottle);
        throttledConfigMap.put(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, -1L);
        waitForBrokerThrottles(admin, throttledBrokers, throttledConfigMap);
    }

    private void waitForBrokerThrottles(Admin admin, Collection<Integer> throttledBrokers, Map<String, Long> throttleConfig) {
        Map<Integer, Map<String, Long>> throttledBrokerConfigs = new HashMap<>();
        unthrottledBrokerConfigs.forEach((brokerId, unthrottledConfig) -> {
            Map<String, Long> expectedThrottleConfig = throttledBrokers.contains(brokerId)
                    ? throttleConfig
                    : unthrottledConfig;
            throttledBrokerConfigs.put(brokerId, expectedThrottleConfig);
        });
        Assertions.assertDoesNotThrow(() -> waitForBrokerLevelThrottles(admin, throttledBrokerConfigs));
    }

    private void waitForBrokerLevelThrottles(Admin admin, Map<Integer, Map<String, Long>> targetThrottles) throws InterruptedException {
        AtomicReference<Map<Integer, Map<String, Long>>> curThrottles = new AtomicReference<>(new HashMap<>());
        TestUtils.waitForCondition(() -> {
            assertDoesNotThrow(() -> curThrottles.set(describeBrokerLevelThrottles(admin, targetThrottles.keySet())));
            return targetThrottles.equals(curThrottles.get());
        }, "timed out waiting for broker throttle to become " + targetThrottles + ".  " +
                "Latest throttles were " + curThrottles.get());
    }

    /**
     * Describe the broker-level throttles in the cluster.
     *
     * @return                A map whose keys are broker IDs and whose values are throttle
     *                        information.  The nested maps are keyed on throttle name.
     */
    private Map<Integer, Map<String, Long>> describeBrokerLevelThrottles(Admin admin, Collection<Integer> brokerIds) {
        return brokerIds.stream().collect(Collectors.toMap(Function.identity(), brokerId -> {
            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString());
            Config brokerConfigs = assertDoesNotThrow(() -> admin.describeConfigs(singleton(brokerResource)).values()
                    .get(brokerResource)
                    .get());
            return BROKER_LEVEL_THROTTLES.stream().collect(Collectors.toMap(Function.identity(),
                    name -> Optional.ofNullable(brokerConfigs.get(name)).map(e -> Long.parseLong(e.value())).orElse(-1L)));
        }));
    }

    static class LogDirReassignment {
        final String json;
        final String currentDir;
        final String targetDir;

        public LogDirReassignment(String json, String currentDir, String targetDir) {
            this.json = json;
            this.currentDir = currentDir;
            this.targetDir = targetDir;
        }
    }

    private LogDirReassignment buildLogDirReassignment(TopicPartition topicPartition,
                                                       int brokerId,
                                                       List<Integer> replicas) throws ExecutionException, InterruptedException {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            DescribeLogDirsResult describeLogDirsResult = admin.describeLogDirs(
                    IntStream.range(0, 4).boxed().collect(Collectors.toList()));

            BrokerDirs logDirInfo = new BrokerDirs(describeLogDirsResult, brokerId);
            assertTrue(logDirInfo.futureLogDirs.isEmpty());

            String currentDir = logDirInfo.curLogDirs.get(topicPartition);
            String newDir = logDirInfo.logDirs.stream().filter(dir -> !dir.equals(currentDir)).findFirst().get();

            List<String> logDirs = replicas.stream().map(replicaId -> {
                if (replicaId == brokerId)
                    return "\"" + newDir + "\"";
                else
                    return "\"any\"";
            }).collect(Collectors.toList());

            String reassignmentJson =
                    " { \"version\": 1," +
                            "  \"partitions\": [" +
                            "    {" +
                            "     \"topic\": \"" + topicPartition.topic() + "\"," +
                            "     \"partition\": " + topicPartition.partition() + "," +
                            "     \"replicas\": [" + replicas.stream().map(Object::toString).collect(Collectors.joining(",")) + "]," +
                            "     \"log_dirs\": [" + String.join(",", logDirs) + "]" +
                            "    }" +
                            "   ]" +
                            "  }";

            return new LogDirReassignment(reassignmentJson, currentDir, newDir);
        }
    }

    private VerifyAssignmentResult runVerifyAssignment(Admin admin,
                                                       String jsonString,
                                                       Boolean preserveThrottles) {
        try {
            return verifyAssignment(admin, jsonString, preserveThrottles);
        } catch (ExecutionException | InterruptedException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void waitForVerifyAssignment(Admin admin,
                                         String jsonString,
                                         Boolean preserveThrottles,
                                         VerifyAssignmentResult expectedResult) throws InterruptedException {
        final VerifyAssignmentResult[] latestResult = {null};
        TestUtils.waitForCondition(
                () -> {
                    latestResult[0] = runVerifyAssignment(admin, jsonString, preserveThrottles);
                    return expectedResult.equals(latestResult[0]);
                },
                "Timed out waiting for verifyAssignment result " + expectedResult
        );
    }

    private void runExecuteAssignment(Boolean additional,
                                      String reassignmentJson,
                                      Long interBrokerThrottle,
                                      Long replicaAlterLogDirsThrottle) throws RuntimeException {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            executeAssignment(admin, additional, reassignmentJson,
                    interBrokerThrottle, replicaAlterLogDirsThrottle, 10000L, Time.SYSTEM);
        } catch (ExecutionException | InterruptedException | JsonProcessingException | TerseException e) {
            throw new RuntimeException(e);
        }
    }

    private Map.Entry<Set<TopicPartition>, Set<TopicPartitionReplica>> runCancelAssignment(
            String jsonString,
            Boolean preserveThrottles,
            Boolean useBootstrapServer
    ) {
        Map<String, Object> config;
        if (useBootstrapServer) {
            config = Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        } else {
            config = Collections.singletonMap(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, clusterInstance.bootstrapControllers());
        }
        try (Admin admin = Admin.create(config)) {
            return cancelAssignment(admin, jsonString, preserveThrottles, 10000L, Time.SYSTEM);
        } catch (ExecutionException | InterruptedException | JsonProcessingException | TerseException e) {
            throw new RuntimeException(e);
        }
    }

    private static class BrokerDirs {
        final DescribeLogDirsResult result;
        final int brokerId;

        final Set<String> logDirs = new HashSet<>();
        final Map<TopicPartition, String> curLogDirs = new HashMap<>();
        final Map<TopicPartition, String> futureLogDirs = new HashMap<>();

        public BrokerDirs(DescribeLogDirsResult result, int brokerId) throws ExecutionException, InterruptedException {
            this.result = result;
            this.brokerId = brokerId;

            result.descriptions().get(brokerId).get().forEach((logDirName, logDirInfo) -> {
                logDirs.add(logDirName);
                logDirInfo.replicaInfos().forEach((part, info) -> {
                    if (info.isFuture()) {
                        futureLogDirs.put(part, logDirName);
                    } else {
                        curLogDirs.put(part, logDirName);
                    }
                });
            });
        }
    }

    private void testCancellationAction(boolean useBootstrapServer) throws InterruptedException {
        createTopics();
        TopicPartition foo0 = new TopicPartition("foo", 0);
        TopicPartition baz1 = new TopicPartition("baz", 1);

        produceMessages(foo0.topic(), foo0.partition(), 200);
        produceMessages(baz1.topic(), baz1.partition(), 200);
        String assignment = "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,1,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}," +
                "{\"topic\":\"baz\",\"partition\":1,\"replicas\":[0,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                "]}";
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            assertEquals(unthrottledBrokerConfigs,
                    describeBrokerLevelThrottles(admin, unthrottledBrokerConfigs.keySet()));
            long interBrokerThrottle = 1L;
            runExecuteAssignment(false, assignment, interBrokerThrottle, -1L);
            waitForInterBrokerThrottle(admin, asList(0, 1, 2, 3), interBrokerThrottle);

            Map<TopicPartition, PartitionReassignmentState> partStates = new HashMap<>();

            partStates.put(foo0, new PartitionReassignmentState(asList(0, 1, 3, 2), asList(0, 1, 3), false));
            partStates.put(baz1, new PartitionReassignmentState(asList(0, 2, 3, 1), asList(0, 2, 3), false));

            // Verify that the reassignment is running.  The very low throttle should keep it
            // from completing before this runs.
            waitForVerifyAssignment(admin, assignment, true,
                    new VerifyAssignmentResult(partStates, true, Collections.emptyMap(), false));
            // Cancel the reassignment.
            assertEquals(new AbstractMap.SimpleImmutableEntry<>(new HashSet<>(asList(foo0, baz1)), Collections.emptySet()), runCancelAssignment(assignment, true, useBootstrapServer));
            // Broker throttles are still active because we passed --preserve-throttles
            waitForInterBrokerThrottle(admin, asList(0, 1, 2, 3), interBrokerThrottle);
            // Cancelling the reassignment again should reveal nothing to cancel.
            assertEquals(new AbstractMap.SimpleImmutableEntry<>(Collections.emptySet(), Collections.emptySet()), runCancelAssignment(assignment, false, useBootstrapServer));
            // This time, the broker throttles were removed.
            waitForBrokerLevelThrottles(admin, unthrottledBrokerConfigs);
            // Verify that there are no ongoing reassignments.
            assertFalse(runVerifyAssignment(admin, assignment, false).partsOngoing);
        }
        // Verify that the partition is removed from cancelled replicas
        verifyReplicaDeleted(new TopicPartitionReplica(foo0.topic(), foo0.partition(), 3));
        verifyReplicaDeleted(new TopicPartitionReplica(baz1.topic(), baz1.partition(), 3));
    }

    /**
     * Remove a set of throttled partitions and reset the overall replication quota.
     */
    private void removeReplicationThrottleForPartitions(TopicPartition part) {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            removePartitionReplicaThrottles(admin, new HashSet<>(singleton(part)));
            assertDoesNotThrow(() -> throttleAllBrokersReplication(admin, asList(0, 1, 2, 3), Integer.MAX_VALUE));
        }
    }

    private void removePartitionReplicaThrottles(Admin adminClient, Set<TopicPartition> partitions) {
        Map<ConfigResource, Collection<AlterConfigOp>> throttles = partitions.stream()
                .map(tp -> {
                    ConfigResource resource = new ConfigResource(TOPIC, tp.topic());
                    return new AbstractMap.SimpleEntry<>(
                            resource,
                            asList(
                                    new AlterConfigOp(new ConfigEntry(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, ""),
                                            AlterConfigOp.OpType.DELETE),
                                    new AlterConfigOp(new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, ""),
                                            AlterConfigOp.OpType.DELETE)
                            )
                    );
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertDoesNotThrow(() -> adminClient.incrementalAlterConfigs(throttles).all().get());
    }

    /**
     * Set broker replication quotas and enable throttling for a set of partitions. This
     * will override any previous replication quotas, but will leave the throttling status
     * of other partitions unaffected.
     */
    private void setReplicationThrottleForPartitions(TopicPartition topicPartition) {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            assertDoesNotThrow(() -> throttleAllBrokersReplication(admin, singletonList(4), 1));
            assertDoesNotThrow(() -> assignThrottledPartitionReplicas(admin, singletonMap(topicPartition, singletonList(4))));
        }
    }
}
