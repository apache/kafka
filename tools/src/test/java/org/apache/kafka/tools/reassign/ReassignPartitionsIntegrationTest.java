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

import kafka.admin.ReassignPartitionsCommand;
import kafka.cluster.Partition;
import kafka.log.UnifiedLog;
import kafka.server.ControllerServer;
import kafka.server.HostedPartition;
import kafka.server.IsrChangePropagationConfig;
import kafka.server.KafkaBroker;
import kafka.server.KafkaConfig;
import kafka.server.ZkAlterPartitionManager;
import kafka.test.ClusterConfig;
import kafka.test.ClusterGenerator;
import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterTemplate;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import kafka.test.junit.RaftClusterInvocationContext.RaftClusterInstance;
import kafka.test.junit.ZkClusterInvocationContext;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.None$;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static kafka.test.annotation.Type.KRAFT;
import static kafka.test.annotation.Type.ZK;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.server.common.MetadataVersion.IBP_2_7_IV1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(brokers = 5)
@Tag("integration")
@Timeout(300)
public class ReassignPartitionsIntegrationTest {
    private static final Map<Integer, String> BROKERS = new HashMap<>();

    private static final Map<String, List<List<Integer>>> TOPICS = new HashMap<>();

    private static final Map<Integer, Map<String, Long>> UNTHROTTLED_BROKER_CONFIGS = new HashMap<>();

    static {
        BROKERS.put(0, "rack0");
        BROKERS.put(1, "rack0");
        BROKERS.put(2, "rack1");
        BROKERS.put(3, "rack1");
        BROKERS.put(4, "rack1");

        TOPICS.put("foo", Arrays.asList(Arrays.asList(0, 1, 2), Arrays.asList(1, 2, 3)));
        TOPICS.put("bar", Arrays.asList(Arrays.asList(3, 2, 1)));
        TOPICS.put("baz", Arrays.asList(Arrays.asList(1, 0, 2), Arrays.asList(2, 0, 1), Arrays.asList(0, 2, 1)));

        Map<String, Long> brokerConfig = new HashMap<>();

        ReassignPartitionsCommand.brokerLevelThrottles().foreach(throttle -> {
            brokerConfig.put(throttle, -1L);
            return null;
        });

        IntStream.range(0, 5).forEach(brokerId -> UNTHROTTLED_BROKER_CONFIGS.put(brokerId, brokerConfig));
    }

    private ClusterInstance cluster;

    private Admin adminClient;

    @ClusterTemplate("zkAndKRaft")
    public void testReassignment(ClusterInstance cluster) throws Exception {
        this.cluster = cluster;
        createTopics();
        executeAndVerifyReassignment();
    }

    public static void zkAlterPartitionDisabled(ClusterGenerator generator) {
        ClusterConfig cfg = defaultConfig(ZK);

        BROKERS.forEach((brokerId, rack) ->
            cfg.brokerServerProperties(brokerId).put(KafkaConfig.InterBrokerProtocolVersionProp(), IBP_2_7_IV1.version())
        );

        generator.accept(cfg);
    }

    @ClusterTemplate("zkAlterPartitionDisabled")
    public void testReassignmentWithAlterPartitionDisabled(ClusterInstance cluster) throws Exception {
        this.cluster = cluster;
        // Test reassignment when the IBP is on an older version which does not use
        // the `AlterPartition` API. In this case, the controller will register individual
        // watches for each reassigning partition so that the reassignment can be
        // completed as soon as the ISR is expanded.
        createTopics();
        executeAndVerifyReassignment();
    }

    public static void zkPartialUpgrade(ClusterGenerator generator) {
        // Override change notification settings so that test is not delayed by ISR
        // change notification delay
        ZkAlterPartitionManager.DefaultIsrPropagationConfig_$eq(new IsrChangePropagationConfig(500, 100, 500));
        ClusterConfig cfg = defaultConfig(ZK);

        for (int brokerId = 1; brokerId < 4; brokerId++)
            cfg.brokerServerProperties(brokerId).put(KafkaConfig.InterBrokerProtocolVersionProp(), IBP_2_7_IV1.version());

        generator.accept(cfg);
    }

    @ClusterTemplate("zkPartialUpgrade") // Note: KRaft requires AlterPartition
    public void testReassignmentCompletionDuringPartialUpgrade(ClusterInstance cluster) throws Exception {
        this.cluster = cluster;
        // Test reassignment during a partial upgrade when some brokers are relying on
        // `AlterPartition` and some rely on the old notification logic through Zookeeper.
        // In this test case, broker 0 starts up first on the latest IBP and is typically
        // elected as controller. The three remaining brokers start up on the older IBP.
        // We want to ensure that reassignment can still complete through the ISR change
        // notification path even though the controller expects `AlterPartition`.
        createTopics();
        executeAndVerifyReassignment();
    }

    private void executeAndVerifyReassignment() throws Exception {
        String assignment = "{\"version\":1,\"partitions\":" +
            "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,1,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}," +
            "{\"topic\":\"bar\",\"partition\":0,\"replicas\":[3,2,0],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
            "]}";

        TopicPartition foo0 = new TopicPartition("foo", 0);
        TopicPartition bar0 = new TopicPartition("bar", 0);

        // Check that the assignment has not yet been started yet.
        Map<TopicPartition, PartitionReassignmentState> initialAssignment = new HashMap<>();

        initialAssignment.put(foo0, new PartitionReassignmentState(Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 3), true));
        initialAssignment.put(bar0, new PartitionReassignmentState(Arrays.asList(3, 2, 1), Arrays.asList(3, 2, 0), true));

        waitForVerifyAssignment(adminClient, assignment, false,
            new VerifyAssignmentResult(initialAssignment));

        // Execute the assignment
        runExecuteAssignment(adminClient, false, assignment, -1L, -1L);
        assertEquals(UNTHROTTLED_BROKER_CONFIGS, describeBrokerLevelThrottles(UNTHROTTLED_BROKER_CONFIGS.keySet()));
        Map<TopicPartition, PartitionReassignmentState> finalAssignment = new HashMap<>();
        finalAssignment.put(foo0, new PartitionReassignmentState(Arrays.asList(0, 1, 3), Arrays.asList(0, 1, 3), true));
        finalAssignment.put(bar0, new PartitionReassignmentState(Arrays.asList(3, 2, 0), Arrays.asList(3, 2, 0), true));

        ReassignPartitionsCommand.VerifyAssignmentResult verifyAssignmentResult = runVerifyAssignment(adminClient, assignment, false);
        assertFalse(verifyAssignmentResult.movesOngoing());

        // Wait for the assignment to complete
        waitForVerifyAssignment(adminClient, assignment, false,
            new VerifyAssignmentResult(finalAssignment));

        assertEquals(UNTHROTTLED_BROKER_CONFIGS,
            describeBrokerLevelThrottles(UNTHROTTLED_BROKER_CONFIGS.keySet()));

        // Verify that partitions are removed from brokers no longer assigned
        verifyReplicaDeleted(foo0, 2);
        verifyReplicaDeleted(bar0, 1);
    }

    @ClusterTemplate("zkAndKRaft")
    public void testHighWaterMarkAfterPartitionReassignment(ClusterInstance cluster) throws Exception {
        this.cluster = cluster;
        createTopics();
        String assignment = "{\"version\":1,\"partitions\":" +
            "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[3,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
            "]}";

        // Set the high water mark of foo-0 to 123 on its leader.
        TopicPartition part = new TopicPartition("foo", 0);
        servers().get(0).replicaManager().logManager().truncateFullyAndStartAt(part, 123L, false, None$.empty());

        // Execute the assignment
        runExecuteAssignment(cluster.createAdminClient(), false, assignment, -1L, -1L);
        Map<TopicPartition, PartitionReassignmentState> finalAssignment = Collections.singletonMap(part,
            new PartitionReassignmentState(Arrays.asList(3, 1, 2), Arrays.asList(3, 1, 2), true));

        // Wait for the assignment to complete
        waitForVerifyAssignment(adminClient, assignment, false,
            new VerifyAssignmentResult(finalAssignment));

        TestUtils.waitUntilTrue(() ->
                servers().get(3).replicaManager().onlinePartition(part).
                    map(Partition::leaderLogIfLocal).isDefined(),
            () -> "broker 3 should be the new leader", org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 10L);
        assertEquals(123L, servers().get(3).replicaManager().localLogOrException(part).highWatermark(),
            "Expected broker 3 to have the correct high water mark for the partition.");
    }

    @ClusterTemplate("zkAndKRaft")
    public void testAlterReassignmentThrottle(ClusterInstance cluster) throws Exception {
        this.cluster = cluster;
        createTopics();
        produceMessages("foo", 0, 50);
        produceMessages("baz", 2, 60);
        String assignment = "{\"version\":1,\"partitions\":" +
            "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,3,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}," +
            "{\"topic\":\"baz\",\"partition\":2,\"replicas\":[3,2,1],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
            "]}";

        // Execute the assignment with a low throttle
        long initialThrottle = 1L;
        runExecuteAssignment(adminClient, false, assignment, initialThrottle, -1L);
        waitForInterBrokerThrottle(Arrays.asList(0, 1, 2, 3), initialThrottle);

        // Now update the throttle and verify the reassignment completes
        long updatedThrottle = 300000L;
        runExecuteAssignment(adminClient, true, assignment, updatedThrottle, -1L);
        waitForInterBrokerThrottle(Arrays.asList(0, 1, 2, 3), updatedThrottle);

        Map<TopicPartition, PartitionReassignmentState> finalAssignment = new HashMap<>();
        finalAssignment.put(new TopicPartition("foo", 0),
            new PartitionReassignmentState(Arrays.asList(0, 3, 2), Arrays.asList(0, 3, 2), true));
        finalAssignment.put(new TopicPartition("baz", 2),
            new PartitionReassignmentState(Arrays.asList(3, 2, 1), Arrays.asList(3, 2, 1), true));

        // Now remove the throttles.
        waitForVerifyAssignment(adminClient, assignment, false,
            new VerifyAssignmentResult(finalAssignment));
        waitForBrokerLevelThrottles(UNTHROTTLED_BROKER_CONFIGS);
    }

    /**
     * Test running a reassignment with the interBrokerThrottle set.
     */
    @ClusterTemplate("zkAndKRaft")
    public void testThrottledReassignment(ClusterInstance cluster) throws Exception {
        this.cluster = cluster;
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
            new PartitionReassignmentState(Arrays.asList(0, 1, 2), Arrays.asList(0, 3, 2), true));
        initialAssignment.put(new TopicPartition("baz", 2),
            new PartitionReassignmentState(Arrays.asList(0, 2, 1), Arrays.asList(3, 2, 1), true));
        assertEquals(asScala(new VerifyAssignmentResult(initialAssignment)), runVerifyAssignment(adminClient, assignment, false));
        assertEquals(UNTHROTTLED_BROKER_CONFIGS, describeBrokerLevelThrottles(UNTHROTTLED_BROKER_CONFIGS.keySet()));

        // Execute the assignment
        long interBrokerThrottle = 300000L;
        runExecuteAssignment(adminClient, false, assignment, interBrokerThrottle, -1L);
        waitForInterBrokerThrottle(Arrays.asList(0, 1, 2, 3), interBrokerThrottle);

        Map<TopicPartition, PartitionReassignmentState> finalAssignment = new HashMap<>();
        finalAssignment.put(new TopicPartition("foo", 0),
            new PartitionReassignmentState(Arrays.asList(0, 3, 2), Arrays.asList(0, 3, 2), true));
        finalAssignment.put(new TopicPartition("baz", 2),
            new PartitionReassignmentState(Arrays.asList(3, 2, 1), Arrays.asList(3, 2, 1), true));

        // Wait for the assignment to complete
        TestUtils.waitUntilTrue(
            () -> {
                // Check the reassignment status.
                ReassignPartitionsCommand.VerifyAssignmentResult result = runVerifyAssignment(adminClient, assignment, true);

                if (!result.partsOngoing()) {
                    return true;
                } else {
                    assertFalse(
                        result.partStates().values().forall(ReassignPartitionsCommand.PartitionReassignmentState::done),
                        "Expected at least one partition reassignment to be ongoing when result = " + result
                    );
                    assertEquals(seq(0, 3, 2), result.partStates().get(new TopicPartition("foo", 0)).get().targetReplicas());
                    assertEquals(seq(3, 2, 1), result.partStates().get(new TopicPartition("baz", 2)).get().targetReplicas());
                    System.out.println("Current result: " + result);
                    waitForInterBrokerThrottle(Arrays.asList(0, 1, 2, 3), interBrokerThrottle);
                    return false;
                }
            }, () -> "Expected reassignment to complete.", org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 100L);
        waitForVerifyAssignment(adminClient, assignment, true,
            new VerifyAssignmentResult(finalAssignment));
        // The throttles should still have been preserved, since we ran with --preserve-throttles
        waitForInterBrokerThrottle(Arrays.asList(0, 1, 2, 3), interBrokerThrottle);
        // Now remove the throttles.
        waitForVerifyAssignment(adminClient, assignment, false,
            new VerifyAssignmentResult(finalAssignment));
        waitForBrokerLevelThrottles(UNTHROTTLED_BROKER_CONFIGS);
    }

    @ClusterTemplate("zkAndKRaft")
    public void testProduceAndConsumeWithReassignmentInProgress(ClusterInstance cluster) throws Exception {
        this.cluster = cluster;
        createTopics();
        produceMessages("baz", 2, 60);
        String assignment = "{\"version\":1,\"partitions\":" +
            "[{\"topic\":\"baz\",\"partition\":2,\"replicas\":[3,2,1],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
            "]}";
        runExecuteAssignment(adminClient, false, assignment, 300L, -1L);
        produceMessages("baz", 2, 100);
        Consumer<byte[], byte[]> consumer = TestUtils.createConsumer(cluster.bootstrapServers(),
            "group",
            "earliest",
            true,
            false,
            500,
            SecurityProtocol.PLAINTEXT,
            None$.empty(),
            None$.empty(),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer()
        );

        TopicPartition part = new TopicPartition("baz", 2);
        try {
            consumer.assign(Collections.singleton(part));
            TestUtils.pollUntilAtLeastNumRecords(consumer, 100, org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS);
        } finally {
            consumer.close();
        }
        TestUtils.removeReplicationThrottleForPartitions(adminClient, seq(0, 1, 2, 3), set(part));
        Map<TopicPartition, PartitionReassignmentState> finalAssignment = Collections.singletonMap(part,
            new PartitionReassignmentState(Arrays.asList(3, 2, 1), Arrays.asList(3, 2, 1), true));
        waitForVerifyAssignment(adminClient, assignment, false,
            new VerifyAssignmentResult(finalAssignment));
    }

    /**
     * Test running a reassignment and then cancelling it.
     */
    @ClusterTemplate("zkAndKRaft")
    public void testCancellation(ClusterInstance cluster) throws Exception {
        this.cluster = cluster;
        TopicPartition foo0 = new TopicPartition("foo", 0);
        TopicPartition baz1 = new TopicPartition("baz", 1);

        createTopics();
        produceMessages(foo0.topic(), foo0.partition(), 200);
        produceMessages(baz1.topic(), baz1.partition(), 200);
        String assignment = "{\"version\":1,\"partitions\":" +
            "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,1,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}," +
            "{\"topic\":\"baz\",\"partition\":1,\"replicas\":[0,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
            "]}";
        assertEquals(UNTHROTTLED_BROKER_CONFIGS,
            describeBrokerLevelThrottles(UNTHROTTLED_BROKER_CONFIGS.keySet()));
        long interBrokerThrottle = 1L;
        runExecuteAssignment(adminClient, false, assignment, interBrokerThrottle, -1L);
        waitForInterBrokerThrottle(Arrays.asList(0, 1, 2, 3), interBrokerThrottle);

        Map<TopicPartition, PartitionReassignmentState> partStates = new HashMap<>();

        partStates.put(foo0, new PartitionReassignmentState(Arrays.asList(0, 1, 3, 2), Arrays.asList(0, 1, 3), false));
        partStates.put(baz1, new PartitionReassignmentState(Arrays.asList(0, 2, 3, 1), Arrays.asList(0, 2, 3), false));

        // Verify that the reassignment is running.  The very low throttle should keep it
        // from completing before this runs.
        waitForVerifyAssignment(adminClient, assignment, true,
            new VerifyAssignmentResult(partStates, true, Collections.emptyMap(), false));
        // Cancel the reassignment.
        assertEquals(new Tuple2<>(set(foo0, baz1), set()), runCancelAssignment(adminClient, assignment, true));
        // Broker throttles are still active because we passed --preserve-throttles
        waitForInterBrokerThrottle(Arrays.asList(0, 1, 2, 3), interBrokerThrottle);
        // Cancelling the reassignment again should reveal nothing to cancel.
        assertEquals(new Tuple2<>(set(), set()), runCancelAssignment(adminClient, assignment, false));
        // This time, the broker throttles were removed.
        waitForBrokerLevelThrottles(UNTHROTTLED_BROKER_CONFIGS);
        // Verify that there are no ongoing reassignments.
        assertFalse(runVerifyAssignment(adminClient, assignment, false).partsOngoing());
        // Verify that the partition is removed from cancelled replicas
        verifyReplicaDeleted(foo0, 3);
        verifyReplicaDeleted(baz1, 3);
    }

    @ClusterTemplate("zkAndKRaft")
    public void testCancellationWithAddingReplicaInIsr(ClusterInstance cluster) throws Exception {
        this.cluster = cluster;
        TopicPartition foo0 = new TopicPartition("foo", 0);

        createTopics();
        produceMessages(foo0.topic(), foo0.partition(), 200);

        // The reassignment will bring replicas 3 and 4 into the replica set and remove 1 and 2.
        String assignment = "{\"version\":1,\"partitions\":" +
            "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,3,4],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
            "]}";

        // We will throttle replica 4 so that only replica 3 joins the ISR
        TestUtils.setReplicationThrottleForPartitions(
            adminClient,
            seq(4),
            set(foo0),
            1
        );

        // Execute the assignment and wait for replica 3 (only) to join the ISR
        runExecuteAssignment(
            adminClient,
            false,
            assignment,
            -1L,
            -1L
        );
        TestUtils.waitUntilTrue(
            () -> Objects.equals(TestUtils.currentIsr(adminClient, foo0), set(0, 1, 2, 3)),
            () -> "Timed out while waiting for replica 3 to join the ISR",
            org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 100L
        );

        // Now cancel the assignment and verify that the partition is removed from cancelled replicas
        assertEquals(new Tuple2<>(set(foo0), set()), runCancelAssignment(adminClient, assignment, true));
        verifyReplicaDeleted(foo0, 3);
        verifyReplicaDeleted(foo0, 4);
    }

    private void verifyReplicaDeleted(
        TopicPartition topicPartition,
        Integer replicaId
    ) {
        TestUtils.waitUntilTrue(() -> {
            KafkaBroker server = servers().get(replicaId);
            HostedPartition partition = server.replicaManager().getPartition(topicPartition);
            Option<UnifiedLog> log = server.logManager().getLog(topicPartition, false);
            return partition == HostedPartition.None$.MODULE$ && log.isEmpty();
        }, () -> "Timed out waiting for replica " + replicaId + " of " + topicPartition + " to be deleted",
            org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 100L);
    }

    private void waitForLogDirThrottle(Set<Integer> throttledBrokers, Long logDirThrottle) {
        Map<String, Long> throttledConfigMap = new HashMap<>();
        throttledConfigMap.put(ReassignPartitionsCommand.brokerLevelLeaderThrottle(), -1L);
        throttledConfigMap.put(ReassignPartitionsCommand.brokerLevelFollowerThrottle(), -1L);
        throttledConfigMap.put(ReassignPartitionsCommand.brokerLevelLogDirThrottle(), logDirThrottle);
        waitForBrokerThrottles(throttledBrokers, throttledConfigMap);
    }

    private void waitForInterBrokerThrottle(List<Integer> throttledBrokers, Long interBrokerThrottle) {
        Map<String, Long> throttledConfigMap = new HashMap<>();
        throttledConfigMap.put(ReassignPartitionsCommand.brokerLevelLeaderThrottle(), interBrokerThrottle);
        throttledConfigMap.put(ReassignPartitionsCommand.brokerLevelFollowerThrottle(), interBrokerThrottle);
        throttledConfigMap.put(ReassignPartitionsCommand.brokerLevelLogDirThrottle(), -1L);
        waitForBrokerThrottles(throttledBrokers, throttledConfigMap);
    }

    private void waitForBrokerThrottles(Collection<Integer> throttledBrokers, Map<String, Long> throttleConfig) {
        Map<Integer, Map<String, Long>> throttledBrokerConfigs = new HashMap<>();
        UNTHROTTLED_BROKER_CONFIGS.forEach((brokerId, unthrottledConfig) -> {
            Map<String, Long> expectedThrottleConfig = throttledBrokers.contains(brokerId)
                ? throttleConfig
                : unthrottledConfig;
            throttledBrokerConfigs.put(brokerId, expectedThrottleConfig);
        });
        waitForBrokerLevelThrottles(throttledBrokerConfigs);
    }

    private void waitForBrokerLevelThrottles(Map<Integer, Map<String, Long>> targetThrottles) {
        AtomicReference<Map<Integer, Map<String, Long>>> curThrottles = new AtomicReference<>(new HashMap<>());
        TestUtils.waitUntilTrue(() -> {
            try {
                curThrottles.set(describeBrokerLevelThrottles(targetThrottles.keySet()));
                return targetThrottles.equals(curThrottles.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, () -> "timed out waiting for broker throttle to become " + targetThrottles + ".  " +
            "Latest throttles were " + curThrottles.get(), org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 25);
    }

    /**
     * Describe the broker-level throttles in the cluster.
     *
     * @return                A map whose keys are broker IDs and whose values are throttle
     *                        information.  The nested maps are keyed on throttle name.
     */
    private Map<Integer, Map<String, Long>> describeBrokerLevelThrottles(Collection<Integer> brokerIds) throws Exception {
        Map<Integer, Map<String, Long>> results = new HashMap<>();
        for (Integer brokerId : brokerIds) {
            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString());
            Config brokerConfigs = adminClient.describeConfigs(Collections.singleton(brokerResource)).values()
                .get(brokerResource)
                .get();

            Map<String, Long> throttles = new HashMap<>();
            ReassignPartitionsCommand.brokerLevelThrottles().foreach(throttleName -> {
                String configValue = Optional.ofNullable(brokerConfigs.get(throttleName)).map(ConfigEntry::value).orElse("-1");
                throttles.put(throttleName, Long.parseLong(configValue));
                return null;
            });
            results.put(brokerId, throttles);
        }
        return results;
    }

    /**
     * Test moving partitions between directories.
     */
    @ClusterTemplate("zk") // JBOD not yet implemented for KRaft
    public void testLogDirReassignment(ClusterInstance cluster) throws Exception {
        this.cluster = cluster;

        TopicPartition topicPartition = new TopicPartition("foo", 0);

        createTopics();
        produceMessages(topicPartition.topic(), topicPartition.partition(), 700);

        int targetBrokerId = 0;
        List<Integer> replicas = Arrays.asList(0, 1, 2);
        LogDirReassignment reassignment = buildLogDirReassignment(topicPartition, targetBrokerId, replicas);

        // Start the replica move, but throttle it to be very slow so that it can't complete
        // before our next checks happen.
        long logDirThrottle = 1L;
        runExecuteAssignment(adminClient, false, reassignment.json,
            -1L, logDirThrottle);

        // Check the output of --verify
        waitForVerifyAssignment(adminClient, reassignment.json, true,
            new VerifyAssignmentResult(Collections.singletonMap(
                topicPartition, new PartitionReassignmentState(Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2), true)
            ), false, Collections.singletonMap(
                new TopicPartitionReplica(topicPartition.topic(), topicPartition.partition(), 0),
                new ActiveMoveState(reassignment.currentDir, reassignment.targetDir, reassignment.targetDir)
            ), true));
        waitForLogDirThrottle(Collections.singleton(0), logDirThrottle);

        // Remove the throttle
        adminClient.incrementalAlterConfigs(Collections.singletonMap(
                new ConfigResource(ConfigResource.Type.BROKER, "0"),
                Collections.singletonList(new AlterConfigOp(
                    new ConfigEntry(ReassignPartitionsCommand.brokerLevelLogDirThrottle(), ""), AlterConfigOp.OpType.DELETE))))
            .all().get();
        waitForBrokerLevelThrottles(UNTHROTTLED_BROKER_CONFIGS);

        // Wait for the directory movement to complete.
        waitForVerifyAssignment(adminClient, reassignment.json, true,
            new VerifyAssignmentResult(Collections.singletonMap(
                topicPartition, new PartitionReassignmentState(Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2), true)
            ), false, Collections.singletonMap(
                new TopicPartitionReplica(topicPartition.topic(), topicPartition.partition(), 0),
                new CompletedMoveState(reassignment.targetDir)
            ), false));

        BrokerDirs info1 = new BrokerDirs(adminClient.describeLogDirs(IntStream.range(0, 4).boxed().collect(Collectors.toList())), 0);
        assertEquals(reassignment.targetDir, info1.curLogDirs.getOrDefault(topicPartition, ""));
    }

    @ClusterTemplate("zk") // JBOD not yet implemented for KRaft
    public void testAlterLogDirReassignmentThrottle(ClusterInstance cluster) throws Exception {
        this.cluster = cluster;

        TopicPartition topicPartition = new TopicPartition("foo", 0);

        createTopics();
        produceMessages(topicPartition.topic(), topicPartition.partition(), 700);

        int targetBrokerId = 0;
        List<Integer> replicas = Arrays.asList(0, 1, 2);
        LogDirReassignment reassignment = buildLogDirReassignment(topicPartition, targetBrokerId, replicas);

        // Start the replica move with a low throttle so it does not complete
        long initialLogDirThrottle = 1L;
        runExecuteAssignment(adminClient, false, reassignment.json,
            -1L, initialLogDirThrottle);
        waitForLogDirThrottle(new HashSet<>(Collections.singletonList(0)), initialLogDirThrottle);

        // Now increase the throttle and verify that the log dir movement completes
        long updatedLogDirThrottle = 3000000L;
        runExecuteAssignment(adminClient, true, reassignment.json,
            -1L, updatedLogDirThrottle);
        waitForLogDirThrottle(Collections.singleton(0), updatedLogDirThrottle);

        waitForVerifyAssignment(adminClient, reassignment.json, true,
            new VerifyAssignmentResult(Collections.singletonMap(
                topicPartition, new PartitionReassignmentState(Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 2), true)
            ), false, Collections.singletonMap(
                new TopicPartitionReplica(topicPartition.topic(), topicPartition.partition(), targetBrokerId),
                new CompletedMoveState(reassignment.targetDir)
            ), false));
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
                                                       List<Integer> replicas) throws Exception {

        DescribeLogDirsResult describeLogDirsResult = adminClient.describeLogDirs(
            IntStream.range(0, 5).boxed().collect(Collectors.toList()));

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

    private ReassignPartitionsCommand.VerifyAssignmentResult runVerifyAssignment(Admin adminClient, String jsonString,
                                                                                 boolean preserveThrottles) {
        System.out.println("==> verifyAssignment(adminClient, jsonString=" + jsonString);
        return ReassignPartitionsCommand.verifyAssignment(adminClient, jsonString, preserveThrottles);
    }

    private void waitForVerifyAssignment(Admin adminClient,
                                         String jsonString,
                                         Boolean preserveThrottles,
                                         VerifyAssignmentResult expectedResult) {
        final ReassignPartitionsCommand.VerifyAssignmentResult expectedResult0 = asScala(expectedResult);

        final ReassignPartitionsCommand.VerifyAssignmentResult[] latestResult = {null};
        TestUtils.waitUntilTrue(
            () -> {
                latestResult[0] = runVerifyAssignment(adminClient, jsonString, preserveThrottles);
                return expectedResult0.equals(latestResult[0]);
            }, () -> "Timed out waiting for verifyAssignment result " + expectedResult + ".  " +
                "The latest result was " + latestResult[0], org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 10L);
    }

    private void runExecuteAssignment(Admin adminClient,
                                      Boolean additional,
                                      String reassignmentJson,
                                      Long interBrokerThrottle,
                                      Long replicaAlterLogDirsThrottle) {
        System.out.println("==> executeAssignment(adminClient, additional=" + additional + ", " +
            "reassignmentJson=" + reassignmentJson + ", " +
            "interBrokerThrottle=" + interBrokerThrottle + ", " +
            "replicaAlterLogDirsThrottle=" + replicaAlterLogDirsThrottle + "))");
        ReassignPartitionsCommand.executeAssignment(adminClient, additional, reassignmentJson,
            interBrokerThrottle, replicaAlterLogDirsThrottle, 10000L, Time.SYSTEM);
    }

    private Tuple2<scala.collection.immutable.Set<TopicPartition>, scala.collection.immutable.Set<TopicPartitionReplica>> runCancelAssignment(Admin adminClient, String jsonString, Boolean preserveThrottles) {
        System.out.println("==> cancelAssignment(adminClient, jsonString=" + jsonString);
        return ReassignPartitionsCommand.cancelAssignment(adminClient, jsonString, preserveThrottles, 10000L, Time.SYSTEM);
    }

    static class BrokerDirs {
        final DescribeLogDirsResult result;
        final int brokerId;

        final Set<String> logDirs = new HashSet<>();
        final Map<TopicPartition, String> curLogDirs = new HashMap<>();
        final Map<TopicPartition, String> futureLogDirs = new HashMap<>();

        public BrokerDirs(DescribeLogDirsResult result, int brokerId) throws Exception {
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

    private void createTopics() throws Exception {
        adminClient = cluster.createAdminClient();

        adminClient.createTopics(TOPICS.entrySet().stream().map(e -> {
            Map<Integer, List<Integer>> partMap = new HashMap<>();

            Iterator<List<Integer>> partsIter = e.getValue().iterator();
            int index = 0;
            while (partsIter.hasNext()) {
                partMap.put(index, partsIter.next());
                index++;
            }
            return new NewTopic(e.getKey(), partMap);
        }).collect(Collectors.toList())).all().get();

        TOPICS.forEach((topicName, parts) ->
            TestUtils.waitForAllPartitionsMetadata(seq(servers()), topicName, parts.size()));

        if (cluster.isKRaftTest()) {
            TestUtils.ensureConsistentKRaftMetadata(
                seq(servers()),
                controllerServer(),
                "Timeout waiting for controller metadata propagating to brokers"
            );
        }
    }

    public void produceMessages(String topic, int partition, int numMessages) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            IntStream.range(0, numMessages).forEach(i ->
                producer.send(new ProducerRecord<>(topic, partition, null, new byte[10000])));
        }
    }

    public List<KafkaBroker> servers() {
        Stream<? extends KafkaBroker> brokers = cluster.isKRaftTest()
            ? ((RaftClusterInstance) cluster).brokers()
            : ((ZkClusterInvocationContext.ZkClusterInstance) cluster).servers();

        return brokers.collect(Collectors.toList());
    }

    public ControllerServer controllerServer() {
        return ((RaftClusterInstance) cluster).controllerServers().iterator().next();
    }

    public static void zkAndKRaft(ClusterGenerator generator) {
        generator.accept(defaultConfig(ZK));
        generator.accept(defaultConfig(KRAFT));
    }

    public static void zk(ClusterGenerator generator) {
        generator.accept(defaultConfig(ZK));
    }

    private static ClusterConfig defaultConfig(Type type) {
        ClusterConfig clusterConfig = ClusterConfig.defaultClusterBuilder()
            .type(type)
            .brokers(5)
            .build();

        BROKERS.forEach((brokerId, rack) -> {
            Properties config = clusterConfig.brokerServerProperties(brokerId);
            config.put(KafkaConfig.RackProp(), rack);
            config.put(KafkaConfig.ControlledShutdownEnableProp(), false); // shorten test time
            String logDirs = IntStream.range(1, 4).mapToObj(i -> {
                // We would like to allow user to specify both relative path and absolute path as log directory for backward-compatibility reason
                // We can verify this by using a mixture of relative path and absolute path as log directories in the test
                return (i % 2 == 0
                    ? TestUtils.tempDir()
                    : TestUtils.tempRelativeDir("data")).getAbsolutePath();
            }).collect(Collectors.joining(","));
            config.put(KafkaConfig.LogDirsProp(), logDirs);
            // shorter backoff to reduce test durations when no active partitions are eligible for fetching due to throttling
            config.setProperty(KafkaConfig.ReplicaFetchBackoffMsProp(), "100");
            // Don't move partition leaders automatically.
            config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp(), "false");
            config.setProperty(KafkaConfig.ReplicaLagTimeMaxMsProp(), "1000");
        });

        return clusterConfig;
    }

    private ReassignPartitionsCommand.VerifyAssignmentResult asScala(VerifyAssignmentResult res) {
        Map<TopicPartition, ReassignPartitionsCommand.PartitionReassignmentState> partStates = new HashMap<>();
        res.partStates.forEach((tp, state) -> partStates.put(tp, asScala(state)));

        Map<TopicPartitionReplica, ReassignPartitionsCommand.LogDirMoveState> moveStates = new HashMap<>();
        res.moveStates.forEach((tpr, state) -> moveStates.put(tpr, asScala(state)));

        return new ReassignPartitionsCommand.VerifyAssignmentResult(asScala(partStates), res.partsOngoing, asScala(moveStates), res.movesOngoing);
    }

    @SuppressWarnings("unchecked")
    private ReassignPartitionsCommand.PartitionReassignmentState asScala(PartitionReassignmentState state) {
        return new ReassignPartitionsCommand.PartitionReassignmentState(
            seq((List) state.currentReplicas),
            seq((List) state.targetReplicas),
            state.done
        );
    }

    private ReassignPartitionsCommand.LogDirMoveState asScala(LogDirMoveState state) {
        if (state instanceof ActiveMoveState) {
            ActiveMoveState s = (ActiveMoveState) state;
            return new ReassignPartitionsCommand.ActiveMoveState(s.currentLogDir, s.targetLogDir, s.futureLogDir);
        } else if (state instanceof CancelledMoveState) {
            CancelledMoveState s = (CancelledMoveState) state;
            return new ReassignPartitionsCommand.CancelledMoveState(s.currentLogDir, s.targetLogDir);
        } else if (state instanceof CompletedMoveState) {
            CompletedMoveState s = (CompletedMoveState) state;
            return new ReassignPartitionsCommand.CompletedMoveState(s.targetLogDir);
        } else if (state instanceof MissingLogDirMoveState) {
            MissingLogDirMoveState s = (MissingLogDirMoveState) state;
            return new ReassignPartitionsCommand.MissingLogDirMoveState(s.targetLogDir);
        } else if (state instanceof MissingReplicaMoveState) {
            MissingReplicaMoveState s = (MissingReplicaMoveState) state;
            return new ReassignPartitionsCommand.MissingReplicaMoveState(s.targetLogDir);
        }

        throw new IllegalArgumentException("Unknown state " + state);
    }

    @SuppressWarnings("unchecked")
    static <T> scala.collection.immutable.Set<T> set(final T... set) {
        return mutableSet(set).toSet();
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    private static <T> scala.collection.mutable.Set<T> mutableSet(final T...set) {
        return JavaConverters.asScalaSet(new HashSet<>(Arrays.asList(set)));
    }

    @SuppressWarnings({"unchecked"})
    private static <T> Seq<T> seq(T... seq) {
        return seq(Arrays.asList(seq));
    }

    @SuppressWarnings({"deprecation"})
    private static <T> Seq<T> seq(Collection<T> seq) {
        return JavaConverters.asScalaIteratorConverter(seq.iterator()).asScala().toSeq();
    }

    @SuppressWarnings("deprecation")
    private static <K, V> scala.collection.Map<K, V> asScala(Map<K, V> jmap) {
        return JavaConverters.mapAsScalaMap(jmap);
    }
}
