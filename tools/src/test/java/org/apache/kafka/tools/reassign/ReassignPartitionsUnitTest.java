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

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.placement.UsableBroker;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.tools.AdminCommandFailedException;
import org.apache.kafka.tools.AdminOperationException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.alterPartitionReassignments;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.alterReplicaLogDirs;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.calculateFollowerThrottles;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.calculateLeaderThrottles;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.calculateMovingBrokers;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.calculateProposedMoveMap;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.calculateReassigningBrokers;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.cancelPartitionReassignments;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.compareTopicPartitionReplicas;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.compareTopicPartitions;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.curReassignmentsToString;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.currentPartitionReplicaAssignmentToString;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.executeAssignment;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.findLogDirMoveStates;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.findPartitionReassignmentStates;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.generateAssignment;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.getBrokerMetadata;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.getReplicaAssignmentForTopics;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.getReplicaToLogDir;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.modifyInterBrokerThrottle;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.modifyLogDirThrottle;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.modifyTopicThrottles;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.parseExecuteAssignmentArgs;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.parseGenerateAssignmentArgs;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.partitionReassignmentStatesToString;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.replicaMoveStatesToString;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.toReplicaIds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(60)
public class ReassignPartitionsUnitTest {
    @BeforeAll
    public static void setUp() {
        Exit.setExitProcedure((statusCode, message) -> {
            throw new IllegalArgumentException(message);
        });
    }

    @AfterAll
    public static void tearDown() {
        Exit.resetExitProcedure();
    }

    @Test
    public void testCompareTopicPartitions() {
        assertTrue(compareTopicPartitions(new TopicPartition("abc", 0),
            new TopicPartition("abc", 1)) < 0);
        assertFalse(compareTopicPartitions(new TopicPartition("def", 0),
            new TopicPartition("abc", 1)) < 0);
    }

    @Test
    public void testCompareTopicPartitionReplicas() {
        assertTrue(compareTopicPartitionReplicas(new TopicPartitionReplica("def", 0, 0),
            new TopicPartitionReplica("abc", 0, 1)) < 0);
        assertFalse(compareTopicPartitionReplicas(new TopicPartitionReplica("def", 0, 0),
            new TopicPartitionReplica("cde", 0, 0)) < 0);
    }

    @Test
    public void testPartitionReassignStatesToString() {
        Map<TopicPartition, PartitionReassignmentState> states = new HashMap<>();

        states.put(new TopicPartition("foo", 0),
            new PartitionReassignmentState(List.of(1, 2, 3), List.of(1, 2, 3), true));
        states.put(new TopicPartition("foo", 1),
            new PartitionReassignmentState(List.of(1, 2, 3), List.of(1, 2, 4), false));
        states.put(new TopicPartition("bar", 0),
            new PartitionReassignmentState(List.of(1, 2, 3), List.of(1, 2, 4), false));

        assertEquals(String.join(System.lineSeparator(),
            "Status of partition reassignment:",
            "Reassignment of partition bar-0 is still in progress.",
            "Reassignment of partition foo-0 is completed.",
            "Reassignment of partition foo-1 is still in progress."),
            partitionReassignmentStatesToString(states));
    }

    private void addTopics(MockAdminClient adminClient) {
        List<Node> b = adminClient.brokers();
        adminClient.addTopic(false, "foo", List.of(
            new TopicPartitionInfo(0, b.get(0),
                List.of(b.get(0), b.get(1), b.get(2)),
                List.of(b.get(0), b.get(1))),
            new TopicPartitionInfo(1, b.get(1),
                List.of(b.get(1), b.get(2), b.get(3)),
                List.of(b.get(1), b.get(2), b.get(3)))
        ), Map.of());
        adminClient.addTopic(false, "bar", List.of(
            new TopicPartitionInfo(0, b.get(2),
                List.of(b.get(2), b.get(3), b.get(0)),
                List.of(b.get(2), b.get(3), b.get(0)))
        ), Map.of());
    }

    @Test
    public void testFindPartitionReassignmentStates() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            // Create a reassignment and test findPartitionReassignmentStates.
            Map<TopicPartition, List<Integer>> reassignments = new HashMap<>();

            reassignments.put(new TopicPartition("foo", 0), List.of(0, 1, 3));
            reassignments.put(new TopicPartition("quux", 0), List.of(1, 2, 3));

            Map<TopicPartition, Throwable> reassignmentResult = alterPartitionReassignments(adminClient, reassignments,  false);

            assertEquals(1, reassignmentResult.size());
            assertEquals(UnknownTopicOrPartitionException.class, reassignmentResult.get(new TopicPartition("quux", 0)).getClass());

            Map<TopicPartition, PartitionReassignmentState> expStates = new HashMap<>();

            expStates.put(new TopicPartition("foo", 0),
                new PartitionReassignmentState(List.of(0, 1, 2), List.of(0, 1, 3), false));
            expStates.put(new TopicPartition("foo", 1),
                new PartitionReassignmentState(List.of(1, 2, 3), List.of(1, 2, 3), true));

            Entry<Map<TopicPartition, PartitionReassignmentState>, Boolean> actual =
                findPartitionReassignmentStates(adminClient, List.of(
                    new SimpleImmutableEntry<>(new TopicPartition("foo", 0), List.of(0, 1, 3)),
                    new SimpleImmutableEntry<>(new TopicPartition("foo", 1), List.of(1, 2, 3))
                ));

            assertEquals(expStates, actual.getKey());
            assertTrue(actual.getValue());

            // Cancel the reassignment and test findPartitionReassignmentStates again.
            Map<TopicPartition, Throwable> cancelResult = cancelPartitionReassignments(adminClient,
                Set.of(new TopicPartition("foo", 0), new TopicPartition("quux", 2)));

            assertEquals(1, cancelResult.size());
            assertEquals(UnknownTopicOrPartitionException.class, cancelResult.get(new TopicPartition("quux", 2)).getClass());

            expStates.clear();

            expStates.put(new TopicPartition("foo", 0),
                new PartitionReassignmentState(List.of(0, 1, 2), List.of(0, 1, 3), true));
            expStates.put(new TopicPartition("foo", 1),
                new PartitionReassignmentState(List.of(1, 2, 3), List.of(1, 2, 3), true));

            actual = findPartitionReassignmentStates(adminClient, List.of(
                new SimpleImmutableEntry<>(new TopicPartition("foo", 0), List.of(0, 1, 3)),
                new SimpleImmutableEntry<>(new TopicPartition("foo", 1), List.of(1, 2, 3))
            ));

            assertEquals(expStates, actual.getKey());
            assertFalse(actual.getValue());
        }
    }

    @Test
    public void testFindLogDirMoveStates() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().
                numBrokers(4).
                brokerLogDirs(List.of(
                    List.of("/tmp/kafka-logs0", "/tmp/kafka-logs1"),
                    List.of("/tmp/kafka-logs0", "/tmp/kafka-logs1"),
                    List.of("/tmp/kafka-logs0", "/tmp/kafka-logs1"),
                    Arrays.asList("/tmp/kafka-logs0", null)))
                .build()) {

            addTopics(adminClient);
            List<Node> b = adminClient.brokers();
            adminClient.addTopic(false, "quux", List.of(
                    new TopicPartitionInfo(0, b.get(2),
                        List.of(b.get(1), b.get(2), b.get(3)),
                        List.of(b.get(1), b.get(2), b.get(3)))),
                Map.of());

            Map<TopicPartitionReplica, String> replicaAssignment = new HashMap<>();

            replicaAssignment.put(new TopicPartitionReplica("foo", 0, 0), "/tmp/kafka-logs1");
            replicaAssignment.put(new TopicPartitionReplica("quux", 0, 0), "/tmp/kafka-logs1");

            adminClient.alterReplicaLogDirs(replicaAssignment).all().get();

            Map<TopicPartitionReplica, LogDirMoveState> states = new HashMap<>();

            states.put(new TopicPartitionReplica("bar", 0, 0), new CompletedMoveState("/tmp/kafka-logs0"));
            states.put(new TopicPartitionReplica("foo", 0, 0), new ActiveMoveState("/tmp/kafka-logs0",
                "/tmp/kafka-logs1", "/tmp/kafka-logs1"));
            states.put(new TopicPartitionReplica("foo", 1, 0), new CancelledMoveState("/tmp/kafka-logs0",
                "/tmp/kafka-logs1"));
            states.put(new TopicPartitionReplica("quux", 1, 0), new MissingLogDirMoveState("/tmp/kafka-logs1"));
            states.put(new TopicPartitionReplica("quuz", 0, 0), new MissingReplicaMoveState("/tmp/kafka-logs0"));

            Map<TopicPartitionReplica, String> targetMoves = new HashMap<>();

            targetMoves.put(new TopicPartitionReplica("bar", 0, 0), "/tmp/kafka-logs0");
            targetMoves.put(new TopicPartitionReplica("foo", 0, 0), "/tmp/kafka-logs1");
            targetMoves.put(new TopicPartitionReplica("foo", 1, 0), "/tmp/kafka-logs1");
            targetMoves.put(new TopicPartitionReplica("quux", 1, 0), "/tmp/kafka-logs1");
            targetMoves.put(new TopicPartitionReplica("quuz", 0, 0), "/tmp/kafka-logs0");

            assertEquals(states, findLogDirMoveStates(adminClient, targetMoves));
        }
    }

    @Test
    public void testReplicaMoveStatesToString() {
        Map<TopicPartitionReplica, LogDirMoveState> states = new HashMap<>();

        states.put(new TopicPartitionReplica("bar", 0, 0), new CompletedMoveState("/tmp/kafka-logs0"));
        states.put(new TopicPartitionReplica("foo", 0, 0), new ActiveMoveState("/tmp/kafka-logs0",
            "/tmp/kafka-logs1", "/tmp/kafka-logs1"));
        states.put(new TopicPartitionReplica("foo", 1, 0), new CancelledMoveState("/tmp/kafka-logs0",
            "/tmp/kafka-logs1"));
        states.put(new TopicPartitionReplica("quux", 0, 0), new MissingReplicaMoveState("/tmp/kafka-logs1"));
        states.put(new TopicPartitionReplica("quux", 1, 1), new ActiveMoveState("/tmp/kafka-logs0",
            "/tmp/kafka-logs1", "/tmp/kafka-logs2"));
        states.put(new TopicPartitionReplica("quux", 2, 1), new MissingLogDirMoveState("/tmp/kafka-logs1"));

        assertEquals(String.join(System.lineSeparator(),
            "Reassignment of replica bar-0-0 completed successfully.",
            "Reassignment of replica foo-0-0 is still in progress.",
            "Partition foo-1 on broker 0 is not being moved from log dir /tmp/kafka-logs0 to /tmp/kafka-logs1.",
            "Partition quux-0 cannot be found in any live log directory on broker 0.",
            "Partition quux-1 on broker 1 is being moved to log dir /tmp/kafka-logs2 instead of /tmp/kafka-logs1.",
            "Partition quux-2 is not found in any live log dir on broker 1. " +
                "There is likely an offline log directory on the broker."),
            replicaMoveStatesToString(states));
    }

    @Test
    public void testGetReplicaAssignments() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);

            Map<TopicPartition, List<Integer>> assignments = new HashMap<>();

            assignments.put(new TopicPartition("foo", 0), List.of(0, 1, 2));
            assignments.put(new TopicPartition("foo", 1), List.of(1, 2, 3));

            assertEquals(
                assignments,
                toReplicaIds(getReplicaAssignmentForTopics(adminClient, List.of("foo")))
            );

            assignments.clear();

            assignments.put(new TopicPartition("foo", 0), List.of(0, 1, 2));
            assignments.put(new TopicPartition("bar", 0), List.of(2, 3, 0));

            Map<TopicPartition, List<Integer>> actualAssignments = toReplicaIds(
                ReassignPartitionsCommand.getReplicasForPartitions(
                    adminClient,
                    Set.of(new TopicPartition("foo", 0), new TopicPartition("bar", 0))
            ));
            assertEquals(
                assignments,
                actualAssignments
            );

            UnknownTopicOrPartitionException exception =
                assertInstanceOf(UnknownTopicOrPartitionException.class,
                    assertThrows(ExecutionException.class,
                        () -> ReassignPartitionsCommand.getReplicasForPartitions(adminClient,
                            Set.of(new TopicPartition("foo", 0), new TopicPartition("foo", 10)))).getCause());
            assertEquals("Unable to find partition: foo-10", exception.getMessage());
        }
    }

    @Test
    public void testGetBrokerRackInformation() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().
            brokers(List.of(new Node(0, "localhost", 9092, "rack0"),
                new Node(1, "localhost", 9093, "rack1"),
                new Node(2, "localhost", 9094, null))).
            build()) {

            assertEquals(List.of(
                new UsableBroker(0, Optional.of("rack0"), false),
                new UsableBroker(1, Optional.of("rack1"), false)
            ), getBrokerMetadata(adminClient, List.of(0, 1), true));
            assertEquals(List.of(
                new UsableBroker(0, Optional.empty(), false),
                new UsableBroker(1, Optional.empty(), false)
            ), getBrokerMetadata(adminClient, List.of(0, 1), false));
            assertStartsWith("Not all brokers have rack information",
                assertThrows(AdminOperationException.class,
                    () -> getBrokerMetadata(adminClient, List.of(1, 2), true)).getMessage());
            assertEquals(List.of(
                new UsableBroker(1, Optional.empty(), false),
                new UsableBroker(2, Optional.empty(), false)
            ), getBrokerMetadata(adminClient, List.of(1, 2), false));
        }
    }

    @Test
    public void testParseGenerateAssignmentArgs() throws Exception {
        assertStartsWith("Broker list contains duplicate entries",
            assertThrows(AdminCommandFailedException.class, () -> parseGenerateAssignmentArgs(
                "{\"topics\": [{\"topic\": \"foo\"}], \"version\":1}", "1,1,2"),
                "Expected to detect duplicate broker list entries").getMessage());
        assertStartsWith("Broker list contains duplicate entries",
            assertThrows(AdminCommandFailedException.class, () -> parseGenerateAssignmentArgs(
                "{\"topics\": [{\"topic\": \"foo\"}], \"version\":1}", "5,2,3,4,5"),
                "Expected to detect duplicate broker list entries").getMessage());
        assertEquals(new SimpleImmutableEntry<>(List.of(5, 2, 3, 4), List.of("foo")),
            parseGenerateAssignmentArgs("{\"topics\": [{\"topic\": \"foo\"}], \"version\":1}", "5,2,3,4"));
        assertStartsWith("List of topics to reassign contains duplicate entries",
            assertThrows(AdminCommandFailedException.class, () -> parseGenerateAssignmentArgs(
                "{\"topics\": [{\"topic\": \"foo\"},{\"topic\": \"foo\"}], \"version\":1}", "5,2,3,4"),
                "Expected to detect duplicate topic entries").getMessage());
        assertEquals(new SimpleImmutableEntry<>(List.of(5, 3, 4), List.of("foo", "bar")),
            parseGenerateAssignmentArgs(
                "{\"topics\": [{\"topic\": \"foo\"},{\"topic\": \"bar\"}], \"version\":1}", "5,3,4"));
    }

    @Test
    public void testGenerateAssignmentFailsWithoutEnoughReplicas() {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            assertStartsWith("The target replication factor of 3 cannot be reached because only 2 broker(s) are registered",
                assertThrows(InvalidReplicationFactorException.class,
                    () -> generateAssignment(adminClient, "{\"topics\":[{\"topic\":\"foo\"},{\"topic\":\"bar\"}]}", "0,1", false),
                    "Expected generateAssignment to fail").getMessage());
        }
    }

    @Test
    public void testGenerateAssignmentWithInvalidPartitionsFails() {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(5).build()) {
            addTopics(adminClient);
            assertStartsWith("Topic quux not found",
                assertThrows(ExecutionException.class,
                    () -> generateAssignment(adminClient, "{\"topics\":[{\"topic\":\"foo\"},{\"topic\":\"quux\"}]}", "0,1", false),
                    "Expected generateAssignment to fail").getCause().getMessage());
        }
    }

    @Test
    public void testGenerateAssignmentWithInconsistentRacks() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().
            brokers(List.of(
                new Node(0, "localhost", 9092, "rack0"),
                new Node(1, "localhost", 9093, "rack0"),
                new Node(2, "localhost", 9094, null),
                new Node(3, "localhost", 9095, "rack1"),
                new Node(4, "localhost", 9096, "rack1"),
                new Node(5, "localhost", 9097, "rack2"))).
            build()) {

            addTopics(adminClient);
            assertStartsWith("Not all brokers have rack information.",
                assertThrows(AdminOperationException.class,
                    () -> generateAssignment(adminClient, "{\"topics\":[{\"topic\":\"foo\"}]}", "0,1,2,3", true),
                    "Expected generateAssignment to fail").getMessage());
            // It should succeed when --disable-rack-aware is used.
            Entry<Map<TopicPartition, List<Integer>>, Map<TopicPartition, List<Integer>>>
                proposedCurrent = generateAssignment(adminClient, "{\"topics\":[{\"topic\":\"foo\"}]}", "0,1,2,3", false);

            Map<TopicPartition, List<Integer>> expCurrent = new HashMap<>();

            expCurrent.put(new TopicPartition("foo", 0), List.of(0, 1, 2));
            expCurrent.put(new TopicPartition("foo", 1), List.of(1, 2, 3));

            assertEquals(expCurrent, proposedCurrent.getValue());
        }
    }

    @Test
    public void testGenerateAssignmentWithFewerBrokers() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            List<Integer> goalBrokers = List.of(0, 1, 3);

            Entry<Map<TopicPartition, List<Integer>>, Map<TopicPartition, List<Integer>>>
                proposedCurrent = generateAssignment(adminClient,
                    "{\"topics\":[{\"topic\":\"foo\"},{\"topic\":\"bar\"}]}",
                    goalBrokers.stream().map(Object::toString).collect(Collectors.joining(",")), false);

            Map<TopicPartition, List<Integer>> expCurrent = new HashMap<>();

            expCurrent.put(new TopicPartition("foo", 0), List.of(0, 1, 2));
            expCurrent.put(new TopicPartition("foo", 1), List.of(1, 2, 3));
            expCurrent.put(new TopicPartition("bar", 0), List.of(2, 3, 0));

            assertEquals(expCurrent, proposedCurrent.getValue());

            // The proposed assignment should only span the provided brokers
            proposedCurrent.getKey().values().forEach(replicas ->
                assertTrue(goalBrokers.containsAll(replicas),
                    "Proposed assignment " + proposedCurrent.getKey() + " puts replicas on brokers other than " + goalBrokers)
            );
        }
    }

    @Test
    public void testCurrentPartitionReplicaAssignmentToString() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder()
                .numBrokers(6)
                .brokerLogDirs(List.of(
                    List.of("/tmp/broker0/logs"),
                    List.of("/tmp/broker1/logs"),
                    List.of("/tmp/broker2/logs"),
                    List.of("/tmp/broker3/logs"),
                    List.of("/tmp/broker4/logs"),
                    List.of("/tmp/broker5/logs")
                ))
                .build()
        ) {

            List<Node> brokers = adminClient.brokers();
            Node broker1 = brokers.get(1);
            Node broker2 = brokers.get(2);
            Node broker3 = brokers.get(3);
            Node broker4 = brokers.get(4);
            Node broker5 = brokers.get(5);

            adminClient.addTopic(false, "foo", List.of(
                new TopicPartitionInfo(1, broker1,
                    List.of(broker1, broker2, broker3),
                    List.of(broker1, broker2, broker3))
            ), Map.of());

            adminClient.addTopic(false, "bar", List.of(
                new TopicPartitionInfo(0, broker4,
                    List.of(broker4, broker5),
                    List.of(broker4, broker5))
            ), Map.of());

            Map<TopicPartition, List<Integer>> proposedParts = new HashMap<>();
            proposedParts.put(new TopicPartition("foo", 1), List.of(0, 1, 2));
            proposedParts.put(new TopicPartition("bar", 0), List.of(3, 4, 5));

            Map<TopicPartition, List<Node>> currentParts = new HashMap<>();
            currentParts.put(new TopicPartition("foo", 1), List.of(broker1, broker2, broker3));
            currentParts.put(new TopicPartition("bar", 0), List.of(broker4, broker5));

            assertEquals(String.join(System.lineSeparator(),
                "Current partition replica assignment",
                "",
                "{\"version\":1,\"partitions\":[{\"topic\":\"bar\",\"partition\":0,\"replicas\":[4,5],\"log_dirs\":[\"/tmp/broker4/logs\",\"/tmp/broker4/logs\"]}," +
                    "{\"topic\":\"foo\",\"partition\":1,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}",
                "",
                "Save this to use as the --reassignment-json-file option during rollback"),
                currentPartitionReplicaAssignmentToString(adminClient, proposedParts, currentParts)
            );
        }
    }

    @Test
    public void testMoveMap() {
        // overwrite foo-0 with different reassignments
        // keep old reassignments of foo-1
        // overwrite foo-2 with same reassignments
        // overwrite foo-3 with new reassignments without overlap of old reassignments
        // overwrite foo-4 with a subset of old reassignments
        // overwrite foo-5 with a superset of old reassignments
        // add new reassignments to bar-0
        Map<TopicPartition, PartitionReassignment> currentReassignments = new HashMap<>();

        currentReassignments.put(new TopicPartition("foo", 0), new PartitionReassignment(
            List.of(1, 2, 3, 4), List.of(4), List.of(3)));
        currentReassignments.put(new TopicPartition("foo", 1), new PartitionReassignment(
            List.of(4, 5, 6, 7, 8), List.of(7, 8), List.of(4, 5)));
        currentReassignments.put(new TopicPartition("foo", 2), new PartitionReassignment(
            List.of(1, 2, 3, 4), List.of(3, 4), List.of(1, 2)));
        currentReassignments.put(new TopicPartition("foo", 3), new PartitionReassignment(
            List.of(1, 2, 3, 4), List.of(3, 4), List.of(1, 2)));
        currentReassignments.put(new TopicPartition("foo", 4), new PartitionReassignment(
            List.of(1, 2, 3, 4), List.of(3, 4), List.of(1, 2)));
        currentReassignments.put(new TopicPartition("foo", 5), new PartitionReassignment(
            List.of(1, 2, 3, 4), List.of(3, 4), List.of(1, 2)));

        Map<TopicPartition, List<Integer>> proposedParts = new HashMap<>();

        proposedParts.put(new TopicPartition("foo", 0), List.of(1, 2, 5));
        proposedParts.put(new TopicPartition("foo", 2), List.of(3, 4));
        proposedParts.put(new TopicPartition("foo", 3), List.of(5, 6));
        proposedParts.put(new TopicPartition("foo", 4), List.of(3));
        proposedParts.put(new TopicPartition("foo", 5), List.of(3, 4, 5, 6));
        proposedParts.put(new TopicPartition("bar", 0), List.of(1, 2, 3));

        Map<TopicPartition, List<Integer>> currentParts = new HashMap<>();

        currentParts.put(new TopicPartition("foo", 0), List.of(1, 2, 3, 4));
        currentParts.put(new TopicPartition("foo", 1), List.of(4, 5, 6, 7, 8));
        currentParts.put(new TopicPartition("foo", 2), List.of(1, 2, 3, 4));
        currentParts.put(new TopicPartition("foo", 3), List.of(1, 2, 3, 4));
        currentParts.put(new TopicPartition("foo", 4), List.of(1, 2, 3, 4));
        currentParts.put(new TopicPartition("foo", 5), List.of(1, 2, 3, 4));
        currentParts.put(new TopicPartition("bar", 0), List.of(2, 3, 4));
        currentParts.put(new TopicPartition("baz", 0), List.of(1, 2, 3));

        Map<String, Map<Integer, PartitionMove>> moveMap = calculateProposedMoveMap(currentReassignments, proposedParts, currentParts);

        Map<Integer, PartitionMove> fooMoves = new HashMap<>();

        fooMoves.put(0, new PartitionMove(Set.of(1, 2, 3), Set.of(5)));
        fooMoves.put(1, new PartitionMove(Set.of(4, 5, 6), Set.of(7, 8)));
        fooMoves.put(2, new PartitionMove(Set.of(1, 2), Set.of(3, 4)));
        fooMoves.put(3, new PartitionMove(Set.of(1, 2), Set.of(5, 6)));
        fooMoves.put(4, new PartitionMove(Set.of(1, 2), Set.of(3)));
        fooMoves.put(5, new PartitionMove(Set.of(1, 2), Set.of(3, 4, 5, 6)));

        Map<Integer, PartitionMove> barMoves = new HashMap<>();

        barMoves.put(0, new PartitionMove(Set.of(2, 3, 4), Set.of(1)));

        assertEquals(fooMoves, moveMap.get("foo"));
        assertEquals(barMoves, moveMap.get("bar"));

        Map<String, String> expLeaderThrottle = new HashMap<>();

        expLeaderThrottle.put("foo", "0:1,0:2,0:3,1:4,1:5,1:6,2:1,2:2,3:1,3:2,4:1,4:2,5:1,5:2");
        expLeaderThrottle.put("bar", "0:2,0:3,0:4");

        assertEquals(expLeaderThrottle, calculateLeaderThrottles(moveMap));

        Map<String, String> expFollowerThrottle = new HashMap<>();

        expFollowerThrottle.put("foo", "0:5,1:7,1:8,2:3,2:4,3:5,3:6,4:3,5:3,5:4,5:5,5:6");
        expFollowerThrottle.put("bar", "0:1");

        assertEquals(expFollowerThrottle, calculateFollowerThrottles(moveMap));

        assertEquals(Set.of(1, 2, 3, 4, 5, 6, 7, 8), calculateReassigningBrokers(moveMap));
        assertEquals(Set.of(0, 2), calculateMovingBrokers(Set.of(
            new TopicPartitionReplica("quux", 0, 0),
            new TopicPartitionReplica("quux", 1, 2))));
    }

    @Test
    public void testParseExecuteAssignmentArgs() throws Exception {
        assertStartsWith("Partition reassignment list cannot be empty",
            assertThrows(AdminCommandFailedException.class,
                () -> parseExecuteAssignmentArgs("{\"version\":1,\"partitions\":[]}"),
                "Expected to detect empty partition reassignment list").getMessage());
        assertStartsWith("Partition reassignment contains duplicate topic partitions",
            assertThrows(AdminCommandFailedException.class, () -> parseExecuteAssignmentArgs(
                "{\"version\":1,\"partitions\":" +
                    "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,1],\"log_dirs\":[\"any\",\"any\"]}," +
                    "{\"topic\":\"foo\",\"partition\":0,\"replicas\":[2,3,4],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                    "]}"), "Expected to detect a partition list with duplicate entries").getMessage());
        assertStartsWith("Partition reassignment contains duplicate topic partitions",
            assertThrows(AdminCommandFailedException.class, () -> parseExecuteAssignmentArgs(
                "{\"version\":1,\"partitions\":" +
                    "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,1],\"log_dirs\":[\"/abc\",\"/def\"]}," +
                    "{\"topic\":\"foo\",\"partition\":0,\"replicas\":[2,3],\"log_dirs\":[\"/abc\",\"/def\"]}" +
                    "]}"), "Expected to detect a partition replica list with duplicate entries").getMessage());
        assertStartsWith("Partition replica lists may not contain duplicate entries",
            assertThrows(AdminCommandFailedException.class, () -> parseExecuteAssignmentArgs(
                "{\"version\":1,\"partitions\":" +
                    "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,0],\"log_dirs\":[\"/abc\",\"/def\"]}," +
                    "{\"topic\":\"foo\",\"partition\":1,\"replicas\":[2,3],\"log_dirs\":[\"/abc\",\"/def\"]}" +
                    "]}"), "Expected to detect a partition replica list with duplicate entries").getMessage());

        Map<TopicPartition, List<Integer>> partitionsToBeReassigned = new HashMap<>();

        partitionsToBeReassigned.put(new TopicPartition("foo", 0), List.of(1, 2, 3));
        partitionsToBeReassigned.put(new TopicPartition("foo", 1), List.of(3, 4, 5));

        Entry<Map<TopicPartition, List<Integer>>, Map<TopicPartitionReplica, String>> actual = parseExecuteAssignmentArgs(
            "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}," +
                "{\"topic\":\"foo\",\"partition\":1,\"replicas\":[3,4,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                "]}");

        assertEquals(partitionsToBeReassigned, actual.getKey());
        assertTrue(actual.getValue().isEmpty());

        Map<TopicPartitionReplica, String> replicaAssignment = new HashMap<>();

        replicaAssignment.put(new TopicPartitionReplica("foo", 0, 1), "/tmp/a");
        replicaAssignment.put(new TopicPartitionReplica("foo", 0, 2), "/tmp/b");
        replicaAssignment.put(new TopicPartitionReplica("foo", 0, 3), "/tmp/c");

        actual = parseExecuteAssignmentArgs(
            "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"/tmp/a\",\"/tmp/b\",\"/tmp/c\"]}" +
                "]}");

        assertEquals(Map.of(new TopicPartition("foo", 0), List.of(1, 2, 3)), actual.getKey());
        assertEquals(replicaAssignment, actual.getValue());
    }

    @Test
    public void testExecuteWithInvalidPartitionsFails() {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(5).build()) {
            addTopics(adminClient);
            assertStartsWith("Topic quux not found",
                assertThrows(ExecutionException.class, () -> executeAssignment(adminClient, false,
                    "{\"version\":1,\"partitions\":" +
                        "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,1],\"log_dirs\":[\"any\",\"any\"]}," +
                        "{\"topic\":\"quux\",\"partition\":0,\"replicas\":[2,3,4],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                        "]}", -1L, -1L, 10000L, Time.SYSTEM, false), "Expected reassignment with non-existent topic to fail").getCause().getMessage());
        }
    }

    @Test
    public void testExecuteWithInvalidBrokerIdFails() {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            assertStartsWith("Unknown broker id 4",
                assertThrows(AdminCommandFailedException.class, () -> executeAssignment(adminClient, false,
                    "{\"version\":1,\"partitions\":" +
                        "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,1],\"log_dirs\":[\"any\",\"any\"]}," +
                        "{\"topic\":\"foo\",\"partition\":1,\"replicas\":[2,3,4],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                        "]}", -1L, -1L, 10000L, Time.SYSTEM, false), "Expected reassignment with non-existent broker id to fail").getMessage());
        }
    }

    @Test
    public void testModifyBrokerInterBrokerThrottle() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            modifyInterBrokerThrottle(adminClient, Set.of(0, 1, 2), 1000);
            modifyInterBrokerThrottle(adminClient, Set.of(0, 3), 100);
            List<ConfigResource> brokers = new ArrayList<>();
            for (int i = 0; i < 4; i++)
                brokers.add(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(i)));
            Map<ConfigResource, Config> results = adminClient.describeConfigs(brokers).all().get();
            verifyBrokerThrottleResults(results.get(brokers.get(0)), 100, -1);
            verifyBrokerThrottleResults(results.get(brokers.get(1)), 1000, -1);
            verifyBrokerThrottleResults(results.get(brokers.get(2)), 1000, -1);
            verifyBrokerThrottleResults(results.get(brokers.get(3)), 100, -1);
        }
    }

    @Test
    public void testModifyLogDirThrottle() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            modifyLogDirThrottle(adminClient, Set.of(0, 1, 2), 2000);
            modifyLogDirThrottle(adminClient, Set.of(0, 3), -1);

            List<ConfigResource> brokers = new ArrayList<>();
            for (int i = 0; i < 4; i++)
                brokers.add(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(i)));

            Map<ConfigResource, Config> results = adminClient.describeConfigs(brokers).all().get();

            verifyBrokerThrottleResults(results.get(brokers.get(0)), -1, 2000);
            verifyBrokerThrottleResults(results.get(brokers.get(1)), -1, 2000);
            verifyBrokerThrottleResults(results.get(brokers.get(2)), -1, 2000);
            verifyBrokerThrottleResults(results.get(brokers.get(3)), -1, -1);
        }
    }

    @Test
    public void testCurReassignmentsToString() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            assertEquals("No partition reassignments found.", curReassignmentsToString(adminClient));

            Map<TopicPartition, List<Integer>> reassignments = new HashMap<>();

            reassignments.put(new TopicPartition("foo", 1), List.of(4, 5, 3));
            reassignments.put(new TopicPartition("foo", 0), List.of(0, 1, 4, 2));
            reassignments.put(new TopicPartition("bar", 0), List.of(2, 3));

            Map<TopicPartition, Throwable> reassignmentResult = alterPartitionReassignments(adminClient, reassignments, false);

            assertTrue(reassignmentResult.isEmpty());
            assertEquals(String.join(System.lineSeparator(),
                "Current partition reassignments:",
                "bar-0: replicas: 2,3,0. removing: 0.",
                "foo-0: replicas: 0,1,2. adding: 4.",
                "foo-1: replicas: 1,2,3. adding: 4,5. removing: 1,2."),
                curReassignmentsToString(adminClient));
        }
    }

    private void verifyBrokerThrottleResults(Config config,
                                             long expectedInterBrokerThrottle,
                                             long expectedReplicaAlterLogDirsThrottle) {
        Map<String, String> configs = new HashMap<>();
        config.entries().forEach(entry -> configs.put(entry.name(), entry.value()));
        if (expectedInterBrokerThrottle >= 0) {
            assertEquals(Long.toString(expectedInterBrokerThrottle),
                configs.getOrDefault(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, ""));
            assertEquals(Long.toString(expectedInterBrokerThrottle),
                configs.getOrDefault(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, ""));
        }
        if (expectedReplicaAlterLogDirsThrottle >= 0) {
            assertEquals(Long.toString(expectedReplicaAlterLogDirsThrottle),
                configs.getOrDefault(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, ""));
        }
    }

    @Test
    public void testModifyTopicThrottles() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);

            Map<String, String> leaderThrottles = new HashMap<>();

            leaderThrottles.put("foo", "leaderFoo");
            leaderThrottles.put("bar", "leaderBar");

            modifyTopicThrottles(adminClient,
                leaderThrottles,
                Map.of("bar", "followerBar"));
            List<ConfigResource> topics = Stream.of("bar", "foo").map(
                id -> new ConfigResource(ConfigResource.Type.TOPIC, id)).toList();
            Map<ConfigResource, Config> results = adminClient.describeConfigs(topics).all().get();
            verifyTopicThrottleResults(results.get(topics.get(0)), "leaderBar", "followerBar");
            verifyTopicThrottleResults(results.get(topics.get(1)), "leaderFoo", "");
        }
    }

    private void verifyTopicThrottleResults(Config config,
                                            String expectedLeaderThrottle,
                                            String expectedFollowerThrottle) {
        Map<String, String> configs = new HashMap<>();
        config.entries().forEach(entry -> configs.put(entry.name(), entry.value()));
        assertEquals(expectedLeaderThrottle,
            configs.getOrDefault(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, ""));
        assertEquals(expectedFollowerThrottle,
            configs.getOrDefault(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, ""));
    }

    @Test
    public void testAlterReplicaLogDirs() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().
            numBrokers(4).
            brokerLogDirs(Collections.nCopies(4,
                List.of("/tmp/kafka-logs0", "/tmp/kafka-logs1"))).
            build()) {

            addTopics(adminClient);

            Map<TopicPartitionReplica, String> assignment = new HashMap<>();

            assignment.put(new TopicPartitionReplica("foo", 0, 0), "/tmp/kafka-logs1");
            assignment.put(new TopicPartitionReplica("quux", 1, 0), "/tmp/kafka-logs1");

            assertEquals(
                Set.of(new TopicPartitionReplica("foo", 0, 0)),
                alterReplicaLogDirs(adminClient, assignment)
            );
        }
    }

    public void assertStartsWith(String prefix, String str) {
        assertTrue(str.startsWith(prefix), String.format("Expected the string to start with %s, but it was %s", prefix, str));
    }

    @Test
    public void testPropagateInvalidJsonError() {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            assertStartsWith("Unexpected character",
                assertThrows(AdminOperationException.class, () -> executeAssignment(adminClient, false, "{invalid_json", -1L, -1L, 10000L, Time.SYSTEM, false)).getMessage());
        }
    }

    @Test
    public void testGetReplicaToLogDir() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder()
                .numBrokers(4)
                .brokerLogDirs(List.of(
                    List.of("/tmp/broker0/logs0"),
                    List.of("/tmp/broker1/logs0"),
                    List.of("/tmp/broker2/logs0"),
                    List.of("/tmp/broker3/logs0")
                )).build()
        ) {
            addTopics(adminClient);

            List<Node> brokers = adminClient.brokers();
            Node broker0 = brokers.get(0);
            Node broker1 = brokers.get(1);
            Node broker2 = brokers.get(2);
            Node broker3 = brokers.get(3);

            Map<TopicPartition, List<Node>> topicPartitionToReplicas = Map.of(
                new TopicPartition("foo", 0), List.of(broker0, broker1, broker2),
                new TopicPartition("foo", 1), List.of(broker1, broker2, broker3),
                new TopicPartition("bar", 0), List.of(broker2, broker3, broker0)
            );

            Map<TopicPartitionReplica, String> result = getReplicaToLogDir(adminClient, topicPartitionToReplicas);

            assertFalse(result.isEmpty());
            assertEquals("/tmp/broker0/logs0", result.get(new TopicPartitionReplica("foo", 0, 0)));
            assertEquals("/tmp/broker0/logs0", result.get(new TopicPartitionReplica("foo", 0, 1)));
            assertEquals("/tmp/broker0/logs0", result.get(new TopicPartitionReplica("foo", 0, 2)));
            assertEquals("/tmp/broker1/logs0", result.get(new TopicPartitionReplica("foo", 1, 1)));
            assertEquals("/tmp/broker1/logs0", result.get(new TopicPartitionReplica("foo", 1, 2)));
            assertEquals("/tmp/broker1/logs0", result.get(new TopicPartitionReplica("foo", 1, 3)));
            assertEquals("/tmp/broker2/logs0", result.get(new TopicPartitionReplica("bar", 0, 0)));
            assertEquals("/tmp/broker2/logs0", result.get(new TopicPartitionReplica("bar", 0, 2)));
            assertEquals("/tmp/broker2/logs0", result.get(new TopicPartitionReplica("bar", 0, 3)));
        }
    }
}
