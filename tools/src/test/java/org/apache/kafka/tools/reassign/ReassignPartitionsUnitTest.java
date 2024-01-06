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

import org.apache.kafka.admin.BrokerMetadata;
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
import org.apache.kafka.server.common.AdminCommandFailedException;
import org.apache.kafka.server.common.AdminOperationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.BROKER_LEVEL_FOLLOWER_THROTTLE;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.BROKER_LEVEL_LEADER_THROTTLE;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.BROKER_LEVEL_LOG_DIR_THROTTLE;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.TOPIC_LEVEL_FOLLOWER_THROTTLE;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.TOPIC_LEVEL_LEADER_THROTTLE;
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
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.getReplicaAssignmentForPartitions;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.getReplicaAssignmentForTopics;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.modifyInterBrokerThrottle;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.modifyLogDirThrottle;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.modifyTopicThrottles;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.parseExecuteAssignmentArgs;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.parseGenerateAssignmentArgs;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.partitionReassignmentStatesToString;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.replicaMoveStatesToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
            new PartitionReassignmentState(asList(1, 2, 3), asList(1, 2, 3), true));
        states.put(new TopicPartition("foo", 1),
            new PartitionReassignmentState(asList(1, 2, 3), asList(1, 2, 4), false));
        states.put(new TopicPartition("bar", 0),
            new PartitionReassignmentState(asList(1, 2, 3), asList(1, 2, 4), false));

        assertEquals(String.join(System.lineSeparator(),
            "Status of partition reassignment:",
            "Reassignment of partition bar-0 is still in progress.",
            "Reassignment of partition foo-0 is completed.",
            "Reassignment of partition foo-1 is still in progress."),
            partitionReassignmentStatesToString(states));
    }

    private void addTopics(MockAdminClient adminClient) {
        List<Node> b = adminClient.brokers();
        adminClient.addTopic(false, "foo", asList(
            new TopicPartitionInfo(0, b.get(0),
                asList(b.get(0), b.get(1), b.get(2)),
                asList(b.get(0), b.get(1))),
            new TopicPartitionInfo(1, b.get(1),
                asList(b.get(1), b.get(2), b.get(3)),
                asList(b.get(1), b.get(2), b.get(3)))
        ), Collections.emptyMap());
        adminClient.addTopic(false, "bar", asList(
            new TopicPartitionInfo(0, b.get(2),
                asList(b.get(2), b.get(3), b.get(0)),
                asList(b.get(2), b.get(3), b.get(0)))
        ), Collections.emptyMap());
    }

    @Test
    public void testFindPartitionReassignmentStates() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            // Create a reassignment and test findPartitionReassignmentStates.
            Map<TopicPartition, List<Integer>> reassignments = new HashMap<>();

            reassignments.put(new TopicPartition("foo", 0), asList(0, 1, 3));
            reassignments.put(new TopicPartition("quux", 0), asList(1, 2, 3));

            Map<TopicPartition, Throwable> reassignmentResult = alterPartitionReassignments(adminClient, reassignments);

            assertEquals(1, reassignmentResult.size());
            assertEquals(UnknownTopicOrPartitionException.class, reassignmentResult.get(new TopicPartition("quux", 0)).getClass());

            Map<TopicPartition, PartitionReassignmentState> expStates = new HashMap<>();

            expStates.put(new TopicPartition("foo", 0),
                new PartitionReassignmentState(asList(0, 1, 2), asList(0, 1, 3), false));
            expStates.put(new TopicPartition("foo", 1),
                new PartitionReassignmentState(asList(1, 2, 3), asList(1, 2, 3), true));

            Tuple2<Map<TopicPartition, PartitionReassignmentState>, Boolean> actual =
                findPartitionReassignmentStates(adminClient, asList(
                    new Tuple2<>(new TopicPartition("foo", 0), asList(0, 1, 3)),
                    new Tuple2<>(new TopicPartition("foo", 1), asList(1, 2, 3))
                ));

            assertEquals(expStates, actual.v1);
            assertTrue(actual.v2);

            // Cancel the reassignment and test findPartitionReassignmentStates again.
            Map<TopicPartition, Throwable> cancelResult = cancelPartitionReassignments(adminClient,
                new HashSet<>(asList(new TopicPartition("foo", 0), new TopicPartition("quux", 2))));

            assertEquals(1, cancelResult.size());
            assertEquals(UnknownTopicOrPartitionException.class, cancelResult.get(new TopicPartition("quux", 2)).getClass());

            expStates.clear();

            expStates.put(new TopicPartition("foo", 0),
                new PartitionReassignmentState(asList(0, 1, 2), asList(0, 1, 3), true));
            expStates.put(new TopicPartition("foo", 1),
                new PartitionReassignmentState(asList(1, 2, 3), asList(1, 2, 3), true));

            actual = findPartitionReassignmentStates(adminClient, asList(
                new Tuple2<>(new TopicPartition("foo", 0), asList(0, 1, 3)),
                new Tuple2<>(new TopicPartition("foo", 1), asList(1, 2, 3))
            ));

            assertEquals(expStates, actual.v1);
            assertFalse(actual.v2);
        }
    }

    @Test
    public void testFindLogDirMoveStates() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().
                numBrokers(4).
                brokerLogDirs(asList(
                    asList("/tmp/kafka-logs0", "/tmp/kafka-logs1"),
                    asList("/tmp/kafka-logs0", "/tmp/kafka-logs1"),
                    asList("/tmp/kafka-logs0", "/tmp/kafka-logs1"),
                    asList("/tmp/kafka-logs0", null)))
                .build()) {

            addTopics(adminClient);
            List<Node> b = adminClient.brokers();
            adminClient.addTopic(false, "quux", asList(
                    new TopicPartitionInfo(0, b.get(2),
                        asList(b.get(1), b.get(2), b.get(3)),
                        asList(b.get(1), b.get(2), b.get(3)))),
                Collections.emptyMap());

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

            assignments.put(new TopicPartition("foo", 0), asList(0, 1, 2));
            assignments.put(new TopicPartition("foo", 1), asList(1, 2, 3));

            assertEquals(assignments, getReplicaAssignmentForTopics(adminClient, asList("foo")));

            assignments.clear();

            assignments.put(new TopicPartition("foo", 0), asList(0, 1, 2));
            assignments.put(new TopicPartition("bar", 0), asList(2, 3, 0));

            assertEquals(assignments,
                getReplicaAssignmentForPartitions(adminClient, new HashSet<>(asList(new TopicPartition("foo", 0), new TopicPartition("bar", 0)))));
        }
    }

    @Test
    public void testGetBrokerRackInformation() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().
            brokers(asList(new Node(0, "localhost", 9092, "rack0"),
                new Node(1, "localhost", 9093, "rack1"),
                new Node(2, "localhost", 9094, null))).
            build()) {

            assertEquals(asList(
                new BrokerMetadata(0, Optional.of("rack0")),
                new BrokerMetadata(1, Optional.of("rack1"))
            ), getBrokerMetadata(adminClient, asList(0, 1), true));
            assertEquals(asList(
                new BrokerMetadata(0, Optional.empty()),
                new BrokerMetadata(1, Optional.empty())
            ), getBrokerMetadata(adminClient, asList(0, 1), false));
            assertStartsWith("Not all brokers have rack information",
                assertThrows(AdminOperationException.class,
                    () -> getBrokerMetadata(adminClient, asList(1, 2), true)).getMessage());
            assertEquals(asList(
                new BrokerMetadata(1, Optional.empty()),
                new BrokerMetadata(2, Optional.empty())
            ), getBrokerMetadata(adminClient, asList(1, 2), false));
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
        assertEquals(new Tuple2<>(asList(5, 2, 3, 4), asList("foo")),
            parseGenerateAssignmentArgs("{\"topics\": [{\"topic\": \"foo\"}], \"version\":1}", "5,2,3,4"));
        assertStartsWith("List of topics to reassign contains duplicate entries",
            assertThrows(AdminCommandFailedException.class, () -> parseGenerateAssignmentArgs(
                "{\"topics\": [{\"topic\": \"foo\"},{\"topic\": \"foo\"}], \"version\":1}", "5,2,3,4"),
                "Expected to detect duplicate topic entries").getMessage());
        assertEquals(new Tuple2<>(asList(5, 3, 4), asList("foo", "bar")),
            parseGenerateAssignmentArgs(
                "{\"topics\": [{\"topic\": \"foo\"},{\"topic\": \"bar\"}], \"version\":1}", "5,3,4"));
    }

    @Test
    public void testGenerateAssignmentFailsWithoutEnoughReplicas() {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            assertStartsWith("Replication factor: 3 larger than available brokers: 2",
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
            brokers(asList(
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
            Tuple2<Map<TopicPartition, List<Integer>>, Map<TopicPartition, List<Integer>>>
                proposedCurrent = generateAssignment(adminClient, "{\"topics\":[{\"topic\":\"foo\"}]}", "0,1,2,3", false);

            Map<TopicPartition, List<Integer>> expCurrent = new HashMap<>();

            expCurrent.put(new TopicPartition("foo", 0), asList(0, 1, 2));
            expCurrent.put(new TopicPartition("foo", 1), asList(1, 2, 3));

            assertEquals(expCurrent, proposedCurrent.v2);
        }
    }

    @Test
    public void testGenerateAssignmentWithFewerBrokers() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            List<Integer> goalBrokers = asList(0, 1, 3);

            Tuple2<Map<TopicPartition, List<Integer>>, Map<TopicPartition, List<Integer>>>
                proposedCurrent = generateAssignment(adminClient,
                    "{\"topics\":[{\"topic\":\"foo\"},{\"topic\":\"bar\"}]}",
                    goalBrokers.stream().map(Object::toString).collect(Collectors.joining(",")), false);

            Map<TopicPartition, List<Integer>> expCurrent = new HashMap<>();

            expCurrent.put(new TopicPartition("foo", 0), asList(0, 1, 2));
            expCurrent.put(new TopicPartition("foo", 1), asList(1, 2, 3));
            expCurrent.put(new TopicPartition("bar", 0), asList(2, 3, 0));

            assertEquals(expCurrent, proposedCurrent.v2);

            // The proposed assignment should only span the provided brokers
            proposedCurrent.v1.values().forEach(replicas ->
                assertTrue(goalBrokers.containsAll(replicas),
                    "Proposed assignment " + proposedCurrent.v1 + " puts replicas on brokers other than " + goalBrokers)
            );
        }
    }

    @Test
    public void testCurrentPartitionReplicaAssignmentToString() throws Exception {
        Map<TopicPartition, List<Integer>> proposedParts = new HashMap<>();

        proposedParts.put(new TopicPartition("foo", 1), asList(1, 2, 3));
        proposedParts.put(new TopicPartition("bar", 0), asList(7, 8, 9));

        Map<TopicPartition, List<Integer>> currentParts = new HashMap<>();

        currentParts.put(new TopicPartition("foo", 0), asList(1, 2, 3));
        currentParts.put(new TopicPartition("foo", 1), asList(4, 5, 6));
        currentParts.put(new TopicPartition("bar", 0), asList(7, 8));
        currentParts.put(new TopicPartition("baz", 0), asList(10, 11, 12));

        assertEquals(String.join(System.lineSeparator(),
            "Current partition replica assignment",
            "",
            "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"bar\",\"partition\":0,\"replicas\":[7,8],\"log_dirs\":[\"any\",\"any\"]}," +
                "{\"topic\":\"foo\",\"partition\":1,\"replicas\":[4,5,6],\"log_dirs\":[\"any\",\"any\",\"any\"]}]" +
                "}",
            "",
            "Save this to use as the --reassignment-json-file option during rollback"),
            currentPartitionReplicaAssignmentToString(proposedParts, currentParts)
        );
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
            asList(1, 2, 3, 4), asList(4), asList(3)));
        currentReassignments.put(new TopicPartition("foo", 1), new PartitionReassignment(
            asList(4, 5, 6, 7, 8), asList(7, 8), asList(4, 5)));
        currentReassignments.put(new TopicPartition("foo", 2), new PartitionReassignment(
            asList(1, 2, 3, 4), asList(3, 4), asList(1, 2)));
        currentReassignments.put(new TopicPartition("foo", 3), new PartitionReassignment(
            asList(1, 2, 3, 4), asList(3, 4), asList(1, 2)));
        currentReassignments.put(new TopicPartition("foo", 4), new PartitionReassignment(
            asList(1, 2, 3, 4), asList(3, 4), asList(1, 2)));
        currentReassignments.put(new TopicPartition("foo", 5), new PartitionReassignment(
            asList(1, 2, 3, 4), asList(3, 4), asList(1, 2)));

        Map<TopicPartition, List<Integer>> proposedParts = new HashMap<>();

        proposedParts.put(new TopicPartition("foo", 0), asList(1, 2, 5));
        proposedParts.put(new TopicPartition("foo", 2), asList(3, 4));
        proposedParts.put(new TopicPartition("foo", 3), asList(5, 6));
        proposedParts.put(new TopicPartition("foo", 4), asList(3));
        proposedParts.put(new TopicPartition("foo", 5), asList(3, 4, 5, 6));
        proposedParts.put(new TopicPartition("bar", 0), asList(1, 2, 3));

        Map<TopicPartition, List<Integer>> currentParts = new HashMap<>();

        currentParts.put(new TopicPartition("foo", 0), asList(1, 2, 3, 4));
        currentParts.put(new TopicPartition("foo", 1), asList(4, 5, 6, 7, 8));
        currentParts.put(new TopicPartition("foo", 2), asList(1, 2, 3, 4));
        currentParts.put(new TopicPartition("foo", 3), asList(1, 2, 3, 4));
        currentParts.put(new TopicPartition("foo", 4), asList(1, 2, 3, 4));
        currentParts.put(new TopicPartition("foo", 5), asList(1, 2, 3, 4));
        currentParts.put(new TopicPartition("bar", 0), asList(2, 3, 4));
        currentParts.put(new TopicPartition("baz", 0), asList(1, 2, 3));

        Map<String, Map<Integer, PartitionMove>> moveMap = calculateProposedMoveMap(currentReassignments, proposedParts, currentParts);

        Map<Integer, PartitionMove> fooMoves = new HashMap<>();

        fooMoves.put(0, new PartitionMove(new HashSet<>(asList(1, 2, 3)), new HashSet<>(asList(5))));
        fooMoves.put(1, new PartitionMove(new HashSet<>(asList(4, 5, 6)), new HashSet<>(asList(7, 8))));
        fooMoves.put(2, new PartitionMove(new HashSet<>(asList(1, 2)), new HashSet<>(asList(3, 4))));
        fooMoves.put(3, new PartitionMove(new HashSet<>(asList(1, 2)), new HashSet<>(asList(5, 6))));
        fooMoves.put(4, new PartitionMove(new HashSet<>(asList(1, 2)), new HashSet<>(asList(3))));
        fooMoves.put(5, new PartitionMove(new HashSet<>(asList(1, 2)), new HashSet<>(asList(3, 4, 5, 6))));

        Map<Integer, PartitionMove> barMoves = new HashMap<>();

        barMoves.put(0, new PartitionMove(new HashSet<>(asList(2, 3, 4)), new HashSet<>(asList(1))));

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

        assertEquals(new HashSet<>(asList(1, 2, 3, 4, 5, 6, 7, 8)), calculateReassigningBrokers(moveMap));
        assertEquals(new HashSet<>(asList(0, 2)), calculateMovingBrokers(new HashSet<>(asList(
            new TopicPartitionReplica("quux", 0, 0),
            new TopicPartitionReplica("quux", 1, 2)))));
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

        partitionsToBeReassigned.put(new TopicPartition("foo", 0), asList(1, 2, 3));
        partitionsToBeReassigned.put(new TopicPartition("foo", 1), asList(3, 4, 5));

        Tuple2<Map<TopicPartition, List<Integer>>, Map<TopicPartitionReplica, String>> actual = parseExecuteAssignmentArgs(
            "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}," +
                "{\"topic\":\"foo\",\"partition\":1,\"replicas\":[3,4,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                "]}");

        assertEquals(partitionsToBeReassigned, actual.v1);
        assertTrue(actual.v2.isEmpty());

        Map<TopicPartitionReplica, String> replicaAssignment = new HashMap<>();

        replicaAssignment.put(new TopicPartitionReplica("foo", 0, 1), "/tmp/a");
        replicaAssignment.put(new TopicPartitionReplica("foo", 0, 2), "/tmp/b");
        replicaAssignment.put(new TopicPartitionReplica("foo", 0, 3), "/tmp/c");

        actual = parseExecuteAssignmentArgs(
            "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"/tmp/a\",\"/tmp/b\",\"/tmp/c\"]}" +
                "]}");

        assertEquals(Collections.singletonMap(new TopicPartition("foo", 0), asList(1, 2, 3)), actual.v1);
        assertEquals(replicaAssignment, actual.v2);
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
                        "]}", -1L, -1L, 10000L, Time.SYSTEM), "Expected reassignment with non-existent topic to fail").getCause().getMessage());
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
                        "]}", -1L, -1L, 10000L, Time.SYSTEM), "Expected reassignment with non-existent broker id to fail").getMessage());
        }
    }

    @Test
    public void testModifyBrokerInterBrokerThrottle() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            modifyInterBrokerThrottle(adminClient, new HashSet<>(asList(0, 1, 2)), 1000);
            modifyInterBrokerThrottle(adminClient, new HashSet<>(asList(0, 3)), 100);
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
            modifyLogDirThrottle(adminClient, new HashSet<>(asList(0, 1, 2)), 2000);
            modifyLogDirThrottle(adminClient, new HashSet<>(asList(0, 3)), -1);

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

            reassignments.put(new TopicPartition("foo", 1), asList(4, 5, 3));
            reassignments.put(new TopicPartition("foo", 0), asList(0, 1, 4, 2));
            reassignments.put(new TopicPartition("bar", 0), asList(2, 3));

            Map<TopicPartition, Throwable> reassignmentResult = alterPartitionReassignments(adminClient, reassignments);

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
                configs.getOrDefault(BROKER_LEVEL_LEADER_THROTTLE, ""));
            assertEquals(Long.toString(expectedInterBrokerThrottle),
                configs.getOrDefault(BROKER_LEVEL_FOLLOWER_THROTTLE, ""));
        }
        if (expectedReplicaAlterLogDirsThrottle >= 0) {
            assertEquals(Long.toString(expectedReplicaAlterLogDirsThrottle),
                configs.getOrDefault(BROKER_LEVEL_LOG_DIR_THROTTLE, ""));
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
                Collections.singletonMap("bar", "followerBar"));
            List<ConfigResource> topics = Stream.of("bar", "foo").map(
                id -> new ConfigResource(ConfigResource.Type.TOPIC, id)).collect(Collectors.toList());
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
            configs.getOrDefault(TOPIC_LEVEL_LEADER_THROTTLE, ""));
        assertEquals(expectedFollowerThrottle,
            configs.getOrDefault(TOPIC_LEVEL_FOLLOWER_THROTTLE, ""));
    }

    @Test
    public void testAlterReplicaLogDirs() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().
            numBrokers(4).
            brokerLogDirs(Collections.nCopies(4,
                asList("/tmp/kafka-logs0", "/tmp/kafka-logs1"))).
            build()) {

            addTopics(adminClient);

            Map<TopicPartitionReplica, String> assignment = new HashMap<>();

            assignment.put(new TopicPartitionReplica("foo", 0, 0), "/tmp/kafka-logs1");
            assignment.put(new TopicPartitionReplica("quux", 1, 0), "/tmp/kafka-logs1");

            assertEquals(
                new HashSet<>(asList(new TopicPartitionReplica("foo", 0, 0))),
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
                assertThrows(AdminOperationException.class, () -> executeAssignment(adminClient, false, "{invalid_json", -1L, -1L, 10000L, Time.SYSTEM)).getMessage());
        }
    }
}
