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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.apache.kafka.common.utils.Utils.entriesToMap;
import static org.apache.kafka.common.utils.Utils.intersection;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class AssignmentTestUtils {

    public static final ProcessId PID_1 = processIdForInt(1);
    public static final ProcessId PID_2 = processIdForInt(2);
    public static final ProcessId PID_3 = processIdForInt(3);
    public static final ProcessId PID_4 = processIdForInt(4);
    public static final ProcessId PID_5 = processIdForInt(5);
    public static final ProcessId PID_6 = processIdForInt(6);
    public static final ProcessId PID_7 = processIdForInt(7);
    public static final ProcessId PID_8 = processIdForInt(8);
    public static final ProcessId PID_9 = processIdForInt(9);

    public static final String RACK_0 = "rack0";
    public static final String RACK_1 = "rack1";
    public static final String RACK_2 = "rack2";
    public static final String RACK_3 = "rack3";
    public static final String RACK_4 = "rack4";

    public static final Node NODE_0 = new Node(0, "node0", 1, RACK_0);
    public static final Node NODE_1 = new Node(1, "node1", 1, RACK_1);
    public static final Node NODE_2 = new Node(2, "node2", 1, RACK_2);
    public static final Node NODE_3 = new Node(3, "node3", 1, RACK_3);
    public static final Node NODE_4 = new Node(4, "node4", 1, RACK_4);
    public static final Node NO_RACK_NODE = new Node(3, "node3", 1);

    public static final Node[] REPLICA_0 = new Node[] {NODE_0, NODE_1};
    public static final Node[] REPLICA_1 = new Node[] {NODE_1, NODE_2};
    public static final Node[] REPLICA_2 = new Node[] {NODE_0, NODE_2};
    public static final Node[] REPLICA_3 = new Node[] {NODE_1, NODE_3};
    public static final Node[] REPLICA_4 = new Node[] {NODE_3, NODE_4};

    public static final String TP_0_NAME = "topic0";
    public static final String TP_1_NAME = "topic1";
    public static final String TP_2_NAME = "topic2";
    public static final String TP_3_NAME = "topic3";

    public static final String CHANGELOG_TP_0_NAME = "store-0-changelog";
    public static final String CHANGELOG_TP_1_NAME = "store-1-changelog";
    public static final String CHANGELOG_TP_2_NAME = "store-2-changelog";
    public static final String CHANGELOG_TP_3_NAME = "store-3-changelog";

    public static final TopicPartition CHANGELOG_TP_0_0 = new TopicPartition(CHANGELOG_TP_0_NAME, 0);
    public static final TopicPartition CHANGELOG_TP_0_1 = new TopicPartition(CHANGELOG_TP_0_NAME, 1);
    public static final TopicPartition CHANGELOG_TP_0_2 = new TopicPartition(CHANGELOG_TP_0_NAME, 2);
    public static final TopicPartition CHANGELOG_TP_0_3 = new TopicPartition(CHANGELOG_TP_0_NAME, 3);
    public static final TopicPartition CHANGELOG_TP_0_4 = new TopicPartition(CHANGELOG_TP_0_NAME, 4);
    public static final TopicPartition CHANGELOG_TP_0_5 = new TopicPartition(CHANGELOG_TP_0_NAME, 5);
    public static final TopicPartition CHANGELOG_TP_0_6 = new TopicPartition(CHANGELOG_TP_0_NAME, 6);
    public static final TopicPartition CHANGELOG_TP_1_0 = new TopicPartition(CHANGELOG_TP_1_NAME, 0);
    public static final TopicPartition CHANGELOG_TP_1_1 = new TopicPartition(CHANGELOG_TP_1_NAME, 1);
    public static final TopicPartition CHANGELOG_TP_1_2 = new TopicPartition(CHANGELOG_TP_1_NAME, 2);
    public static final TopicPartition CHANGELOG_TP_1_3 = new TopicPartition(CHANGELOG_TP_1_NAME, 3);
    public static final TopicPartition CHANGELOG_TP_2_0 = new TopicPartition(CHANGELOG_TP_2_NAME, 0);
    public static final TopicPartition CHANGELOG_TP_2_1 = new TopicPartition(CHANGELOG_TP_2_NAME, 1);
    public static final TopicPartition CHANGELOG_TP_2_2 = new TopicPartition(CHANGELOG_TP_2_NAME, 2);
    public static final TopicPartition CHANGELOG_TP_2_3 = new TopicPartition(CHANGELOG_TP_2_NAME, 3);
    public static final TopicPartition CHANGELOG_TP_3_0 = new TopicPartition(CHANGELOG_TP_3_NAME, 0);
    public static final TopicPartition CHANGELOG_TP_3_1 = new TopicPartition(CHANGELOG_TP_3_NAME, 1);
    public static final TopicPartition CHANGELOG_TP_3_2 = new TopicPartition(CHANGELOG_TP_3_NAME, 2);

    public static final TopicPartition TP_0_0 = new TopicPartition(TP_0_NAME, 0);
    public static final TopicPartition TP_0_1 = new TopicPartition(TP_0_NAME, 1);
    public static final TopicPartition TP_0_2 = new TopicPartition(TP_0_NAME, 2);
    public static final TopicPartition TP_0_3 = new TopicPartition(TP_0_NAME, 3);
    public static final TopicPartition TP_0_4 = new TopicPartition(TP_0_NAME, 4);
    public static final TopicPartition TP_0_5 = new TopicPartition(TP_0_NAME, 5);
    public static final TopicPartition TP_0_6 = new TopicPartition(TP_0_NAME, 6);
    public static final TopicPartition TP_1_0 = new TopicPartition(TP_1_NAME, 0);
    public static final TopicPartition TP_1_1 = new TopicPartition(TP_1_NAME, 1);
    public static final TopicPartition TP_1_2 = new TopicPartition(TP_1_NAME, 2);
    public static final TopicPartition TP_1_3 = new TopicPartition(TP_1_NAME, 3);
    public static final TopicPartition TP_2_0 = new TopicPartition(TP_2_NAME, 0);
    public static final TopicPartition TP_2_1 = new TopicPartition(TP_2_NAME, 1);
    public static final TopicPartition TP_2_2 = new TopicPartition(TP_2_NAME, 2);
    public static final TopicPartition TP_2_3 = new TopicPartition(TP_2_NAME, 3);
    public static final TopicPartition TP_3_0 = new TopicPartition(TP_3_NAME, 0);
    public static final TopicPartition TP_3_1 = new TopicPartition(TP_3_NAME, 1);
    public static final TopicPartition TP_3_2 = new TopicPartition(TP_3_NAME, 2);

    public static final PartitionInfo PI_0_0 = new PartitionInfo(TP_0_NAME, 0, NODE_0, REPLICA_0, REPLICA_0);
    public static final PartitionInfo PI_0_1 = new PartitionInfo(TP_0_NAME, 1, NODE_1, REPLICA_1, REPLICA_1);
    public static final PartitionInfo PI_0_2 = new PartitionInfo(TP_0_NAME, 2, NODE_1, REPLICA_1, REPLICA_1);
    public static final PartitionInfo PI_0_3 = new PartitionInfo(TP_0_NAME, 3, NODE_2, REPLICA_2, REPLICA_2);
    public static final PartitionInfo PI_0_4 = new PartitionInfo(TP_0_NAME, 4, NODE_3, REPLICA_3, REPLICA_3);
    public static final PartitionInfo PI_0_5 = new PartitionInfo(TP_0_NAME, 5, NODE_4, REPLICA_4, REPLICA_4);
    public static final PartitionInfo PI_0_6 = new PartitionInfo(TP_0_NAME, 6, NODE_2, REPLICA_2, REPLICA_2);
    public static final PartitionInfo PI_1_0 = new PartitionInfo(TP_1_NAME, 0, NODE_2, REPLICA_2, REPLICA_2);
    public static final PartitionInfo PI_1_1 = new PartitionInfo(TP_1_NAME, 1, NODE_3, REPLICA_3, REPLICA_3);
    public static final PartitionInfo PI_1_2 = new PartitionInfo(TP_1_NAME, 2, NODE_0, REPLICA_0, REPLICA_0);
    public static final PartitionInfo PI_1_3 = new PartitionInfo(TP_1_NAME, 3, NODE_1, REPLICA_1, REPLICA_1);
    public static final PartitionInfo PI_2_0 = new PartitionInfo(TP_2_NAME, 0, NODE_4, REPLICA_4, REPLICA_4);
    public static final PartitionInfo PI_2_1 = new PartitionInfo(TP_2_NAME, 1, NODE_3, REPLICA_3, REPLICA_3);
    public static final PartitionInfo PI_2_2 = new PartitionInfo(TP_2_NAME, 2, NODE_1, REPLICA_4, REPLICA_4);
    public static final PartitionInfo PI_2_3 = new PartitionInfo(TP_2_NAME, 3, NODE_0, REPLICA_0, REPLICA_0);
    public static final PartitionInfo PI_3_0 = new PartitionInfo(TP_3_NAME, 0, NODE_2, REPLICA_2, REPLICA_2);
    public static final PartitionInfo PI_3_1 = new PartitionInfo(TP_3_NAME, 1, NODE_3, REPLICA_3, REPLICA_3);
    public static final PartitionInfo PI_3_2 = new PartitionInfo(TP_3_NAME, 2, NODE_4, REPLICA_4, REPLICA_4);

    public static final TaskId TASK_0_0 = new TaskId(0, 0);
    public static final TaskId TASK_0_1 = new TaskId(0, 1);
    public static final TaskId TASK_0_2 = new TaskId(0, 2);
    public static final TaskId TASK_0_3 = new TaskId(0, 3);
    public static final TaskId TASK_0_4 = new TaskId(0, 4);
    public static final TaskId TASK_0_5 = new TaskId(0, 5);
    public static final TaskId TASK_0_6 = new TaskId(0, 6);
    public static final TaskId TASK_1_0 = new TaskId(1, 0);
    public static final TaskId TASK_1_1 = new TaskId(1, 1);
    public static final TaskId TASK_1_2 = new TaskId(1, 2);
    public static final TaskId TASK_1_3 = new TaskId(1, 3);
    public static final TaskId TASK_2_0 = new TaskId(2, 0);
    public static final TaskId TASK_2_1 = new TaskId(2, 1);
    public static final TaskId TASK_2_2 = new TaskId(2, 2);
    public static final TaskId TASK_2_3 = new TaskId(2, 3);
    public static final TaskId TASK_3_0 = new TaskId(3, 0);
    public static final TaskId TASK_3_1 = new TaskId(3, 1);
    public static final TaskId TASK_3_2 = new TaskId(3, 2);

    public static final TaskId NAMED_TASK_T0_0_0 = new TaskId(0, 0, "topology0");
    public static final TaskId NAMED_TASK_T0_0_1 = new TaskId(0, 1, "topology0");
    public static final TaskId NAMED_TASK_T0_1_0 = new TaskId(1, 0, "topology0");
    public static final TaskId NAMED_TASK_T0_1_1 = new TaskId(1, 1, "topology0");
    public static final TaskId NAMED_TASK_T1_0_0 = new TaskId(0, 0, "topology1");
    public static final TaskId NAMED_TASK_T1_0_1 = new TaskId(0, 1, "topology1");
    public static final TaskId NAMED_TASK_T2_0_0 = new TaskId(0, 0, "topology2");
    public static final TaskId NAMED_TASK_T2_2_0 = new TaskId(2, 0, "topology2");

    public static final Subtopology SUBTOPOLOGY_0 = new Subtopology(0, null);
    public static final Subtopology SUBTOPOLOGY_1 = new Subtopology(1, null);
    public static final Subtopology SUBTOPOLOGY_2 = new Subtopology(2, null);

    public static final Set<TaskId> EMPTY_TASKS = emptySet();
    public static final Map<TopicPartition, Long> EMPTY_CHANGELOG_END_OFFSETS = new HashMap<>();
    public static final List<String> EMPTY_RACK_AWARE_ASSIGNMENT_TAGS = Collections.emptyList();
    public static final Map<String, String> EMPTY_CLIENT_TAGS = Collections.emptyMap();

    private static final String USER_END_POINT = "localhost:8080";
    private static final String APPLICATION_ID = "stream-partition-assignor-test";
    private static Random random;
    public static final String TOPIC_PREFIX = "topic";
    public static final String CHANGELOG_TOPIC_PREFIX = "changelog-topic";
    public static final String RACK_PREFIX = "rack";

    private AssignmentTestUtils() {}

    static Map<ProcessId, ClientState> getClientStatesMap(final ClientState... states) {
        final Map<ProcessId, ClientState> clientStates = new HashMap<>();
        int nthState = 1;
        for (final ClientState state : states) {
            clientStates.put(processIdForInt(nthState), state);
            ++nthState;
        }
        return clientStates;
    }

    // If you don't care about setting the end offsets for each specific topic partition, the helper method
    // getTopicPartitionOffsetMap is useful for building this input map for all partitions
    public static AdminClient createMockAdminClientForAssignor(final Map<TopicPartition, Long> changelogEndOffsets, final boolean mockListOffsets) {
        final AdminClient adminClient = mock(AdminClient.class);

        final ListOffsetsResult result = mock(ListOffsetsResult.class);
        if (mockListOffsets) {
            when(adminClient.listOffsets(any())).thenReturn(result);
        }
        for (final Map.Entry<TopicPartition, Long> entry : changelogEndOffsets.entrySet()) {
            final KafkaFutureImpl<ListOffsetsResultInfo> partitionFuture = new KafkaFutureImpl<>();
            final ListOffsetsResultInfo info = mock(ListOffsetsResultInfo.class);
            lenient().when(info.offset()).thenReturn(entry.getValue());
            partitionFuture.complete(info);
            lenient().when(result.partitionResult(entry.getKey())).thenReturn(partitionFuture);
        }

        return adminClient;
    }

    public static SubscriptionInfo getInfo(final ProcessId processId,
                                           final Set<TaskId> prevTasks,
                                           final Set<TaskId> standbyTasks) {
        return new SubscriptionInfo(
            LATEST_SUPPORTED_VERSION, LATEST_SUPPORTED_VERSION, processId, null, getTaskOffsetSums(prevTasks, standbyTasks), (byte) 0, 0, EMPTY_CLIENT_TAGS);
    }

    public static SubscriptionInfo getInfo(final ProcessId processId,
                                           final Set<TaskId> prevTasks,
                                           final Set<TaskId> standbyTasks,
                                           final String userEndPoint) {
        return new SubscriptionInfo(
            LATEST_SUPPORTED_VERSION, LATEST_SUPPORTED_VERSION, processId, userEndPoint, getTaskOffsetSums(prevTasks, standbyTasks), (byte) 0, 0, EMPTY_CLIENT_TAGS);
    }

    public static SubscriptionInfo getInfo(final ProcessId processId,
                                           final Set<TaskId> prevTasks,
                                           final Set<TaskId> standbyTasks,
                                           final byte uniqueField) {
        return new SubscriptionInfo(
            LATEST_SUPPORTED_VERSION, LATEST_SUPPORTED_VERSION, processId, null, getTaskOffsetSums(prevTasks, standbyTasks), uniqueField, 0, EMPTY_CLIENT_TAGS);
    }

    public static SubscriptionInfo getInfo(final ProcessId processId,
                                           final Set<TaskId> prevTasks,
                                           final Set<TaskId> standbyTasks,
                                           final byte uniqueField,
                                           final Map<String, String> clientTags) {
        return new SubscriptionInfo(
            LATEST_SUPPORTED_VERSION, LATEST_SUPPORTED_VERSION, processId, null, getTaskOffsetSums(prevTasks, standbyTasks), uniqueField, 0, clientTags);
    }

    // Stub offset sums for when we only care about the prev/standby task sets, not the actual offsets
    private static Map<TaskId, Long> getTaskOffsetSums(final Collection<TaskId> activeTasks, final Collection<TaskId> standbyTasks) {
        final Map<TaskId, Long> taskOffsetSums = activeTasks.stream().collect(Collectors.toMap(t -> t, t -> Task.LATEST_OFFSET));
        taskOffsetSums.putAll(standbyTasks.stream().collect(Collectors.toMap(t -> t, t -> 0L)));
        return taskOffsetSums;
    }

    /**
     * Builds a ProcessId with a UUID by repeating the given number n. For valid n, it is guaranteed that the returned UUIDs satisfy
     * the same relation relative to others as their parameter n does: iff n < m, then uuidForInt(n) < uuidForInt(m)
     */
    public static ProcessId processIdForInt(final int n) {
        return new ProcessId(new UUID(0, n));
    }

    static void assertValidAssignment(final int numStandbyReplicas,
                                      final Set<TaskId> statefulTasks,
                                      final Set<TaskId> statelessTasks,
                                      final Map<ProcessId, ClientState> assignedStates,
                                      final StringBuilder failureContext) {
        assertValidAssignment(
            numStandbyReplicas,
            0,
            statefulTasks,
            statelessTasks,
            assignedStates,
            failureContext
        );
    }

    static void assertValidAssignment(final int numStandbyReplicas,
                                      final int maxWarmupReplicas,
                                      final Set<TaskId> statefulTasks,
                                      final Set<TaskId> statelessTasks,
                                      final Map<ProcessId, ClientState> assignedStates,
                                      final StringBuilder failureContext) {
        final Map<TaskId, Set<ProcessId>> assignments = new TreeMap<>();
        for (final TaskId taskId : statefulTasks) {
            assignments.put(taskId, new TreeSet<>());
        }
        for (final TaskId taskId : statelessTasks) {
            assignments.put(taskId, new TreeSet<>());
        }
        for (final Map.Entry<ProcessId, ClientState> entry : assignedStates.entrySet()) {
            validateAndAddActiveAssignments(statefulTasks, statelessTasks, failureContext, assignments, entry);
            validateAndAddStandbyAssignments(statefulTasks, statelessTasks, failureContext, assignments, entry);
        }

        final AtomicInteger remainingWarmups = new AtomicInteger(maxWarmupReplicas);

        final TreeMap<TaskId, Set<ProcessId>> misassigned =
            assignments
                .entrySet()
                .stream()
                .filter(entry -> {
                    final int expectedActives = 1;
                    final boolean isStateless = statelessTasks.contains(entry.getKey());
                    final int expectedStandbys = isStateless ? 0 : numStandbyReplicas;
                    // We'll never assign even the expected number of standbys if they don't actually fit in the cluster
                    final int expectedAssignments = Math.min(
                        assignedStates.size(),
                        expectedActives + expectedStandbys
                    );
                    final int actualAssignments = entry.getValue().size();
                    if (actualAssignments == expectedAssignments) {
                        return false; // not misassigned
                    } else {
                        if (actualAssignments == expectedAssignments + 1 && remainingWarmups.get() > 0) {
                            remainingWarmups.getAndDecrement();
                            return false; // it's a warmup, so it's fine
                        } else {
                            return true; // misassigned
                        }
                    }
                })
                .collect(entriesToMap(TreeMap::new));

        if (!misassigned.isEmpty()) {
            assertThat("Found some over- or under-assigned tasks in the final assignment with " + numStandbyReplicas +
                    " and max warmups " + maxWarmupReplicas + " standby replicas, stateful tasks:" + statefulTasks +
                    ", and stateless tasks:" + statelessTasks + failureContext, misassigned, is(emptyMap()));
        }
    }

    private static void validateAndAddStandbyAssignments(final Set<TaskId> statefulTasks,
                                                         final Set<TaskId> statelessTasks,
                                                         final StringBuilder failureContext,
                                                         final Map<TaskId, Set<ProcessId>> assignments,
                                                         final Map.Entry<ProcessId, ClientState> entry) {
        for (final TaskId standbyTask : entry.getValue().standbyTasks()) {
            if (statelessTasks.contains(standbyTask)) {
                throw new AssertionError("Found a standby task for stateless task " + standbyTask + " on client " +
                        entry + " stateless tasks:" + statelessTasks + failureContext);
            } else if (assignments.containsKey(standbyTask)) {
                assignments.get(standbyTask).add(entry.getKey());
            } else {
                throw new AssertionError("Found an extra standby task " + standbyTask + " on client " +
                        entry + " but expected stateful tasks:" + statefulTasks + failureContext);
            }
        }
    }

    private static void validateAndAddActiveAssignments(final Set<TaskId> statefulTasks,
                                                        final Set<TaskId> statelessTasks,
                                                        final StringBuilder failureContext,
                                                        final Map<TaskId, Set<ProcessId>> assignments,
                                                        final Map.Entry<ProcessId, ClientState> entry) {
        for (final TaskId activeTask : entry.getValue().activeTasks()) {
            if (assignments.containsKey(activeTask)) {
                assignments.get(activeTask).add(entry.getKey());
            } else {
                throw new AssertionError("Found an extra active task " + activeTask + " on client " + entry + " but expected stateful tasks:" + statefulTasks + " and stateless tasks:" + statelessTasks + failureContext);
            }
        }
    }

    static void assertBalancedStatefulAssignment(final Set<TaskId> allStatefulTasks,
                                                 final Map<ProcessId, ClientState> clientStates,
                                                 final StringBuilder failureContext) {
        double maxStateful = Double.MIN_VALUE;
        double minStateful = Double.MAX_VALUE;
        for (final ClientState clientState : clientStates.values()) {
            final Set<TaskId> statefulTasks =
                intersection(HashSet::new, clientState.assignedTasks(), allStatefulTasks);
            final double statefulTaskLoad = 1.0 * statefulTasks.size() / clientState.capacity();
            maxStateful = Math.max(maxStateful, statefulTaskLoad);
            minStateful = Math.min(minStateful, statefulTaskLoad);
        }
        final double statefulDiff = maxStateful - minStateful;

        if (statefulDiff > 1.0) {
            final StringBuilder builder = new StringBuilder()
                .append("detected a stateful assignment balance factor violation: ")
                .append(statefulDiff)
                .append(">")
                .append(1.0)
                .append(" in: ");
            appendClientStates(builder, clientStates);
            fail(builder.append(failureContext).toString());
        }
    }

    static void assertBalancedActiveAssignment(final Map<ProcessId, ClientState> clientStates,
                                               final StringBuilder failureContext) {
        double maxActive = Double.MIN_VALUE;
        double minActive = Double.MAX_VALUE;
        for (final ClientState clientState : clientStates.values()) {
            final double activeTaskLoad = clientState.activeTaskLoad();
            maxActive = Math.max(maxActive, activeTaskLoad);
            minActive = Math.min(minActive, activeTaskLoad);
        }
        final double activeDiff = maxActive - minActive;
        if (activeDiff > 1.0) {
            final StringBuilder builder = new StringBuilder()
                .append("detected an active assignment balance factor violation: ")
                .append(activeDiff)
                .append(">")
                .append(1.0)
                .append(" in: ");
            appendClientStates(builder, clientStates);
            fail(builder.append(failureContext).toString());
        }
    }

    static void assertBalancedTasks(final Map<ProcessId, ClientState> clientStates) {
        assertBalancedTasks(clientStates, 1);
    }

    static void assertBalancedTasks(final Map<ProcessId, ClientState> clientStates, final int skewThreshold) {
        final TaskSkewReport taskSkewReport = analyzeTaskAssignmentBalance(clientStates, skewThreshold);
        if (taskSkewReport.totalSkewedTasks() > 0) {
            fail("Expected a balanced task assignment, but was: " + taskSkewReport);
        }
    }

    static TaskSkewReport analyzeTaskAssignmentBalance(final Map<ProcessId, ClientState> clientStates, final int skewThreshold) {
        final Function<Integer, Map<ProcessId, AtomicInteger>> initialClientCounts =
            i -> clientStates.keySet().stream().collect(Collectors.toMap(c -> c, c -> new AtomicInteger(0)));

        final Map<Integer, Map<ProcessId, AtomicInteger>> subtopologyToClientsWithPartition = new TreeMap<>();
        for (final Map.Entry<ProcessId, ClientState> entry : clientStates.entrySet()) {
            final ProcessId client = entry.getKey();
            final ClientState clientState = entry.getValue();
            for (final TaskId task : clientState.activeTasks()) {
                final int subtopology = task.subtopology();
                subtopologyToClientsWithPartition
                    .computeIfAbsent(subtopology, initialClientCounts)
                    .get(client)
                    .incrementAndGet();
            }
        }

        int maxTaskSkew = 0;
        final Set<Integer> skewedSubtopologies = new TreeSet<>();

        for (final Map.Entry<Integer, Map<ProcessId, AtomicInteger>> entry : subtopologyToClientsWithPartition.entrySet()) {
            final Map<ProcessId, AtomicInteger> clientsWithPartition = entry.getValue();
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            for (final AtomicInteger count : clientsWithPartition.values()) {
                max = Math.max(max, count.get());
                min = Math.min(min, count.get());
            }
            final int taskSkew = max - min;
            maxTaskSkew = Math.max(maxTaskSkew, taskSkew);
            if (taskSkew > skewThreshold) {
                skewedSubtopologies.add(entry.getKey());
            }
        }

        return new TaskSkewReport(maxTaskSkew, skewedSubtopologies, subtopologyToClientsWithPartition);
    }

    static Matcher<ClientState> hasAssignedTasks(final int taskCount) {
        return hasProperty("assignedTasks", ClientState::assignedTaskCount, taskCount);
    }

    static Matcher<ClientState> hasActiveTasks(final int taskCount) {
        return hasProperty("activeTasks", ClientState::activeTaskCount, taskCount);
    }

    static Matcher<ClientState> hasStandbyTasks(final int taskCount) {
        return hasProperty("standbyTasks", ClientState::standbyTaskCount, taskCount);
    }

    static <V> Matcher<ClientState> hasProperty(final String propertyName,
                                                final Function<ClientState, V> propertyExtractor,
                                                final V propertyValue) {
        return new BaseMatcher<ClientState>() {
            @Override
            public void describeTo(final Description description) {
                description.appendText(propertyName).appendText(":").appendValue(propertyValue);
            }

            @Override
            public boolean matches(final Object actual) {
                if (actual instanceof ClientState) {
                    return Objects.equals(propertyExtractor.apply((ClientState) actual), propertyValue);
                } else {
                    return false;
                }
            }
        };
    }

    static void appendClientStates(final StringBuilder stringBuilder,
                                   final Map<ProcessId, ClientState> clientStates) {
        stringBuilder.append('{').append('\n');
        for (final Map.Entry<ProcessId, ClientState> entry : clientStates.entrySet()) {
            stringBuilder.append("  ").append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
        }
        stringBuilder.append('}').append('\n');
    }

    static final class TaskSkewReport {
        private final int maxTaskSkew;
        private final Set<Integer> skewedSubtopologies;
        private final Map<Integer, Map<ProcessId, AtomicInteger>> subtopologyToClientsWithPartition;

        private TaskSkewReport(final int maxTaskSkew,
                               final Set<Integer> skewedSubtopologies,
                               final Map<Integer, Map<ProcessId, AtomicInteger>> subtopologyToClientsWithPartition) {
            this.maxTaskSkew = maxTaskSkew;
            this.skewedSubtopologies = skewedSubtopologies;
            this.subtopologyToClientsWithPartition = subtopologyToClientsWithPartition;
        }

        int totalSkewedTasks() {
            return skewedSubtopologies.size();
        }

        Set<Integer> skewedSubtopologies() {
            return skewedSubtopologies;
        }

        @Override
        public String toString() {
            return "TaskSkewReport{" +
                "maxTaskSkew=" + maxTaskSkew +
                ", skewedSubtopologies=" + skewedSubtopologies +
                ", subtopologyToClientsWithPartition=" + subtopologyToClientsWithPartition +
                '}';
        }
    }

    static List<Node> getRandomNodes(final int nodeSize) {
        final List<Node> nodeList = new ArrayList<>(nodeSize);
        for (int i = 0; i < nodeSize; i++) {
            nodeList.add(new Node(i, "node" + i, 1, RACK_PREFIX + i));
        }
        final Random rand = getRandom();
        Collections.shuffle(nodeList, rand);
        return nodeList;
    }

    static Node[] getRandomReplica(final List<Node> nodeList, final int index, final int partition) {
        final Node firstNode = nodeList.get((index * partition) % nodeList.size());
        final Node secondNode = nodeList.get((index * partition + 1) % nodeList.size());
        return new Node[] {firstNode, secondNode};
    }

    static Cluster getRandomCluster(final int nodeSize, final int tpSize, final int partitionSize) {
        final List<Node> nodeList = getRandomNodes(nodeSize);
        final Set<PartitionInfo> partitionInfoSet = new HashSet<>();
        for (int i = 0; i < tpSize; i++) {
            for (int j = 0; j < partitionSize; j++) {
                final Node[] replica = getRandomReplica(nodeList, i, j);
                partitionInfoSet.add(
                    new PartitionInfo(TOPIC_PREFIX + i, j, replica[0], replica, replica));
            }
        }

        return new Cluster(
            "cluster",
            new HashSet<>(nodeList),
            partitionInfoSet,
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    static Map<ProcessId, Map<String, Optional<String>>> getRandomProcessRacks(final int clientSize, final int nodeSize) {
        final List<String> racks = new ArrayList<>(nodeSize);
        for (int i = 0; i < nodeSize; i++) {
            racks.add(RACK_PREFIX + i);
        }
        final Random rand = getRandom();
        Collections.shuffle(racks, rand);
        final Map<ProcessId, Map<String, Optional<String>>> processRacks = new HashMap<>();
        for (int i = 1; i <= clientSize; i++) {
            final String rack = racks.get(i % nodeSize);
            processRacks.put(processIdForInt(i), mkMap(mkEntry("1", Optional.of(rack))));
        }
        return processRacks;
    }

    static SortedMap<TaskId, Set<TopicPartition>> getTaskTopicPartitionMap(final int tpSize, final int partitionSize, final boolean changelog) {
        final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = new TreeMap<>();
        final String topicName = changelog ? CHANGELOG_TOPIC_PREFIX : TOPIC_PREFIX;
        for (int i = 0; i < tpSize; i++) {
            for (int j = 0; j < partitionSize; j++) {
                taskTopicPartitionMap.put(new TaskId(i, j), new HashSet<>(List.of(
                    new TopicPartition(topicName + i, j),
                    new TopicPartition(topicName + ((i + 1) % tpSize), j))
                ));
            }
        }
        return taskTopicPartitionMap;
    }

    static Map<Subtopology, Set<TaskId>> getTasksForTopicGroup(final int tpSize, final int partitionSize) {
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = new HashMap<>();
        for (int i = 0; i < tpSize; i++) {
            for (int j = 0; j < partitionSize; j++) {
                final Subtopology subtopology = new Subtopology(i, null);
                tasksForTopicGroup.computeIfAbsent(subtopology, k -> new HashSet<>()).add(new TaskId(i, j));
            }
        }
        return tasksForTopicGroup;
    }

    static Map<String, Object> configProps(final String rackAwareConfig) {
        return configProps(rackAwareConfig, 0);
    }

    static Map<String, Object> configProps(final String rackAwareConfig, final int replicaNum) {
        final Map<String, Object> configurationMap = new HashMap<>();
        configurationMap.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        configurationMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, USER_END_POINT);
        configurationMap.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, replicaNum);
        configurationMap.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG, rackAwareConfig);

        final ReferenceContainer referenceContainer = new ReferenceContainer();
        configurationMap.put(InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR, referenceContainer);
        return configurationMap;
    }

    static InternalTopicManager mockInternalTopicManagerForRandomChangelog(final int nodeSize, final int tpSize, final int partitionSize) {
        final MockTime time = new MockTime();
        final StreamsConfig streamsConfig = new StreamsConfig(configProps(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC));
        final MockClientSupplier mockClientSupplier = new MockClientSupplier();
        final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            time,
            streamsConfig,
            mockClientSupplier.restoreConsumer,
            false
        );

        final Set<String> changelogNames = new HashSet<>();
        final List<Node> nodeList = getRandomNodes(nodeSize);
        final Map<String, List<TopicPartitionInfo>> topicPartitionInfo = new HashMap<>();
        for (int i = 0; i < tpSize; i++) {
            final String topicName = CHANGELOG_TOPIC_PREFIX + i;
            changelogNames.add(topicName);
            for (int j = 0; j < partitionSize; j++) {

                final Node[] replica = getRandomReplica(nodeList, i, j);
                final TopicPartitionInfo info = new TopicPartitionInfo(j, replica[0],
                    Arrays.asList(replica), Arrays.asList(replica));

                topicPartitionInfo.computeIfAbsent(topicName, tp -> new ArrayList<>()).add(info);
            }
        }

        final MockInternalTopicManager spyTopicManager = spy(mockInternalTopicManager);
        doReturn(topicPartitionInfo).when(spyTopicManager).getTopicPartitionInfo(changelogNames);
        return spyTopicManager;
    }

    static SortedMap<ProcessId, ClientState> getRandomClientState(final int clientSize, final int tpSize, final int partitionSize, final int maxCapacity, final Set<TaskId> statefulTasks) {
        return getRandomClientState(clientSize, tpSize, partitionSize, maxCapacity, true, statefulTasks);
    }

    static List<Set<TaskId>> getRandomSubset(final Set<TaskId> taskIds, final int listSize) {
        final Random rand = getRandom();
        final List<TaskId> taskIdList = new ArrayList<>(taskIds);
        Collections.shuffle(taskIdList, rand);
        int start = 0;
        final List<Set<TaskId>> subSets = new ArrayList<>(listSize);
        for (int i = 0; i < listSize; i++) {
            final int remaining = taskIdList.size() - start;
            final Set<TaskId> subset = new HashSet<>();
            if (remaining != 0) {
                // In last round, get all tasks
                final int subSetSize = (i == listSize - 1) ? remaining : rand.nextInt(remaining) + 1;
                for (int j = 0; j < subSetSize; j++) {
                    subset.add(taskIdList.get(start + j));
                }
                start += subSetSize;
            }
            subSets.add(subset);
        }
        return subSets;
    }

    static SortedMap<ProcessId, ClientState> getRandomClientState(final int clientSize, final int tpSize, final int partitionSize, final int maxCapacity, final boolean initialAssignment, final Set<TaskId> statefulTasks) {
        final SortedMap<ProcessId, ClientState> clientStates = new TreeMap<>();
        final Map<TaskId, Long> taskLags = statefulTasks.stream().collect(Collectors.toMap(taskId -> taskId, taskId -> 0L));
        final Set<TaskId> taskIds = new HashSet<>();
        for (int i = 0; i < tpSize; i++) {
            for (int j = 0; j < partitionSize; j++) {
                taskIds.add(new TaskId(i, j));
            }
        }

        final Set<TaskId> missingTaskIds = taskLags.keySet().stream().filter(id -> !taskIds.contains(id)).collect(
            Collectors.toSet());
        if (!missingTaskIds.isEmpty()) {
            throw new IllegalArgumentException(missingTaskIds + " missing in all task ids " + taskIds);
        }

        final List<Set<TaskId>> previousActives = getRandomSubset(taskIds, clientSize);
        final List<Set<TaskId>> previousStandbys = getRandomSubset(statefulTasks, clientSize);

        final Random rand = getRandom();

        for (int i = 1; i <= clientSize; i++) {
            final int capacity = rand.nextInt(maxCapacity) + 1;
            final ProcessId processId = processIdForInt(i);
            final ClientState clientState = new ClientState(previousActives.get(i - 1), previousStandbys.get(i - 1), taskLags, EMPTY_CLIENT_TAGS, capacity, processId);
            clientStates.put(processId, clientState);
        }

        if (initialAssignment) {
            Iterator<Entry<ProcessId, ClientState>> iterator = clientStates.entrySet().iterator();
            final List<TaskId> taskIdList = new ArrayList<>(taskIds);
            Collections.shuffle(taskIdList, rand);
            for (final TaskId taskId : taskIdList) {
                if (!iterator.hasNext()) {
                    iterator = clientStates.entrySet().iterator();
                }
                iterator.next().getValue().assignActive(taskId);
            }
        }
        return clientStates;
    }

    static Cluster getClusterForAllTopics() {
        return new Cluster(
            "cluster",
            Set.of(NODE_0, NODE_1, NODE_2, NODE_3, NODE_4),
            Set.of(
                PI_0_0,
                PI_0_1,
                PI_0_2,
                PI_0_3,
                PI_0_4,
                PI_0_5,
                PI_0_6,
                PI_1_0,
                PI_1_1,
                PI_1_2,
                PI_1_3,
                PI_2_0,
                PI_2_1,
                PI_2_2,
                PI_2_3,
                PI_3_0,
                PI_3_1,
                PI_3_2
            ),
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    static Map<TaskId, Set<TopicPartition>> getTaskTopicPartitionMapForAllTasks() {
        return mkMap(
            mkEntry(TASK_0_0, Set.of(TP_0_0)),
            mkEntry(TASK_0_1, Set.of(TP_0_1)),
            mkEntry(TASK_0_2, Set.of(TP_0_2)),
            mkEntry(TASK_0_3, Set.of(TP_0_3)),
            mkEntry(TASK_0_4, Set.of(TP_0_4)),
            mkEntry(TASK_0_5, Set.of(TP_0_5)),
            mkEntry(TASK_0_6, Set.of(TP_0_6)),
            mkEntry(TASK_1_0, Set.of(TP_1_0)),
            mkEntry(TASK_1_1, Set.of(TP_1_1)),
            mkEntry(TASK_1_2, Set.of(TP_1_2)),
            mkEntry(TASK_1_3, Set.of(TP_1_3)),
            mkEntry(TASK_2_0, Set.of(TP_2_0)),
            mkEntry(TASK_2_1, Set.of(TP_2_1)),
            mkEntry(TASK_2_2, Set.of(TP_2_2)),
            mkEntry(TASK_2_3, Set.of(TP_2_3)),
            mkEntry(TASK_3_0, Set.of(TP_3_0)),
            mkEntry(TASK_3_1, Set.of(TP_3_1)),
            mkEntry(TASK_3_2, Set.of(TP_3_2))
        );
    }

    static Map<Subtopology, Set<TaskId>> getTasksForTopicGroup() {
        return mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_0_4, TASK_0_5, TASK_0_6)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2, TASK_1_3)),
            mkEntry(new Subtopology(2, null), Set.of(TASK_2_0, TASK_2_1, TASK_2_2, TASK_2_3)),
            mkEntry(new Subtopology(3, null), Set.of(TASK_3_0, TASK_3_1, TASK_3_2))
        );
    }

    static Map<TaskId, Set<TopicPartition>> getTaskChangelogMapForAllTasks() {
        return mkMap(
            mkEntry(TASK_0_0, Set.of(CHANGELOG_TP_0_0)),
            mkEntry(TASK_0_1, Set.of(CHANGELOG_TP_0_1)),
            mkEntry(TASK_0_2, Set.of(CHANGELOG_TP_0_2)),
            mkEntry(TASK_0_3, Set.of(CHANGELOG_TP_0_3)),
            mkEntry(TASK_0_4, Set.of(CHANGELOG_TP_0_4)),
            mkEntry(TASK_0_5, Set.of(CHANGELOG_TP_0_5)),
            mkEntry(TASK_0_6, Set.of(CHANGELOG_TP_0_6)),
            mkEntry(TASK_1_0, Set.of(CHANGELOG_TP_1_0)),
            mkEntry(TASK_1_1, Set.of(CHANGELOG_TP_1_1)),
            mkEntry(TASK_1_2, Set.of(CHANGELOG_TP_1_2)),
            mkEntry(TASK_1_3, Set.of(CHANGELOG_TP_1_3)),
            mkEntry(TASK_2_0, Set.of(CHANGELOG_TP_2_0)),
            mkEntry(TASK_2_1, Set.of(CHANGELOG_TP_2_1)),
            mkEntry(TASK_2_2, Set.of(CHANGELOG_TP_2_2)),
            mkEntry(TASK_2_3, Set.of(CHANGELOG_TP_2_3)),
            mkEntry(TASK_3_0, Set.of(CHANGELOG_TP_3_0)),
            mkEntry(TASK_3_1, Set.of(CHANGELOG_TP_3_1)),
            mkEntry(TASK_3_2, Set.of(CHANGELOG_TP_3_2))
        );
    }

    static InternalTopicManager mockInternalTopicManagerForChangelog() {
        final MockTime time = new MockTime();
        final StreamsConfig streamsConfig = new StreamsConfig(configProps(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC));
        final MockClientSupplier mockClientSupplier = new MockClientSupplier();
        final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            time,
            streamsConfig,
            mockClientSupplier.restoreConsumer,
            false
        );

        final MockInternalTopicManager spyTopicManager = spy(mockInternalTopicManager);
        doReturn(
            mkMap(
                mkEntry(
                    CHANGELOG_TP_0_NAME, Arrays.asList(
                        new TopicPartitionInfo(0, NODE_0, Arrays.asList(REPLICA_0), Collections.emptyList()),
                        new TopicPartitionInfo(1, NODE_1, Arrays.asList(REPLICA_1), Collections.emptyList()),
                        new TopicPartitionInfo(2, NODE_1, Arrays.asList(REPLICA_1), Collections.emptyList()),
                        new TopicPartitionInfo(3, NODE_2, Arrays.asList(REPLICA_2), Collections.emptyList()),
                        new TopicPartitionInfo(4, NODE_3, Arrays.asList(REPLICA_3), Collections.emptyList()),
                        new TopicPartitionInfo(5, NODE_4, Arrays.asList(REPLICA_4), Collections.emptyList()),
                        new TopicPartitionInfo(6, NODE_0, Arrays.asList(REPLICA_0), Collections.emptyList())
                    )
                ),
                mkEntry(
                    CHANGELOG_TP_1_NAME, Arrays.asList(
                        new TopicPartitionInfo(0, NODE_2, Arrays.asList(REPLICA_2), Collections.emptyList()),
                        new TopicPartitionInfo(1, NODE_3, Arrays.asList(REPLICA_3), Collections.emptyList()),
                        new TopicPartitionInfo(2, NODE_0, Arrays.asList(REPLICA_0), Collections.emptyList()),
                        new TopicPartitionInfo(3, NODE_4, Arrays.asList(REPLICA_4), Collections.emptyList())
                    )
                ),
                mkEntry(
                    CHANGELOG_TP_2_NAME, Arrays.asList(
                        new TopicPartitionInfo(0, NODE_1, Arrays.asList(REPLICA_1), Collections.emptyList()),
                        new TopicPartitionInfo(1, NODE_2, Arrays.asList(REPLICA_2), Collections.emptyList()),
                        new TopicPartitionInfo(2, NODE_4, Arrays.asList(REPLICA_4), Collections.emptyList()),
                        new TopicPartitionInfo(3, NODE_3, Arrays.asList(REPLICA_3), Collections.emptyList())
                    )
                ),
                mkEntry(
                    CHANGELOG_TP_3_NAME, Arrays.asList(
                        new TopicPartitionInfo(0, NODE_4, Arrays.asList(REPLICA_4), Collections.emptyList()),
                        new TopicPartitionInfo(1, NODE_3, Arrays.asList(REPLICA_3), Collections.emptyList()),
                        new TopicPartitionInfo(2, NODE_1, Arrays.asList(REPLICA_1), Collections.emptyList())
                    )
                )
            )
        ).when(spyTopicManager).getTopicPartitionInfo(anySet());
        return spyTopicManager;
    }

    static Map<Subtopology, Set<TaskId>> getTopologyGroupTaskMap() {
        return Collections.singletonMap(SUBTOPOLOGY_0, Collections.singleton(new TaskId(1, 1)));
    }

    static void verifyStandbySatisfyRackReplica(
        final Set<TaskId> taskIds,
        final Map<ProcessId, String> racksForProcess,
        final Map<ProcessId, ClientState> clientStateMap,
        final Integer replica,
        final boolean relaxRackCheck,
        final Map<ProcessId, Integer> standbyTaskCount
    ) {
        if (standbyTaskCount != null) {
            for (final Entry<ProcessId, ClientState> entry : clientStateMap.entrySet()) {
                final int expected = standbyTaskCount.get(entry.getKey());
                final int actual = entry.getValue().standbyTaskCount();
                assertEquals(expected, actual, "StandbyTaskCount for " + entry.getKey() + " doesn't match");
            }
        }
        for (final TaskId taskId : taskIds) {
            int activeCount = 0;
            int standbyCount = 0;
            final Map<String, ProcessId> racks = new HashMap<>();
            for (final Map.Entry<ProcessId, ClientState> entry : clientStateMap.entrySet()) {
                final ProcessId processId = entry.getKey();
                final ClientState clientState = entry.getValue();

                if (!relaxRackCheck && clientState.hasAssignedTask(taskId)) {
                    final String rack = racksForProcess.get(processId);
                    assertThat("Task " + taskId + " appears in both " + processId + " and " + racks.get(rack), racks.keySet(), not(hasItems(rack)));
                    racks.put(rack, processId);
                }

                boolean hasActive = false;
                if (clientState.hasActiveTask(taskId)) {
                    activeCount++;
                    hasActive = true;
                }

                boolean hasStandby = false;
                if (clientState.hasStandbyTask(taskId)) {
                    standbyCount++;
                    hasStandby = true;
                }

                assertFalse(hasActive && hasStandby, clientState + " has both active and standby task " + taskId);
            }

            assertEquals(1, activeCount, "Task " + taskId + " should have 1 active task");
            if (replica != null) {
                assertEquals(replica.intValue(), standbyCount, "Task " + taskId + " has wrong replica count");
            }
        }
    }

    static Map<ProcessId, Integer> clientTaskCount(final Map<ProcessId, ClientState> clientStateMap,
        final Function<ClientState, Integer> taskFunc) {
        return clientStateMap.entrySet().stream().collect(Collectors.toMap(Entry::getKey, v -> taskFunc.apply(v.getValue())));
    }

    static Map<ProcessId, Map<String, Optional<String>>> getProcessRacksForAllProcess() {
        return mkMap(
            mkEntry(PID_1, mkMap(mkEntry("1", Optional.of(RACK_0)))),
            mkEntry(PID_2, mkMap(mkEntry("1", Optional.of(RACK_1)))),
            mkEntry(PID_3, mkMap(mkEntry("1", Optional.of(RACK_2)))),
            mkEntry(PID_4, mkMap(mkEntry("1", Optional.of(RACK_3)))),
            mkEntry(PID_5, mkMap(mkEntry("1", Optional.of(RACK_4)))),
            mkEntry(PID_6, mkMap(mkEntry("1", Optional.of(RACK_0)))),
            mkEntry(PID_7, mkMap(mkEntry("1", Optional.of(RACK_1))))
        );
    }

    static RackAwareTaskAssignor getRackAwareTaskAssignor(final AssignmentConfigs configs) {
        return getRackAwareTaskAssignor(configs, mkMap());
    }

    static RackAwareTaskAssignor getRackAwareTaskAssignor(final AssignmentConfigs configs, final Map<Subtopology, Set<TaskId>> taskForTopicGroup) {
        return spy(
            new RackAwareTaskAssignor(
                getClusterForAllTopics(),
                getTaskTopicPartitionMapForAllTasks(),
                getTaskChangelogMapForAllTasks(),
                taskForTopicGroup,
                getProcessRacksForAllProcess(),
                mockInternalTopicManagerForChangelog(),
                configs,
                new MockTime()
            )
        );
    }

    static void verifyTaskPlacementWithRackAwareAssignor(final RackAwareTaskAssignor rackAwareTaskAssignor,
                                                         final Set<TaskId> allTaskIds,
                                                         final Map<ProcessId, ClientState> clientStates,
                                                         final boolean hasStandby,
                                                         final boolean enableRackAwareTaskAssignor) {
        // Verifies active and standby are in different clients
        verifyStandbySatisfyRackReplica(allTaskIds, rackAwareTaskAssignor.racksForProcess(), clientStates, null, true, null);

        if (enableRackAwareTaskAssignor) {
            verify(rackAwareTaskAssignor, times(2)).optimizeActiveTasks(any(), any(), anyInt(), anyInt());
            verify(rackAwareTaskAssignor, hasStandby ? times(1) : never()).optimizeStandbyTasks(any(), anyInt(), anyInt(), any());
        } else {
            verify(rackAwareTaskAssignor, never()).optimizeActiveTasks(any(), any(), anyInt(), anyInt());
            verify(rackAwareTaskAssignor, never()).optimizeStandbyTasks(any(), anyInt(), anyInt(), any());
        }
    }

    static SortedMap<ProcessId, ClientState> copyClientStateMap(final Map<ProcessId, ClientState> originalMap) {
        return new TreeMap<>(originalMap
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Entry::getKey,
                    entry -> new ClientState(entry.getValue())
                )
            )
        );
    }

    static synchronized Random getRandom() {
        if (random == null) {
            final long seed = System.currentTimeMillis();
            System.out.println("seed for getRandom: " + seed);
            random = new Random(seed);
        }
        return random;
    }
}
