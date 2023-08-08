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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.SortedMap;
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
import org.apache.kafka.streams.processor.internals.InternalTopicManager;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;

import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public final class AssignmentTestUtils {

    public static final UUID UUID_1 = uuidForInt(1);
    public static final UUID UUID_2 = uuidForInt(2);
    public static final UUID UUID_3 = uuidForInt(3);
    public static final UUID UUID_4 = uuidForInt(4);
    public static final UUID UUID_5 = uuidForInt(5);
    public static final UUID UUID_6 = uuidForInt(6);
    public static final UUID UUID_7 = uuidForInt(7);
    public static final UUID UUID_8 = uuidForInt(8);
    public static final UUID UUID_9 = uuidForInt(9);

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

    public static final String CHANGELOG_TP_0_NAME = "store-0-changelog";
    public static final String CHANGELOG_TP_1_NAME = "store-1-changelog";
    public static final String CHANGELOG_TP_2_NAME = "store-2-changelog";

    public static final TopicPartition CHANGELOG_TP_0_0 = new TopicPartition(CHANGELOG_TP_0_NAME, 0);
    public static final TopicPartition CHANGELOG_TP_0_1 = new TopicPartition(CHANGELOG_TP_0_NAME, 1);
    public static final TopicPartition CHANGELOG_TP_0_2 = new TopicPartition(CHANGELOG_TP_0_NAME, 2);
    public static final TopicPartition CHANGELOG_TP_0_3 = new TopicPartition(CHANGELOG_TP_0_NAME, 3);
    public static final TopicPartition CHANGELOG_TP_1_0 = new TopicPartition(CHANGELOG_TP_1_NAME, 0);
    public static final TopicPartition CHANGELOG_TP_1_1 = new TopicPartition(CHANGELOG_TP_1_NAME, 1);
    public static final TopicPartition CHANGELOG_TP_1_2 = new TopicPartition(CHANGELOG_TP_1_NAME, 2);
    public static final TopicPartition CHANGELOG_TP_1_3 = new TopicPartition(CHANGELOG_TP_1_NAME, 3);
    public static final TopicPartition CHANGELOG_TP_2_0 = new TopicPartition(CHANGELOG_TP_2_NAME, 0);
    public static final TopicPartition CHANGELOG_TP_2_1 = new TopicPartition(CHANGELOG_TP_2_NAME, 1);
    public static final TopicPartition CHANGELOG_TP_2_2 = new TopicPartition(CHANGELOG_TP_2_NAME, 2);

    public static final TopicPartition TP_0_0 = new TopicPartition(TP_0_NAME, 0);
    public static final TopicPartition TP_0_1 = new TopicPartition(TP_0_NAME, 1);
    public static final TopicPartition TP_0_2 = new TopicPartition(TP_0_NAME, 2);
    public static final TopicPartition TP_0_3 = new TopicPartition(TP_0_NAME, 3);
    public static final TopicPartition TP_1_0 = new TopicPartition(TP_1_NAME, 0);
    public static final TopicPartition TP_1_1 = new TopicPartition(TP_1_NAME, 1);
    public static final TopicPartition TP_1_2 = new TopicPartition(TP_1_NAME, 2);
    public static final TopicPartition TP_1_3 = new TopicPartition(TP_1_NAME, 3);
    public static final TopicPartition TP_2_0 = new TopicPartition(TP_2_NAME, 0);
    public static final TopicPartition TP_2_1 = new TopicPartition(TP_2_NAME, 1);
    public static final TopicPartition TP_2_2 = new TopicPartition(TP_2_NAME, 2);

    public static final PartitionInfo PI_0_0 = new PartitionInfo(TP_0_NAME, 0, NODE_0, REPLICA_0, REPLICA_0);
    public static final PartitionInfo PI_0_1 = new PartitionInfo(TP_0_NAME, 1, NODE_1, REPLICA_1, REPLICA_1);
    public static final PartitionInfo PI_0_2 = new PartitionInfo(TP_0_NAME, 2, NODE_1, REPLICA_1, REPLICA_1);
    public static final PartitionInfo PI_0_3 = new PartitionInfo(TP_0_NAME, 3, NODE_2, REPLICA_2, REPLICA_2);
    public static final PartitionInfo PI_1_0 = new PartitionInfo(TP_1_NAME, 0, NODE_2, REPLICA_2, REPLICA_2);
    public static final PartitionInfo PI_1_1 = new PartitionInfo(TP_1_NAME, 1, NODE_3, REPLICA_3, REPLICA_3);
    public static final PartitionInfo PI_1_2 = new PartitionInfo(TP_1_NAME, 2, NODE_0, REPLICA_0, REPLICA_0);
    public static final PartitionInfo PI_1_3 = new PartitionInfo(TP_1_NAME, 3, NODE_1, REPLICA_1, REPLICA_1);
    public static final PartitionInfo PI_2_0 = new PartitionInfo(TP_2_NAME, 0, NODE_4, REPLICA_4, REPLICA_4);
    public static final PartitionInfo PI_2_1 = new PartitionInfo(TP_2_NAME, 1, NODE_3, REPLICA_3, REPLICA_3);
    public static final PartitionInfo PI_2_2 = new PartitionInfo(TP_2_NAME, 2, NODE_1, REPLICA_4, REPLICA_4);

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
    private static final String TOPIC_PREFIX = "topic";
    private static final String CHANGELOG_TOPIC_PREFIX = "changelog-topic";
    private static final String RACK_PREFIX = "rack";

    private AssignmentTestUtils() {}

    static Map<UUID, ClientState> getClientStatesMap(final ClientState... states) {
        final Map<UUID, ClientState> clientStates = new HashMap<>();
        int nthState = 1;
        for (final ClientState state : states) {
            clientStates.put(uuidForInt(nthState), state);
            ++nthState;
        }
        return clientStates;
    }

    // If you don't care about setting the end offsets for each specific topic partition, the helper method
    // getTopicPartitionOffsetMap is useful for building this input map for all partitions
    public static AdminClient createMockAdminClientForAssignor(final Map<TopicPartition, Long> changelogEndOffsets) {
        final AdminClient adminClient = mock(AdminClient.class);

        final ListOffsetsResult result = mock(ListOffsetsResult.class);
        when(adminClient.listOffsets(any())).thenReturn(result);
        for (final Map.Entry<TopicPartition, Long> entry : changelogEndOffsets.entrySet()) {
            final KafkaFutureImpl<ListOffsetsResultInfo> partitionFuture = new KafkaFutureImpl<>();
            final ListOffsetsResultInfo info = mock(ListOffsetsResultInfo.class);
            lenient().when(info.offset()).thenReturn(entry.getValue());
            partitionFuture.complete(info);
            lenient().when(result.partitionResult(entry.getKey())).thenReturn(partitionFuture);
        }

        return adminClient;
    }

    public static SubscriptionInfo getInfo(final UUID processId,
                                           final Set<TaskId> prevTasks,
                                           final Set<TaskId> standbyTasks) {
        return new SubscriptionInfo(
            LATEST_SUPPORTED_VERSION, LATEST_SUPPORTED_VERSION, processId, null, getTaskOffsetSums(prevTasks, standbyTasks), (byte) 0, 0, EMPTY_CLIENT_TAGS);
    }

    public static SubscriptionInfo getInfo(final UUID processId,
                                           final Set<TaskId> prevTasks,
                                           final Set<TaskId> standbyTasks,
                                           final String userEndPoint) {
        return new SubscriptionInfo(
            LATEST_SUPPORTED_VERSION, LATEST_SUPPORTED_VERSION, processId, userEndPoint, getTaskOffsetSums(prevTasks, standbyTasks), (byte) 0, 0, EMPTY_CLIENT_TAGS);
    }

    public static SubscriptionInfo getInfo(final UUID processId,
                                           final Set<TaskId> prevTasks,
                                           final Set<TaskId> standbyTasks,
                                           final byte uniqueField) {
        return new SubscriptionInfo(
            LATEST_SUPPORTED_VERSION, LATEST_SUPPORTED_VERSION, processId, null, getTaskOffsetSums(prevTasks, standbyTasks), uniqueField, 0, EMPTY_CLIENT_TAGS);
    }

    public static SubscriptionInfo getInfo(final UUID processId,
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
     * Builds a UUID by repeating the given number n. For valid n, it is guaranteed that the returned UUIDs satisfy
     * the same relation relative to others as their parameter n does: iff n < m, then uuidForInt(n) < uuidForInt(m)
     */
    public static UUID uuidForInt(final int n) {
        return new UUID(0, n);
    }

    static void assertValidAssignment(final int numStandbyReplicas,
                                      final Set<TaskId> statefulTasks,
                                      final Set<TaskId> statelessTasks,
                                      final Map<UUID, ClientState> assignedStates,
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
                                      final Map<UUID, ClientState> assignedStates,
                                      final StringBuilder failureContext) {
        final Map<TaskId, Set<UUID>> assignments = new TreeMap<>();
        for (final TaskId taskId : statefulTasks) {
            assignments.put(taskId, new TreeSet<>());
        }
        for (final TaskId taskId : statelessTasks) {
            assignments.put(taskId, new TreeSet<>());
        }
        for (final Map.Entry<UUID, ClientState> entry : assignedStates.entrySet()) {
            validateAndAddActiveAssignments(statefulTasks, statelessTasks, failureContext, assignments, entry);
            validateAndAddStandbyAssignments(statefulTasks, statelessTasks, failureContext, assignments, entry);
        }

        final AtomicInteger remainingWarmups = new AtomicInteger(maxWarmupReplicas);

        final TreeMap<TaskId, Set<UUID>> misassigned =
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
            assertThat(
                new StringBuilder().append("Found some over- or under-assigned tasks in the final assignment with ")
                                   .append(numStandbyReplicas)
                                   .append(" and max warmups ")
                                   .append(maxWarmupReplicas)
                                   .append(" standby replicas, stateful tasks:")
                                   .append(statefulTasks)
                                   .append(", and stateless tasks:")
                                   .append(statelessTasks)
                                   .append(failureContext)
                                   .toString(),
                misassigned,
                is(emptyMap()));
        }
    }

    private static void validateAndAddStandbyAssignments(final Set<TaskId> statefulTasks,
                                                         final Set<TaskId> statelessTasks,
                                                         final StringBuilder failureContext,
                                                         final Map<TaskId, Set<UUID>> assignments,
                                                         final Map.Entry<UUID, ClientState> entry) {
        for (final TaskId standbyTask : entry.getValue().standbyTasks()) {
            if (statelessTasks.contains(standbyTask)) {
                throw new AssertionError(
                    new StringBuilder().append("Found a standby task for stateless task ")
                                       .append(standbyTask)
                                       .append(" on client ")
                                       .append(entry)
                                       .append(" stateless tasks:")
                                       .append(statelessTasks)
                                       .append(failureContext)
                                       .toString()
                );
            } else if (assignments.containsKey(standbyTask)) {
                assignments.get(standbyTask).add(entry.getKey());
            } else {
                throw new AssertionError(
                    new StringBuilder().append("Found an extra standby task ")
                                       .append(standbyTask)
                                       .append(" on client ")
                                       .append(entry)
                                       .append(" but expected stateful tasks:")
                                       .append(statefulTasks)
                                       .append(failureContext)
                                       .toString()
                );
            }
        }
    }

    private static void validateAndAddActiveAssignments(final Set<TaskId> statefulTasks,
                                                        final Set<TaskId> statelessTasks,
                                                        final StringBuilder failureContext,
                                                        final Map<TaskId, Set<UUID>> assignments,
                                                        final Map.Entry<UUID, ClientState> entry) {
        for (final TaskId activeTask : entry.getValue().activeTasks()) {
            if (assignments.containsKey(activeTask)) {
                assignments.get(activeTask).add(entry.getKey());
            } else {
                throw new AssertionError(
                    new StringBuilder().append("Found an extra active task ")
                                       .append(activeTask)
                                       .append(" on client ")
                                       .append(entry)
                                       .append(" but expected stateful tasks:")
                                       .append(statefulTasks)
                                       .append(" and stateless tasks:")
                                       .append(statelessTasks)
                                       .append(failureContext)
                                       .toString()
                );
            }
        }
    }

    static void assertBalancedStatefulAssignment(final Set<TaskId> allStatefulTasks,
                                                 final Map<UUID, ClientState> clientStates,
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

    static void assertBalancedActiveAssignment(final Map<UUID, ClientState> clientStates,
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

    static void assertBalancedTasks(final Map<UUID, ClientState> clientStates) {
        final TaskSkewReport taskSkewReport = analyzeTaskAssignmentBalance(clientStates);
        if (taskSkewReport.totalSkewedTasks() > 0) {
            fail("Expected a balanced task assignment, but was: " + taskSkewReport);
        }
    }

    static TaskSkewReport analyzeTaskAssignmentBalance(final Map<UUID, ClientState> clientStates) {
        final Function<Integer, Map<UUID, AtomicInteger>> initialClientCounts =
            i -> clientStates.keySet().stream().collect(Collectors.toMap(c -> c, c -> new AtomicInteger(0)));

        final Map<Integer, Map<UUID, AtomicInteger>> subtopologyToClientsWithPartition = new TreeMap<>();
        for (final Map.Entry<UUID, ClientState> entry : clientStates.entrySet()) {
            final UUID client = entry.getKey();
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

        for (final Map.Entry<Integer, Map<UUID, AtomicInteger>> entry : subtopologyToClientsWithPartition.entrySet()) {
            final Map<UUID, AtomicInteger> clientsWithPartition = entry.getValue();
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            for (final AtomicInteger count : clientsWithPartition.values()) {
                max = Math.max(max, count.get());
                min = Math.min(min, count.get());
            }
            final int taskSkew = max - min;
            maxTaskSkew = Math.max(maxTaskSkew, taskSkew);
            if (taskSkew > 1) {
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
                                   final Map<UUID, ClientState> clientStates) {
        stringBuilder.append('{').append('\n');
        for (final Map.Entry<UUID, ClientState> entry : clientStates.entrySet()) {
            stringBuilder.append("  ").append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
        }
        stringBuilder.append('}').append('\n');
    }

    static final class TaskSkewReport {
        private final int maxTaskSkew;
        private final Set<Integer> skewedSubtopologies;
        private final Map<Integer, Map<UUID, AtomicInteger>> subtopologyToClientsWithPartition;

        private TaskSkewReport(final int maxTaskSkew,
                               final Set<Integer> skewedSubtopologies,
                               final Map<Integer, Map<UUID, AtomicInteger>> subtopologyToClientsWithPartition) {
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
        Collections.shuffle(nodeList);
        return nodeList;
    }

    static Cluster getRandomCluster(final int nodeSize, final int tpSize) {
        final List<Node> nodeList = getRandomNodes(nodeSize);
        final Set<PartitionInfo> partitionInfoSet = new HashSet<>();
        for (int i = 0; i < tpSize; i++) {
            final Node firstNode = nodeList.get(i % nodeSize);
            final Node secondNode = nodeList.get((i + 1) % nodeSize);
            final Node[] replica = new Node[] {firstNode, secondNode};
            partitionInfoSet.add(new PartitionInfo(TOPIC_PREFIX + i, 0, firstNode, replica, replica));
        }

        return new Cluster(
            "cluster",
            new HashSet<>(nodeList),
            partitionInfoSet,
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    static Map<UUID, Map<String, Optional<String>>> getRandomProcessRacks(final int clientSize, final int nodeSize) {
        final List<String> racks = new ArrayList<>(nodeSize);
        for (int i = 0; i < nodeSize; i++) {
            racks.add(RACK_PREFIX + i);
        }
        Collections.shuffle(racks);
        final Map<UUID, Map<String, Optional<String>>> processRacks = new HashMap<>();
        for (int i = 0; i < clientSize; i++) {
            final String rack = racks.get(i % nodeSize);
            processRacks.put(uuidForInt(i), mkMap(mkEntry("1", Optional.of(rack))));
        }
        return processRacks;
    }

    static SortedMap<TaskId, Set<TopicPartition>> getTaskTopicPartitionMap(final int tpSize, final boolean changelog) {
        final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = new TreeMap<>();
        final String topicName = changelog ? CHANGELOG_TOPIC_PREFIX : TOPIC_PREFIX;
        for (int i = 0; i < tpSize; i++) {
            taskTopicPartitionMap.put(new TaskId(i, 0), mkSet(
                new TopicPartition(topicName + i, 0),
                new TopicPartition(topicName + (i + 1) % tpSize, 0)
            ));
        }
        return taskTopicPartitionMap;
    }

    static Map<String, Object> configProps(final boolean enableRackAwareAssignor) {
        return configProps(enableRackAwareAssignor, 0);
    }

    static Map<String, Object> configProps(final boolean enableRackAwareAssignor, final int replicaNum) {
        final Map<String, Object> configurationMap = new HashMap<>();
        configurationMap.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        configurationMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, USER_END_POINT);
        configurationMap.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, replicaNum);
        if (enableRackAwareAssignor) {
            configurationMap.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC);
        }

        final ReferenceContainer referenceContainer = new ReferenceContainer();
        configurationMap.put(InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR, referenceContainer);
        return configurationMap;
    }

    static InternalTopicManager mockInternalTopicManagerForRandomChangelog(final int nodeSize, final int tpSize) {
        final MockTime time = new MockTime();
        final StreamsConfig streamsConfig = new StreamsConfig(configProps(true));
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

            final Node firstNode = nodeList.get(i % nodeSize);
            final Node secondNode = nodeList.get((i + 1) % nodeSize);
            final TopicPartitionInfo info = new TopicPartitionInfo(0, firstNode, Arrays.asList(firstNode, secondNode), Collections.emptyList());

            topicPartitionInfo.computeIfAbsent(topicName, tp -> new ArrayList<>()).add(info);
        }

        final MockInternalTopicManager spyTopicManager = spy(mockInternalTopicManager);
        doReturn(topicPartitionInfo).when(spyTopicManager).getTopicPartitionInfo(changelogNames);
        return spyTopicManager;
    }

    static SortedMap<UUID, ClientState> getRandomClientState(final int clientSize, final int tpSize, final int maxCapacity) {
        return getRandomClientState(clientSize, tpSize, maxCapacity, true);
    }

    static SortedMap<UUID, ClientState> getRandomClientState(final int clientSize, final int tpSize, final int maxCapacity, final boolean initialAssignment) {
        final SortedMap<UUID, ClientState> clientStates = new TreeMap<>();
        final Map<TaskId, Long> taskLags = new HashMap<>();
        for (int i = 0; i < tpSize; i++) {
            taskLags.put(new TaskId(i, 0), 0L);
        }

        final long seed = System.currentTimeMillis();
        System.out.println("seed: " + seed);
        final Random random = new Random(seed);
        for (int i = 0; i < clientSize; i++) {
            final int capacity = random.nextInt(maxCapacity) + 1;
            final ClientState clientState = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, capacity, uuidForInt(i));
            clientStates.put(uuidForInt(i), clientState);
        }

        if (initialAssignment) {
            Iterator<Entry<UUID, ClientState>> iterator = clientStates.entrySet().iterator();
            final List<TaskId> taskIds = new ArrayList<>(tpSize);
            for (int i = 0; i < tpSize; i++) {
                taskIds.add(new TaskId(i, 0));
            }
            Collections.shuffle(taskIds);
            for (final TaskId taskId : taskIds) {
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
            mkSet(NODE_0, NODE_1, NODE_2, NODE_3, NODE_4),
            mkSet(PI_0_0, PI_0_1, PI_0_2, PI_0_3, PI_1_0, PI_1_1, PI_1_2, PI_1_3, PI_2_0, PI_2_1, PI_2_2),
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    static Map<TaskId, Set<TopicPartition>> getTaskTopicPartitionMapForAllTasks() {
        return mkMap(
            mkEntry(TASK_0_0, mkSet(TP_0_0)),
            mkEntry(TASK_0_1, mkSet(TP_0_1)),
            mkEntry(TASK_0_2, mkSet(TP_0_2)),
            mkEntry(TASK_0_3, mkSet(TP_0_3)),
            mkEntry(TASK_1_0, mkSet(TP_1_0)),
            mkEntry(TASK_1_1, mkSet(TP_1_1)),
            mkEntry(TASK_1_2, mkSet(TP_1_2)),
            mkEntry(TASK_1_3, mkSet(TP_1_3)),
            mkEntry(TASK_2_0, mkSet(TP_2_0)),
            mkEntry(TASK_2_1, mkSet(TP_2_1)),
            mkEntry(TASK_2_2, mkSet(TP_2_2))
        );
    }

    static Map<TaskId, Set<TopicPartition>> getTaskChangelogMapForAllTasks() {
        return mkMap(
            mkEntry(TASK_0_0, mkSet(CHANGELOG_TP_0_0)),
            mkEntry(TASK_0_1, mkSet(CHANGELOG_TP_0_1)),
            mkEntry(TASK_0_2, mkSet(CHANGELOG_TP_0_2)),
            mkEntry(TASK_0_3, mkSet(CHANGELOG_TP_0_3)),
            mkEntry(TASK_1_0, mkSet(CHANGELOG_TP_1_0)),
            mkEntry(TASK_1_1, mkSet(CHANGELOG_TP_1_1)),
            mkEntry(TASK_1_2, mkSet(CHANGELOG_TP_1_2)),
            mkEntry(TASK_1_3, mkSet(CHANGELOG_TP_1_3)),
            mkEntry(TASK_2_0, mkSet(CHANGELOG_TP_2_0)),
            mkEntry(TASK_2_1, mkSet(CHANGELOG_TP_2_1)),
            mkEntry(TASK_2_2, mkSet(CHANGELOG_TP_2_2))
        );
    }

    static InternalTopicManager mockInternalTopicManagerForChangelog() {
        final MockTime time = new MockTime();
        final StreamsConfig streamsConfig = new StreamsConfig(configProps(true));
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
                        new TopicPartitionInfo(3, NODE_2, Arrays.asList(REPLICA_2), Collections.emptyList())
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
                        new TopicPartitionInfo(2, NODE_4, Arrays.asList(REPLICA_4), Collections.emptyList())
                    )
                )
            )
        ).when(spyTopicManager).getTopicPartitionInfo(mkSet(CHANGELOG_TP_0_NAME, CHANGELOG_TP_1_NAME, CHANGELOG_TP_2_NAME));
        return spyTopicManager;
    }

    static Map<Subtopology, Set<TaskId>> getTopologyGroupTaskMap() {
        return Collections.singletonMap(SUBTOPOLOGY_0, Collections.singleton(new TaskId(1, 1)));
    }

    static void verifyStandbySatisfyRackReplica(
        final Set<TaskId> taskIds,
        final Map<UUID, String> racksForProcess,
        final Map<UUID, ClientState> clientStateMap,
        final Integer replica,
        final boolean relaxRackCheck,
        final Map<UUID, Integer> standbyTaskCount
    ) {
        if (standbyTaskCount != null) {
            for (final Entry<UUID, ClientState> entry : clientStateMap.entrySet()) {
                final int expected = standbyTaskCount.get(entry.getKey());
                final int actual = entry.getValue().standbyTaskCount();
                assertEquals("StandbyTaskCount for " + entry.getKey() + " doesn't match", expected, actual);
            }
        }
        for (final TaskId taskId : taskIds) {
            int activeCount = 0;
            int standbyCount = 0;
            final Map<String, UUID> racks = new HashMap<>();
            for (final Map.Entry<UUID, ClientState> entry : clientStateMap.entrySet()) {
                final UUID processId = entry.getKey();
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

                assertFalse(clientState + " has both active and standby task " + taskId, hasActive && hasStandby);
            }

            assertEquals("Task " + taskId + " should have 1 active task", 1, activeCount);
            if (replica != null) {
                assertEquals("Task " + taskId + " has wrong replica count", replica.intValue(), standbyCount);
            }
        }
    }

    static Map<UUID, Integer> clientTaskCount(final Map<UUID, ClientState> clientStateMap,
        final Function<ClientState, Integer> taskFunc) {
        return clientStateMap.entrySet().stream().collect(Collectors.toMap(Entry::getKey, v -> taskFunc.apply(v.getValue())));
    }

    static Map<UUID, Map<String, Optional<String>>> getProcessRacksForAllProcess() {
        return mkMap(
            mkEntry(UUID_1, mkMap(mkEntry("1", Optional.of(RACK_0)))),
            mkEntry(UUID_2, mkMap(mkEntry("1", Optional.of(RACK_1)))),
            mkEntry(UUID_3, mkMap(mkEntry("1", Optional.of(RACK_2)))),
            mkEntry(UUID_4, mkMap(mkEntry("1", Optional.of(RACK_3)))),
            mkEntry(UUID_5, mkMap(mkEntry("1", Optional.of(RACK_4)))),
            mkEntry(UUID_6, mkMap(mkEntry("1", Optional.of(RACK_0)))),
            mkEntry(UUID_7, mkMap(mkEntry("1", Optional.of(RACK_1))))
        );
    }
}
