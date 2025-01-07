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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.CHANGELOG_TOPIC_PREFIX;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_RACK_AWARE_ASSIGNMENT_TAGS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.RACK_PREFIX;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TOPIC_PREFIX;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.appendClientStates;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertBalancedActiveAssignment;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertBalancedStatefulAssignment;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertValidAssignment;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.configProps;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRandomNodes;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRandomReplica;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.processIdForInt;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class TaskAssignorConvergenceTest {
    private static Random random;
    private static final Time TIME = new MockTime();

    @BeforeAll
    public static void beforeClass() {
        final long seed = System.currentTimeMillis();
        System.out.println("Seed is " + seed);
        random = new Random(seed);
    }

    private static final class Harness {
        private final Set<TaskId> statelessTasks;
        private final Map<TaskId, Long> statefulTaskEndOffsetSums;
        private final Map<ProcessId, ClientState> clientStates;
        private final Map<ProcessId, ClientState> droppedClientStates;
        private final StringBuilder history = new StringBuilder();

        public final Map<TaskId, Set<TopicPartition>> partitionsForTask;
        public final Map<TaskId, Set<TopicPartition>> changelogPartitionsForTask;
        public final Map<Subtopology, Set<TaskId>> tasksForTopicGroup;
        public final Cluster fullMetadata;
        public final Map<ProcessId, Map<String, Optional<String>>> racksForProcessConsumer;
        public final InternalTopicManager internalTopicManager;



        private static Harness initializeCluster(final int numStatelessTasks,
                                                 final int numStatefulTasks,
                                                 final int numClients,
                                                 final Supplier<Integer> partitionCountSupplier,
                                                 final int numNodes) {
            int subtopology = 0;
            final Set<TaskId> statelessTasks = new TreeSet<>();
            int remainingStatelessTasks = numStatelessTasks;
            final List<Node> nodes = getRandomNodes(numNodes);
            int nodeIndex = 0;
            final Set<PartitionInfo> partitionInfoSet = new HashSet<>();
            final Map<TaskId, Set<TopicPartition>> partitionsForTask = new HashMap<>();
            final Map<TaskId, Set<TopicPartition>> changelogPartitionsForTask = new HashMap<>();
            final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = new HashMap<>();

            while (remainingStatelessTasks > 0) {
                final int partitions = Math.min(remainingStatelessTasks, partitionCountSupplier.get());
                for (int i = 0; i < partitions; i++) {
                    final TaskId taskId = new TaskId(subtopology, i);
                    statelessTasks.add(taskId);
                    remainingStatelessTasks--;

                    final Node[] replica = getRandomReplica(nodes, nodeIndex, i);
                    partitionInfoSet.add(new PartitionInfo(TOPIC_PREFIX + "_" + subtopology, i, replica[0], replica, replica));
                    nodeIndex++;

                    partitionsForTask.put(taskId, Set.of(new TopicPartition(TOPIC_PREFIX + "_" + subtopology, i)));
                    tasksForTopicGroup.computeIfAbsent(new Subtopology(subtopology, null), k -> new HashSet<>()).add(taskId);
                }
                subtopology++;
            }

            final Map<TaskId, Long> statefulTaskEndOffsetSums = new TreeMap<>();
            final Map<String, List<TopicPartitionInfo>> topicPartitionInfo = new HashMap<>();
            final Set<String> changelogNames = new HashSet<>();
            int remainingStatefulTasks = numStatefulTasks;
            while (remainingStatefulTasks > 0) {
                final String changelogTopicName = CHANGELOG_TOPIC_PREFIX + "_" + subtopology;
                changelogNames.add(changelogTopicName);
                final int partitions = Math.min(remainingStatefulTasks, partitionCountSupplier.get());
                for (int i = 0; i < partitions; i++) {
                    final TaskId taskId = new TaskId(subtopology, i);
                    statefulTaskEndOffsetSums.put(taskId, 150000L);
                    remainingStatefulTasks--;

                    Node[] replica = getRandomReplica(nodes, nodeIndex, i);
                    partitionInfoSet.add(new PartitionInfo(TOPIC_PREFIX + "_" + subtopology, i, replica[0], replica, replica));
                    nodeIndex++;

                    partitionsForTask.put(taskId, Set.of(new TopicPartition(TOPIC_PREFIX + "_" + subtopology, i)));
                    changelogPartitionsForTask.put(taskId, Set.of(new TopicPartition(changelogTopicName, i)));
                    tasksForTopicGroup.computeIfAbsent(new Subtopology(subtopology, null), k -> new HashSet<>()).add(taskId);

                    final int changelogNodeIndex = random.nextInt(nodes.size());
                    replica = getRandomReplica(nodes, changelogNodeIndex, i);
                    final TopicPartitionInfo info = new TopicPartitionInfo(i, replica[0], Arrays.asList(replica[0], replica[1]), Collections.emptyList());
                    topicPartitionInfo.computeIfAbsent(changelogTopicName, tp -> new ArrayList<>()).add(info);
                }
                subtopology++;
            }

            final MockTime time = new MockTime();
            final StreamsConfig streamsConfig = new StreamsConfig(configProps(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC));
            final MockClientSupplier mockClientSupplier = new MockClientSupplier();
            final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
                time,
                streamsConfig,
                mockClientSupplier.restoreConsumer,
                false
            );
            final InternalTopicManager spyTopicManager = spy(mockInternalTopicManager);
            doReturn(topicPartitionInfo).when(spyTopicManager).getTopicPartitionInfo(changelogNames);

            final Cluster cluster = new Cluster(
                "cluster",
                new HashSet<>(nodes),
                partitionInfoSet,
                Collections.emptySet(),
                Collections.emptySet()
            );

            final Map<ProcessId, ClientState> clientStates = new TreeMap<>();
            final Map<ProcessId, Map<String, Optional<String>>> racksForProcessConsumer = new HashMap<>();
            for (int i = 0; i < numClients; i++) {
                final ProcessId uuid = processIdForInt(i);
                clientStates.put(uuid, emptyInstance(uuid, statefulTaskEndOffsetSums));
                final String rack = RACK_PREFIX + random.nextInt(nodes.size());
                racksForProcessConsumer.put(uuid, mkMap(mkEntry("consumer", Optional.of(rack))));
            }

            return new Harness(statelessTasks, statefulTaskEndOffsetSums, clientStates, cluster, partitionsForTask, changelogPartitionsForTask, tasksForTopicGroup, racksForProcessConsumer, spyTopicManager);
        }

        private Harness(final Set<TaskId> statelessTasks,
                        final Map<TaskId, Long> statefulTaskEndOffsetSums,
                        final Map<ProcessId, ClientState> clientStates,
                        final Cluster fullMetadata,
                        final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                        final Map<TaskId, Set<TopicPartition>> changelogPartitionsForTask,
                        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup,
                        final Map<ProcessId, Map<String, Optional<String>>> racksForProcessConsumer,
                        final InternalTopicManager internalTopicManager) {
            this.statelessTasks = statelessTasks;
            this.statefulTaskEndOffsetSums = statefulTaskEndOffsetSums;
            this.clientStates = clientStates;
            this.fullMetadata = fullMetadata;
            this.partitionsForTask = partitionsForTask;
            this.changelogPartitionsForTask = changelogPartitionsForTask;
            this.tasksForTopicGroup = tasksForTopicGroup;
            this.racksForProcessConsumer = racksForProcessConsumer;
            this.internalTopicManager = internalTopicManager;

            droppedClientStates = new TreeMap<>();
            history.append('\n');
            history.append("Cluster and application initial state: \n");
            history.append("Stateless tasks: ").append(statelessTasks).append('\n');
            history.append("Stateful tasks:  ").append(statefulTaskEndOffsetSums.keySet()).append('\n');
            history.append("Full metadata:  ").append(fullMetadata).append('\n');
            history.append("Partitions for tasks:  ").append(partitionsForTask).append('\n');
            history.append("Changelog partitions for tasks:  ").append(changelogPartitionsForTask).append('\n');
            history.append("Tasks for subtopology:  ").append(tasksForTopicGroup).append('\n');
            history.append("Racks for process consumer:  ").append(racksForProcessConsumer).append('\n');
            formatClientStates(true);
            history.append("History of the cluster: \n");
        }

        private void addClient() {
            final ProcessId uuid = processIdForInt(clientStates.size() + droppedClientStates.size());
            history.append("Adding new node ").append(uuid).append('\n');
            clientStates.put(uuid, emptyInstance(uuid, statefulTaskEndOffsetSums));
            final int nodeSize = fullMetadata.nodes().size();
            final String rack = RACK_PREFIX + random.nextInt(nodeSize);
            racksForProcessConsumer.computeIfAbsent(uuid, k -> new HashMap<>()).put("consumer", Optional.of(rack));
        }

        private static ClientState emptyInstance(final ProcessId uuid, final Map<TaskId, Long> allTaskEndOffsetSums) {
            final ClientState clientState = new ClientState(uuid, 1);
            clientState.computeTaskLags(uuid, allTaskEndOffsetSums);
            return clientState;
        }

        private void addOrResurrectClientsRandomly(final Random prng, final int limit) {
            final int numberToAdd = prng.nextInt(limit);
            for (int i = 0; i < numberToAdd; i++) {
                final boolean addNew = prng.nextBoolean();
                if (addNew || droppedClientStates.isEmpty()) {
                    addClient();
                } else {
                    final ProcessId uuid = selectRandomElement(prng, droppedClientStates);
                    history.append("Resurrecting node ").append(uuid).append('\n');
                    clientStates.put(uuid, droppedClientStates.get(uuid));
                    droppedClientStates.remove(uuid);
                }
            }
        }

        private void dropClient() {
            if (clientStates.isEmpty()) {
                throw new NoSuchElementException("There are no nodes to drop");
            } else {
                final ProcessId toDrop = clientStates.keySet().iterator().next();
                dropClient(toDrop);
            }
        }

        private void dropRandomClients(final int numNode, final Random prng) {
            int dropped = 0;
            while (!clientStates.isEmpty() && dropped < numNode) {
                final ProcessId toDrop = selectRandomElement(prng, clientStates);
                dropClient(toDrop);
                dropped++;
            }
            history.append("Stateless tasks: ").append(statelessTasks).append('\n');
            history.append("Stateful tasks:  ").append(statefulTaskEndOffsetSums.keySet()).append('\n');
            formatClientStates(true);
        }

        private void dropClient(final ProcessId toDrop) {
            final ClientState clientState = clientStates.remove(toDrop);
            history.append("Dropping node ").append(toDrop).append(": ").append(clientState).append('\n');
            droppedClientStates.put(toDrop, clientState);
        }

        private static ProcessId selectRandomElement(final Random prng, final Map<ProcessId, ClientState> clients) {
            int dropIndex = prng.nextInt(clients.size());
            ProcessId toDrop = null;
            for (final ProcessId uuid : clients.keySet()) {
                if (dropIndex == 0) {
                    toDrop = uuid;
                    break;
                } else {
                    dropIndex--;
                }
            }
            return toDrop;
        }

        /**
         * Flip the cluster states from "assigned" to "subscribed" so they can be used for another round of assignments.
         */
        private void prepareForNextRebalance() {
            final Map<ProcessId, ClientState> newClientStates = new TreeMap<>();
            for (final Map.Entry<ProcessId, ClientState> entry : clientStates.entrySet()) {
                final ProcessId uuid = entry.getKey();
                final ClientState newClientState = new ClientState(uuid, 1);
                final ClientState clientState = entry.getValue();
                final Map<TaskId, Long> taskOffsetSums = new TreeMap<>();
                for (final TaskId taskId : clientState.activeTasks()) {
                    if (statefulTaskEndOffsetSums.containsKey(taskId)) {
                        taskOffsetSums.put(taskId, statefulTaskEndOffsetSums.get(taskId));
                    }
                }
                for (final TaskId taskId : clientState.standbyTasks()) {
                    if (statefulTaskEndOffsetSums.containsKey(taskId)) {
                        taskOffsetSums.put(taskId, statefulTaskEndOffsetSums.get(taskId));
                    }
                }
                newClientState.addPreviousActiveTasks(clientState.activeTasks());
                newClientState.addPreviousStandbyTasks(clientState.standbyTasks());
                newClientState.addPreviousTasksAndOffsetSums("consumer", taskOffsetSums);
                newClientState.computeTaskLags(uuid, statefulTaskEndOffsetSums);
                newClientStates.put(uuid, newClientState);
            }

            clientStates.clear();
            clientStates.putAll(newClientStates);
        }

        private void recordConfig(final AssignmentConfigs configuration) {
            history.append("Creating assignor with configuration: ")
                   .append(configuration)
                   .append('\n');
        }

        private void recordBefore(final int iteration) {
            history.append("Starting Iteration: ").append(iteration).append('\n');
            formatClientStates(false);
        }

        private void recordAfter(final int iteration, final boolean rebalancePending) {
            history.append("After assignment:  ").append(iteration).append('\n');
            history.append("Rebalance pending: ").append(rebalancePending).append('\n');
            formatClientStates(true);
            history.append('\n');
        }

        private void formatClientStates(final boolean printUnassigned) {
            appendClientStates(history, clientStates);
            if (printUnassigned) {
                final Set<TaskId> unassignedTasks = new TreeSet<>();
                unassignedTasks.addAll(statefulTaskEndOffsetSums.keySet());
                unassignedTasks.addAll(statelessTasks);
                for (final Map.Entry<ProcessId, ClientState> entry : clientStates.entrySet()) {
                    unassignedTasks.removeAll(entry.getValue().assignedTasks());
                }
                history.append("Unassigned Tasks: ").append(unassignedTasks).append('\n');
            }
        }
    }

    private int skewThreshold = 1;

    public void setUp(final String rackAwareStrategy) {
        if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)) {
            // We take ceiling of [task_in_subtopology / total_task * original_task_assigned_to_client] as the capacity from
            // stage 1 client to stage 2 client which can result in the skew to be at most 2
            // For example, suppose there are 2 subtopologies s1 and s2. s1 has 2 tasks [t1, t2], s2 has 1 task t3. There are 2 clients c1 and c2 with
            // originally 2 tasks and 1 task. Then the capacity from stage 1 c1 to stage 2 c1 is ceil(2 * 2.0 / 3 * 2) = 2 which can result in both
            // t1 and t2 assigned to c1. So the max skew for stateful or stateless assignment could be 2 + 2 = 4.
            // Details in https://cwiki.apache.org/confluence/display/KAFKA/KIP-925%3A+Rack+aware+task+assignment+in+Kafka+Streams
            skewThreshold = 4;
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void staticAssignmentShouldConvergeWithTheFirstAssignment(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final AssignmentConfigs configs = new AssignmentConfigs(100L,
                                                                2,
                                                                0,
                                                                60_000L,
                                                                EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
                                                                null,
                                                                null,
                                                                rackAwareStrategy);

        final Harness harness = Harness.initializeCluster(1, 1, 1, () -> 1, 1);

        testForConvergence(harness, configs, 1);
        verifyValidAssignment(0, harness);
        verifyBalancedAssignment(harness, skewThreshold);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void assignmentShouldConvergeAfterAddingNode(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final int numStatelessTasks = 7;
        final int numStatefulTasks = 11;
        final int maxWarmupReplicas = 2;
        final int numStandbyReplicas = 0;
        final int numNodes = 10;

        final AssignmentConfigs configs = new AssignmentConfigs(100L,
                                                                maxWarmupReplicas,
                                                                numStandbyReplicas,
                                                                60_000L,
                                                                EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
                                                                null,
                                                                null,
                                                                rackAwareStrategy);

        final Harness harness = Harness.initializeCluster(numStatelessTasks, numStatefulTasks, 1, () -> 5, numNodes);
        testForConvergence(harness, configs, 1);
        harness.addClient();
        // we expect convergence to involve moving each task at most once, and we can move "maxWarmupReplicas" number
        // of tasks at once, hence the iteration limit
        testForConvergence(harness, configs, numStatefulTasks / maxWarmupReplicas + 1);
        verifyValidAssignment(numStandbyReplicas, harness);

        // min-cost rack aware assignor doesn't balance subtopolgy
        if (!rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)) {
            verifyBalancedAssignment(harness, skewThreshold);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void droppingNodesShouldConverge(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final int numStatelessTasks = 11;
        final int numStatefulTasks = 13;
        final int maxWarmupReplicas = 2;
        final int numStandbyReplicas = 0;
        final int numNodes = 10;

        final AssignmentConfigs configs = new AssignmentConfigs(100L,
                                                                maxWarmupReplicas,
                                                                numStandbyReplicas,
                                                                60_000L,
                                                                EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
                                                                null,
                                                                null,
                                                                rackAwareStrategy);

        final Harness harness = Harness.initializeCluster(numStatelessTasks, numStatefulTasks, 7, () -> 5, numNodes);
        testForConvergence(harness, configs, 1);
        harness.dropClient();
        // This time, we allow one extra iteration because the
        // first stateful task needs to get shuffled back to the first node
        testForConvergence(harness, configs, numStatefulTasks / maxWarmupReplicas + 2);

        verifyValidAssignment(numStandbyReplicas, harness);

        // min-cost rack aware assignor doesn't balance subtopolgy
        if (!rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)) {
            verifyBalancedAssignment(harness, skewThreshold);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void randomClusterPerturbationsShouldConverge(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        // do as many tests as we can in 10 seconds
        final long deadline = System.currentTimeMillis() + 10_000L;
        do {
            final long seed = new Random().nextLong();
            runRandomizedScenario(seed, rackAwareStrategy);
        } while (System.currentTimeMillis() < deadline);
    }

    private void runRandomizedScenario(final long seed, final String rackAwareStrategy) {
        Harness harness = null;
        try {
            final Random prng = new Random(seed);

            // These are all rand(limit)+1 because we need them to be at least 1 and the upper bound is exclusive
            final int initialClusterSize = prng.nextInt(10) + 1;
            final int numStatelessTasks = prng.nextInt(10) + 1;
            final int numStatefulTasks = prng.nextInt(10) + 1;
            final int maxWarmupReplicas = prng.nextInt(numStatefulTasks) + 1;
            // This one is rand(limit+1) because we _want_ to test zero and the upper bound is exclusive
            final int numStandbyReplicas = prng.nextInt(initialClusterSize + 1);
            final int numNodes = numStatefulTasks + numStatelessTasks;

            final int numberOfEvents = prng.nextInt(10) + 1;

            final AssignmentConfigs configs = new AssignmentConfigs(100L,
                                                                    maxWarmupReplicas,
                                                                    numStandbyReplicas,
                                                                    60_000L,
                                                                    EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
                                                                    null,
                                                                    null,
                                                                    rackAwareStrategy);

            harness = Harness.initializeCluster(
                numStatelessTasks,
                numStatefulTasks,
                initialClusterSize,
                () -> prng.nextInt(10) + 1,
                numNodes
            );
            testForConvergence(harness, configs, 1);
            verifyValidAssignment(numStandbyReplicas, harness);

            // min-cost rack aware assignor doesn't balance subtopolgy
            if (!rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)) {
                verifyBalancedAssignment(harness, skewThreshold);
            }

            for (int i = 0; i < numberOfEvents; i++) {
                final int event = prng.nextInt(2);
                switch (event) {
                    case 0:
                        harness.dropRandomClients(prng.nextInt(initialClusterSize), prng);
                        break;
                    case 1:
                        harness.addOrResurrectClientsRandomly(prng, initialClusterSize);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected event: " + event);
                }
                if (!harness.clientStates.isEmpty()) {
                    testForConvergence(harness, configs, 2 * (numStatefulTasks + numStatefulTasks * numStandbyReplicas));
                    verifyValidAssignment(numStandbyReplicas, harness);
                    // min-cost rack aware assignor doesn't balance subtopolgy
                    if (!rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)) {
                        verifyBalancedAssignment(harness, skewThreshold);
                    }
                }
            }
        } catch (final AssertionError t) {
            throw new AssertionError(
                "Assertion failed in randomized test. Reproduce with: `runRandomizedScenario(" + seed + ")`.",
                t
            );
        } catch (final Throwable t) {
            final StringBuilder builder =
                new StringBuilder()
                    .append("Exception in randomized scenario. Reproduce with: `runRandomizedScenario(")
                    .append(seed)
                    .append(")`. ");
            if (harness != null) {
                builder.append(harness.history);
            }
            throw new AssertionError(builder.toString(), t);
        }
    }

    private static void verifyBalancedAssignment(final Harness harness, final int skewThreshold) {
        final Set<TaskId> allStatefulTasks = harness.statefulTaskEndOffsetSums.keySet();
        final Map<ProcessId, ClientState> clientStates = harness.clientStates;
        final StringBuilder failureContext = harness.history;

        assertBalancedActiveAssignment(clientStates, failureContext);
        assertBalancedStatefulAssignment(allStatefulTasks, clientStates, failureContext);
        final AssignmentTestUtils.TaskSkewReport taskSkewReport = AssignmentTestUtils.analyzeTaskAssignmentBalance(harness.clientStates, skewThreshold);
        if (taskSkewReport.totalSkewedTasks() > 0) {
            fail("Expected a balanced task assignment, but was: " + taskSkewReport + '\n' + failureContext);
        }
    }

    private static void verifyValidAssignment(final int numStandbyReplicas, final Harness harness) {
        final Set<TaskId> statefulTasks = harness.statefulTaskEndOffsetSums.keySet();
        final Set<TaskId> statelessTasks = harness.statelessTasks;
        final Map<ProcessId, ClientState> assignedStates = harness.clientStates;
        final StringBuilder failureContext = harness.history;

        assertValidAssignment(numStandbyReplicas, statefulTasks, statelessTasks, assignedStates, failureContext);
    }

    private static void testForConvergence(final Harness harness,
                                           final AssignmentConfigs configs,
                                           final int iterationLimit) {
        final Set<TaskId> allTasks = new TreeSet<>();
        allTasks.addAll(harness.statelessTasks);
        allTasks.addAll(harness.statefulTaskEndOffsetSums.keySet());

        harness.recordConfig(configs);

        boolean rebalancePending = true;
        int iteration = 0;
        final RackAwareTaskAssignor rackAwareTaskAssignor = new RackAwareTaskAssignor(
            harness.fullMetadata,
            harness.partitionsForTask,
            harness.changelogPartitionsForTask,
            harness.tasksForTopicGroup,
            harness.racksForProcessConsumer,
            harness.internalTopicManager,
            configs,
            TIME
        );
        while (rebalancePending && iteration < iterationLimit) {
            iteration++;
            harness.prepareForNextRebalance();
            harness.recordBefore(iteration);
            rebalancePending = new HighAvailabilityTaskAssignor().assign(
                harness.clientStates,
                allTasks,
                harness.statefulTaskEndOffsetSums.keySet(),
                rackAwareTaskAssignor,
                configs
            );
            harness.recordAfter(iteration, rebalancePending);
        }

        if (rebalancePending) {
            fail("Rebalances have not converged after iteration cutoff: " + iterationLimit + harness.history);
        }
    }
}
