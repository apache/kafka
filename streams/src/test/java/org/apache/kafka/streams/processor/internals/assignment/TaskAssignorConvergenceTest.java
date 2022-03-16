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

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.junit.Test;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Supplier;

import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_RACK_AWARE_ASSIGNMENT_TAGS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.appendClientStates;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertBalancedActiveAssignment;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertBalancedStatefulAssignment;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertValidAssignment;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.uuidForInt;
import static org.junit.Assert.fail;

public class TaskAssignorConvergenceTest {
    private static final class Harness {
        private final Set<TaskId> statelessTasks;
        private final Map<TaskId, Long> statefulTaskEndOffsetSums;
        private final Map<UUID, ClientState> clientStates;
        private final Map<UUID, ClientState> droppedClientStates;
        private final StringBuilder history = new StringBuilder();

        private static Harness initializeCluster(final int numStatelessTasks,
                                                 final int numStatefulTasks,
                                                 final int numNodes,
                                                 final Supplier<Integer> partitionCountSupplier) {
            int subtopology = 0;
            final Set<TaskId> statelessTasks = new TreeSet<>();
            int remainingStatelessTasks = numStatelessTasks;
            while (remainingStatelessTasks > 0) {
                final int partitions = Math.min(remainingStatelessTasks, partitionCountSupplier.get());
                for (int i = 0; i < partitions; i++) {
                    statelessTasks.add(new TaskId(subtopology, i));
                    remainingStatelessTasks--;
                }
                subtopology++;
            }

            final Map<TaskId, Long> statefulTaskEndOffsetSums = new TreeMap<>();
            int remainingStatefulTasks = numStatefulTasks;
            while (remainingStatefulTasks > 0) {
                final int partitions = Math.min(remainingStatefulTasks, partitionCountSupplier.get());
                for (int i = 0; i < partitions; i++) {
                    statefulTaskEndOffsetSums.put(new TaskId(subtopology, i), 150000L);
                    remainingStatefulTasks--;
                }
                subtopology++;
            }

            final Map<UUID, ClientState> clientStates = new TreeMap<>();
            for (int i = 0; i < numNodes; i++) {
                final UUID uuid = uuidForInt(i);
                clientStates.put(uuid, emptyInstance(uuid, statefulTaskEndOffsetSums));
            }

            return new Harness(statelessTasks, statefulTaskEndOffsetSums, clientStates);
        }

        private Harness(final Set<TaskId> statelessTasks,
                        final Map<TaskId, Long> statefulTaskEndOffsetSums,
                        final Map<UUID, ClientState> clientStates) {
            this.statelessTasks = statelessTasks;
            this.statefulTaskEndOffsetSums = statefulTaskEndOffsetSums;
            this.clientStates = clientStates;
            droppedClientStates = new TreeMap<>();
            history.append('\n');
            history.append("Cluster and application initial state: \n");
            history.append("Stateless tasks: ").append(statelessTasks).append('\n');
            history.append("Stateful tasks:  ").append(statefulTaskEndOffsetSums.keySet()).append('\n');
            formatClientStates(true);
            history.append("History of the cluster: \n");
        }

        private void addNode() {
            final UUID uuid = uuidForInt(clientStates.size() + droppedClientStates.size());
            history.append("Adding new node ").append(uuid).append('\n');
            clientStates.put(uuid, emptyInstance(uuid, statefulTaskEndOffsetSums));
        }

        private static ClientState emptyInstance(final UUID uuid, final Map<TaskId, Long> allTaskEndOffsetSums) {
            final ClientState clientState = new ClientState(1);
            clientState.computeTaskLags(uuid, allTaskEndOffsetSums);
            return clientState;
        }

        private void addOrResurrectNodesRandomly(final Random prng, final int limit) {
            final int numberToAdd = prng.nextInt(limit);
            for (int i = 0; i < numberToAdd; i++) {
                final boolean addNew = prng.nextBoolean();
                if (addNew || droppedClientStates.isEmpty()) {
                    addNode();
                } else {
                    final UUID uuid = selectRandomElement(prng, droppedClientStates);
                    history.append("Resurrecting node ").append(uuid).append('\n');
                    clientStates.put(uuid, droppedClientStates.get(uuid));
                    droppedClientStates.remove(uuid);
                }
            }
        }

        private void dropNode() {
            if (clientStates.isEmpty()) {
                throw new NoSuchElementException("There are no nodes to drop");
            } else {
                final UUID toDrop = clientStates.keySet().iterator().next();
                dropNode(toDrop);
            }
        }

        private void dropRandomNodes(final int numNode, final Random prng) {
            int dropped = 0;
            while (!clientStates.isEmpty() && dropped < numNode) {
                final UUID toDrop = selectRandomElement(prng, clientStates);
                dropNode(toDrop);
                dropped++;
            }
            history.append("Stateless tasks: ").append(statelessTasks).append('\n');
            history.append("Stateful tasks:  ").append(statefulTaskEndOffsetSums.keySet()).append('\n');
            formatClientStates(true);
        }

        private void dropNode(final UUID toDrop) {
            final ClientState clientState = clientStates.remove(toDrop);
            history.append("Dropping node ").append(toDrop).append(": ").append(clientState).append('\n');
            droppedClientStates.put(toDrop, clientState);
        }

        private static UUID selectRandomElement(final Random prng, final Map<UUID, ClientState> clients) {
            int dropIndex = prng.nextInt(clients.size());
            UUID toDrop = null;
            for (final UUID uuid : clients.keySet()) {
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
            final Map<UUID, ClientState> newClientStates = new TreeMap<>();
            for (final Map.Entry<UUID, ClientState> entry : clientStates.entrySet()) {
                final UUID uuid = entry.getKey();
                final ClientState newClientState = new ClientState(1);
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
                for (final Map.Entry<UUID, ClientState> entry : clientStates.entrySet()) {
                    unassignedTasks.removeAll(entry.getValue().assignedTasks());
                }
                history.append("Unassigned Tasks: ").append(unassignedTasks).append('\n');
            }
        }
    }

    @Test
    public void staticAssignmentShouldConvergeWithTheFirstAssignment() {
        final AssignmentConfigs configs = new AssignmentConfigs(100L,
                                                                2,
                                                                0,
                                                                60_000L,
                                                                EMPTY_RACK_AWARE_ASSIGNMENT_TAGS);

        final Harness harness = Harness.initializeCluster(1, 1, 1, () -> 1);

        testForConvergence(harness, configs, 1);
        verifyValidAssignment(0, harness);
        verifyBalancedAssignment(harness);
    }

    @Test
    public void assignmentShouldConvergeAfterAddingNode() {
        final int numStatelessTasks = 7;
        final int numStatefulTasks = 11;
        final int maxWarmupReplicas = 2;
        final int numStandbyReplicas = 0;

        final AssignmentConfigs configs = new AssignmentConfigs(100L,
                                                                maxWarmupReplicas,
                                                                numStandbyReplicas,
                                                                60_000L,
                                                                EMPTY_RACK_AWARE_ASSIGNMENT_TAGS);

        final Harness harness = Harness.initializeCluster(numStatelessTasks, numStatefulTasks, 1, () -> 5);
        testForConvergence(harness, configs, 1);
        harness.addNode();
        // we expect convergence to involve moving each task at most once, and we can move "maxWarmupReplicas" number
        // of tasks at once, hence the iteration limit
        testForConvergence(harness, configs, numStatefulTasks / maxWarmupReplicas + 1);
        verifyValidAssignment(numStandbyReplicas, harness);
        verifyBalancedAssignment(harness);
    }

    @Test
    public void droppingNodesShouldConverge() {
        final int numStatelessTasks = 11;
        final int numStatefulTasks = 13;
        final int maxWarmupReplicas = 2;
        final int numStandbyReplicas = 0;

        final AssignmentConfigs configs = new AssignmentConfigs(100L,
                                                                maxWarmupReplicas,
                                                                numStandbyReplicas,
                                                                60_000L,
                                                                EMPTY_RACK_AWARE_ASSIGNMENT_TAGS);

        final Harness harness = Harness.initializeCluster(numStatelessTasks, numStatefulTasks, 7, () -> 5);
        testForConvergence(harness, configs, 1);
        harness.dropNode();
        // This time, we allow one extra iteration because the
        // first stateful task needs to get shuffled back to the first node
        testForConvergence(harness, configs, numStatefulTasks / maxWarmupReplicas + 2);

        verifyValidAssignment(numStandbyReplicas, harness);
        verifyBalancedAssignment(harness);
    }

    @Test
    public void randomClusterPerturbationsShouldConverge() {
        // do as many tests as we can in 10 seconds
        final long deadline = System.currentTimeMillis() + 10_000L;
        do {
            final long seed = new Random().nextLong();
            runRandomizedScenario(seed);
        } while (System.currentTimeMillis() < deadline);
    }

    private static void runRandomizedScenario(final long seed) {
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

            final int numberOfEvents = prng.nextInt(10) + 1;

            final AssignmentConfigs configs = new AssignmentConfigs(100L,
                                                                    maxWarmupReplicas,
                                                                    numStandbyReplicas,
                                                                    60_000L,
                                                                    EMPTY_RACK_AWARE_ASSIGNMENT_TAGS);

            harness = Harness.initializeCluster(
                numStatelessTasks,
                numStatefulTasks,
                initialClusterSize,
                () -> prng.nextInt(10) + 1
            );
            testForConvergence(harness, configs, 1);
            verifyValidAssignment(numStandbyReplicas, harness);
            verifyBalancedAssignment(harness);

            for (int i = 0; i < numberOfEvents; i++) {
                final int event = prng.nextInt(2);
                switch (event) {
                    case 0:
                        harness.dropRandomNodes(prng.nextInt(initialClusterSize), prng);
                        break;
                    case 1:
                        harness.addOrResurrectNodesRandomly(prng, initialClusterSize);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected event: " + event);
                }
                if (!harness.clientStates.isEmpty()) {
                    testForConvergence(harness, configs, 2 * (numStatefulTasks + numStatefulTasks * numStandbyReplicas));
                    verifyValidAssignment(numStandbyReplicas, harness);
                    verifyBalancedAssignment(harness);
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

    private static void verifyBalancedAssignment(final Harness harness) {
        final Set<TaskId> allStatefulTasks = harness.statefulTaskEndOffsetSums.keySet();
        final Map<UUID, ClientState> clientStates = harness.clientStates;
        final StringBuilder failureContext = harness.history;

        assertBalancedActiveAssignment(clientStates, failureContext);
        assertBalancedStatefulAssignment(allStatefulTasks, clientStates, failureContext);
        final AssignmentTestUtils.TaskSkewReport taskSkewReport = AssignmentTestUtils.analyzeTaskAssignmentBalance(harness.clientStates);
        if (taskSkewReport.totalSkewedTasks() > 0) {
            fail(
                new StringBuilder().append("Expected a balanced task assignment, but was: ")
                                   .append(taskSkewReport)
                                   .append('\n')
                                   .append(failureContext)
                                   .toString()
            );
        }
    }

    private static void verifyValidAssignment(final int numStandbyReplicas, final Harness harness) {
        final Set<TaskId> statefulTasks = harness.statefulTaskEndOffsetSums.keySet();
        final Set<TaskId> statelessTasks = harness.statelessTasks;
        final Map<UUID, ClientState> assignedStates = harness.clientStates;
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
        while (rebalancePending && iteration < iterationLimit) {
            iteration++;
            harness.prepareForNextRebalance();
            harness.recordBefore(iteration);
            rebalancePending = new HighAvailabilityTaskAssignor().assign(
                harness.clientStates,
                allTasks,
                harness.statefulTaskEndOffsetSums.keySet(),
                configs
            );
            harness.recordAfter(iteration, rebalancePending);
        }

        if (rebalancePending) {
            final StringBuilder message =
                new StringBuilder().append("Rebalances have not converged after iteration cutoff: ")
                                   .append(iterationLimit)
                                   .append(harness.history);
            fail(message.toString());
        }
    }


}
