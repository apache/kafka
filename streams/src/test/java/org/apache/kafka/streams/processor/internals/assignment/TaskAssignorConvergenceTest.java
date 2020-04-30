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
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import static java.util.Collections.emptyMap;
import static org.apache.kafka.common.utils.Utils.entriesToMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.uuidForInt;
import static org.hamcrest.Matchers.is;
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
                                                 final int numNodes) {
            int subtopology = 0;
            final Set<TaskId> statelessTasks = new TreeSet<>();
            {
                int partition = 0;
                for (int i = 0; i < numStatelessTasks; i++) {
                    statelessTasks.add(new TaskId(subtopology, partition));
                    if (partition == 4) {
                        subtopology++;
                        partition = 0;
                    } else {
                        partition++;
                    }
                }
            }

            final Map<TaskId, Long> statefulTaskEndOffsetSums = new TreeMap<>();
            {
                subtopology++;
                int partition = 0;
                for (int i = 0; i < numStatefulTasks; i++) {
                    statefulTaskEndOffsetSums.put(new TaskId(subtopology, partition), 150000L);

                    if (partition == 4) {
                        subtopology++;
                        partition = 0;
                    } else {
                        partition++;
                    }
                }
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
                newClientState.addPreviousTasksAndOffsetSums(taskOffsetSums);
                newClientState.computeTaskLags(uuid, statefulTaskEndOffsetSums);
                newClientStates.put(uuid, newClientState);
            }

            clientStates.clear();
            clientStates.putAll(newClientStates);
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
            final Set<TaskId> unassignedTasks = new TreeSet<>();
            unassignedTasks.addAll(statefulTaskEndOffsetSums.keySet());
            unassignedTasks.addAll(statelessTasks);
            history.append('{').append('\n');
            for (final Map.Entry<UUID, ClientState> entry : clientStates.entrySet()) {
                history.append("  ").append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
                unassignedTasks.removeAll(entry.getValue().assignedTasks());
            }
            history.append('}').append('\n');
            if (printUnassigned) {
                history.append("Unassigned Tasks: ").append(unassignedTasks).append('\n');
            }
        }

    }

    @Test
    public void staticAssignmentShouldConvergeWithTheFirstAssignment() {
        final AssignmentConfigs configs = new AssignmentConfigs(100L,
                                                                1,
                                                                2,
                                                                0,
                                                                1000L);

        final Harness harness = Harness.initializeCluster(1, 1, 1);

        testForConvergence(harness, configs, 1);
        verifyValidAssignment(0, harness);
    }

    @Test
    public void assignmentShouldConvergeAfterAddingNode() {
        final int numStatelessTasks = 15;
        final int numStatefulTasks = 13;
        final int maxWarmupReplicas = 2;
        final int numStandbyReplicas = 0;

        final AssignmentConfigs configs = new AssignmentConfigs(100L,
                                                                1,
                                                                maxWarmupReplicas,
                                                                numStandbyReplicas,
                                                                1000L);

        final Harness harness = Harness.initializeCluster(numStatelessTasks, numStatefulTasks, 1);
        testForConvergence(harness, configs, 1);
        harness.addNode();
        // we expect convergence to involve moving each task at most once, and we can move "maxWarmupReplicas" number
        // of tasks at once, hence the iteration limit
        testForConvergence(harness, configs, numStatefulTasks / maxWarmupReplicas + 1);
        verifyValidAssignment(numStandbyReplicas, harness);
    }

    @Test
    public void droppingNodesShouldConverge() {
        final int numStatelessTasks = 15;
        final int numStatefulTasks = 13;
        final int maxWarmupReplicas = 2;
        final int numStandbyReplicas = 0;

        final AssignmentConfigs configs = new AssignmentConfigs(100L,
                                                                1,
                                                                maxWarmupReplicas,
                                                                numStandbyReplicas,
                                                                1000L);

        final Harness harness = Harness.initializeCluster(numStatelessTasks, numStatefulTasks, 7);
        testForConvergence(harness, configs, 1);
        harness.dropNode();
        // This time, we allow one extra iteration because the
        // first stateful task needs to get shuffled back to the first node
        testForConvergence(harness, configs, numStatefulTasks / maxWarmupReplicas + 2);

        verifyValidAssignment(numStandbyReplicas, harness);
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
                                                                    1,
                                                                    maxWarmupReplicas,
                                                                    numStandbyReplicas,
                                                                    1000L);

            harness = Harness.initializeCluster(numStatelessTasks, numStatefulTasks, initialClusterSize);
            testForConvergence(harness, configs, 1);
            verifyValidAssignment(numStandbyReplicas, harness);

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
                    testForConvergence(harness, configs, numStatefulTasks * 2);
                    verifyValidAssignment(numStandbyReplicas, harness);
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

    private static void verifyValidAssignment(final int numStandbyReplicas, final Harness harness) {
        final Map<TaskId, Set<UUID>> assignments = new TreeMap<>();
        for (final TaskId taskId : harness.statefulTaskEndOffsetSums.keySet()) {
            assignments.put(taskId, new TreeSet<>());
        }
        for (final TaskId taskId : harness.statelessTasks) {
            assignments.put(taskId, new TreeSet<>());
        }
        for (final Map.Entry<UUID, ClientState> entry : harness.clientStates.entrySet()) {
            for (final TaskId activeTask : entry.getValue().activeTasks()) {
                if (assignments.containsKey(activeTask)) {
                    assignments.get(activeTask).add(entry.getKey());
                }
            }
            for (final TaskId standbyTask : entry.getValue().standbyTasks()) {
                assignments.get(standbyTask).add(entry.getKey());
            }
        }
        final TreeMap<TaskId, Set<UUID>> misassigned =
            assignments
                .entrySet()
                .stream()
                .filter(entry -> {
                    final int expectedActives = 1;
                    final boolean isStateless = harness.statelessTasks.contains(entry.getKey());
                    final int expectedStandbys = isStateless ? 0 : numStandbyReplicas;
                    // We'll never assign even the expected number of standbys if they don't actually fit in the cluster
                    final int expectedAssignments = Math.min(
                        harness.clientStates.size(),
                        expectedActives + expectedStandbys
                    );
                    return entry.getValue().size() != expectedAssignments;
                })
                .collect(entriesToMap(TreeMap::new));

        MatcherAssert.assertThat(
            new StringBuilder().append("Found some over- or under-assigned tasks in the final assignment with ")
                               .append(numStandbyReplicas)
                               .append(" standby replicas.")
                               .append(harness.history)
                               .toString(),
            misassigned,
            is(emptyMap()));
    }

    private static void testForConvergence(final Harness harness,
                                           final AssignmentConfigs configs,
                                           final int iterationLimit) {
        final Set<TaskId> allTasks = new TreeSet<>();
        allTasks.addAll(harness.statelessTasks);
        allTasks.addAll(harness.statefulTaskEndOffsetSums.keySet());

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
