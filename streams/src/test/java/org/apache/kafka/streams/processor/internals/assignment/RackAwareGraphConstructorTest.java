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

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertBalancedTasks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertValidAssignment;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRandomClientState;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getTaskTopicPartitionMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getTasksForTopicGroup;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

@RunWith(Parameterized.class)
public class RackAwareGraphConstructorTest {
    private static final String MIN_COST = "min_cost";
    private static final String BALANCE_SUBTOPOLOGY = "balance_sub_topology";

    private static final int TP_SIZE = 40;
    private static final int PARTITION_SIZE = 3;
    private static final int TOPIC_GROUP_SIZE = TP_SIZE;
    private static final int CLIENT_SIZE = 20;

    private Graph<Integer> graph;
    private final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = getTaskTopicPartitionMap(
        TP_SIZE, PARTITION_SIZE, false);
    private final SortedSet<TaskId> taskIds = (SortedSet<TaskId>) taskTopicPartitionMap.keySet();
    private final List<TaskId> taskIdList = new ArrayList<>(taskIds);
    private final SortedMap<UUID, ClientState> clientStateMap = getRandomClientState(CLIENT_SIZE,
        TP_SIZE, PARTITION_SIZE, 1, false, taskIds);
    private final List<UUID> clientList = new ArrayList<>(clientStateMap.keySet());
    private final Map<TaskId, UUID> taskClientMap = new HashMap<>();
    private final Map<UUID, Integer> originalAssignedTaskNumber = new HashMap<>();
    private final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = getTasksForTopicGroup(TP_SIZE,
        PARTITION_SIZE);
    private RackAwareGraphConstructor constructor;

    @Parameter
    public String constructorType;

    @Parameterized.Parameters(name = "constructorType={0}")
    public static Collection<Object[]> getParamStoreType() {
        return asList(new Object[][] {
            {MIN_COST},
            {BALANCE_SUBTOPOLOGY}
        });
    }

    @Before
    public void setUp() {
        randomAssignTasksToClient(taskIdList, clientStateMap);

        if (constructorType.equals(MIN_COST)) {
            constructor = new MinTrafficGraphConstructor();
        } else if (constructorType.equals(BALANCE_SUBTOPOLOGY)) {
            constructor = new BalanceSubtopologyGraphConstructor(tasksForTopicGroup);
        }
        graph = constructor.constructTaskGraph(
            clientList, taskIdList, clientStateMap, taskClientMap, originalAssignedTaskNumber, ClientState::hasAssignedTask, this::getCost, 10, 1, false, false);
    }

    private int getCost(final TaskId taskId, final UUID processId, final boolean inCurrentAssignment, final int trafficCost, final int nonOverlapCost, final boolean isStandby) {
        return 1;
    }

    @Test
    public void testSubtopologyShouldContainAllTasks() {
        if (constructorType.equals(MIN_COST)) {
            return;
        }
        taskIdList.add(new TaskId(41, 0)); // Extra task not in subtopology map
        assertThrows(IllegalStateException.class, () -> graph = constructor.constructTaskGraph(
            clientList, taskIdList, clientStateMap, taskClientMap, originalAssignedTaskNumber,
            ClientState::hasAssignedTask, this::getCost, 10, 1, false, false));
    }

    @Test
    public void testMinCostGraphConstructor() {
        if (constructorType.equals(BALANCE_SUBTOPOLOGY)) {
            return;
        }
        // Flow should equal to task size which means each task is assigned now
        assertEquals(taskIdList.size(), graph.flow());

        // Total node size should match. Extra 2 is for source and sink nodes
        assertEquals(taskIdList.size() + clientList.size() + 2, graph.nodes().size());

        // Check edges from source to task nodes
        SortedMap<Integer, Graph<Integer>.Edge> edges = graph.edges(RackAwareGraphConstructor.SOURCE_ID);
        for (final Graph<Integer>.Edge edge : edges.values()) {
            assertEquals(1, edge.flow);
            assertEquals(1, edge.capacity);
            assertEquals(0, edge.residualFlow);
            assertEquals(0, edge.cost);
            Assertions.assertTrue(edge.forwardEdge);
        }

        // Check edges from task nodes to clients nodes,
        for (int taskNodeId = 0; taskNodeId < taskIdList.size(); taskNodeId++) {
            edges = graph.edges(taskNodeId);
            assertEquals(clientList.size(), edges.size());
            int assignedClient = 0;
            for (final Graph<Integer>.Edge edge : edges.values()) {
                final int flow = edge.flow;
                if (flow == 1) {
                    assignedClient++;
                }
                assertEquals(1, edge.capacity);
                assertEquals(flow == 1 ? 0 : 1, edge.residualFlow);
                assertEquals(1, edge.cost);
                Assertions.assertTrue(edge.forwardEdge);
            }
            // Task should be assigned to exact 1 client
            assertEquals(1, assignedClient);
            taskNodeId++;
        }

        // Check edges from second stage client node to sink
        final int sinkId = clientList.size() + taskIdList.size();
        int totalFlow = 0;
        for (int i = 0; i < clientList.size(); i++) {
            final UUID clientId = clientList.get(i);
            final int originalAssignedCount = originalAssignedTaskNumber.get(clientId);

            final int clientNodeId = i + taskIdList.size();
            edges = graph.edges(clientNodeId);
            assertEquals(1, edges.size());
            for (final Entry<Integer, Graph<Integer>.Edge> nodeEdge : edges.entrySet()) {
                final Integer nodeId = nodeEdge.getKey();
                assertEquals(sinkId, nodeId);
                totalFlow += nodeEdge.getValue().flow;
                assertEquals(originalAssignedCount, nodeEdge.getValue().capacity);
                Assertions.assertTrue(nodeEdge.getValue().forwardEdge);
            }
        }
        assertEquals(taskIdList.size(), totalFlow);
    }

    @Test
    public void testBalanceSubtopologyGraphConstructor() {
        if (constructorType.equals(MIN_COST)) {
            return;
        }
        // Flow should equal to task size which means each task is assigned now
        assertEquals(taskIdList.size(), graph.flow());

        // Total node size should match. Extra 2 is for source and sink nodes
        assertEquals(taskIdList.size() + TOPIC_GROUP_SIZE * clientList.size() + clientList.size() + 2, graph.nodes().size());

        // Check edges from source to task nodes
        SortedMap<Integer, Graph<Integer>.Edge> edges = graph.edges(RackAwareGraphConstructor.SOURCE_ID);
        for (final Graph<Integer>.Edge edge : edges.values()) {
            assertEquals(1, edge.flow);
            assertEquals(1, edge.capacity);
            assertEquals(0, edge.residualFlow);
            assertEquals(0, edge.cost);
            Assertions.assertTrue(edge.forwardEdge);
        }

        // Check edges from task nodes to first stage clients node,
        int taskNodeId = 0;
        for (final Set<TaskId> unsortedTaskIds : tasksForTopicGroup.values()) {
            for (int i = 0; i < unsortedTaskIds.size(); i++) {
                edges = graph.edges(taskNodeId);
                assertEquals(clientList.size(), edges.size());
                int assignedClient = 0;
                for (final Graph<Integer>.Edge edge : edges.values()) {
                    final int flow = edge.flow;
                    if (flow == 1) {
                        assignedClient++;
                    }
                    assertEquals(1, edge.capacity);
                    assertEquals(flow == 1 ? 0 : 1, edge.residualFlow);
                    assertEquals(1, edge.cost);
                    Assertions.assertTrue(edge.forwardEdge);
                }
                // Task should be assigned to exact 1 client
                assertEquals(1, assignedClient);
                taskNodeId++;
            }
        }

        // Check edges from first stage client node to second stage client node
        int topicGroupIndex = 0;
        for (final Set<TaskId> tasks : tasksForTopicGroup.values()) {
            final int taskCount = tasks.size();
            for (int j = 0; j < clientList.size(); j++) {
                final UUID clientId = clientList.get(j);
                final int originalAssignedCount = originalAssignedTaskNumber.get(clientId);
                final int expectedCapacity = (int) Math.ceil(originalAssignedCount * 1.0 / taskIdList.size() * taskCount);
                final int clientNodeId = topicGroupIndex * clientList.size() + taskIdList.size() + j;
                edges = graph.edges(clientNodeId);
                assertEquals(1, edges.size());
                for (final Entry<Integer, Graph<Integer>.Edge> nodeEdge : edges.entrySet()) {
                    final Integer nodeId = nodeEdge.getKey();
                    assertEquals(clientList.size() * tasksForTopicGroup.size() + taskIdList.size() + j, nodeId);
                    final Graph<Integer>.Edge edge = nodeEdge.getValue();
                    assertEquals(expectedCapacity, edge.capacity);
                    assertEquals(0, edge.cost);
                    Assertions.assertTrue(edge.forwardEdge);
                }
            }
            topicGroupIndex++;
        }

        // Check edges from second stage client node to sink
        final int sinkId = clientList.size() + tasksForTopicGroup.size() * clientList.size() + taskIdList.size();
        int totalFlow = 0;
        for (int i = 0; i < clientList.size(); i++) {
            final UUID clientId = clientList.get(i);
            final int originalAssignedCount = originalAssignedTaskNumber.get(clientId);

            final int clientNodeId =
                i + tasksForTopicGroup.size() * clientList.size() + taskIdList.size();
            edges = graph.edges(clientNodeId);
            assertEquals(1, edges.size());
            for (final Entry<Integer, Graph<Integer>.Edge> nodeEdge : edges.entrySet()) {
                final Integer nodeId = nodeEdge.getKey();
                assertEquals(sinkId, nodeId);
                totalFlow += nodeEdge.getValue().flow;
                assertEquals(originalAssignedCount, nodeEdge.getValue().capacity);
                Assertions.assertTrue(nodeEdge.getValue().forwardEdge);
            }
        }
        assertEquals(taskIdList.size(), totalFlow);
    }

    @Test
    public void testAssignTaskFromMinCostFlow() {
        graph.solveMinCostFlow();
        constructor.assignTaskFromMinCostFlow(
            graph,
            clientList,
            taskIdList,
            clientStateMap,
            originalAssignedTaskNumber,
            taskClientMap,
            ClientState::assignActive,
            ClientState::unassignActive,
            ClientState::hasAssignedTask
        );
        assertValidAssignment(0, taskIds, emptySet(), clientStateMap, new StringBuilder());
        if (constructorType.equals(BALANCE_SUBTOPOLOGY)) {
            assertBalancedTasks(clientStateMap);
        }
    }

    private void randomAssignTasksToClient(final List<TaskId> taskIdList, final SortedMap<UUID, ClientState> clientStateMap) {
        int totalAssigned = 0;
        for (final ClientState clientState : clientStateMap.values()) {
            clientState.assignActive(taskIdList.get(totalAssigned++));
            clientState.assignActive(taskIdList.get(totalAssigned++));
        }
        while (totalAssigned < taskIdList.size()) {
            for (final ClientState clientState : clientStateMap.values()) {
                if (AssignmentTestUtils.getRandom().nextInt(3) == 0) {
                    clientState.assignActive(taskIdList.get(totalAssigned));
                    totalAssigned++;
                    if (totalAssigned >= taskIdList.size()) {
                        break;
                    }
                }
            }
        }
    }
}
