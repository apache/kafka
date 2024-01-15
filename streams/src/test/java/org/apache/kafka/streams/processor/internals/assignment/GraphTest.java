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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class GraphTest {
    private Graph<Integer> graph;

    @Before
    public void setUp() {
        /*
         * Node 0 and 2 are both connected to node 1 and 3. There's a flow of 1 unit from 0 to 1 and 2 to
         * 3. The total cost in this case is 5. Min cost should be 2 by flowing 1 unit from 0 to 3 and 2
         * to 1
         */
        graph = new Graph<>();
        graph.addEdge(0, 1, 1, 3, 1);
        graph.addEdge(0, 3, 1, 1, 0);
        graph.addEdge(2, 1, 1, 1, 0);
        graph.addEdge(2, 3, 1, 2, 1);
        graph.addEdge(-1, 0, 1, 0, 1);
        graph.addEdge(-1, 2, 1, 0, 1);
        graph.addEdge(1, 99, 1, 0, 1);
        graph.addEdge(3, 99, 1, 0, 1);
        graph.setSourceNode(-1);
        graph.setSinkNode(99);
    }

    @Test
    public void testBasic() {
        final Set<Integer> nodes = graph.nodes();
        assertEquals(6, nodes.size());
        assertThat(nodes, contains(-1, 0, 1, 2, 3, 99));

        Map<Integer, Graph<Integer>.Edge> edges = graph.edges(0);
        assertEquals(2, edges.size());
        assertEquals(getEdge(1, 1, 3, 0, 1), edges.get(1));
        assertEquals(getEdge(3, 1, 1, 1, 0), edges.get(3));

        edges = graph.edges(2);
        assertEquals(2, edges.size());
        assertEquals(getEdge(1, 1, 1, 1, 0), edges.get(1));
        assertEquals(getEdge(3, 1, 2, 0, 1), edges.get(3));

        edges = graph.edges(1);
        assertEquals(1, edges.size());
        assertEquals(getEdge(99, 1, 0, 0, 1), edges.get(99));

        edges = graph.edges(3);
        assertEquals(1, edges.size());
        assertEquals(getEdge(99, 1, 0, 0, 1), edges.get(99));

        edges = graph.edges(-1);
        assertEquals(2, edges.size());
        assertEquals(getEdge(0, 1, 0, 0, 1), edges.get(0));
        assertEquals(getEdge(2, 1, 0, 0, 1), edges.get(2));

        edges = graph.edges(99);
        assertTrue(edges.isEmpty());

        assertFalse(graph.isResidualGraph());
    }

    @Test
    public void testResidualGraph() {
        final Graph<Integer> residualGraph = graph.residualGraph();
        final Graph<Integer> residualGraph1 = residualGraph.residualGraph();
        assertSame(residualGraph1, residualGraph);

        final Set<Integer> nodes = residualGraph.nodes();
        assertEquals(6, nodes.size());
        assertThat(nodes, contains(-1, 0, 1, 2, 3, 99));

        Map<Integer, Graph<Integer>.Edge> edges = residualGraph.edges(0);
        assertEquals(3, edges.size());
        assertEquals(getEdge(1, 1, 3, 0, 1), edges.get(1));
        assertEquals(getEdge(3, 1, 1, 1, 0), edges.get(3));
        assertEquals(getEdge(-1, 1, 0, 1, 0, false), edges.get(-1));

        edges = residualGraph.edges(2);
        assertEquals(3, edges.size());
        assertEquals(getEdge(1, 1, 1, 1, 0), edges.get(1));
        assertEquals(getEdge(3, 1, 2, 0, 1), edges.get(3));
        assertEquals(getEdge(-1, 1, 0, 1, 0, false), edges.get(-1));

        edges = residualGraph.edges(1);
        assertEquals(3, edges.size());
        assertEquals(getEdge(0, 1, -3, 1, 0, false), edges.get(0));
        assertEquals(getEdge(2, 1, -1, 0, 0, false), edges.get(2));
        assertEquals(getEdge(99, 1, 0, 0, 1), edges.get(99));

        edges = residualGraph.edges(3);
        assertEquals(3, edges.size());
        assertEquals(getEdge(0, 1, -1, 0, 0, false), edges.get(0));
        assertEquals(getEdge(2, 1, -2, 1, 0, false), edges.get(2));
        assertEquals(getEdge(99, 1, 0, 0, 1), edges.get(99));

        assertTrue(residualGraph.isResidualGraph());
    }

    @Test
    public void testInvalidOperation() {
        final Graph<Integer> graph1 = new Graph<>();
        Exception exception = assertThrows(IllegalArgumentException.class, () -> graph1.addEdge(0, 1, -1, 0, 0));
        assertEquals("Edge capacity cannot be negative", exception.getMessage());

        exception = assertThrows(IllegalArgumentException.class, () -> graph1.addEdge(0, 1, 1, 0, 2));
        assertEquals("Edge flow 2 cannot exceed capacity 1", exception.getMessage());

        graph1.addEdge(0, 1, 1, 1, 1);
        exception = assertThrows(IllegalArgumentException.class, () -> graph1.addEdge(1, 0, 1, 0, 0));
        assertEquals("There is already an edge from 0 to 1. Can not add an edge from 1 to 0 since "
            + "there will create a cycle between two nodes", exception.getMessage());

        final Graph<Integer> residualGraph = graph1.residualGraph();
        exception = assertThrows(IllegalStateException.class, residualGraph::solveMinCostFlow);
        assertEquals("Should not be residual graph to solve min cost flow", exception.getMessage());

    }

    @Test
    public void testInvalidSource() {
        final Graph<Integer> graph1 = new Graph<>();
        graph1.addEdge(0, 1, 1, 1, 0);
        graph1.addEdge(1, 2, 1, 1, 0);
        graph1.setSourceNode(1);
        graph1.setSinkNode(2);
        final Exception exception = assertThrows(IllegalStateException.class, graph1::solveMinCostFlow);
        assertEquals("Source node 1 shouldn't have input 0", exception.getMessage());
    }

    @Test
    public void testInvalidSink() {
        final Graph<Integer> graph1 = new Graph<>();
        graph1.addEdge(0, 1, 1, 1, 0);
        graph1.addEdge(1, 2, 1, 1, 0);
        graph1.setSourceNode(0);
        graph1.setSinkNode(1);
        final Exception exception = assertThrows(IllegalStateException.class, graph1::solveMinCostFlow);
        assertEquals("Sink node 1 shouldn't have output", exception.getMessage());
    }

    @Test
    public void testInvalidFlow() {
        final Graph<Integer> graph1 = new Graph<>();
        graph1.addEdge(0, 1, 1, 1, 1);
        graph1.addEdge(0, 2, 2, 1, 2);
        graph1.addEdge(1, 3, 1, 1, 1);
        graph1.addEdge(2, 3, 2, 1, 0); // Missing flow from 2 to 3
        graph1.setSourceNode(0);
        graph1.setSinkNode(3);
        final Exception exception = assertThrows(IllegalStateException.class, graph1::solveMinCostFlow);
        assertEquals("Input flow for node 2 is 2 which doesn't match output flow 0", exception.getMessage());
    }

    @Test
    public void testMissingSource() {
        final Graph<Integer> graph1 = new Graph<>();
        graph1.addEdge(0, 1, 1, 1, 1);
        graph1.addEdge(0, 2, 2, 1, 2);
        graph1.addEdge(1, 3, 1, 1, 1);
        graph1.addEdge(2, 3, 2, 1, 2);
        graph1.setSinkNode(3);
        final Exception exception = assertThrows(IllegalStateException.class, graph1::solveMinCostFlow);
        assertEquals("Output flow for source null is null which doesn't match input flow 3 for sink 3",
            exception.getMessage());
    }

    @Test
    public void testDisconnectedGraph() {
        final Graph<Integer> graph1 = new Graph<>();
        graph1.addEdge(0, 1, 1, 1, 1);
        graph1.addEdge(2, 3, 2, 1, 2);
        graph1.setSourceNode(0);
        graph1.setSinkNode(1);
        final Exception exception = assertThrows(IllegalStateException.class, graph1::solveMinCostFlow);
        assertEquals("Input flow for node 3 is 2 which doesn't match output flow null",
            exception.getMessage());
    }

    @Test
    public void testDisconnectedGraphCrossSourceSink() {
        final Graph<Integer> graph1 = new Graph<>();
        graph1.addEdge(0, 1, 1, 1, 1);
        graph1.addEdge(2, 3, 2, 1, 2);
        graph1.setSourceNode(0);
        graph1.setSinkNode(3);
        final Exception exception = assertThrows(IllegalStateException.class, graph1::solveMinCostFlow);
        assertEquals("Input flow for node 1 is 1 which doesn't match output flow null",
            exception.getMessage());
    }

    @Test
    public void testNullNode() {
        final Graph<Integer> graph1 = new Graph<>();
        assertThrows(NullPointerException.class, () -> graph1.addEdge(null, 1, 1, 1, 1));
        assertThrows(NullPointerException.class, () -> graph1.addEdge(1, null, 1, 1, 1));
    }

    @Test
    public void testJustSourceSink() {
        final Graph<Integer> graph1 = new Graph<>();
        graph1.addEdge(0, 1, 1, 1, 1);
        graph1.setSourceNode(0);
        graph1.setSinkNode(1);
        graph1.solveMinCostFlow();
        assertEquals(1, graph1.totalCost());
    }

    @Test
    public void testMinCostFlow() {
        // Original graph, flow from 0 to 1 and 2 to 3
        Map<Integer, Graph<Integer>.Edge> edges = graph.edges(0);
        Graph<Integer>.Edge edge = edges.get(1);
        assertEquals(1, edge.flow);
        assertEquals(0, edge.residualFlow);

        edge = edges.get(3);
        assertEquals(0, edge.flow);
        assertEquals(1, edge.residualFlow);

        edges = graph.edges(2);
        edge = edges.get(3);
        assertEquals(1, edge.flow);
        assertEquals(0, edge.residualFlow);

        edge = edges.get(1);
        assertEquals(0, edge.flow);
        assertEquals(1, edge.residualFlow);

        assertEquals(5, graph.totalCost());

        graph.solveMinCostFlow();

        assertEquals(2, graph.totalCost());

        edges = graph.edges(0);
        assertEquals(2, edges.size());

        // No flow from 0 to 1
        edge = edges.get(1);
        assertEquals(0, edge.flow);
        assertEquals(1, edge.residualFlow);

        // Flow from 0 to 3 now
        edge = edges.get(3);
        assertEquals(1, edge.flow);
        assertEquals(0, edge.residualFlow);

        edges = graph.edges(2);
        assertEquals(2, edges.size());

        // No flow from 2 to 3
        edge = edges.get(3);
        assertEquals(0, edge.flow);
        assertEquals(1, edge.residualFlow);

        // Flow from 2 to 1 now
        edge = edges.get(1);
        assertEquals(1, edge.flow);
        assertEquals(0, edge.residualFlow);
    }

    @Test
    public void testMinCostDetectNodeNotInNegativeCycle() {
        final Graph<Integer> graph1 = new Graph<>();

        graph1.addEdge(-1, 0, 1, 0, 1);
        graph1.addEdge(-1, 1, 1, 0, 1);

        graph1.addEdge(0, 2, 1, 1, 0);
        graph1.addEdge(0, 3, 1, 1, 0);
        graph1.addEdge(0, 4, 1, 10, 1);

        graph1.addEdge(1, 2, 1, 1, 0);
        graph1.addEdge(1, 3, 1, 10, 1);
        graph1.addEdge(1, 4, 1, 1, 0);

        graph1.addEdge(2, 99, 0, 0, 0);
        graph1.addEdge(3, 99, 1, 0, 1);
        graph1.addEdge(4, 99, 1, 0, 1);

        graph1.setSourceNode(-1);
        graph1.setSinkNode(99);

        assertEquals(20, graph1.totalCost());

        // In this graph, the node we found for negative cycle is 2. However 2 isn't in the negative
        // cycle itself. Negative cycle is 1 -> 4 -> 0 -> 3 -> 1
        graph1.solveMinCostFlow();
        assertEquals(2, graph1.totalCost());

        Map<Integer, Graph<Integer>.Edge> edges = graph1.edges(-1);
        assertEquals(getEdge(0, 1, 0, 0, 1), edges.get(0));
        assertEquals(getEdge(1, 1, 0, 0, 1), edges.get(1));

        edges = graph1.edges(0);
        assertEquals(getEdge(2, 1, 1, 1, 0), edges.get(2));
        assertEquals(getEdge(3, 1, 1, 0, 1), edges.get(3));
        assertEquals(getEdge(4, 1, 10, 1, 0), edges.get(4));

        edges = graph1.edges(1);
        assertEquals(getEdge(2, 1, 1, 1, 0), edges.get(2));
        assertEquals(getEdge(3, 1, 10, 1, 0), edges.get(3));
        assertEquals(getEdge(4, 1, 1, 0, 1), edges.get(4));

        edges = graph1.edges(2);
        assertEquals(getEdge(99, 0, 0, 0, 0), edges.get(99));

        edges = graph1.edges(3);
        assertEquals(getEdge(99, 1, 0, 0, 1), edges.get(99));

        edges = graph1.edges(4);
        assertEquals(getEdge(99, 1, 0, 0, 1), edges.get(99));
    }

    @Test
    public void testDeterministic() {
        final List<TestEdge> edgeList = new ArrayList<>();
        edgeList.add(new TestEdge(0, 1, 1, 2, 1));
        edgeList.add(new TestEdge(0, 2, 1, 1, 0));
        edgeList.add(new TestEdge(0, 3, 1, 1, 0));
        edgeList.add(new TestEdge(0, 4, 1, 1, 0));
        edgeList.add(new TestEdge(1, 5, 1, 1, 1));
        edgeList.add(new TestEdge(2, 5, 1, 1, 0));
        edgeList.add(new TestEdge(3, 5, 1, 1, 0));
        edgeList.add(new TestEdge(4, 5, 1, 1, 0));

        // Test no matter the order of adding edges, min cost flow flows from 0 to 2 and then from 2 to 5
        for (int i = 0; i < 10; i++) {
            Collections.shuffle(edgeList);
            final Graph<Integer> graph1 = new Graph<>();
            for (final TestEdge edge : edgeList) {
                graph1.addEdge(edge.source, edge.destination, edge.capacity, edge.cost, edge.flow);
            }
            graph1.setSourceNode(0);
            graph1.setSinkNode(5);
            assertEquals(3, graph1.totalCost());

            graph1.solveMinCostFlow();
            assertEquals(2, graph1.totalCost());

            Map<Integer, Graph<Integer>.Edge> edges = graph1.edges(0);
            assertEquals(4, edges.size());
            assertEquals(getEdge(1, 1, 2, 1, 0), edges.get(1));
            assertEquals(getEdge(2, 1, 1, 0, 1), edges.get(2));
            assertEquals(getEdge(3, 1, 1, 1, 0), edges.get(3));
            assertEquals(getEdge(4, 1, 1, 1, 0), edges.get(4));

            edges = graph1.edges(1);
            assertEquals(1, edges.size());
            assertEquals(getEdge(5, 1, 1, 1, 0), edges.get(5));

            edges = graph1.edges(2);
            assertEquals(1, edges.size());
            assertEquals(getEdge(5, 1, 1, 0, 1), edges.get(5));

            edges = graph1.edges(3);
            assertEquals(1, edges.size());
            assertEquals(getEdge(5, 1, 1, 1, 0), edges.get(5));

            edges = graph1.edges(4);
            assertEquals(1, edges.size());
            assertEquals(getEdge(5, 1, 1, 1, 0), edges.get(5));
        }
    }

    @Test
    public void testMaxFlowOnlySourceAndSink() {
        final Graph<Integer> graph1 = new Graph<>();
        graph1.addEdge(0, 1, 100, 0, 0);
        graph1.setSourceNode(0);
        graph1.setSinkNode(1);

        final long maxFlow = graph1.calculateMaxFlow();
        assertEquals(100, maxFlow);

        final Map<Integer, Graph<Integer>.Edge> edges = graph1.edges(0);
        assertEquals(100, edges.get(1).flow);
        assertEquals(0, edges.get(1).residualFlow);

    }

    @Test
    public void testMaxFlowBoundBySinkEdges() {
        // Edges connected to sink have less capacity
        final Graph<Integer> graph1 = new Graph<>();
        graph1.addEdge(0, 1, 10, 0, 0);
        graph1.addEdge(0, 2, 10, 0, 0);
        graph1.addEdge(0, 3, 10, 0, 0);
        graph1.addEdge(1, 4, 5, 0, 0);
        graph1.addEdge(2, 5, 5, 0, 0);
        graph1.addEdge(3, 6, 5, 0, 0);
        graph1.addEdge(4, 7, 2, 0, 0);
        graph1.addEdge(5, 7, 2, 0, 0);
        graph1.addEdge(6, 7, 2, 0, 0);
        graph1.setSourceNode(0);
        graph1.setSinkNode(7);
        final long maxFlow = graph1.calculateMaxFlow();
        assertEquals(6, maxFlow);

        Map<Integer, Graph<Integer>.Edge> edges = graph1.edges(0);
        assertEquals(2, edges.get(1).flow);
        assertEquals(2, edges.get(2).flow);
        assertEquals(2, edges.get(3).flow);

        edges = graph1.edges(1);
        assertEquals(2, edges.get(4).flow);

        edges = graph1.edges(2);
        assertEquals(2, edges.get(5).flow);

        edges = graph1.edges(3);
        assertEquals(2, edges.get(6).flow);

        edges = graph1.edges(4);
        assertEquals(2, edges.get(7).flow);

        edges = graph1.edges(5);
        assertEquals(2, edges.get(7).flow);

        edges = graph1.edges(6);
        assertEquals(2, edges.get(7).flow);
    }

    @Test
    public void testMaxFlowBoundBySourceEdges() {
        // Edges connected to source have less capacity
        final Graph<Integer> graph1 = new Graph<>();
        graph1.addEdge(0, 1, 1, 0, 0);
        graph1.addEdge(0, 2, 2, 0, 0);
        graph1.addEdge(0, 3, 3, 0, 0);
        graph1.addEdge(1, 4, 5, 0, 0);
        graph1.addEdge(2, 5, 5, 0, 0);
        graph1.addEdge(3, 6, 5, 0, 0);
        graph1.addEdge(4, 7, 10, 0, 0);
        graph1.addEdge(5, 7, 10, 0, 0);
        graph1.addEdge(6, 7, 10, 0, 0);
        graph1.setSourceNode(0);
        graph1.setSinkNode(7);
        final long maxFlow = graph1.calculateMaxFlow();
        assertEquals(6, maxFlow);

        Map<Integer, Graph<Integer>.Edge> edges = graph1.edges(0);
        assertEquals(1, edges.get(1).flow);
        assertEquals(2, edges.get(2).flow);
        assertEquals(3, edges.get(3).flow);

        edges = graph1.edges(1);
        assertEquals(1, edges.get(4).flow);

        edges = graph1.edges(2);
        assertEquals(2, edges.get(5).flow);

        edges = graph1.edges(3);
        assertEquals(3, edges.get(6).flow);

        edges = graph1.edges(4);
        assertEquals(1, edges.get(7).flow);

        edges = graph1.edges(5);
        assertEquals(2, edges.get(7).flow);

        edges = graph1.edges(6);
        assertEquals(3, edges.get(7).flow);
    }

    private static Graph<Integer>.Edge getEdge(final int destination, final int capacity, final int cost, final int residualFlow, final int flow) {
        return getEdge(destination, capacity, cost, residualFlow, flow, true);
    }

    private static Graph<Integer>.Edge getEdge(final int destination, final int capacity, final int cost, final int residualFlow, final int flow, final boolean forwardEdge) {
        return new Graph<Integer>().new Edge(destination, capacity, cost, residualFlow, flow, forwardEdge);
    }

    private static class TestEdge {
        final int source;
        final int destination;
        final int capacity;
        final int cost;
        final int flow;

        TestEdge(final int source, final int destination, final int capacity, final int cost, final int flow) {
            this.source = source;
            this.destination = destination;
            this.capacity = capacity;
            this.cost = cost;
            this.flow = flow;
        }
    }
}
