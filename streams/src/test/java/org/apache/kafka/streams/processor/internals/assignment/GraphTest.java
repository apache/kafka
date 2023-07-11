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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        graph.addEdge(0, 1, 1,3, 1);
        graph.addEdge(0, 3, 1, 1, 0);
        graph.addEdge(2, 1, 1,1, 0);
        graph.addEdge(2, 3, 1, 2, 1);
        graph.addEdge(4, 0, 1, 0, 1);
        graph.addEdge(4, 2, 1, 0, 1);
        graph.addEdge(1, 5, 1, 0, 1);
        graph.addEdge(3, 5, 1, 0, 1);
        graph.setSourceNode(4);
        graph.setSinkNode(5);
    }

    @Test
    public void testBasic() {
        final Set<Integer> nodes = graph.getNodes();
        assertEquals(6, nodes.size());
        assertThat(nodes, contains(0, 1, 2, 3, 4, 5));

        Map<Integer, Graph<Integer>.Edge> edges = graph.getEdges(0);
        assertEquals(2, edges.size());
        assertEquals(new Graph<Integer>().new Edge(1, 1, 3, 0, 1), edges.get(1));
        assertEquals(new Graph<Integer>().new Edge(3, 1, 1, 1, 0), edges.get(3));

        edges = graph.getEdges(2);
        assertEquals(2, edges.size());
        assertEquals(new Graph<Integer>().new Edge(1, 1, 1, 1, 0), edges.get(1));
        assertEquals(new Graph<Integer>().new Edge(3, 1, 2, 0, 1), edges.get(3));

        edges = graph.getEdges(1);
        assertEquals(1, edges.size());
        assertEquals(new Graph<Integer>().new Edge(5, 1, 0, 0, 1), edges.get(5));

        edges = graph.getEdges(3);
        assertEquals(1, edges.size());
        assertEquals(new Graph<Integer>().new Edge(5, 1, 0, 0, 1), edges.get(5));

        edges = graph.getEdges(4);
        assertEquals(2, edges.size());
        assertEquals(new Graph<Integer>().new Edge(0, 1, 0, 0, 1), edges.get(0));
        assertEquals(new Graph<Integer>().new Edge(2, 1, 0, 0, 1), edges.get(2));

        edges = graph.getEdges(5);
        assertNull(edges);

        assertFalse(graph.isResidualGraph());
    }

    @Test
    public void testResidualGraph() {
        final Graph<Integer> residualGraph = graph.getResidualGraph();
        final Graph<Integer> residualGraph1 = residualGraph.getResidualGraph();
        assertSame(residualGraph1, residualGraph);

        final Set<Integer> nodes = residualGraph.getNodes();
        assertEquals(6, nodes.size());
        assertThat(nodes, contains(0, 1, 2, 3, 4, 5));

        Map<Integer, Graph<Integer>.Edge> edges = residualGraph.getEdges(0);
        assertEquals(3, edges.size());
        assertEquals(new Graph<Integer>().new Edge(1, 1, 3, 0, 1), edges.get(1));
        assertEquals(new Graph<Integer>().new Edge(3, 1, 1, 1, 0), edges.get(3));
        assertEquals(new Graph<Integer>().new Edge(4, 1, 0, 1, 0), edges.get(4));

        edges = residualGraph.getEdges(2);
        assertEquals(3, edges.size());
        assertEquals(new Graph<Integer>().new Edge(1, 1, 1, 1, 0), edges.get(1));
        assertEquals(new Graph<Integer>().new Edge(3, 1, 2, 0, 1), edges.get(3));
        assertEquals(new Graph<Integer>().new Edge(4, 1, 0, 1, 0), edges.get(4));

        edges = residualGraph.getEdges(1);
        assertEquals(3, edges.size());
        assertEquals(new Graph<Integer>().new Edge(0, 1, -3, 1, 0), edges.get(0));
        assertEquals(new Graph<Integer>().new Edge(2, 1, -1, 0, 0), edges.get(2));
        assertEquals(new Graph<Integer>().new Edge(5, 1, 0, 0, 1), edges.get(5));

        edges = residualGraph.getEdges(3);
        assertEquals(3, edges.size());
        assertEquals(new Graph<Integer>().new Edge(0, 1, -1, 0, 0), edges.get(0));
        assertEquals(new Graph<Integer>().new Edge(2, 1, -2, 1, 0), edges.get(2));
        assertEquals(new Graph<Integer>().new Edge(5, 1, 0, 0, 1), edges.get(5));

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

        final Graph<Integer> residualGraph = graph1.getResidualGraph();
        exception = assertThrows(IllegalStateException.class, residualGraph::solveMinCostFlow);
        assertEquals("Should not be residual graph to solve min cost flow", exception.getMessage());

    }

    @Test
    public void testMinCostFlow() {
        // Original graph, flow from 0 to 1 and 2 to 3
        Map<Integer, Graph<Integer>.Edge> edges = graph.getEdges(0);
        Graph<Integer>.Edge edge = edges.get(1);
        assertEquals(1, edge.flow);
        assertEquals(0, edge.residualFlow);

        edge = edges.get(3);
        assertEquals(0, edge.flow);
        assertEquals(1, edge.residualFlow);

        edges = graph.getEdges(2);
        edge = edges.get(3);
        assertEquals(1, edge.flow);
        assertEquals(0, edge.residualFlow);

        edge = edges.get(1);
        assertEquals(0, edge.flow);
        assertEquals(1, edge.residualFlow);

        assertEquals(5, graph.getTotalCost());

        graph.solveMinCostFlow();

        assertEquals(2, graph.getTotalCost());

        edges = graph.getEdges(0);
        assertEquals(2, edges.size());

        // No flow from 0 to 1
        edge = edges.get(1);
        assertEquals(0, edge.flow);
        assertEquals(1, edge.residualFlow);

        // Flow from 0 to 3 now
        edge = edges.get(3);
        assertEquals(1, edge.flow);
        assertEquals(0, edge.residualFlow);

        edges = graph.getEdges(2);
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

}
