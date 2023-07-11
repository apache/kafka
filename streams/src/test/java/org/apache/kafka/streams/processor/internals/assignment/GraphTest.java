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

import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class GraphTest {
    private Graph<Integer> graph;

    @Before
    public void setUp() {
        graph = new Graph<>();
        graph.addEdge(0, 1, 1,3, 1);
        graph.addEdge(0, 3, 1, 1, 0);
        graph.addEdge(2, 1, 1,1, 0);
        graph.addEdge(2, 3, 1, 2, 1);
    }

    @Test
    public void testBasic() {
        final Set<Integer> nodes = graph.getNodes();
        assertEquals(4, nodes.size());
        assertThat(nodes, contains(0, 1, 2, 3));

        Map<Integer, Graph<Integer>.Edge> edges = graph.getEdges(0);
        assertEquals(2, edges.size());
        assertEquals(new Graph<Integer>().new Edge(1, 1, 3, 0, 1), edges.get(1));
        assertEquals(new Graph<Integer>().new Edge(3, 1, 1, 1, 0), edges.get(3));

        edges = graph.getEdges(2);
        assertEquals(2, edges.size());
        assertEquals(new Graph<Integer>().new Edge(1, 1, 1, 1, 0), edges.get(1));
        assertEquals(new Graph<Integer>().new Edge(3, 1, 2, 0, 1), edges.get(3));

        edges = graph.getEdges(1);
        assertNull(edges);

        edges = graph.getEdges(3);
        assertNull(edges);

        assertFalse(graph.isResidualGraph());
    }

    @Test
    public void testResidualGraph() {
        final Graph<Integer> residualGraph = graph.getResidualGraph();
    }

}
