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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class Graph<V extends Comparable<V>> {
    public class Edge implements Comparable<Edge> {
        final V destination;
        final int capacity;
        final int cost;
        int residualFlow;
        int flow;
        Edge counterEdge;

        public Edge(final V destination, final int capacity, final int cost, final int residualFlow, final int flow) {
            Objects.requireNonNull(destination);
            this.destination = destination;
            this.capacity = capacity;
            this.cost = cost;
            this.residualFlow = residualFlow;
            this.flow = flow;
        }

        @Override
        public int compareTo(final Edge o) {
            int compare = destination.compareTo(o.destination);
            if (compare != 0) {
                return compare;
            }

            compare = capacity - o.capacity;
            if (compare != 0) {
                return compare;
            }

            return cost - o.cost;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || other.getClass() != getClass()) {
                return false;
            }

            final Graph<?>.Edge otherEdge = (Graph<?>.Edge) other;

            return destination.equals(otherEdge.destination) && capacity == otherEdge.capacity
                && cost == otherEdge.cost && residualFlow == otherEdge.residualFlow && flow == otherEdge.flow;
        }

        @Override
        public int hashCode() {
            return Objects.hash(destination, capacity, cost, residualFlow, flow);
        }

        @Override
        public String toString() {
            return "{destination= " + destination + ", capacity=" + capacity + ", cost=" + cost
                + ", residualFlow=" + residualFlow + ", flow=" + flow;
        }
    }

    private final SortedMap<V, SortedMap<V, Edge>> adjList = new TreeMap<>();
    private final SortedSet<V> nodes = new TreeSet<>();
    private final boolean isResidualGraph;

    public Graph() {
        this(false);
    }

    public Graph(final boolean isResidualGraph) {
        this.isResidualGraph = isResidualGraph;
    }

    public void addEdge(final V u, final V v, final int capacity, final int cost, final int flow) {
        addEdge(u, new Edge(v, capacity, cost, capacity - flow, flow));
    }

    public Set<V> getNodes() {
        return nodes;
    }

    public Map<V, Edge> getEdges(final V node) {
        return adjList.get(node);
    }

    public boolean isResidualGraph() {
        return isResidualGraph;
    }

    private void addEdge(final V u, final Edge edge) {
         if (edge.capacity < 0) {
            throw new IllegalArgumentException("Edge capacity cannot be negative");
        }
        if (edge.flow > edge.capacity) {
            throw new IllegalArgumentException(String.format("Edge flow %d cannot exceed capacity %d",
                edge.flow, edge.capacity));
        }
        adjList.computeIfAbsent(u, set -> new TreeMap<>()).put(edge.destination, edge);
        nodes.add(u);
        nodes.add(edge.destination);
    }

    /**
     * Get residual graph of this graph.
     * Residual graph definition:
     * If there is an edge in original graph from u to v with capacity c, cost w and flow f,
     * then in the new graph there are two edges e1 and e2. e1 is from u to v with capacity c - f,
     * cost w and flow f. e2 is from v to u with capacity f, cost -w and flow 0.
     *
     * @return Residual graph
     */
    public Graph<V> getResidualGraph() {
        if (isResidualGraph) {
            return this;
        }

        final Graph<V> residulGraph = new Graph<>(true);
        for (final Map.Entry<V, SortedMap<V, Edge>> nodeEdges : adjList.entrySet()) {
            final V node = nodeEdges.getKey();
            final SortedMap<V, Edge> edges = nodeEdges.getValue();
            for (final Entry<V, Edge> nodeEdge : edges.entrySet()) {
                final Edge edge = nodeEdge.getValue();
                final Edge forwardEdge = new Edge(edge.destination, edge.capacity, edge.cost, edge.capacity - edge.flow, edge.flow);
                final Edge backwardEdge = new Edge(node, edge.capacity, edge.cost * -1, edge.flow, 0);
                forwardEdge.counterEdge = backwardEdge;
                backwardEdge.counterEdge = forwardEdge;
                addEdge(node, forwardEdge);
                addEdge(edge.destination, backwardEdge);
            }
        }
        return residulGraph;
    }

    /**
     * Solve min cost flow with cycle canceling algorithm.
     */
    public void solveMinCostFlow() {
        if (isResidualGraph) {
            throw new IllegalStateException("Should not be residual graph to solve min cost flow");
        }
        final Graph<V> residualGraph = getResidualGraph();
        residualGraph.cancelNegativeCycles();

        for (final Entry<V, SortedMap<V, Edge>> nodeEdges : adjList.entrySet()) {
            final V node = nodeEdges.getKey();
            for (final Entry<V, Edge> nodeEdge : nodeEdges.getValue().entrySet()) {
                final V destination = nodeEdge.getKey();
                final Edge edge = nodeEdge.getValue();
                final Edge residualEdge = residualGraph.adjList.get(node).get(destination);
                edge.flow = residualEdge.flow;
                edge.residualFlow = residualEdge.residualFlow;
            }
        }
    }

    private void cancelNegativeCycles() {
        if (!isResidualGraph) {
            throw new IllegalStateException("Should be residual graph to cancel negative cycles");
        }
        boolean cyclePossible = true;
        while (cyclePossible) {
            cyclePossible = false;
            for (final V node : nodes) {
                final Map<V, V> parentNodes = new HashMap<>();
                final Map<V, Edge> parentEdges = new HashMap<>();
                V nodeInCycle = detectNegativeCycles(node, parentNodes, parentEdges);
                if (nodeInCycle == null) {
                    continue;
                }
                cyclePossible = true;
                V parentNode = parentNodes.get(nodeInCycle);
                Edge parentEdge = parentEdges.get(nodeInCycle);

                // Find max possible negative flow
                int possibleFlow = parentEdge.residualFlow;
                for (V curNode = parentNode; curNode != nodeInCycle; curNode = parentNodes.get(curNode)) {
                    parentEdge = parentEdges.get(curNode);
                    possibleFlow = Math.min(possibleFlow, parentEdge.residualFlow);
                }

                // Update graph by removing negative flow
                parentNode = parentNodes.get(nodeInCycle);
                parentEdge = parentEdges.get(nodeInCycle);
                Edge counterEdge = parentEdge.counterEdge;
                parentEdge.residualFlow -= possibleFlow;
                parentEdge.flow += possibleFlow;
                counterEdge.residualFlow += possibleFlow;
                counterEdge.flow -= possibleFlow;
                for (V curNode = parentNode; curNode != nodeInCycle; curNode = parentNodes.get(curNode)) {
                    parentEdge = parentEdges.get(nodeInCycle);
                    counterEdge = parentEdge.counterEdge;
                    parentEdge.residualFlow -= possibleFlow;
                    parentEdge.flow += possibleFlow;
                    counterEdge.residualFlow += possibleFlow;
                    counterEdge.flow -= possibleFlow;
                }
            }
        }
    }

    private V detectNegativeCycles(final V source, final Map<V, V> parentNodes, final Map<V, Edge> parentEdges) {
        // Use long to account for any overflow
        final Map<V, Long> distance = nodes.stream().collect(Collectors.toMap(node -> node, node -> Long.MAX_VALUE));
        distance.put(source, 0L);
        final int nodeCount = nodes.size();

        for (int i = 0; i < nodeCount; i++) {
            for (final Entry<V, SortedMap<V, Edge>> nodeEdges : adjList.entrySet()) {
                final V u = nodeEdges.getKey();
                for (final Entry<V, Edge> nodeEdge : nodeEdges.getValue().entrySet()) {
                    final Edge edge = nodeEdge.getValue();
                    if (edge.residualFlow == 0) {
                        continue;
                    }
                    final V v = edge.destination;
                    if (distance.get(v) > distance.get(u) + edge.cost) {
                        if (i == nodeCount - 1) {
                            return v;
                        }
                        distance.put(v, distance.get(u) + edge.cost);
                        parentNodes.put(v, u);
                        parentEdges.put(v, edge);
                    }
                }
            }
        }

        return null;
    }
}
