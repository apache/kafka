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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class Graph<V extends Comparable<V>> {
    public class Edge {
        final V destination;
        final int capacity;
        final int cost;
        int residualFlow;
        int flow;
        Edge counterEdge;
        boolean forwardEdge;

        public Edge(final V destination, final int capacity, final int cost, final int residualFlow, final int flow) {
            this(destination, capacity, cost, residualFlow, flow, true);
        }

        public Edge(final V destination,
                    final int capacity,
                    final int cost,
                    final int residualFlow,
                    final int flow,
                    final boolean forwardEdge) {
            Objects.requireNonNull(destination);
            if (capacity < 0) {
                throw new IllegalArgumentException("Edge capacity cannot be negative");
            }
            if (flow > capacity) {
                throw new IllegalArgumentException(String.format("Edge flow %d cannot exceed capacity %d",
                    flow, capacity));
            }

            this.destination = destination;
            this.capacity = capacity;
            this.cost = cost;
            this.residualFlow = residualFlow;
            this.flow = flow;
            this.forwardEdge = forwardEdge;
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

            return destination.equals(otherEdge.destination)
                && capacity == otherEdge.capacity
                && cost == otherEdge.cost
                && residualFlow == otherEdge.residualFlow
                && flow == otherEdge.flow
                && forwardEdge == otherEdge.forwardEdge;
        }

        @Override
        public int hashCode() {
            return Objects.hash(destination, capacity, cost, residualFlow, flow, forwardEdge);
        }

        @Override
        public String toString() {
            return "Edge {"
                + "destination= " + destination
                + ", capacity=" + capacity
                + ", cost=" + cost
                + ", residualFlow=" + residualFlow
                + ", flow=" + flow
                + ", forwardEdge=" + forwardEdge
                + "}";

        }
    }

    // Allow null as special internal node
    private final SortedMap<V, SortedMap<V, Edge>> adjList = new TreeMap<>(Comparator.nullsFirst(Comparator.naturalOrder()));
    // Allow null as special internal node
    private final SortedSet<V> nodes = new TreeSet<>(Comparator.nullsFirst(Comparator.naturalOrder()));
    private final boolean isResidualGraph;
    private V sourceNode, sinkNode;

    public Graph() {
        this(false);
    }

    private Graph(final boolean isResidualGraph) {
        this.isResidualGraph = isResidualGraph;
    }

    public void addEdge(final V u, final V v, final int capacity, final int cost, final int flow) {
        Objects.requireNonNull(u);
        Objects.requireNonNull(v);
        addEdge(u, new Edge(v, capacity, cost, capacity - flow, flow));
    }

    public SortedSet<V> nodes() {
        return nodes;
    }

    public SortedMap<V, Edge> edges(final V node) {
        final SortedMap<V, Edge> edge = adjList.get(node);
        return edge == null ? new TreeMap<>() : edge;
    }

    public boolean isResidualGraph() {
        return isResidualGraph;
    }

    public void setSourceNode(final V node) {
        sourceNode = node;
    }

    public void setSinkNode(final V node) {
        sinkNode = node;
    }

    public long totalCost() {
        long totalCost = 0;
        for (final Map.Entry<V, SortedMap<V, Edge>> nodeEdges : adjList.entrySet()) {
            final SortedMap<V, Edge> edges = nodeEdges.getValue();
            for (final Edge nodeEdge : edges.values()) {
                totalCost += (long) nodeEdge.cost * nodeEdge.flow;
            }
        }
        return totalCost;
    }

    private void addEdge(final V u, final Edge edge) {
        if (!isResidualGraph) {
            // Check if there's already an edge from u to v
            final Map<V, Edge> edgeMap = adjList.get(edge.destination);
            if (edgeMap != null && edgeMap.containsKey(u)) {
                throw new IllegalArgumentException(
                    "There is already an edge from " + edge.destination
                        + " to " + u + ". Can not add an edge from " + u + " to " + edge.destination
                        + " since there will create a cycle between two nodes");
            }
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
    public Graph<V> residualGraph() {
        if (isResidualGraph) {
            return this;
        }

        final Graph<V> residualGraph = new Graph<>(true);
        for (final Map.Entry<V, SortedMap<V, Edge>> nodeEdges : adjList.entrySet()) {
            final V node = nodeEdges.getKey();
            final SortedMap<V, Edge> edges = nodeEdges.getValue();
            for (final Entry<V, Edge> nodeEdge : edges.entrySet()) {
                final Edge edge = nodeEdge.getValue();
                final Edge forwardEdge = new Edge(edge.destination, edge.capacity, edge.cost, edge.capacity - edge.flow, edge.flow);
                final Edge backwardEdge = new Edge(node, edge.capacity, edge.cost * -1, edge.flow, 0, false);
                forwardEdge.counterEdge = backwardEdge;
                backwardEdge.counterEdge = forwardEdge;
                residualGraph.addEdge(node, forwardEdge);
                residualGraph.addEdge(edge.destination, backwardEdge);
            }
        }
        residualGraph.setSourceNode(sourceNode);
        residualGraph.setSinkNode(sinkNode);
        return residualGraph;
    }

    private void addDummySourceNode(final Graph<V> residualGraph) {
        if (!residualGraph.isResidualGraph) {
            throw new IllegalStateException("Graph should be residual graph to add dummy source node");
        }

        // Add a dummy null node connected to every existing node with residual flow 1 and cost 0
        // Then try to find negative cylce starting using dummy node as source node. Since there's no
        // path from original nodes to null node, negative cycles must be within original nodes.
        final TreeMap<V, Edge> destMap = new TreeMap<>();
        for (final V node : residualGraph.nodes) {
            final Edge edge = new Edge(node, 1, 0, 1, 0);
            destMap.put(node, edge);
        }
        residualGraph.adjList.put(null, destMap);
        residualGraph.nodes.add(null);
    }

    private void removeDummySourceNode(final Graph<V> residualGraph) {
        if (!residualGraph.isResidualGraph) {
            throw new IllegalStateException("Graph should be residual graph to remove dummy source node");
        }
        residualGraph.adjList.remove(null);
        residualGraph.nodes.remove(null);
    }

    /**
     * Solve min cost flow with cycle canceling algorithm.
     */
    public void solveMinCostFlow() {
        validateMinCostGraph();
        final Graph<V> residualGraph = residualGraph();
        addDummySourceNode(residualGraph);
        residualGraph.cancelNegativeCycles();
        removeDummySourceNode(residualGraph);

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

    public long flow() {
        long flow = 0;
        final SortedMap<V, Edge> edges = adjList.get(sourceNode);
        if (edges != null) {
            for (final Edge edge : edges.values()) {
                flow += edge.flow;
            }
        }
        return flow;
    }

    public long calculateMaxFlow() {
        final Graph<V> residualGraph = residualGraph();
        residualGraph.fordFulkson();

        long maxFlow = 0;
        for (final Entry<V, SortedMap<V, Edge>> nodeEdges : adjList.entrySet()) {
            final V node = nodeEdges.getKey();
            for (final Entry<V, Edge> nodeEdge : nodeEdges.getValue().entrySet()) {
                final V destination = nodeEdge.getKey();
                final Edge edge = nodeEdge.getValue();
                final Edge residualEdge = residualGraph.adjList.get(node).get(destination);
                edge.flow = residualEdge.flow;
                edge.residualFlow = residualEdge.residualFlow;

                if (node == sourceNode) {
                    maxFlow += edge.flow;
                }
            }
        }

        return maxFlow;
    }

    private void fordFulkson() {
        if (!isResidualGraph) {
            throw new IllegalStateException("Should be residual graph to cancel negative cycles");
        }

        Map<V, V> parents = new HashMap<>();
        while (breadthFirstSearch(sourceNode, sinkNode, parents)) {
            int possibleFlow = Integer.MAX_VALUE;
            for (V node = sinkNode; node != sourceNode; node = parents.get(node)) {
                final V parent = parents.get(node);
                possibleFlow = Math.min(possibleFlow, adjList.get(parent).get(node).residualFlow);
            }

            for (V node = sinkNode; node != sourceNode; node = parents.get(node)) {
                final V parent = parents.get(node);
                final Edge parentEdge = adjList.get(parent).get(node);
                final Edge counterEdge = parentEdge.counterEdge;
                parentEdge.residualFlow -= possibleFlow;
                if (parentEdge.forwardEdge) {
                    parentEdge.flow += possibleFlow;
                }
                counterEdge.residualFlow += possibleFlow;
                if (counterEdge.forwardEdge && counterEdge.flow >= possibleFlow) {
                    counterEdge.flow -= possibleFlow;
                }
            }

            parents = new HashMap<>();
        }
    }

    private boolean breadthFirstSearch(final V source, final V target, final Map<V, V> parents) {
        final Set<V> visited = new HashSet<>();
        final Queue<V> queue = new LinkedList<>();
        queue.add(source);
        visited.add(source);
        while (!queue.isEmpty()) {
            final V node = queue.poll();
            final SortedMap<V, Edge> nodeEdges = adjList.get(node);
            for (final Entry<V, Edge> nodeEdge : nodeEdges.entrySet()) {
                final V nextNode = nodeEdge.getKey();
                if (visited.contains(nextNode) || nodeEdge.getValue().residualFlow <= 0) {
                    continue;
                }
                if (nodeEdge.getKey().equals(target)) {
                    parents.put(target, node);
                    return true;
                }
                queue.add(nodeEdge.getKey());
                parents.put(nodeEdge.getKey(), node);
                visited.add(nodeEdge.getKey());
            }
        }

        return false;
    }

    private void populateInOutFlow(final Map<V, Long> inFlow, final Map<V, Long> outFlow) {
        for (final Entry<V, SortedMap<V, Edge>> nodeEdges : adjList.entrySet()) {
            final V node = nodeEdges.getKey();
            if (node.equals(sinkNode)) {
                throw new IllegalStateException("Sink node " + sinkNode + " shouldn't have output");
            }
            for (final Entry<V, Edge> nodeEdge : nodeEdges.getValue().entrySet()) {
                final V destination = nodeEdge.getKey();
                if (destination.equals(sourceNode)) {
                    throw new IllegalStateException("Source node " + sourceNode + " shouldn't have input " + node);
                }
                final Edge edge = nodeEdge.getValue();
                Long count = outFlow.get(node);
                if (count == null) {
                    outFlow.put(node, (long) edge.flow);
                } else {
                    outFlow.put(node, count + edge.flow);
                }

                count = inFlow.get(destination);
                if (count == null) {
                    inFlow.put(destination, (long) edge.flow);
                } else {
                    inFlow.put(destination, count + edge.flow);
                }
            }
        }
    }

    private void validateMinCostGraph() {
        if (isResidualGraph) {
            throw new IllegalStateException("Should not be residual graph to solve min cost flow");
        }

        /*
         Check provided flow satisfying below constraints:
         1. Input flow and output flow for each node should be the same except for source and destination node
         2. Output flow of source and input flow of destination should be the same
        */

        final Map<V, Long> inFlow = new HashMap<>();
        final Map<V, Long> outFlow = new HashMap<>();
        populateInOutFlow(inFlow, outFlow);

        for (final Entry<V, Long> in : inFlow.entrySet()) {
            if (in.getKey().equals(sourceNode) || in.getKey().equals(sinkNode)) {
                continue;
            }
            final Long out = outFlow.get(in.getKey());
            if (!Objects.equals(in.getValue(), out)) {
                throw new IllegalStateException("Input flow for node " + in.getKey() + " is " +
                    in.getValue() + " which doesn't match output flow " + out);
            }
        }

        final Long sourceOutput = outFlow.get(sourceNode);
        final Long sinkInput = inFlow.get(sinkNode);
        if (!Objects.equals(sourceOutput, sinkInput)) {
            throw new IllegalStateException("Output flow for source " + sourceNode + " is " + sourceOutput
                + " which doesn't match input flow " + sinkInput + " for sink " + sinkNode);
        }
    }

    private void cancelNegativeCycles() {
        if (!isResidualGraph) {
            throw new IllegalStateException("Should be residual graph to cancel negative cycles");
        }
        boolean cyclePossible = true;
        while (cyclePossible) {
            cyclePossible = false;
            final Map<V, V> parentNodes = new HashMap<>();
            final Map<V, Edge> parentEdges = new HashMap<>();
            final V possibleNodeInCycle = detectNegativeCycles(null, parentNodes, parentEdges);

            if (possibleNodeInCycle == null) {
                continue;
            }

            final Set<V> visited = new HashSet<>();
            V nodeInCycle = possibleNodeInCycle;
            while (!visited.contains(nodeInCycle)) {
                visited.add(nodeInCycle);
                nodeInCycle = parentNodes.get(nodeInCycle);
            }

            cyclePossible = true;
            cancelNegativeCycle(nodeInCycle, parentNodes, parentEdges);
        }
    }

    private void cancelNegativeCycle(final V nodeInCycle, final Map<V, V> parentNodes, final Map<V, Edge> parentEdges) {
        // Start from parentNode since nodeInCyle is used as exit condition in below loops
        final V parentNode = parentNodes.get(nodeInCycle);
        Edge parentEdge = parentEdges.get(nodeInCycle);

        // Find max possible negative flow
        int possibleFlow = parentEdge.residualFlow;
        for (V curNode = parentNode; curNode != nodeInCycle; curNode = parentNodes.get(curNode)) {
            parentEdge = parentEdges.get(curNode);
            possibleFlow = Math.min(possibleFlow, parentEdge.residualFlow);
        }

        // Update graph by removing negative flow
        parentEdge = parentEdges.get(nodeInCycle);
        Edge counterEdge = parentEdge.counterEdge;
        parentEdge.residualFlow -= possibleFlow;
        if (parentEdge.forwardEdge) {
            parentEdge.flow += possibleFlow;
        }
        counterEdge.residualFlow += possibleFlow;
        if (counterEdge.forwardEdge && counterEdge.flow >= possibleFlow) {
            counterEdge.flow -= possibleFlow;
        }
        for (V curNode = parentNode; curNode != nodeInCycle; curNode = parentNodes.get(curNode)) {
            parentEdge = parentEdges.get(curNode);
            counterEdge = parentEdge.counterEdge;
            parentEdge.residualFlow -= possibleFlow;
            if (parentEdge.forwardEdge) {
                parentEdge.flow += possibleFlow;
            }
            counterEdge.residualFlow += possibleFlow;
            if (counterEdge.forwardEdge && counterEdge.flow >= possibleFlow) {
                counterEdge.flow -= possibleFlow;
            }
        }
    }

    /**
     * Detect negative cycle using Bellman-ford's shortest path algorithm.
     * @param source Source node
     * @param parentNodes Parent nodes to store negative cycle nodes
     * @param parentEdges Parent edges to store negative cycle edges
     *
     * @return One node which can lead to negative cycle if exists or null if there's no negative cycle
     */
    V detectNegativeCycles(final V source, final Map<V, V> parentNodes, final Map<V, Edge> parentEdges) {
        // Use long to account for any overflow
        final Map<V, Long> distance = new HashMap<>();
        distance.put(source, 0L);
        final int nodeCount = nodes.size();

        // Iterate nodeCount iterations since Bellaman-Ford will find the shortest path in nodeCount - 1
        // iterations. If the distance can still be relaxed in nodeCount iteration, there's a negative
        // cycle
        for (int i = 0; i < nodeCount; i++) {
            // Iterate through all edges
            for (final Entry<V, SortedMap<V, Edge>> nodeEdges : adjList.entrySet()) {
                final V start = nodeEdges.getKey();
                for (final Entry<V, Edge> nodeEdge : nodeEdges.getValue().entrySet()) {
                    final Edge edge = nodeEdge.getValue();
                    if (edge.residualFlow == 0) {
                        continue;
                    }
                    final V end = edge.destination;
                    final Long distanceStart = distance.get(start);
                    final Long distanceEnd = distance.get(end);
                    // There's a path to u and either we haven't computed V or distance to V is shorter
                    if (distanceStart != null && (distanceEnd == null || distanceEnd > distanceStart + edge.cost)) {
                        distance.put(end, distanceStart + edge.cost);
                        parentNodes.put(end, start);
                        parentEdges.put(end, edge);
                        if (i == nodeCount - 1) {
                            return end;
                        }
                    }
                }
            }
        }
        return null;
    }
}
