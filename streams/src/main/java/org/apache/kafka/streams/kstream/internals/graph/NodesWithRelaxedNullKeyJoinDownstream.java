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
package org.apache.kafka.streams.kstream.internals.graph;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class NodesWithRelaxedNullKeyJoinDownstream {

    private final HashSet<GraphNode> visited;
    private final HashSet<GraphNode> nonOptimizable;
    private final GraphNode start;

    public NodesWithRelaxedNullKeyJoinDownstream(final GraphNode root) {
        this.start = root;
        this.visited = new HashSet<>();
        this.nonOptimizable = new HashSet<>();
    }

    public Set<BaseRepartitionNode<?, ?>> find() {
        traverseGraph(this.start);
        return visited.stream()
            .filter(node -> node instanceof BaseRepartitionNode && !nonOptimizable.contains(node))
            .map(node -> (BaseRepartitionNode<?, ?>) node)
            .collect(Collectors.toSet());
    }

    /**
     * Post-order traversal that adds every node reachable from {@code root} to {@code visited}, and adds a node
     * to {@code nonOptimizable} if any of its children has a relaxed null-key join label or is non-optimizable
     * itself. It uses an explicit stack instead of recursion, because recursion depth would grow with the length
     * of the longest processor chain and overflow the call stack for very deep topologies.
     */
    private void traverseGraph(final GraphNode root) {
        final Deque<GraphNode> stack = new ArrayDeque<>();
        final Set<GraphNode> childrenPushed = new HashSet<>();
        stack.push(root);
        while (!stack.isEmpty()) {
            final GraphNode node = stack.peek();
            if (visited.contains(node)) {
                stack.pop();
            } else if (childrenPushed.add(node)) {
                // first time on top of the stack: process the children before the node itself
                for (final GraphNode child : node.children()) {
                    if (!visited.contains(child)) {
                        stack.push(child);
                    }
                }
            } else {
                // all children are visited, so their nonOptimizable status is final
                stack.pop();
                for (final GraphNode child : node.children()) {
                    if (child.labels().contains(GraphNode.Label.NULL_KEY_RELAXED_JOIN) || nonOptimizable.contains(child)) {
                        nonOptimizable.add(node);
                    }
                }
                visited.add(node);
            }
        }
    }
}
