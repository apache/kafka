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

    private void traverseGraph(final GraphNode node) {
        if (!visited.contains(node)) {
            for (final GraphNode child : node.children()) {
                traverseGraph(child);
                if (child.labels().contains(GraphNode.Label.NULL_KEY_RELAXED_JOIN) || nonOptimizable.contains(child)) {
                    nonOptimizable.add(node);
                }
            }
            visited.add(node);
        }
    }
}
