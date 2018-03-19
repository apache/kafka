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

package org.apache.kafka.streams.kstream.internals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The builder of the Streams Intermediate DAG used to make
 * optimizations of a Kafka Streams topology and sub-topologies
 */
public abstract class StreamsTopologyGraph {

    protected final StreamsGraphNode root = new StreamsGraphNode("TOPOLOGY_ROOT", TopologyNodeType.TOPOLOGY_PARENT);

    protected final Map<StreamsGraphNode, Set<StreamsGraphNode>> repartitioningNodeToRepartitioned = new HashMap<>();
    protected final Map<StreamsGraphNode, StreamsGraphNode> stateStoreNodeToSinkNodes = new HashMap<>();
    protected final Map<String, StreamsGraphNode> nameToGraphNode = new HashMap<>();

    /**
     * Add a StreamsGraphNode to the graph
     *
     * @param metadata        the metadata node to add
     */
    abstract void addNode(StreamsGraphNode metadata);

    /**
     * Retrieves the root node of the DAG of metadata for the topology
     * allowing access and traversal of entire topology metadata
     *
     * @return StreamsGraphNode root
     */
    public StreamsGraphNode root() {
        return root;
    }

    /**
     * Used for hints when a node in the topology triggers a repartition and the repartition flag
     * is propagated down through the descendant nodes of the topology.  This can be used to help make an
     * optimization where the triggering node does an eager "through" operation and the child nodes can ignore
     * the need to repartition.
     *
     * @return Map&lt;StreamsGraphNode, Set&lt;StreamGraphNode&gt;&gt;
     */
    public Map<StreamsGraphNode, Set<StreamsGraphNode>> getRepartitioningNodeToRepartitioned() {
        Map<StreamsGraphNode, Set<StreamsGraphNode>> copy = new HashMap<>(repartitioningNodeToRepartitioned);
        return Collections.unmodifiableMap(copy);
    }

    /**
     * Used for hints when an Aggregation operation is directly output to a Sink topic.
     * This map can be used to help optimize this case and use the Sink topic as the changelog topic
     * for the state store of the aggregation.
     *
     * @return Map&lt;StreamsGraphNode, StreamGraphNode&gt;
     */
    public Map<StreamsGraphNode, StreamsGraphNode> getStateStoreNodeToSinkNodes() {
        Map<StreamsGraphNode, StreamsGraphNode> copy = new HashMap<>(stateStoreNodeToSinkNodes);
        return Collections.unmodifiableMap(copy);
    }

    /**
     * Used for tracking the Streams generated names back to the original StreamsGraphNode
     * to enable the predecessor - descendant relationship
     * @return Map&lt;String, SteamsGraphNode&gt;
     */
    public Map<String, StreamsGraphNode> getNameToGraphNode() {
        Map<String, StreamsGraphNode> copy = new HashMap<>(nameToGraphNode);
        return Collections.unmodifiableMap(copy);
    }
}
