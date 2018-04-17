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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamsTopologyGraphImpl implements StreamsTopologyGraph {

    private static final Logger LOG = LoggerFactory.getLogger(StreamsTopologyGraphImpl.class);
    private final AtomicInteger kgroupedCounter = new AtomicInteger(0);
    private StreamsGraphNode previousNode;
    private final AtomicInteger nodeIdCounter = new AtomicInteger(0);
    public static final String TOPOLOGY_ROOT = "root";
    protected final StreamsGraphNode root = new StreamsGraphNode(TOPOLOGY_ROOT, TopologyNodeType.TOPOLOGY_PARENT);

    private final Map<StreamsGraphNode, Set<StreamsGraphNode>> repartitioningNodeToRepartitioned = new HashMap<>();
    private final Map<StreamsGraphNode, StreamsGraphNode> stateStoreNodeToSinkNodes = new HashMap<>();
    private final Map<String, StreamsGraphNode> nameToGraphNode = new HashMap<>();

    public StreamsTopologyGraphImpl() {
        nameToGraphNode.put(TOPOLOGY_ROOT, root);
    }


    @Override
    public void addNode(final StreamsGraphNode node) {
        node.setId(nodeIdCounter.getAndIncrement());

        if (node.topologyNodeType == TopologyNodeType.KGROUPED_STREAM) {
            node.setName(node.name() + "-" + kgroupedCounter.incrementAndGet());
        }

        if (node.getPredecessorName() == null) {
            LOG.warn("Updating node {} with predecessor name {}", node, previousNode.name());
            node.setPredecessorName(previousNode.name());
        }

        LOG.debug("Adding node {}", node);

        final StreamsGraphNode predecessorNode = nameToGraphNode.get(node.getPredecessorName());

        if (predecessorNode == null) {
            throw new IllegalStateException(
                "Nodes should not have a null predecessor.  Name: " + node.name() + " Type: " + node.getType() + " predecessor name " + node
                    .getPredecessorName());
        }

        node.setPredecessor(predecessorNode);
        predecessorNode.addDescendant(node);

        if (node.triggersRepartitioning()) {
            repartitioningNodeToRepartitioned.put(node, new HashSet<StreamsGraphNode>());
        } else if (node.needsRepartitioning()) {
            StreamsGraphNode currentNode = node;
            while (currentNode != null) {
                final StreamsGraphNode predecessor = currentNode.getPredecessor();
                if (predecessor.triggersRepartitioning()) {
                    repartitioningNodeToRepartitioned.get(predecessor).add(node);
                    break;
                }
                currentNode = predecessor.getPredecessor();
            }
        } else if (node.getType() == TopologyNodeType.SINK) {
            final StreamsGraphNode sinkParent = node.getPredecessor();
            if (sinkParent != null) {
                final StreamsGraphNode sinkGrandparent = sinkParent.getPredecessor();
                if (sinkParent.getType() == TopologyNodeType.TO_STREAM
                    && sinkGrandparent.getType() == TopologyNodeType.AGGREGATE_TYPE) {
                    stateStoreNodeToSinkNodes.put(sinkGrandparent, node);
                }
            }
        }

        if (!nameToGraphNode.containsKey(node.name())) {
            nameToGraphNode.put(node.name(), node);
        }

        previousNode = node;
    }

    @Override
    public StreamsGraphNode getRoot() {
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
     *
     * @return Map&lt;String, SteamsGraphNode&gt;
     */
    public Map<String, StreamsGraphNode> getNameToGraphNode() {
        Map<String, StreamsGraphNode> copy = new HashMap<>(nameToGraphNode);
        return Collections.unmodifiableMap(copy);
    }

}
