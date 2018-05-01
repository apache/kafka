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

import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

class StreamsTopologyGraph {

    private static final Logger LOG = LoggerFactory.getLogger(StreamsTopologyGraph.class);
    public static final String TOPOLOGY_ROOT = "root";

    protected final StreamsGraphNode root = new StreamsGraphNode(null, TOPOLOGY_ROOT, false) {
        @Override
        void writeToTopology(InternalTopologyBuilder topologyBuilder) {
            // no-op for root node
        }
    };

    private final AtomicInteger nodeIdCounter = new AtomicInteger(0);
    private final Map<StreamsGraphNode, Set<StreamsGraphNode>> repartitioningNodeToRepartitioned = new HashMap<>();
    private final Map<StreamsGraphNode, StreamSinkNode> stateStoreNodeToSinkNodes = new HashMap<>();
    private final Map<String, StreamsGraphNode> nameToGraphNode = new HashMap<>();

    private StreamsGraphNode previousNode;

    StreamsTopologyGraph() {
        nameToGraphNode.put(TOPOLOGY_ROOT, root);
    }


    public void addNode(final StreamsGraphNode node) {
        node.setId(nodeIdCounter.getAndIncrement());

        if (node.parentProcessorNodeName() == null && !node.processorNodeName().equals(TOPOLOGY_ROOT)) {
            LOG.warn("Updating node {} with predecessor name {}", node, previousNode.processorNodeName());
            node.setParentProcessorNodeName(previousNode.processorNodeName());
        }

        LOG.debug("Adding node {}", node);

        final StreamsGraphNode predecessorNode =  nameToGraphNode.get(node.parentProcessorNodeName());

        if (predecessorNode == null) {
            throw new IllegalStateException(
                "Nodes should not have a null predecessor.  Name: " + node.processorNodeName() + " Type: "
                + node.getClass().getSimpleName() + " predecessor name " + node.parentProcessorNodeName());
        }

        node.setParentNode(predecessorNode);
        predecessorNode.addChildNode(node);

        if (node.triggersRepartitioning()) {
            repartitioningNodeToRepartitioned.put(node, new HashSet<StreamsGraphNode>());
        } else if (node.repartitionRequired()) {
            StreamsGraphNode currentNode = node;
            while (currentNode != null) {
                final StreamsGraphNode parentNode = currentNode.parentNode();
                if (parentNode.triggersRepartitioning()) {
                    repartitioningNodeToRepartitioned.get(parentNode).add(node);
                    break;
                }
                currentNode = parentNode.parentNode();
            }
        }

        if (!nameToGraphNode.containsKey(node.processorNodeName())) {
            nameToGraphNode.put(node.processorNodeName(), node);
        }

        previousNode = node;
    }

    public StreamsGraphNode getRoot() {
        return root;
    }

    /**
     * Used for hints when a node in the topology triggers a repartition and the repartition flag
     * is propagated down through the descendant nodes of the topology.  This can be used to help make an
     * optimization where the triggering node does an eager "through" operation and the child nodes can ignore
     * the need to repartition.
     *
     * @return Map&lt;StreamGraphNode, Set&lt;StreamGraphNode&gt;&gt;
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
     * @return Map&lt;StreamGraphNode, StreamSinkNode&gt;
     */
    public Map<StreamsGraphNode, StreamSinkNode> getStateStoreNodeToSinkNodes() {
        Map<StreamsGraphNode, StreamSinkNode> copy = new HashMap<>(stateStoreNodeToSinkNodes);
        return Collections.unmodifiableMap(copy);
    }

    /**
     * Used for tracking the Streams generated names back to the original StreamGraphNode
     * to enable the predecessor - descendant relationship
     *
     * @return Map&lt;String, SteamsGraphNode&gt;
     */
    public Map<String, StreamsGraphNode> getNameToGraphNode() {
        Map<String, StreamsGraphNode> copy = new HashMap<>(nameToGraphNode);
        return Collections.unmodifiableMap(copy);
    }

}
