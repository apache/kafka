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

class StreamsTopologyGraphImpl implements StreamsTopologyGraph {

    private static final Logger LOG = LoggerFactory.getLogger(StreamsTopologyGraphImpl.class);
    private StreamGraphNode previousNode;
    private final AtomicInteger nodeIdCounter = new AtomicInteger(0);
    public static final String TOPOLOGY_ROOT = "root";
    protected final StreamGraphNode root = new StreamGraphNode(TOPOLOGY_ROOT, TOPOLOGY_ROOT, false);

    private final Map<StreamGraphNode, Set<StreamGraphNode>> repartitioningNodeToRepartitioned = new HashMap<>();
    private final Map<StreamGraphNode, StreamGraphNode> stateStoreNodeToSinkNodes = new HashMap<>();
    private final Map<String, StreamGraphNode> nameToGraphNode = new HashMap<>();

    StreamsTopologyGraphImpl() {
        nameToGraphNode.put(TOPOLOGY_ROOT, root);
    }


    @Override
    public <K, V> void addNode(final StreamGraphNode<K, V> node) {
        node.setId(nodeIdCounter.getAndIncrement());

        if (node.parentNodeName() == null) {
            LOG.warn("Updating node {} with predecessor name {}", node, previousNode.nodeName());
            node.setParentNodeName(previousNode.nodeName());
        }

        LOG.debug("Adding node {}", node);

        final StreamGraphNode<K, V> predecessorNode =  nameToGraphNode.get(node.parentNodeName());

        if (predecessorNode == null) {
            throw new IllegalStateException(
                "Nodes should not have a null predecessor.  Name: " + node.nodeName() + " Type: "
                + node.getClass().getSimpleName() + " predecessor name " + node.parentNodeName());
        }

        node.setParentNode(predecessorNode);
        predecessorNode.addChildNode(node);

        if (node.isTriggersRepartitioning()) {
            repartitioningNodeToRepartitioned.put(node, new HashSet<StreamGraphNode>());
        } else if (node.isRepartitionRequired()) {
            StreamGraphNode currentNode = node;
            while (currentNode != null) {
                final StreamGraphNode parentNode = currentNode.parentNode();
                if (parentNode.isTriggersRepartitioning()) {
                    repartitioningNodeToRepartitioned.get(parentNode).add(node);
                    break;
                }
                currentNode = parentNode.parentNode();
            }
        }

        if (!nameToGraphNode.containsKey(node.nodeName())) {
            nameToGraphNode.put(node.nodeName(), node);
        }

        previousNode = node;
    }

    @Override
    public StreamGraphNode getRoot() {
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
    public Map<StreamGraphNode, Set<StreamGraphNode>> getRepartitioningNodeToRepartitioned() {
        Map<StreamGraphNode, Set<StreamGraphNode>> copy = new HashMap<>(repartitioningNodeToRepartitioned);
        return Collections.unmodifiableMap(copy);
    }

    /**
     * Used for hints when an Aggregation operation is directly output to a Sink topic.
     * This map can be used to help optimize this case and use the Sink topic as the changelog topic
     * for the state store of the aggregation.
     *
     * @return Map&lt;StreamGraphNode, StreamGraphNode&gt;
     */
    public Map<StreamGraphNode, StreamGraphNode> getStateStoreNodeToSinkNodes() {
        Map<StreamGraphNode, StreamGraphNode> copy = new HashMap<>(stateStoreNodeToSinkNodes);
        return Collections.unmodifiableMap(copy);
    }

    /**
     * Used for tracking the Streams generated names back to the original StreamGraphNode
     * to enable the predecessor - descendant relationship
     *
     * @return Map&lt;String, SteamsGraphNode&gt;
     */
    public Map<String, StreamGraphNode> getNameToGraphNode() {
        Map<String, StreamGraphNode> copy = new HashMap<>(nameToGraphNode);
        return Collections.unmodifiableMap(copy);
    }

}
