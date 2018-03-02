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

import org.apache.kafka.streams.errors.TopologyException;

import java.util.HashSet;

public class StreamsTopologyGraphImpl extends StreamsTopologyGraph {

    @Override
    public void addNode(final StreamsGraphNode node) {

        final StreamsGraphNode predecessorNode = node.getPredecessorName() != null ? nameToGraphNode.get(node.getPredecessorName()) : null;

        if (predecessorNode == null && node.getType() != StreamsGraphNode.TopologyNodeType.SOURCE) {
            throw new TopologyException("Only SOURCE nodes can have a null predecessor.  Type" + node.getType() + " predecessor name" + node.getPredecessorName());
        }

        if (predecessorNode == null) {
            root.addDescendant(node);
        } else {
            node.setPredecessor(predecessorNode);
            predecessorNode.addDescendant(node);
        }

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
        } else if (node.getType() == StreamsGraphNode.TopologyNodeType.SINK) {
            final StreamsGraphNode sinkParent = node.getPredecessor();
            if (sinkParent != null) {
                final StreamsGraphNode sinkGrandparent = sinkParent.getPredecessor();
                if (sinkParent.getType() == StreamsGraphNode.TopologyNodeType.TO_STREAM
                    && sinkGrandparent.getType() == StreamsGraphNode.TopologyNodeType.AGGREGATE_TYPE) {
                    stateStoreNodeToSinkNodes.put(sinkGrandparent, node);
                }
            }
        }

        if (!nameToGraphNode.containsKey(node.name())) {
            nameToGraphNode.put(node.name(), node);
        }
    }

}
