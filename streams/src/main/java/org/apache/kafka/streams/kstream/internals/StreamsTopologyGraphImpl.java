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

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamsTopologyGraphImpl extends StreamsTopologyGraph {

    private static final Logger LOG = LoggerFactory.getLogger(StreamsTopologyGraphImpl.class);
    private final AtomicInteger kgroupedCounter = new AtomicInteger(0);
    private final AtomicInteger streamStreamJoinCounter = new AtomicInteger(0);
    private StreamsGraphNode previousNode;

    @Override
    public void addNode(final StreamsGraphNode node) {

        if (node.topologyNodeType == TopologyNodeType.KGROUPED_STREAM) {
            node.setName(node.name() + "-" + kgroupedCounter.incrementAndGet());
            node.setPredecessorName(previousNode.name());
            System.out.println("UPDATED KGroupedStream node " + node);
        } else if (node.getPredecessorName() == null) {
            System.out.println("UPDATED regular node " + node);
            node.setPredecessorName(previousNode.name());
        }
        System.out.println("Adding node " + node);
        if (nameToGraphNode.get(node.getPredecessorName()) == null) {
            node.setPredecessor(previousNode);
        }

        final StreamsGraphNode predecessorNode = node.getPredecessorName() != null ? nameToGraphNode.get(node.getPredecessorName()) : null;

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

}
