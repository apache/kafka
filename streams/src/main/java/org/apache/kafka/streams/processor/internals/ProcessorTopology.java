/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;
import org.apache.kafka.streams.processor.StateStoreSupplier;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Queue;
import java.util.LinkedList;

import static org.apache.kafka.streams.kstream.internals.KStreamImpl.SINK_NAME;

public class ProcessorTopology {

    private final List<ProcessorNode> processorNodes;
    private final Map<String, SourceNode> sourceByTopics;
    private final Map<String, SinkNode> sinkByTopics;
    private final List<StateStoreSupplier> stateStoreSuppliers;

    public ProcessorTopology(List<ProcessorNode> processorNodes,
                             Map<String, SourceNode> sourceByTopics,
                             Map<String, SinkNode> sinkByTopics,
                             List<StateStoreSupplier> stateStoreSuppliers) {
        this.processorNodes = Collections.unmodifiableList(processorNodes);
        this.sourceByTopics = Collections.unmodifiableMap(sourceByTopics);
        this.sinkByTopics   = Collections.unmodifiableMap(sinkByTopics);
        this.stateStoreSuppliers = Collections.unmodifiableList(stateStoreSuppliers);
    }

    public Set<String> sourceTopics() {
        return sourceByTopics.keySet();
    }

    public SourceNode source(String topic) {
        return sourceByTopics.get(topic);
    }

    public Set<SourceNode> sources() {
        return new HashSet<>(sourceByTopics.values());
    }

    public Set<String> sinkTopics() {
        return sinkByTopics.keySet();
    }

    public SinkNode sink(String topic) {
        return sinkByTopics.get(topic);
    }

    public Set<SinkNode> sinks() {
        return new HashSet<>(sinkByTopics.values());
    }

    public List<ProcessorNode> processors() {
        return processorNodes;
    }

    public List<StateStoreSupplier> stateStoreSuppliers() {
        return stateStoreSuppliers;
    }


    /**
     * Produces a string representation contain useful information this topology by performing a
     * breadth-first traversal of the DAG from source nodes.
     * This is useful in debugging scenarios.
     * @return A string representation of this instance.
     */
    public String toString() {
        Set<String> visitedNodes = new HashSet<String>();
        Queue queue = new LinkedList();
        StringBuilder sb = new StringBuilder("ProcessorTopology[");
        Set<SourceNode> tmpSources = sources();

        // pre-process source nodes to get reverse mapping
        Map<String, String> sourceNameToTopic = new HashMap<>();
        for (String topic : sourceByTopics.keySet()) {
            SourceNode source = sourceByTopics.get(topic);
            sourceNameToTopic.put(source.name(), topic);
        }

        // pre-process sink nodes to get reverse mapping
        Map<String, String> sinkNameToTopic = new HashMap<>();
        for (String topic : sinkByTopics.keySet()) {
            SinkNode sink = sinkByTopics.get(topic);
            sinkNameToTopic.put(sink.name(), topic);
        }

        // add all sources to a queue for Breadth-First traversal
        if (tmpSources != null) {
            // add all sources
            for (String topic : sourceByTopics.keySet()) {
                SourceNode source = sourceByTopics.get(topic);
                // mark as visited
                visitedNodes.add(source.name());
                // put in queue
                queue.add(source);
            }

            // iterate to next level
            while (!queue.isEmpty()) {
                Object o = queue.remove();
                List<ProcessorNode<?, ?>> children = null;
                if (o instanceof SourceNode) {
                    SourceNode node = (SourceNode) o;
                    children = node.children();
                    // print value
                    sb.append("sourceNode=" + node.toString() + "," + "sourceTopic=" + sourceNameToTopic.get(node.name()) + "-->");
                } else if (o instanceof ProcessorNode) {
                    ProcessorNode node = (ProcessorNode) o;
                    children = node.children();
                }
                // get unvisited children only
                for (ProcessorNode child : children) {
                    Boolean visited = visitedNodes.contains(child.name());
                    if (visited == null || !visited.booleanValue()) {
                        // mark as visited
                        visitedNodes.add(child.name());
                        // put in queue
                        queue.add(child);
                        // print value
                        sb.append("node=" + child.toString() + ",");
                        if (sinkNameToTopic.containsKey(child.name())) {
                            sb.append("sinkTopic=" + sinkNameToTopic.get(child.name()) + ",");
                        }
                        sb.append("-->");
                    }
                }
                sb.append("\t\t\n");
            }
        }
        sb.append("]");

        return sb.toString();
    }
}
