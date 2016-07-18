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

public class ProcessorTopology {

    private final List<ProcessorNode> processorNodes;
    private final Map<String, SourceNode> sourceByTopics;
    private final Map<String, SinkNode> sinkByTopics;
    private final List<StateStoreSupplier> stateStoreSuppliers;
    private final Map<String, String> sourceNameToTopic;
    private final Map<String, String> sinkNameToTopic;

    public ProcessorTopology(List<ProcessorNode> processorNodes,
                             Map<String, SourceNode> sourceByTopics,
                             Map<String, SinkNode> sinkByTopics,
                             List<StateStoreSupplier> stateStoreSuppliers) {
        this.processorNodes = Collections.unmodifiableList(processorNodes);
        this.sourceByTopics = Collections.unmodifiableMap(sourceByTopics);
        this.sinkByTopics   = Collections.unmodifiableMap(sinkByTopics);
        this.stateStoreSuppliers = Collections.unmodifiableList(stateStoreSuppliers);

        // pre-process source nodes to get reverse mapping
        sourceNameToTopic = new HashMap<>();
        for (String topic : sourceByTopics.keySet()) {
            SourceNode source = sourceByTopics.get(topic);
            sourceNameToTopic.put(source.name(), topic);
        }

        // pre-process sink nodes to get reverse mapping
        sinkNameToTopic = new HashMap<>();
        for (String topic : sinkByTopics.keySet()) {
            SinkNode sink = sinkByTopics.get(topic);
            sinkNameToTopic.put(sink.name(), topic);
        }
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


    private String childrenToString(List<ProcessorNode<?, ?>> children) {
        if (children == null || children.isEmpty()) {
            return null;
        }

        StringBuilder sb = new StringBuilder("children [");
        for (ProcessorNode child : children) {
            sb.append(child.name() + ",");
        }
        sb.setLength(sb.length() - 1);
        sb.append("]\n");

        // recursively print children
        for (ProcessorNode child : children) {
            sb.append("\t\t" + child.name() + ": ");
            if (sinkNameToTopic.containsKey(child.name())) {
                sb.append("topics:" + sinkNameToTopic.get(child.name()) + " ");
            }
            if (child.stateStores != null && !child.stateStores.isEmpty()) {
                sb.append("stateStores [");
                for (String store : (Set<String>)child.stateStores) {
                    sb.append(store + ",");
                }
                sb.setLength(sb.length() - 1);
                sb.append("] ");
            }
            if (!sinkNameToTopic.containsKey(child.name())) {
                sb.append(childrenToString(child.children()));
            }
        }
        return sb.toString();
    }

    /**
     * Produces a string representation contain useful information this topology.
     * This is useful in debugging scenarios.
     * @return A string representation of this instance.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder("\t\tProcessorTopology:\n");

        // start from sources
        for (String topic : sourceByTopics.keySet()) {
            SourceNode source = sourceByTopics.get(topic);
            sb.append("\t\t" + source.name() + ": " + "topics: " + sourceNameToTopic.get(source.name()) + ", ");
            sb.append(childrenToString(source.children()));
            sb.append("\n");
        }
        return sb.toString();
    }
}
