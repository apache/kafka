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

import org.apache.kafka.streams.processor.StateStore;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProcessorTopology {

    private final List<ProcessorNode> processorNodes;
    private final List<StateStore> stateStores;
    private final List<StateStore> globalStateStores;
    private final Map<String, SourceNode> sourceByTopics;
    private final Map<String, SinkNode> sinkByTopics;
    private final Map<String, String> storeToChangelogTopic;

    public ProcessorTopology(final List<ProcessorNode> processorNodes,
                             final Map<String, SourceNode> sourceByTopics,
                             final Map<String, SinkNode> sinkByTopics,
                             final List<StateStore> stateStores,
                             final Map<String, String> storeToChangelogTopic,
                             final List<StateStore> globalStateStores) {
        this.processorNodes = Collections.unmodifiableList(processorNodes);
        this.sourceByTopics = Collections.unmodifiableMap(sourceByTopics);
        this.sinkByTopics   = Collections.unmodifiableMap(sinkByTopics);
        this.stateStores    = Collections.unmodifiableList(stateStores);
        this.storeToChangelogTopic = Collections.unmodifiableMap(storeToChangelogTopic);
        this.globalStateStores = Collections.unmodifiableList(globalStateStores);
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

    public List<StateStore> stateStores() {
        return stateStores;
    }

    public Map<String, String> storeToChangelogTopic() {
        return storeToChangelogTopic;
    }

    public List<StateStore> globalStateStores() {
        return globalStateStores;
    }

    private String childrenToString(String indent, List<ProcessorNode<?, ?>> children) {
        if (children == null || children.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder(indent + "\tchildren:\t[");
        for (ProcessorNode child : children) {
            sb.append(child.name());
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);  // remove the last comma
        sb.append("]\n");

        // recursively print children
        for (ProcessorNode<?, ?> child : children) {
            sb.append(child.toString(indent)).append(childrenToString(indent, child.children()));
        }
        return sb.toString();
    }

    /**
     * Produces a string representation containing useful information this topology starting with the given indent.
     * This is useful in debugging scenarios.
     * @return A string representation of this instance.
     */
    @Override
    public String toString() {
        return toString("");
    }

    /**
     * Produces a string representation containing useful information this topology.
     * This is useful in debugging scenarios.
     * @return A string representation of this instance.
     */
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder(indent + "ProcessorTopology:\n");

        // start from sources
        for (SourceNode<?, ?> source : sourceByTopics.values()) {
            sb.append(source.toString(indent + "\t")).append(childrenToString(indent + "\t", source.children()));
        }
        return sb.toString();
    }

}
