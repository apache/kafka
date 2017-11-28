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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.StateStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProcessorTopology {

    private final List<ProcessorNode> processorNodes;
    private final Map<String, SourceNode> sourcesByTopic;
    private final Map<String, SinkNode> sinksByTopic;
    private final List<StateStore> stateStores;
    private final List<StateStore> globalStateStores;
    private final Map<String, String> storeToChangelogTopic;
    private final Set<String> repartitionTopics;

    public static ProcessorTopology with(final Map<String, ProcessorNode> processorsByName,
                                         final Map<String, SourceNode> sourcesByTopic,
                                         final Map<String, StateStore> stateStoresByName,
                                         final Map<String, String> storeToChangelogTopic) {
        return new ProcessorTopology(processorsByName,
                sourcesByTopic,
                Collections.<String, SinkNode>emptyMap(),
                stateStoresByName,
                Collections.<String, StateStore>emptyMap(),
                storeToChangelogTopic,
                Collections.<String>emptySet());
    }

    public static ProcessorTopology withSources(final Map<String, ProcessorNode> processorsByName,
                                                final Map<String, SourceNode> sourcesByTopic) {
        return new ProcessorTopology(processorsByName,
                sourcesByTopic,
                Collections.<String, SinkNode>emptyMap(),
                Collections.<String, StateStore>emptyMap(),
                Collections.<String, StateStore>emptyMap(),
                Collections.<String, String>emptyMap(),
                Collections.<String>emptySet());
    }

    public static ProcessorTopology withLocalStores(final Map<String, StateStore> stateStoresByName,
                                                    final Map<String, String> storeToChangelogTopic) {
        return new ProcessorTopology(Collections.<String, ProcessorNode>emptyMap(),
                Collections.<String, SourceNode>emptyMap(),
                Collections.<String, SinkNode>emptyMap(),
                stateStoresByName,
                Collections.<String, StateStore>emptyMap(),
                storeToChangelogTopic,
                Collections.<String>emptySet());
    }

    public static ProcessorTopology withGlobalStores(final Map<String, StateStore> stateStoresByName,
                                                    final Map<String, String> storeToChangelogTopic) {
        return new ProcessorTopology(Collections.<String, ProcessorNode>emptyMap(),
                Collections.<String, SourceNode>emptyMap(),
                Collections.<String, SinkNode>emptyMap(),
                Collections.<String, StateStore>emptyMap(),
                stateStoresByName,
                storeToChangelogTopic,
                Collections.<String>emptySet());
    }

    public ProcessorTopology(final Map<String, ProcessorNode> processorsByName,
                             final Map<String, SourceNode> sourcesByTopic,
                             final Map<String, SinkNode> sinksByTopic,
                             final Map<String, StateStore> stateStoresByName,
                             final Map<String, StateStore> globalStateStoresByName,
                             final Map<String, String> stateStoreToChangelogTopic,
                             final Set<String> repartitionTopics) {
        this.processorNodes = Collections.unmodifiableList(new ArrayList<>(processorsByName.values()));
        this.sourcesByTopic = Collections.unmodifiableMap(sourcesByTopic);
        this.sinksByTopic = Collections.unmodifiableMap(sinksByTopic);
        this.stateStores = Collections.unmodifiableList(new ArrayList<>(stateStoresByName.values()));
        this.globalStateStores = Collections.unmodifiableList(new ArrayList<>(globalStateStoresByName.values()));
        this.storeToChangelogTopic = Collections.unmodifiableMap(stateStoreToChangelogTopic);
        this.repartitionTopics = Collections.unmodifiableSet(repartitionTopics);
    }

    public Set<String> sourceTopics() {
        return sourcesByTopic.keySet();
    }

    public SourceNode source(String topic) {
        return sourcesByTopic.get(topic);
    }

    public Set<SourceNode> sources() {
        return new HashSet<>(sourcesByTopic.values());
    }

    public Set<String> sinkTopics() {
        return sinksByTopic.keySet();
    }

    public SinkNode sink(String topic) {
        return sinksByTopic.get(topic);
    }

    public Set<SinkNode> sinks() {
        return new HashSet<>(sinksByTopic.values());
    }

    public List<ProcessorNode> processors() {
        return processorNodes;
    }

    public List<StateStore> stateStores() {
        return stateStores;
    }

    public List<StateStore> globalStateStores() {
        return globalStateStores;
    }

    public Map<String, String> storeToChangelogTopic() {
        return storeToChangelogTopic;
    }

    public boolean isTopicInternalTransient(String topic) {
        return repartitionTopics.contains(topic);
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
        for (SourceNode<?, ?> source : sourcesByTopic.values()) {
            sb.append(source.toString(indent + "\t")).append(childrenToString(indent + "\t", source.children()));
        }
        return sb.toString();
    }

}
