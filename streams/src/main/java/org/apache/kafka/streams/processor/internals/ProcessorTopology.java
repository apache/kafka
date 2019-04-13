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

    public ProcessorTopology(final List<ProcessorNode> processorNodes,
                             final Map<String, SourceNode> sourcesByTopic,
                             final Map<String, SinkNode> sinksByTopic,
                             final List<StateStore> stateStores,
                             final List<StateStore> globalStateStores,
                             final Map<String, String> storeToChangelogTopic,
                             final Set<String> repartitionTopics) {
        this.processorNodes = Collections.unmodifiableList(processorNodes);
        this.sourcesByTopic = Collections.unmodifiableMap(sourcesByTopic);
        this.sinksByTopic = Collections.unmodifiableMap(sinksByTopic);
        this.stateStores = Collections.unmodifiableList(stateStores);
        this.globalStateStores = Collections.unmodifiableList(globalStateStores);
        this.storeToChangelogTopic = Collections.unmodifiableMap(storeToChangelogTopic);
        this.repartitionTopics = Collections.unmodifiableSet(repartitionTopics);
    }

    public Set<String> sourceTopics() {
        return sourcesByTopic.keySet();
    }

    public SourceNode source(final String topic) {
        return sourcesByTopic.get(topic);
    }

    public Set<SourceNode> sources() {
        return new HashSet<>(sourcesByTopic.values());
    }

    public Set<String> sinkTopics() {
        return sinksByTopic.keySet();
    }

    public SinkNode sink(final String topic) {
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

    boolean isRepartitionTopic(final String topic) {
        return repartitionTopics.contains(topic);
    }

    public boolean hasPersistentLocalStore() {
        for (final StateStore store : stateStores) {
            if (store.persistent()) {
                return true;
            }
        }
        return false;
    }

    public boolean hasPersistentGlobalStore() {
        for (final StateStore store : globalStateStores) {
            if (store.persistent()) {
                return true;
            }
        }
        return false;
    }

    private String childrenToString(final String indent, final List<ProcessorNode<?, ?>> children) {
        if (children == null || children.isEmpty()) {
            return "";
        }

        final StringBuilder sb = new StringBuilder(indent + "\tchildren:\t[");
        for (final ProcessorNode child : children) {
            sb.append(child.name());
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);  // remove the last comma
        sb.append("]\n");

        // recursively print children
        for (final ProcessorNode<?, ?> child : children) {
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
        for (final SourceNode<?, ?> source : sourcesByTopic.values()) {
            sb.append(source.toString(indent + "\t")).append(childrenToString(indent + "\t", source.children()));
        }
        return sb.toString();
    }

    // for testing only
    public Set<String> processorConnectedStateStores(final String processorName) {
        for (final ProcessorNode<?, ?> node : processorNodes) {
            if (node.name().equals(processorName)) {
                return node.stateStores;
            }
        }

        return Collections.emptySet();
    }
}
