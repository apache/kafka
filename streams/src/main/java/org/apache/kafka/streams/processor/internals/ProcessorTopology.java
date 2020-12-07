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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ProcessorTopology {
    private final Logger log = LoggerFactory.getLogger(ProcessorTopology.class);

    private final List<ProcessorNode<?, ?, ?, ?>> processorNodes;
    private final Map<String, SourceNode<?, ?, ?, ?>> sourceNodesByName;
    private final Map<String, SourceNode<?, ?, ?, ?>> sourceNodesByTopic;
    private final Map<String, SinkNode<?, ?, ?, ?>> sinksByTopic;
    private final Set<String> terminalNodes;
    private final List<StateStore> stateStores;
    private final Set<String> stateStoreNames;
    private final Set<String> repartitionTopics;

    // the following contains entries for the entire topology, eg stores that do not belong to this ProcessorTopology
    private final List<StateStore> globalStateStores;
    private final Map<String, String> storeToChangelogTopic;

    public ProcessorTopology(final List<ProcessorNode<?, ?, ?, ?>> processorNodes,
                             final Map<String, SourceNode<?, ?, ?, ?>> sourceNodesByTopic,
                             final Map<String, SinkNode<?, ?, ?, ?>> sinksByTopic,
                             final List<StateStore> stateStores,
                             final List<StateStore> globalStateStores,
                             final Map<String, String> storeToChangelogTopic,
                             final Set<String> repartitionTopics) {
        this.processorNodes = Collections.unmodifiableList(processorNodes);
        this.sourceNodesByTopic = new HashMap<>(sourceNodesByTopic);
        this.sinksByTopic = Collections.unmodifiableMap(sinksByTopic);
        this.stateStores = Collections.unmodifiableList(stateStores);
        stateStoreNames = stateStores.stream().map(StateStore::name).collect(Collectors.toSet());
        this.globalStateStores = Collections.unmodifiableList(globalStateStores);
        this.storeToChangelogTopic = Collections.unmodifiableMap(storeToChangelogTopic);
        this.repartitionTopics = Collections.unmodifiableSet(repartitionTopics);

        this.terminalNodes = new HashSet<>();
        for (final ProcessorNode<?, ?, ?, ?> node : processorNodes) {
            if (node.isTerminalNode()) {
                terminalNodes.add(node.name());
            }
        }

        this.sourceNodesByName = new HashMap<>();
        for (final SourceNode<?, ?, ?, ?> source : sourceNodesByTopic.values()) {
            sourceNodesByName.put(source.name(), source);
        }
    }

    public Set<String> sourceTopics() {
        return sourceNodesByTopic.keySet();
    }

    public SourceNode<?, ?, ?, ?> source(final String topic) {
        return sourceNodesByTopic.get(topic);
    }

    public Set<SourceNode<?, ?, ?, ?>> sources() {
        return new HashSet<>(sourceNodesByTopic.values());
    }

    public Set<String> sinkTopics() {
        return sinksByTopic.keySet();
    }

    public SinkNode<?, ?, ?, ?> sink(final String topic) {
        return sinksByTopic.get(topic);
    }

    public Set<String> terminalNodes() {
        return terminalNodes;
    }

    public List<ProcessorNode<?, ?, ?, ?>> processors() {
        return processorNodes;
    }

    public List<StateStore> stateStores() {
        return stateStores;
    }

    public boolean hasStore(final String storeName) {
        return stateStoreNames.contains(storeName);
    }

    public List<StateStore> globalStateStores() {
        return Collections.unmodifiableList(globalStateStores);
    }

    public Map<String, String> storeToChangelogTopic() {
        return Collections.unmodifiableMap(storeToChangelogTopic);
    }

    boolean isRepartitionTopic(final String topic) {
        return repartitionTopics.contains(topic);
    }

    boolean hasStateWithChangelogs() {
        for (final StateStore stateStore : stateStores) {
            if (storeToChangelogTopic.containsKey(stateStore.name())) {
                return true;
            }
        }
        return false;
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

    public void updateSourceTopics(final Map<String, List<String>> allSourceTopicsByNodeName) {
        sourceNodesByTopic.clear();
        for (final Map.Entry<String, SourceNode<?, ?, ?, ?>> sourceNodeEntry : sourceNodesByName.entrySet()) {
            final String sourceNodeName = sourceNodeEntry.getKey();
            final SourceNode<?, ?, ?, ?> sourceNode = sourceNodeEntry.getValue();

            final List<String> updatedSourceTopics = allSourceTopicsByNodeName.get(sourceNodeName);
            if (updatedSourceTopics == null) {
                log.error("Unable to find source node {} in updated topics map {}",
                          sourceNodeName, allSourceTopicsByNodeName);
                throw new IllegalStateException("Node " + sourceNodeName + " not found in full topology");
            }

            log.trace("Updating source node {} with new topics {}", sourceNodeName, updatedSourceTopics);
            for (final String topic : updatedSourceTopics) {
                if (sourceNodesByTopic.containsKey(topic)) {
                    log.error("Tried to subscribe topic {} to two nodes when updating topics from {}",
                              topic, allSourceTopicsByNodeName);
                    throw new IllegalStateException("Topic " + topic + " was already registered to source node "
                                                        + sourceNodesByTopic.get(topic).name());
                }
                sourceNodesByTopic.put(topic, sourceNode);
            }
        }
    }

    private String childrenToString(final String indent, final List<? extends ProcessorNode<?, ?, ?, ?>> children) {
        if (children == null || children.isEmpty()) {
            return "";
        }

        final StringBuilder sb = new StringBuilder(indent + "\tchildren:\t[");
        for (final ProcessorNode<?, ?, ?, ?> child : children) {
            sb.append(child.name());
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);  // remove the last comma
        sb.append("]\n");

        // recursively print children
        for (final ProcessorNode<?, ?, ?, ?> child : children) {
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
        final Map<SourceNode<?, ?, ?, ?>, List<String>> sourceToTopics = new HashMap<>();
        for (final Map.Entry<String, SourceNode<?, ?, ?, ?>> sourceNodeEntry : sourceNodesByTopic.entrySet()) {
            final String topic = sourceNodeEntry.getKey();
            final SourceNode<?, ?, ?, ?> source = sourceNodeEntry.getValue();
            sourceToTopics.computeIfAbsent(source, s -> new ArrayList<>());
            sourceToTopics.get(source).add(topic);
        }

        final StringBuilder sb = new StringBuilder(indent + "ProcessorTopology:\n");

        // start from sources
        for (final Map.Entry<SourceNode<?, ?, ?, ?>, List<String>> sourceNodeEntry : sourceToTopics.entrySet()) {
            final SourceNode<?, ?, ?, ?> source = sourceNodeEntry.getKey();
            final List<String> topics = sourceNodeEntry.getValue();
            sb.append(source.toString(indent + "\t"))
                .append(topicsToString(indent + "\t", topics))
                .append(childrenToString(indent + "\t", source.children()));
        }
        return sb.toString();
    }

    private static String topicsToString(final String indent, final List<String> topics) {
        final StringBuilder sb = new StringBuilder();
        sb.append(indent).append("\ttopics:\t\t[");
        for (final String topic : topics) {
            sb.append(topic);
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);  // remove the last comma
        sb.append("]\n");
        return sb.toString();
    }

    // for testing only
    public Set<String> processorConnectedStateStores(final String processorName) {
        for (final ProcessorNode<?, ?, ?, ?> node : processorNodes) {
            if (node.name().equals(processorName)) {
                return node.stateStores;
            }
        }

        return Collections.emptySet();
    }
}
