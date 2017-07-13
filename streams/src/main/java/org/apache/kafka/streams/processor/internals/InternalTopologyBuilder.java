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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.StreamPartitionAssignor.SubscriptionUpdates;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.WindowStoreSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;


public class InternalTopologyBuilder {

    private static final Logger log = LoggerFactory.getLogger(InternalTopologyBuilder.class);

    private static final Pattern EMPTY_ZERO_LENGTH_PATTERN = Pattern.compile("");

    // node factories in a topological order
    private final LinkedHashMap<String, NodeFactory> nodeFactories = new LinkedHashMap<>();

    // state factories
    private final Map<String, StateStoreFactory> stateFactories = new HashMap<>();

    // global state factories
    private final Map<String, StateStore> globalStateStores = new LinkedHashMap<>();

    // all topics subscribed from source processors (without application-id prefix for internal topics)
    private final Set<String> sourceTopicNames = new HashSet<>();

    // all internal topics auto-created by the topology builder and used in source / sink processors
    private final Set<String> internalTopicNames = new HashSet<>();

    // groups of source processors that need to be copartitioned
    private final List<Set<String>> copartitionSourceGroups = new ArrayList<>();

    // map from source processor names to subscribed topics (without application-id prefix for internal topics)
    private final HashMap<String, List<String>> nodeToSourceTopics = new HashMap<>();

    // map from source processor names to regex subscription patterns
    private final HashMap<String, Pattern> nodeToSourcePatterns = new LinkedHashMap<>();

    // map from sink processor names to subscribed topic (without application-id prefix for internal topics)
    private final HashMap<String, String> nodeToSinkTopic = new HashMap<>();

    // map from topics to their matched regex patterns, this is to ensure one topic is passed through on source node
    // even if it can be matched by multiple regex patterns
    private final HashMap<String, Pattern> topicToPatterns = new HashMap<>();

    // map from state store names to all the topics subscribed from source processors that
    // are connected to these state stores
    private final Map<String, Set<String>> stateStoreNameToSourceTopics = new HashMap<>();

    // map from state store names to all the regex subscribed topics from source processors that
    // are connected to these state stores
    private final Map<String, Set<Pattern>> stateStoreNameToSourceRegex = new HashMap<>();

    // map from state store names to this state store's corresponding changelog topic if possible,
    // this is used in the extended KStreamBuilder.
    private final Map<String, String> storeToChangelogTopic = new HashMap<>();

    // all global topics
    private final Set<String> globalTopics = new HashSet<>();

    private final Set<String> earliestResetTopics = new HashSet<>();

    private final Set<String> latestResetTopics = new HashSet<>();

    private final Set<Pattern> earliestResetPatterns = new HashSet<>();

    private final Set<Pattern> latestResetPatterns = new HashSet<>();

    private final QuickUnion<String> nodeGrouper = new QuickUnion<>();

    private SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();

    private String applicationId = null;

    private Pattern topicPattern = null;

    private Map<Integer, Set<String>> nodeGroups = null;

    private static class StateStoreFactory {
        public final Set<String> users;

        public final StateStoreSupplier supplier;

        StateStoreFactory(final StateStoreSupplier supplier) {
            this.supplier = supplier;
            users = new HashSet<>();
        }
    }

    private static abstract class NodeFactory {
        public final String name;

        NodeFactory(final String name) {
            this.name = name;
        }

        public abstract ProcessorNode build();
    }

    private static class ProcessorNodeFactory extends NodeFactory {
        private final String[] parents;
        private final ProcessorSupplier<?, ?> supplier;
        private final Set<String> stateStoreNames = new HashSet<>();

        ProcessorNodeFactory(final String name,
                             final String[] parents,
                             final ProcessorSupplier<?, ?> supplier) {
            super(name);
            this.parents = parents.clone();
            this.supplier = supplier;
        }

        public void addStateStore(final String stateStoreName) {
            stateStoreNames.add(stateStoreName);
        }

        @Override
        public ProcessorNode build() {
            return new ProcessorNode<>(name, supplier.get(), stateStoreNames);
        }
    }

    private class SourceNodeFactory extends NodeFactory {
        private final List<String> topics;
        private final Pattern pattern;
        private final Deserializer<?> keyDeserializer;
        private final Deserializer<?> valDeserializer;
        private final TimestampExtractor timestampExtractor;

        private SourceNodeFactory(final String name,
                                  final String[] topics,
                                  final Pattern pattern,
                                  final TimestampExtractor timestampExtractor,
                                  final Deserializer<?> keyDeserializer,
                                  final Deserializer<?> valDeserializer) {
            super(name);
            this.topics = topics != null ? Arrays.asList(topics) : new ArrayList<String>();
            this.pattern = pattern;
            this.keyDeserializer = keyDeserializer;
            this.valDeserializer = valDeserializer;
            this.timestampExtractor = timestampExtractor;
        }

        List<String> getTopics(final Collection<String> subscribedTopics) {
            // if it is subscribed via patterns, it is possible that the topic metadata has not been updated
            // yet and hence the map from source node to topics is stale, in this case we put the pattern as a place holder;
            // this should only happen for debugging since during runtime this function should always be called after the metadata has updated.
            if (subscribedTopics.isEmpty()) {
                return Collections.singletonList("" + pattern + "");
            }

            final List<String> matchedTopics = new ArrayList<>();
            for (final String update : subscribedTopics) {
                if (pattern == topicToPatterns.get(update)) {
                    matchedTopics.add(update);
                } else if (topicToPatterns.containsKey(update) && isMatch(update)) {
                    // the same topic cannot be matched to more than one pattern
                    // TODO: we should lift this requirement in the future
                    throw new TopologyBuilderException("Topic " + update +
                            " is already matched for another regex pattern " + topicToPatterns.get(update) +
                            " and hence cannot be matched to this regex pattern " + pattern + " any more.");
                } else if (isMatch(update)) {
                    topicToPatterns.put(update, pattern);
                    matchedTopics.add(update);
                }
            }
            return matchedTopics;
        }

        @Override
        public ProcessorNode build() {
            final List<String> sourceTopics = nodeToSourceTopics.get(name);

            // if it is subscribed via patterns, it is possible that the topic metadata has not been updated
            // yet and hence the map from source node to topics is stale, in this case we put the pattern as a place holder;
            // this should only happen for debugging since during runtime this function should always be called after the metadata has updated.
            if (sourceTopics == null) {
                return new SourceNode<>(name, Collections.singletonList("" + pattern + ""), timestampExtractor, keyDeserializer, valDeserializer);
            } else {
                return new SourceNode<>(name, maybeDecorateInternalSourceTopics(sourceTopics), timestampExtractor, keyDeserializer, valDeserializer);
            }
        }

        private boolean isMatch(final String topic) {
            return pattern.matcher(topic).matches();
        }
    }

    private class SinkNodeFactory<K, V> extends NodeFactory {
        private final String[] parents;
        private final String topic;
        private final Serializer<K> keySerializer;
        private final Serializer<V> valSerializer;
        private final StreamPartitioner<? super K, ? super V> partitioner;

        private SinkNodeFactory(final String name,
                                final String[] parents,
                                final String topic,
                                final Serializer<K> keySerializer,
                                final Serializer<V> valSerializer,
                                final StreamPartitioner<? super K, ? super V> partitioner) {
            super(name);
            this.parents = parents.clone();
            this.topic = topic;
            this.keySerializer = keySerializer;
            this.valSerializer = valSerializer;
            this.partitioner = partitioner;
        }

        @Override
        public ProcessorNode build() {
            if (internalTopicNames.contains(topic)) {
                // prefix the internal topic name with the application id
                return new SinkNode<>(name, decorateTopic(topic), keySerializer, valSerializer, partitioner);
            } else {
                return new SinkNode<>(name, topic, keySerializer, valSerializer, partitioner);
            }
        }
    }

    public synchronized final InternalTopologyBuilder setApplicationId(final String applicationId) {
        Objects.requireNonNull(applicationId, "applicationId can't be null");
        this.applicationId = applicationId;

        return this;
    }

    public final void addSource(final TopologyBuilder.AutoOffsetReset offsetReset,
                                final String name,
                                final TimestampExtractor timestampExtractor,
                                final Deserializer keyDeserializer,
                                final Deserializer valDeserializer,
                                final String... topics) {
        if (topics.length == 0) {
            throw new TopologyBuilderException("You must provide at least one topic");
        }
        Objects.requireNonNull(name, "name must not be null");
        if (nodeFactories.containsKey(name)) {
            throw new TopologyBuilderException("Processor " + name + " is already added.");
        }

        for (final String topic : topics) {
            Objects.requireNonNull(topic, "topic names cannot be null");
            validateTopicNotAlreadyRegistered(topic);
            maybeAddToResetList(earliestResetTopics, latestResetTopics, offsetReset, topic);
            sourceTopicNames.add(topic);
        }

        nodeFactories.put(name, new SourceNodeFactory(name, topics, null, timestampExtractor, keyDeserializer, valDeserializer));
        nodeToSourceTopics.put(name, Arrays.asList(topics));
        nodeGrouper.add(name);
    }

    public final void addGlobalStore(final StateStoreSupplier<KeyValueStore> storeSupplier,
                                     final String sourceName,
                                     final TimestampExtractor timestampExtractor,
                                     final Deserializer keyDeserializer,
                                     final Deserializer valueDeserializer,
                                     final String topic,
                                     final String processorName,
                                     final ProcessorSupplier stateUpdateSupplier) {
        Objects.requireNonNull(storeSupplier, "store supplier must not be null");
        Objects.requireNonNull(sourceName, "sourceName must not be null");
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(stateUpdateSupplier, "supplier must not be null");
        Objects.requireNonNull(processorName, "processorName must not be null");
        if (nodeFactories.containsKey(sourceName)) {
            throw new TopologyBuilderException("Processor " + sourceName + " is already added.");
        }
        if (nodeFactories.containsKey(processorName)) {
            throw new TopologyBuilderException("Processor " + processorName + " is already added.");
        }
        if (stateFactories.containsKey(storeSupplier.name()) || globalStateStores.containsKey(storeSupplier.name())) {
            throw new TopologyBuilderException("StateStore " + storeSupplier.name() + " is already added.");
        }
        if (storeSupplier.loggingEnabled()) {
            throw new TopologyBuilderException("StateStore " + storeSupplier.name() + " for global table must not have logging enabled.");
        }
        if (sourceName.equals(processorName)) {
            throw new TopologyBuilderException("sourceName and processorName must be different.");
        }

        validateTopicNotAlreadyRegistered(topic);

        globalTopics.add(topic);
        final String[] topics = {topic};
        nodeFactories.put(sourceName, new SourceNodeFactory(sourceName, topics, null, timestampExtractor, keyDeserializer, valueDeserializer));
        nodeToSourceTopics.put(sourceName, Arrays.asList(topics));
        nodeGrouper.add(sourceName);

        final String[] parents = {sourceName};
        final ProcessorNodeFactory nodeFactory = new ProcessorNodeFactory(processorName, parents, stateUpdateSupplier);
        nodeFactory.addStateStore(storeSupplier.name());
        nodeFactories.put(processorName, nodeFactory);
        nodeGrouper.add(processorName);
        nodeGrouper.unite(processorName, parents);

        globalStateStores.put(storeSupplier.name(), storeSupplier.get());
        connectSourceStoreAndTopic(storeSupplier.name(), topic);
    }

    private void validateTopicNotAlreadyRegistered(final String topic) {
        if (sourceTopicNames.contains(topic) || globalTopics.contains(topic)) {
            throw new TopologyBuilderException("Topic " + topic + " has already been registered by another source.");
        }

        for (final Pattern pattern : nodeToSourcePatterns.values()) {
            if (pattern.matcher(topic).matches()) {
                throw new TopologyBuilderException("Topic " + topic + " matches a Pattern already registered by another source.");
            }
        }
    }

    public final void addSource(final TopologyBuilder.AutoOffsetReset offsetReset,
                                final String name,
                                final TimestampExtractor timestampExtractor,
                                final Deserializer keyDeserializer,
                                final Deserializer valDeserializer,
                                final Pattern topicPattern) {
        Objects.requireNonNull(topicPattern, "topicPattern can't be null");
        Objects.requireNonNull(name, "name can't be null");

        if (nodeFactories.containsKey(name)) {
            throw new TopologyBuilderException("Processor " + name + " is already added.");
        }

        for (final String sourceTopicName : sourceTopicNames) {
            if (topicPattern.matcher(sourceTopicName).matches()) {
                throw new TopologyBuilderException("Pattern  " + topicPattern + " will match a topic that has already been registered by another source.");
            }
        }

        maybeAddToResetList(earliestResetPatterns, latestResetPatterns, offsetReset, topicPattern);

        nodeFactories.put(name, new SourceNodeFactory(name, null, topicPattern, timestampExtractor, keyDeserializer, valDeserializer));
        nodeToSourcePatterns.put(name, topicPattern);
        nodeGrouper.add(name);
    }

    public final <K, V> void addSink(final String name,
                                     final String topic,
                                     final Serializer<K> keySerializer,
                                     final Serializer<V> valSerializer,
                                     final StreamPartitioner<? super K, ? super V> partitioner,
                                     final String... parentNames) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(topic, "topic must not be null");
        if (nodeFactories.containsKey(name)) {
            throw new TopologyBuilderException("Processor " + name + " is already added.");
        }

        for (final String parent : parentNames) {
            if (parent.equals(name)) {
                throw new TopologyBuilderException("Processor " + name + " cannot be a parent of itself.");
            }
            if (!nodeFactories.containsKey(parent)) {
                throw new TopologyBuilderException("Parent processor " + parent + " is not added yet.");
            }
        }

        nodeFactories.put(name, new SinkNodeFactory<>(name, parentNames, topic, keySerializer, valSerializer, partitioner));
        nodeToSinkTopic.put(name, topic);
        nodeGrouper.add(name);
        nodeGrouper.unite(name, parentNames);
    }

    public final void addProcessor(final String name,
                                   final ProcessorSupplier supplier,
                                   final String... parentNames) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        if (nodeFactories.containsKey(name)) {
            throw new TopologyBuilderException("Processor " + name + " is already added.");
        }

        for (final String parent : parentNames) {
            if (parent.equals(name)) {
                throw new TopologyBuilderException("Processor " + name + " cannot be a parent of itself.");
            }
            if (!nodeFactories.containsKey(parent)) {
                throw new TopologyBuilderException("Parent processor " + parent + " is not added yet.");
            }
        }

        nodeFactories.put(name, new ProcessorNodeFactory(name, parentNames, supplier));
        nodeGrouper.add(name);
        nodeGrouper.unite(name, parentNames);
    }

    public final void addStateStore(final StateStoreSupplier supplier,
                                    final String... processorNames) {
        Objects.requireNonNull(supplier, "supplier can't be null");
        if (stateFactories.containsKey(supplier.name())) {
            throw new TopologyBuilderException("StateStore " + supplier.name() + " is already added.");
        }

        stateFactories.put(supplier.name(), new StateStoreFactory(supplier));

        if (processorNames != null) {
            for (final String processorName : processorNames) {
                connectProcessorAndStateStore(processorName, supplier.name());
            }
        }
    }

    public final void connectProcessorAndStateStores(final String processorName,
                                                     final String... stateStoreNames) {
        Objects.requireNonNull(processorName, "processorName can't be null");
        if (stateStoreNames != null) {
            for (final String stateStoreName : stateStoreNames) {
                connectProcessorAndStateStore(processorName, stateStoreName);
            }
        }
    }

    public final void connectSourceStoreAndTopic(final String sourceStoreName,
                                                  final String topic) {
        if (storeToChangelogTopic.containsKey(sourceStoreName)) {
            throw new TopologyBuilderException("Source store " + sourceStoreName + " is already added.");
        }
        storeToChangelogTopic.put(sourceStoreName, topic);
    }

    public final void connectProcessors(final String... processorNames) {
        if (processorNames.length < 2) {
            throw new TopologyBuilderException("At least two processors need to participate in the connection.");
        }

        for (final String processorName : processorNames) {
            if (!nodeFactories.containsKey(processorName)) {
                throw new TopologyBuilderException("Processor " + processorName + " is not added yet.");
            }
        }

        nodeGrouper.unite(processorNames[0], Arrays.copyOfRange(processorNames, 1, processorNames.length));
    }

    public final void addInternalTopic(final String topicName) {
        Objects.requireNonNull(topicName, "topicName can't be null");
        internalTopicNames.add(topicName);
    }

    public final void copartitionSources(final Collection<String> sourceNodes) {
        copartitionSourceGroups.add(Collections.unmodifiableSet(new HashSet<>(sourceNodes)));
    }

    private void connectProcessorAndStateStore(final String processorName,
                                               final String stateStoreName) {
        if (!stateFactories.containsKey(stateStoreName)) {
            throw new TopologyBuilderException("StateStore " + stateStoreName + " is not added yet.");
        }
        if (!nodeFactories.containsKey(processorName)) {
            throw new TopologyBuilderException("Processor " + processorName + " is not added yet.");
        }

        final StateStoreFactory stateStoreFactory = stateFactories.get(stateStoreName);
        final Iterator<String> iter = stateStoreFactory.users.iterator();
        if (iter.hasNext()) {
            final String user = iter.next();
            nodeGrouper.unite(user, processorName);
        }
        stateStoreFactory.users.add(processorName);

        final NodeFactory nodeFactory = nodeFactories.get(processorName);
        if (nodeFactory instanceof ProcessorNodeFactory) {
            final ProcessorNodeFactory processorNodeFactory = (ProcessorNodeFactory) nodeFactory;
            processorNodeFactory.addStateStore(stateStoreName);
            connectStateStoreNameToSourceTopicsOrPattern(stateStoreName, processorNodeFactory);
        } else {
            throw new TopologyBuilderException("cannot connect a state store " + stateStoreName + " to a source node or a sink node.");
        }
    }

    private Set<SourceNodeFactory> findSourcesForProcessorParents(final String[] parents) {
        final Set<SourceNodeFactory> sourceNodes = new HashSet<>();
        for (final String parent : parents) {
            final NodeFactory nodeFactory = nodeFactories.get(parent);
            if (nodeFactory instanceof SourceNodeFactory) {
                sourceNodes.add((SourceNodeFactory) nodeFactory);
            } else if (nodeFactory instanceof ProcessorNodeFactory) {
                sourceNodes.addAll(findSourcesForProcessorParents(((ProcessorNodeFactory) nodeFactory).parents));
            }
        }
        return sourceNodes;
    }

    private void connectStateStoreNameToSourceTopicsOrPattern(final String stateStoreName,
                                                              final ProcessorNodeFactory processorNodeFactory) {
        // we should never update the mapping from state store names to source topics if the store name already exists
        // in the map; this scenario is possible, for example, that a state store underlying a source KTable is
        // connecting to a join operator whose source topic is not the original KTable's source topic but an internal repartition topic.

        if (stateStoreNameToSourceTopics.containsKey(stateStoreName) || stateStoreNameToSourceRegex.containsKey(stateStoreName)) {
            return;
        }

        final Set<String> sourceTopics = new HashSet<>();
        final Set<Pattern> sourcePatterns = new HashSet<>();
        final Set<SourceNodeFactory> sourceNodesForParent = findSourcesForProcessorParents(processorNodeFactory.parents);

        for (final SourceNodeFactory sourceNodeFactory : sourceNodesForParent) {
            if (sourceNodeFactory.pattern != null) {
                sourcePatterns.add(sourceNodeFactory.pattern);
            } else {
                sourceTopics.addAll(sourceNodeFactory.topics);
            }
        }

        if (!sourceTopics.isEmpty()) {
            stateStoreNameToSourceTopics.put(stateStoreName,
                    Collections.unmodifiableSet(sourceTopics));
        }

        if (!sourcePatterns.isEmpty()) {
            stateStoreNameToSourceRegex.put(stateStoreName,
                    Collections.unmodifiableSet(sourcePatterns));
        }

    }

    private <T> void maybeAddToResetList(final Collection<T> earliestResets,
                                         final Collection<T> latestResets,
                                         final TopologyBuilder.AutoOffsetReset offsetReset,
                                         final T item) {
        if (offsetReset != null) {
            switch (offsetReset) {
                case EARLIEST:
                    earliestResets.add(item);
                    break;
                case LATEST:
                    latestResets.add(item);
                    break;
                default:
                    throw new TopologyBuilderException(String.format("Unrecognized reset format %s", offsetReset));
            }
        }
    }

    public synchronized Map<Integer, Set<String>> nodeGroups() {
        if (nodeGroups == null) {
            nodeGroups = makeNodeGroups();
        }
        return nodeGroups;
    }

    private Map<Integer, Set<String>> makeNodeGroups() {
        final HashMap<Integer, Set<String>> nodeGroups = new LinkedHashMap<>();
        final HashMap<String, Set<String>> rootToNodeGroup = new HashMap<>();

        int nodeGroupId = 0;

        // Go through source nodes first. This makes the group id assignment easy to predict in tests
        final HashSet<String> allSourceNodes = new HashSet<>(nodeToSourceTopics.keySet());
        allSourceNodes.addAll(nodeToSourcePatterns.keySet());

        for (final String nodeName : Utils.sorted(allSourceNodes)) {
            final String root = nodeGrouper.root(nodeName);
            Set<String> nodeGroup = rootToNodeGroup.get(root);
            if (nodeGroup == null) {
                nodeGroup = new HashSet<>();
                rootToNodeGroup.put(root, nodeGroup);
                nodeGroups.put(nodeGroupId++, nodeGroup);
            }
            nodeGroup.add(nodeName);
        }

        // Go through non-source nodes
        for (final String nodeName : Utils.sorted(nodeFactories.keySet())) {
            if (!nodeToSourceTopics.containsKey(nodeName)) {
                final String root = nodeGrouper.root(nodeName);
                Set<String> nodeGroup = rootToNodeGroup.get(root);
                if (nodeGroup == null) {
                    nodeGroup = new HashSet<>();
                    rootToNodeGroup.put(root, nodeGroup);
                    nodeGroups.put(nodeGroupId++, nodeGroup);
                }
                nodeGroup.add(nodeName);
            }
        }

        return nodeGroups;
    }

    public synchronized ProcessorTopology build(final Integer topicGroupId) {
        final Set<String> nodeGroup;
        if (topicGroupId != null) {
            nodeGroup = nodeGroups().get(topicGroupId);
        } else {
            // when topicGroupId is null, we build the full topology minus the global groups
            final Set<String> globalNodeGroups = globalNodeGroups();
            final Collection<Set<String>> values = nodeGroups().values();
            nodeGroup = new HashSet<>();
            for (final Set<String> value : values) {
                nodeGroup.addAll(value);
            }
            nodeGroup.removeAll(globalNodeGroups);


        }
        return build(nodeGroup);
    }

    /**
     * Builds the topology for any global state stores
     * @return ProcessorTopology
     */
    public synchronized ProcessorTopology buildGlobalStateTopology() {
        final Set<String> globalGroups = globalNodeGroups();
        if (globalGroups.isEmpty()) {
            return null;
        }
        return build(globalGroups);
    }

    private Set<String> globalNodeGroups() {
        final Set<String> globalGroups = new HashSet<>();
        for (final Map.Entry<Integer, Set<String>> nodeGroup : nodeGroups().entrySet()) {
            final Set<String> nodes = nodeGroup.getValue();
            for (final String node : nodes) {
                final NodeFactory nodeFactory = nodeFactories.get(node);
                if (nodeFactory instanceof SourceNodeFactory) {
                    final List<String> topics = ((SourceNodeFactory) nodeFactory).topics;
                    if (topics != null && topics.size() == 1 && globalTopics.contains(topics.get(0))) {
                        globalGroups.addAll(nodes);
                    }
                }
            }
        }
        return globalGroups;
    }

    private ProcessorTopology build(final Set<String> nodeGroup) {
        final List<ProcessorNode> processorNodes = new ArrayList<>(nodeFactories.size());
        final Map<String, ProcessorNode> processorMap = new HashMap<>();
        final Map<String, SourceNode> topicSourceMap = new HashMap<>();
        final Map<String, SinkNode> topicSinkMap = new HashMap<>();
        final Map<String, StateStore> stateStoreMap = new LinkedHashMap<>();

        // create processor nodes in a topological order ("nodeFactories" is already topologically sorted)
        for (final NodeFactory factory : nodeFactories.values()) {
            if (nodeGroup == null || nodeGroup.contains(factory.name)) {
                final ProcessorNode node = factory.build();
                processorNodes.add(node);
                processorMap.put(node.name(), node);

                if (factory instanceof ProcessorNodeFactory) {
                    for (final String parent : ((ProcessorNodeFactory) factory).parents) {
                        final ProcessorNode<?, ?> parentNode = processorMap.get(parent);
                        parentNode.addChild(node);
                    }
                    for (final String stateStoreName : ((ProcessorNodeFactory) factory).stateStoreNames) {
                        if (!stateStoreMap.containsKey(stateStoreName)) {
                            final StateStore stateStore;

                            if (stateFactories.containsKey(stateStoreName)) {
                                final StateStoreSupplier supplier = stateFactories.get(stateStoreName).supplier;
                                stateStore = supplier.get();

                                // remember the changelog topic if this state store is change-logging enabled
                                if (supplier.loggingEnabled() && !storeToChangelogTopic.containsKey(stateStoreName)) {
                                    final String changelogTopic = ProcessorStateManager.storeChangelogTopic(this.applicationId, stateStoreName);
                                    storeToChangelogTopic.put(stateStoreName, changelogTopic);
                                }
                            } else {
                                stateStore = globalStateStores.get(stateStoreName);
                            }

                            stateStoreMap.put(stateStoreName, stateStore);
                        }
                    }
                } else if (factory instanceof SourceNodeFactory) {
                    final SourceNodeFactory sourceNodeFactory = (SourceNodeFactory) factory;
                    final List<String> topics = (sourceNodeFactory.pattern != null) ?
                            sourceNodeFactory.getTopics(subscriptionUpdates.getUpdates()) :
                            sourceNodeFactory.topics;

                    for (final String topic : topics) {
                        if (internalTopicNames.contains(topic)) {
                            // prefix the internal topic name with the application id
                            topicSourceMap.put(decorateTopic(topic), (SourceNode) node);
                        } else {
                            topicSourceMap.put(topic, (SourceNode) node);
                        }
                    }
                } else if (factory instanceof SinkNodeFactory) {
                    final SinkNodeFactory sinkNodeFactory = (SinkNodeFactory) factory;

                    for (final String parent : sinkNodeFactory.parents) {
                        processorMap.get(parent).addChild(node);
                        if (internalTopicNames.contains(sinkNodeFactory.topic)) {
                            // prefix the internal topic name with the application id
                            topicSinkMap.put(decorateTopic(sinkNodeFactory.topic), (SinkNode) node);
                        } else {
                            topicSinkMap.put(sinkNodeFactory.topic, (SinkNode) node);
                        }
                    }
                } else {
                    throw new TopologyBuilderException("Unknown definition class: " + factory.getClass().getName());
                }
            }
        }

        return new ProcessorTopology(processorNodes, topicSourceMap, topicSinkMap, new ArrayList<>(stateStoreMap.values()), storeToChangelogTopic, new ArrayList<>(globalStateStores.values()));
    }

    /**
     * Get any global {@link StateStore}s that are part of the
     * topology
     * @return map containing all global {@link StateStore}s
     */
    public Map<String, StateStore> globalStateStores() {
        return Collections.unmodifiableMap(globalStateStores);
    }

    /**
     * Returns the map of topic groups keyed by the group id.
     * A topic group is a group of topics in the same task.
     *
     * @return groups of topic names
     */
    public synchronized Map<Integer, TopologyBuilder.TopicsInfo> topicGroups() {
        final Map<Integer, TopologyBuilder.TopicsInfo> topicGroups = new LinkedHashMap<>();

        if (nodeGroups == null) {
            nodeGroups = makeNodeGroups();
        }

        for (final Map.Entry<Integer, Set<String>> entry : nodeGroups.entrySet()) {
            final Set<String> sinkTopics = new HashSet<>();
            final Set<String> sourceTopics = new HashSet<>();
            final Map<String, InternalTopicConfig> internalSourceTopics = new HashMap<>();
            final Map<String, InternalTopicConfig> stateChangelogTopics = new HashMap<>();
            for (final String node : entry.getValue()) {
                // if the node is a source node, add to the source topics
                final List<String> topics = nodeToSourceTopics.get(node);
                if (topics != null) {
                    // if some of the topics are internal, add them to the internal topics
                    for (final String topic : topics) {
                        // skip global topic as they don't need partition assignment
                        if (globalTopics.contains(topic)) {
                            continue;
                        }
                        if (this.internalTopicNames.contains(topic)) {
                            // prefix the internal topic name with the application id
                            final String internalTopic = decorateTopic(topic);
                            internalSourceTopics.put(internalTopic, new InternalTopicConfig(internalTopic,
                                                                                            Collections.singleton(InternalTopicConfig.CleanupPolicy.delete),
                                                                                            Collections.<String, String>emptyMap()));
                            sourceTopics.add(internalTopic);
                        } else {
                            sourceTopics.add(topic);
                        }
                    }
                }

                // if the node is a sink node, add to the sink topics
                final String topic = nodeToSinkTopic.get(node);
                if (topic != null) {
                    if (internalTopicNames.contains(topic)) {
                        // prefix the change log topic name with the application id
                        sinkTopics.add(decorateTopic(topic));
                    } else {
                        sinkTopics.add(topic);
                    }
                }

                // if the node is connected to a state, add to the state topics
                for (final StateStoreFactory stateFactory : stateFactories.values()) {
                    final StateStoreSupplier supplier = stateFactory.supplier;
                    if (supplier.loggingEnabled() && stateFactory.users.contains(node)) {
                        final String name = ProcessorStateManager.storeChangelogTopic(applicationId, supplier.name());
                        final InternalTopicConfig internalTopicConfig = createInternalTopicConfig(supplier, name);
                        stateChangelogTopics.put(name, internalTopicConfig);
                    }
                }
            }
            if (!sourceTopics.isEmpty()) {
                topicGroups.put(entry.getKey(), new TopologyBuilder.TopicsInfo(
                        Collections.unmodifiableSet(sinkTopics),
                        Collections.unmodifiableSet(sourceTopics),
                        Collections.unmodifiableMap(internalSourceTopics),
                        Collections.unmodifiableMap(stateChangelogTopics)));
            }
        }

        return Collections.unmodifiableMap(topicGroups);
    }

    private void setRegexMatchedTopicsToSourceNodes() {
        if (subscriptionUpdates.hasUpdates()) {
            for (final Map.Entry<String, Pattern> stringPatternEntry : nodeToSourcePatterns.entrySet()) {
                final SourceNodeFactory sourceNode = (SourceNodeFactory) nodeFactories.get(stringPatternEntry.getKey());
                //need to update nodeToSourceTopics with topics matched from given regex
                nodeToSourceTopics.put(stringPatternEntry.getKey(), sourceNode.getTopics(subscriptionUpdates.getUpdates()));
                log.debug("nodeToSourceTopics {}", nodeToSourceTopics);
            }
        }
    }

    private void setRegexMatchedTopicToStateStore() {
        if (subscriptionUpdates.hasUpdates()) {
            for (final Map.Entry<String, Set<Pattern>> storePattern : stateStoreNameToSourceRegex.entrySet()) {
                final Set<String> updatedTopicsForStateStore = new HashSet<>();
                for (final String subscriptionUpdateTopic : subscriptionUpdates.getUpdates()) {
                    for (final Pattern pattern : storePattern.getValue()) {
                        if (pattern.matcher(subscriptionUpdateTopic).matches()) {
                            updatedTopicsForStateStore.add(subscriptionUpdateTopic);
                        }
                    }
                }
                if (!updatedTopicsForStateStore.isEmpty()) {
                    final Collection<String> storeTopics = stateStoreNameToSourceTopics.get(storePattern.getKey());
                    if (storeTopics != null) {
                        updatedTopicsForStateStore.addAll(storeTopics);
                    }
                    stateStoreNameToSourceTopics.put(storePattern.getKey(), Collections.unmodifiableSet(updatedTopicsForStateStore));
                }
            }
        }
    }
    
    private InternalTopicConfig createInternalTopicConfig(final StateStoreSupplier<?> supplier,
                                                          final String name) {
        if (!(supplier instanceof WindowStoreSupplier)) {
            return new InternalTopicConfig(name, Collections.singleton(InternalTopicConfig.CleanupPolicy.compact), supplier.logConfig());
        }

        final WindowStoreSupplier windowStoreSupplier = (WindowStoreSupplier) supplier;
        final InternalTopicConfig config = new InternalTopicConfig(name,
                                                                   Utils.mkSet(InternalTopicConfig.CleanupPolicy.compact,
                                                                           InternalTopicConfig.CleanupPolicy.delete),
                                                                   supplier.logConfig());
        config.setRetentionMs(windowStoreSupplier.retentionPeriod());
        return config;
    }

    public synchronized Pattern earliestResetTopicsPattern() {
        final List<String> topics = maybeDecorateInternalSourceTopics(earliestResetTopics);
        final Pattern earliestPattern =  buildPatternForOffsetResetTopics(topics, earliestResetPatterns);

        ensureNoRegexOverlap(earliestPattern, latestResetPatterns, latestResetTopics);

        return earliestPattern;
    }

    public synchronized Pattern latestResetTopicsPattern() {
        final List<String> topics = maybeDecorateInternalSourceTopics(latestResetTopics);
        final Pattern latestPattern = buildPatternForOffsetResetTopics(topics, latestResetPatterns);

        ensureNoRegexOverlap(latestPattern, earliestResetPatterns, earliestResetTopics);

        return  latestPattern;
    }

    private void ensureNoRegexOverlap(final Pattern builtPattern,
                                      final Set<Pattern> otherPatterns,
                                      final Set<String> otherTopics) {
        for (final Pattern otherPattern : otherPatterns) {
            if (builtPattern.pattern().contains(otherPattern.pattern())) {
                throw new TopologyBuilderException(
                    String.format("Found overlapping regex [%s] against [%s] for a KStream with auto offset resets",
                        otherPattern.pattern(),
                        builtPattern.pattern()));
            }
        }

        for (final String otherTopic : otherTopics) {
            if (builtPattern.matcher(otherTopic).matches()) {
                throw new TopologyBuilderException(
                    String.format("Found overlapping regex [%s] matching topic [%s] for a KStream with auto offset resets",
                        builtPattern.pattern(),
                        otherTopic));
            }
        }
    }

    private static Pattern buildPatternForOffsetResetTopics(final Collection<String> sourceTopics,
                                                            final Collection<Pattern> sourcePatterns) {
        final StringBuilder builder = new StringBuilder();

        for (final String topic : sourceTopics) {
            builder.append(topic).append("|");
        }

        for (final Pattern sourcePattern : sourcePatterns) {
            builder.append(sourcePattern.pattern()).append("|");
        }

        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
            return Pattern.compile(builder.toString());
        }

        return EMPTY_ZERO_LENGTH_PATTERN;
    }

    public Map<String, List<String>> stateStoreNameToSourceTopics() {
        final Map<String, List<String>> results = new HashMap<>();
        for (final Map.Entry<String, Set<String>> entry : stateStoreNameToSourceTopics.entrySet()) {
            results.put(entry.getKey(), maybeDecorateInternalSourceTopics(entry.getValue()));
        }
        return results;
    }

    public synchronized Collection<Set<String>> copartitionGroups() {
        final List<Set<String>> list = new ArrayList<>(copartitionSourceGroups.size());
        for (final Set<String> nodeNames : copartitionSourceGroups) {
            final Set<String> copartitionGroup = new HashSet<>();
            for (final String node : nodeNames) {
                final List<String> topics = nodeToSourceTopics.get(node);
                if (topics != null) {
                    copartitionGroup.addAll(maybeDecorateInternalSourceTopics(topics));
                }
            }
            list.add(Collections.unmodifiableSet(copartitionGroup));
        }
        return Collections.unmodifiableList(list);
    }

    private List<String> maybeDecorateInternalSourceTopics(final Collection<String> sourceTopics) {
        final List<String> decoratedTopics = new ArrayList<>();
        for (final String topic : sourceTopics) {
            if (internalTopicNames.contains(topic)) {
                decoratedTopics.add(decorateTopic(topic));
            } else {
                decoratedTopics.add(topic);
            }
        }
        return decoratedTopics;
    }

    private String decorateTopic(final String topic) {
        if (applicationId == null) {
            throw new TopologyBuilderException("there are internal topics and "
                    + "applicationId hasn't been set. Call "
                    + "setApplicationId first");
        }

        return applicationId + "-" + topic;
    }

    public SubscriptionUpdates subscriptionUpdates() {
        return subscriptionUpdates;
    }

    public synchronized Pattern sourceTopicPattern() {
        if (topicPattern == null) {
            final List<String> allSourceTopics = new ArrayList<>();
            if (!nodeToSourceTopics.isEmpty()) {
                for (final List<String> topics : nodeToSourceTopics.values()) {
                    allSourceTopics.addAll(maybeDecorateInternalSourceTopics(topics));
                }
            }
            Collections.sort(allSourceTopics);

            topicPattern = buildPatternForOffsetResetTopics(allSourceTopics, nodeToSourcePatterns.values());
        }

        return topicPattern;
    }

    public synchronized void updateSubscriptions(final SubscriptionUpdates subscriptionUpdates,
                                                 final String threadId) {
        log.debug("stream-thread [{}] updating builder with {} topic(s) with possible matching regex subscription(s)",
            threadId, subscriptionUpdates);
        this.subscriptionUpdates = subscriptionUpdates;
        setRegexMatchedTopicsToSourceNodes();
        setRegexMatchedTopicToStateStore();
    }
}
