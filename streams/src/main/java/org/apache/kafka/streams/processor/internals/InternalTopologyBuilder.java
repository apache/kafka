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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.WindowStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;


public class InternalTopologyBuilder {

    private static final Logger log = LoggerFactory.getLogger(InternalTopologyBuilder.class);

    private static final Pattern EMPTY_ZERO_LENGTH_PATTERN = Pattern.compile("");

    private static final String[] NO_PREDECESSORS = {};

    // node factories in a topological order
    private final Map<String, NodeFactory> nodeFactories = new LinkedHashMap<>();

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
    private final Map<String, List<String>> nodeToSourceTopics = new HashMap<>();

    // map from source processor names to regex subscription patterns
    private final Map<String, Pattern> nodeToSourcePatterns = new LinkedHashMap<>();

    // map from sink processor names to subscribed topic (without application-id prefix for internal topics)
    private final Map<String, String> nodeToSinkTopic = new HashMap<>();

    // map from topics to their matched regex patterns, this is to ensure one topic is passed through on source node
    // even if it can be matched by multiple regex patterns
    private final Map<String, Pattern> topicToPatterns = new HashMap<>();

    // map from state store names to all the topics subscribed from source processors that
    // are connected to these state stores
    private final Map<String, Set<String>> stateStoreNameToSourceTopics = new HashMap<>();

    // map from state store names to all the regex subscribed topics from source processors that
    // are connected to these state stores
    private final Map<String, Set<Pattern>> stateStoreNameToSourceRegex = new HashMap<>();

    // map from state store names to this state store's corresponding changelog topic if possible
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

    // TODO: this is only temporary for 2.0 and should be removed
    public final Map<StoreBuilder, String> storeToSourceChangelogTopic = new HashMap<>();

    public interface StateStoreFactory {
        Set<String> users();
        boolean loggingEnabled();
        StateStore build();
        String name();
        boolean isWindowStore();
        Map<String, String> logConfig();
        long retentionPeriod();
    }

    private static abstract class AbstractStateStoreFactory implements StateStoreFactory {
        private final Set<String> users = new HashSet<>();
        private final String name;
        private final boolean loggingEnabled;
        private final boolean windowStore;
        private final Map<String, String> logConfig;

        AbstractStateStoreFactory(final String name,
                                  final boolean loggingEnabled,
                                  final boolean windowStore,
                                  final Map<String, String> logConfig) {
            this.name = name;
            this.loggingEnabled = loggingEnabled;
            this.windowStore = windowStore;
            this.logConfig = logConfig;
        }

        @Override
        public Set<String> users() {
            return users;
        }

        @Override
        public boolean loggingEnabled() {
            return loggingEnabled;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean isWindowStore() {
            return windowStore;
        }

        @Override
        public Map<String, String> logConfig() {
            return logConfig;
        }
    }

    private static class StoreBuilderFactory extends AbstractStateStoreFactory {
        private final StoreBuilder builder;

        StoreBuilderFactory(final StoreBuilder<?> builder) {
            super(builder.name(),
                  builder.loggingEnabled(),
                  builder instanceof WindowStoreBuilder,
                  builder.logConfig());
            this.builder = builder;
        }

        @Override
        public StateStore build() {
            return builder.build();
        }

        @Override
        public long retentionPeriod() {
            if (!isWindowStore()) {
                throw new IllegalStateException("retentionPeriod is not supported when not a window store");
            }
            return ((WindowStoreBuilder) builder).retentionPeriod();
        }
    }

    private static abstract class NodeFactory {
        final String name;
        final String[] predecessors;

        NodeFactory(final String name,
                    final String[] predecessors) {
            this.name = name;
            this.predecessors = predecessors;
        }

        public abstract ProcessorNode build();

        abstract AbstractNode describe();
    }

    private static class ProcessorNodeFactory extends NodeFactory {
        private final ProcessorSupplier<?, ?> supplier;
        private final Set<String> stateStoreNames = new HashSet<>();

        ProcessorNodeFactory(final String name,
                             final String[] predecessors,
                             final ProcessorSupplier<?, ?> supplier) {
            super(name, predecessors.clone());
            this.supplier = supplier;
        }

        public void addStateStore(final String stateStoreName) {
            stateStoreNames.add(stateStoreName);
        }

        @Override
        public ProcessorNode build() {
            return new ProcessorNode<>(name, supplier.get(), stateStoreNames);
        }

        @Override
        Processor describe() {
            return new Processor(name, new HashSet<>(stateStoreNames));
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
            super(name, NO_PREDECESSORS);
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
                return Collections.singletonList(String.valueOf(pattern));
            }

            final List<String> matchedTopics = new ArrayList<>();
            for (final String update : subscribedTopics) {
                if (pattern == topicToPatterns.get(update)) {
                    matchedTopics.add(update);
                } else if (topicToPatterns.containsKey(update) && isMatch(update)) {
                    // the same topic cannot be matched to more than one pattern
                    // TODO: we should lift this requirement in the future
                    throw new TopologyException("Topic " + update +
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
                return new SourceNode<>(name, Collections.singletonList(String.valueOf(pattern)), timestampExtractor, keyDeserializer, valDeserializer);
            } else {
                return new SourceNode<>(name, maybeDecorateInternalSourceTopics(sourceTopics), timestampExtractor, keyDeserializer, valDeserializer);
            }
        }

        private boolean isMatch(final String topic) {
            return pattern.matcher(topic).matches();
        }

        @Override
        Source describe() {
            String sourceTopics;

            if (pattern == null) {
                sourceTopics = topics.toString();
            } else {
                sourceTopics = pattern.toString();
            }

            return new Source(name, sourceTopics);
        }
    }

    private class SinkNodeFactory<K, V> extends NodeFactory {
        private final Serializer<K> keySerializer;
        private final Serializer<V> valSerializer;
        private final StreamPartitioner<? super K, ? super V> partitioner;
        private final TopicNameExtractor<K, V> topicExtractor;

        private SinkNodeFactory(final String name,
                                final String[] predecessors,
                                final TopicNameExtractor<K, V> topicExtractor,
                                final Serializer<K> keySerializer,
                                final Serializer<V> valSerializer,
                                final StreamPartitioner<? super K, ? super V> partitioner) {
            super(name, predecessors.clone());
            this.topicExtractor = topicExtractor;
            this.keySerializer = keySerializer;
            this.valSerializer = valSerializer;
            this.partitioner = partitioner;
        }

        @Override
        public ProcessorNode build() {
            if (topicExtractor instanceof StaticTopicNameExtractor) {
                final String topic = ((StaticTopicNameExtractor) topicExtractor).topicName;
                if (internalTopicNames.contains(topic)) {
                    // prefix the internal topic name with the application id
                    return new SinkNode<>(name, new StaticTopicNameExtractor<K, V>(decorateTopic(topic)), keySerializer, valSerializer, partitioner);
                } else {
                    return new SinkNode<>(name, topicExtractor, keySerializer, valSerializer, partitioner);
                }
            } else {
                return new SinkNode<>(name, topicExtractor, keySerializer, valSerializer, partitioner);
            }
        }

        @Override
        Sink describe() {
            return new Sink(name, topicExtractor);
        }
    }

    public synchronized final InternalTopologyBuilder setApplicationId(final String applicationId) {
        Objects.requireNonNull(applicationId, "applicationId can't be null");
        this.applicationId = applicationId;

        return this;
    }

    public final void addSource(final Topology.AutoOffsetReset offsetReset,
                                final String name,
                                final TimestampExtractor timestampExtractor,
                                final Deserializer keyDeserializer,
                                final Deserializer valDeserializer,
                                final String... topics) {
        if (topics.length == 0) {
            throw new TopologyException("You must provide at least one topic");
        }
        Objects.requireNonNull(name, "name must not be null");
        if (nodeFactories.containsKey(name)) {
            throw new TopologyException("Processor " + name + " is already added.");
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

    public final void addSource(final Topology.AutoOffsetReset offsetReset,
                                final String name,
                                final TimestampExtractor timestampExtractor,
                                final Deserializer keyDeserializer,
                                final Deserializer valDeserializer,
                                final Pattern topicPattern) {
        Objects.requireNonNull(topicPattern, "topicPattern can't be null");
        Objects.requireNonNull(name, "name can't be null");

        if (nodeFactories.containsKey(name)) {
            throw new TopologyException("Processor " + name + " is already added.");
        }

        for (final String sourceTopicName : sourceTopicNames) {
            if (topicPattern.matcher(sourceTopicName).matches()) {
                throw new TopologyException("Pattern " + topicPattern + " will match a topic that has already been registered by another source.");
            }
        }

        for (final Pattern otherPattern : earliestResetPatterns) {
            if (topicPattern.pattern().contains(otherPattern.pattern()) || otherPattern.pattern().contains(topicPattern.pattern())) {
                throw new TopologyException("Pattern " + topicPattern + " will overlap with another pattern " + otherPattern + " already been registered by another source");
            }
        }

        for (final Pattern otherPattern : latestResetPatterns) {
            if (topicPattern.pattern().contains(otherPattern.pattern()) || otherPattern.pattern().contains(topicPattern.pattern())) {
                throw new TopologyException("Pattern " + topicPattern + " will overlap with another pattern " + otherPattern + " already been registered by another source");
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
                                     final String... predecessorNames) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(predecessorNames, "predecessor names must not be null");
        if (predecessorNames.length == 0) {
            throw new TopologyException("Sink " + name + " must have at least one parent");
        }

        addSink(name, new StaticTopicNameExtractor<K, V>(topic), keySerializer, valSerializer, partitioner, predecessorNames);
        nodeToSinkTopic.put(name, topic);
    }

    public final <K, V> void addSink(final String name,
                                     final TopicNameExtractor<K, V> topicExtractor,
                                     final Serializer<K> keySerializer,
                                     final Serializer<V> valSerializer,
                                     final StreamPartitioner<? super K, ? super V> partitioner,
                                     final String... predecessorNames) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(topicExtractor, "topic extractor must not be null");
        Objects.requireNonNull(predecessorNames, "predecessor names must not be null");
        if (nodeFactories.containsKey(name)) {
            throw new TopologyException("Processor " + name + " is already added.");
        }
        if (predecessorNames.length == 0) {
            throw new TopologyException("Sink " + name + " must have at least one parent");
        }

        for (final String predecessor : predecessorNames) {
            Objects.requireNonNull(predecessor, "predecessor name can't be null");
            if (predecessor.equals(name)) {
                throw new TopologyException("Processor " + name + " cannot be a predecessor of itself.");
            }
            if (!nodeFactories.containsKey(predecessor)) {
                throw new TopologyException("Predecessor processor " + predecessor + " is not added yet.");
            }
            if (nodeToSinkTopic.containsKey(predecessor)) {
                throw new TopologyException("Sink " + predecessor + " cannot be used a parent.");
            }
        }

        nodeFactories.put(name, new SinkNodeFactory<>(name, predecessorNames, topicExtractor, keySerializer, valSerializer, partitioner));
        nodeGrouper.add(name);
        nodeGrouper.unite(name, predecessorNames);
    }

    public final void addProcessor(final String name,
                                   final ProcessorSupplier supplier,
                                   final String... predecessorNames) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        Objects.requireNonNull(predecessorNames, "predecessor names must not be null");
        if (nodeFactories.containsKey(name)) {
            throw new TopologyException("Processor " + name + " is already added.");
        }
        if (predecessorNames.length == 0) {
            throw new TopologyException("Processor " + name + " must have at least one parent");
        }

        for (final String predecessor : predecessorNames) {
            Objects.requireNonNull(predecessor, "predecessor name must not be null");
            if (predecessor.equals(name)) {
                throw new TopologyException("Processor " + name + " cannot be a predecessor of itself.");
            }
            if (!nodeFactories.containsKey(predecessor)) {
                throw new TopologyException("Predecessor processor " + predecessor + " is not added yet.");
            }
        }

        nodeFactories.put(name, new ProcessorNodeFactory(name, predecessorNames, supplier));
        nodeGrouper.add(name);
        nodeGrouper.unite(name, predecessorNames);
    }

    public final void addStateStore(final StoreBuilder storeBuilder,
                                    final String... processorNames) {
        addStateStore(storeBuilder, false, processorNames);
    }

    public final void addStateStore(final StoreBuilder storeBuilder,
                                    final boolean allowOverride,
                                    final String... processorNames) {
        Objects.requireNonNull(storeBuilder, "storeBuilder can't be null");
        if (!allowOverride && stateFactories.containsKey(storeBuilder.name())) {
            throw new TopologyException("StateStore " + storeBuilder.name() + " is already added.");
        }

        stateFactories.put(storeBuilder.name(), new StoreBuilderFactory(storeBuilder));

        if (processorNames != null) {
            for (final String processorName : processorNames) {
                Objects.requireNonNull(processorName, "processor name must not be null");
                connectProcessorAndStateStore(processorName, storeBuilder.name());
            }
        }
    }

    public final void addGlobalStore(final StoreBuilder<KeyValueStore> storeBuilder,
                                     final String sourceName,
                                     final TimestampExtractor timestampExtractor,
                                     final Deserializer keyDeserializer,
                                     final Deserializer valueDeserializer,
                                     final String topic,
                                     final String processorName,
                                     final ProcessorSupplier stateUpdateSupplier) {
        Objects.requireNonNull(storeBuilder, "store builder must not be null");
        validateGlobalStoreArguments(sourceName,
                                     topic,
                                     processorName,
                                     stateUpdateSupplier,
                                     storeBuilder.name(),
                                     storeBuilder.loggingEnabled());
        validateTopicNotAlreadyRegistered(topic);

        addGlobalStore(sourceName,
                       timestampExtractor,
                       keyDeserializer,
                       valueDeserializer,
                       topic,
                       processorName,
                       stateUpdateSupplier,
                       storeBuilder.name(),
                       storeBuilder.build());
    }

    private void validateTopicNotAlreadyRegistered(final String topic) {
        if (sourceTopicNames.contains(topic) || globalTopics.contains(topic)) {
            throw new TopologyException("Topic " + topic + " has already been registered by another source.");
        }

        for (final Pattern pattern : nodeToSourcePatterns.values()) {
            if (pattern.matcher(topic).matches()) {
                throw new TopologyException("Topic " + topic + " matches a Pattern already registered by another source.");
            }
        }
    }

    public final void connectProcessorAndStateStores(final String processorName,
                                                     final String... stateStoreNames) {
        Objects.requireNonNull(processorName, "processorName can't be null");
        Objects.requireNonNull(stateStoreNames, "state store list must not be null");
        if (stateStoreNames.length == 0) {
            throw new TopologyException("Must provide at least one state store name.");
        }
        for (final String stateStoreName : stateStoreNames) {
            Objects.requireNonNull(stateStoreName, "state store name must not be null");
            connectProcessorAndStateStore(processorName, stateStoreName);
        }
    }

    public final void connectSourceStoreAndTopic(final String sourceStoreName,
                                                 final String topic) {
        if (storeToChangelogTopic.containsKey(sourceStoreName)) {
            throw new TopologyException("Source store " + sourceStoreName + " is already added.");
        }
        storeToChangelogTopic.put(sourceStoreName, topic);
    }

    public final void markSourceStoreAndTopic(final StoreBuilder storeBuilder,
                                              final String topic) {
        if (storeToSourceChangelogTopic.containsKey(storeBuilder)) {
            throw new TopologyException("Source store " + storeBuilder.name() + " is already used.");
        }
        storeToSourceChangelogTopic.put(storeBuilder, topic);
    }

    public final void addInternalTopic(final String topicName) {
        Objects.requireNonNull(topicName, "topicName can't be null");
        internalTopicNames.add(topicName);
    }

    public final void copartitionSources(final Collection<String> sourceNodes) {
        copartitionSourceGroups.add(Collections.unmodifiableSet(new HashSet<>(sourceNodes)));
    }

    private void validateGlobalStoreArguments(final String sourceName,
                                              final String topic,
                                              final String processorName,
                                              final ProcessorSupplier stateUpdateSupplier,
                                              final String storeName,
                                              final boolean loggingEnabled) {
        Objects.requireNonNull(sourceName, "sourceName must not be null");
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(stateUpdateSupplier, "supplier must not be null");
        Objects.requireNonNull(processorName, "processorName must not be null");
        if (nodeFactories.containsKey(sourceName)) {
            throw new TopologyException("Processor " + sourceName + " is already added.");
        }
        if (nodeFactories.containsKey(processorName)) {
            throw new TopologyException("Processor " + processorName + " is already added.");
        }
        if (stateFactories.containsKey(storeName) || globalStateStores.containsKey(storeName)) {
            throw new TopologyException("StateStore " + storeName + " is already added.");
        }
        if (loggingEnabled) {
            throw new TopologyException("StateStore " + storeName + " for global table must not have logging enabled.");
        }
        if (sourceName.equals(processorName)) {
            throw new TopologyException("sourceName and processorName must be different.");
        }
    }

    private void addGlobalStore(final String sourceName,
                                final TimestampExtractor timestampExtractor,
                                final Deserializer keyDeserializer,
                                final Deserializer valueDeserializer,
                                final String topic,
                                final String processorName,
                                final ProcessorSupplier stateUpdateSupplier,
                                final String name,
                                final KeyValueStore store) {
        final String[] topics = {topic};
        final String[] predecessors = {sourceName};
        final ProcessorNodeFactory nodeFactory = new ProcessorNodeFactory(processorName,
                                                                          predecessors,
                                                                          stateUpdateSupplier);
        globalTopics.add(topic);
        nodeFactories.put(sourceName, new SourceNodeFactory(sourceName,
                                                            topics,
                                                            null,
                                                            timestampExtractor,
                                                            keyDeserializer,
                                                            valueDeserializer));
        nodeToSourceTopics.put(sourceName, Arrays.asList(topics));
        nodeGrouper.add(sourceName);
        nodeFactory.addStateStore(name);
        nodeFactories.put(processorName, nodeFactory);
        nodeGrouper.add(processorName);
        nodeGrouper.unite(processorName, predecessors);
        globalStateStores.put(name, store);
        connectSourceStoreAndTopic(name, topic);
    }

    private void connectProcessorAndStateStore(final String processorName,
                                               final String stateStoreName) {
        if (globalStateStores.containsKey(stateStoreName)) {
            throw new TopologyException("Global StateStore " + stateStoreName +
                    " can be used by a Processor without being specified; it should not be explicitly passed.");
        }
        if (!stateFactories.containsKey(stateStoreName)) {
            throw new TopologyException("StateStore " + stateStoreName + " is not added yet.");
        }
        if (!nodeFactories.containsKey(processorName)) {
            throw new TopologyException("Processor " + processorName + " is not added yet.");
        }

        final StateStoreFactory stateStoreFactory = stateFactories.get(stateStoreName);
        final Iterator<String> iter = stateStoreFactory.users().iterator();
        if (iter.hasNext()) {
            final String user = iter.next();
            nodeGrouper.unite(user, processorName);
        }
        stateStoreFactory.users().add(processorName);

        final NodeFactory nodeFactory = nodeFactories.get(processorName);
        if (nodeFactory instanceof ProcessorNodeFactory) {
            final ProcessorNodeFactory processorNodeFactory = (ProcessorNodeFactory) nodeFactory;
            processorNodeFactory.addStateStore(stateStoreName);
            connectStateStoreNameToSourceTopicsOrPattern(stateStoreName, processorNodeFactory);
        } else {
            throw new TopologyException("cannot connect a state store " + stateStoreName + " to a source node or a sink node.");
        }
    }

    private Set<SourceNodeFactory> findSourcesForProcessorPredecessors(final String[] predecessors) {
        final Set<SourceNodeFactory> sourceNodes = new HashSet<>();
        for (final String predecessor : predecessors) {
            final NodeFactory nodeFactory = nodeFactories.get(predecessor);
            if (nodeFactory instanceof SourceNodeFactory) {
                sourceNodes.add((SourceNodeFactory) nodeFactory);
            } else if (nodeFactory instanceof ProcessorNodeFactory) {
                sourceNodes.addAll(findSourcesForProcessorPredecessors(((ProcessorNodeFactory) nodeFactory).predecessors));
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
        final Set<SourceNodeFactory> sourceNodesForPredecessor = findSourcesForProcessorPredecessors(processorNodeFactory.predecessors);

        for (final SourceNodeFactory sourceNodeFactory : sourceNodesForPredecessor) {
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
                                         final Topology.AutoOffsetReset offsetReset,
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
                    throw new TopologyException(String.format("Unrecognized reset format %s", offsetReset));
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
        final Map<Integer, Set<String>> nodeGroups = new LinkedHashMap<>();
        final Map<String, Set<String>> rootToNodeGroup = new HashMap<>();

        int nodeGroupId = 0;

        // Go through source nodes first. This makes the group id assignment easy to predict in tests
        final Set<String> allSourceNodes = new HashSet<>(nodeToSourceTopics.keySet());
        allSourceNodes.addAll(nodeToSourcePatterns.keySet());

        for (final String nodeName : Utils.sorted(allSourceNodes)) {
            nodeGroupId = putNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
        }

        // Go through non-source nodes
        for (final String nodeName : Utils.sorted(nodeFactories.keySet())) {
            if (!nodeToSourceTopics.containsKey(nodeName)) {
                nodeGroupId = putNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
            }
        }

        return nodeGroups;
    }

    private int putNodeGroupName(final String nodeName,
                                 final int nodeGroupId,
                                 final Map<Integer, Set<String>> nodeGroups,
                                 final Map<String, Set<String>> rootToNodeGroup) {
        int newNodeGroupId = nodeGroupId;
        final String root = nodeGrouper.root(nodeName);
        Set<String> nodeGroup = rootToNodeGroup.get(root);
        if (nodeGroup == null) {
            nodeGroup = new HashSet<>();
            rootToNodeGroup.put(root, nodeGroup);
            nodeGroups.put(newNodeGroupId++, nodeGroup);
        }
        nodeGroup.add(nodeName);
        return newNodeGroupId;
    }

    public synchronized ProcessorTopology build() {
        return build((Integer) null);
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
                if (isGlobalSource(node)) {
                    globalGroups.addAll(nodes);
                }
            }
        }
        return globalGroups;
    }

    private ProcessorTopology build(final Set<String> nodeGroup) {
        final Map<String, ProcessorNode> processorMap = new LinkedHashMap<>();
        final Map<String, SourceNode> topicSourceMap = new HashMap<>();
        final Map<String, SinkNode> topicSinkMap = new HashMap<>();
        final Map<String, StateStore> stateStoreMap = new LinkedHashMap<>();
        final Set<String> repartitionTopics = new HashSet<>();

        // create processor nodes in a topological order ("nodeFactories" is already topologically sorted)
        // also make sure the state store map values following the insertion ordering
        for (final NodeFactory factory : nodeFactories.values()) {
            if (nodeGroup == null || nodeGroup.contains(factory.name)) {
                final ProcessorNode node = factory.build();
                processorMap.put(node.name(), node);

                if (factory instanceof ProcessorNodeFactory) {
                    buildProcessorNode(processorMap,
                                       stateStoreMap,
                                       (ProcessorNodeFactory) factory,
                                       node);

                } else if (factory instanceof SourceNodeFactory) {
                    buildSourceNode(topicSourceMap,
                                    repartitionTopics,
                                    (SourceNodeFactory) factory,
                                    (SourceNode) node);

                } else if (factory instanceof SinkNodeFactory) {
                    buildSinkNode(processorMap,
                                  topicSinkMap,
                                  repartitionTopics,
                                  (SinkNodeFactory) factory,
                                  (SinkNode) node);
                } else {
                    throw new TopologyException("Unknown definition class: " + factory.getClass().getName());
                }
            }
        }

        return new ProcessorTopology(new ArrayList<>(processorMap.values()),
                                     topicSourceMap,
                                     topicSinkMap,
                                     new ArrayList<>(stateStoreMap.values()),
                                     new ArrayList<>(globalStateStores.values()),
                                     storeToChangelogTopic,
                                     repartitionTopics);
    }

    @SuppressWarnings("unchecked")
    private void buildSinkNode(final Map<String, ProcessorNode> processorMap,
                               final Map<String, SinkNode> topicSinkMap,
                               final Set<String> repartitionTopics,
                               final SinkNodeFactory sinkNodeFactory,
                               final SinkNode node) {

        for (final String predecessor : sinkNodeFactory.predecessors) {
            processorMap.get(predecessor).addChild(node);
            if (sinkNodeFactory.topicExtractor instanceof StaticTopicNameExtractor) {
                final String topic = ((StaticTopicNameExtractor) sinkNodeFactory.topicExtractor).topicName;

                if (internalTopicNames.contains(topic)) {
                    // prefix the internal topic name with the application id
                    final String decoratedTopic = decorateTopic(topic);
                    topicSinkMap.put(decoratedTopic, node);
                    repartitionTopics.add(decoratedTopic);
                } else {
                    topicSinkMap.put(topic, node);
                }

            }
        }
    }

    private void buildSourceNode(final Map<String, SourceNode> topicSourceMap,
                                 final Set<String> repartitionTopics,
                                 final SourceNodeFactory sourceNodeFactory,
                                 final SourceNode node) {

        final List<String> topics = (sourceNodeFactory.pattern != null) ?
                                    sourceNodeFactory.getTopics(subscriptionUpdates.getUpdates()) :
                                    sourceNodeFactory.topics;

        for (final String topic : topics) {
            if (internalTopicNames.contains(topic)) {
                // prefix the internal topic name with the application id
                final String decoratedTopic = decorateTopic(topic);
                topicSourceMap.put(decoratedTopic, node);
                repartitionTopics.add(decoratedTopic);
            } else {
                topicSourceMap.put(topic, node);
            }
        }
    }

    private void buildProcessorNode(final Map<String, ProcessorNode> processorMap,
                                    final Map<String, StateStore> stateStoreMap,
                                    final ProcessorNodeFactory factory,
                                    final ProcessorNode node) {

        for (final String predecessor : factory.predecessors) {
            final ProcessorNode<?, ?> predecessorNode = processorMap.get(predecessor);
            predecessorNode.addChild(node);
        }
        for (final String stateStoreName : factory.stateStoreNames) {
            if (!stateStoreMap.containsKey(stateStoreName)) {
                if (stateFactories.containsKey(stateStoreName)) {
                    final StateStoreFactory stateStoreFactory = stateFactories.get(stateStoreName);

                    // remember the changelog topic if this state store is change-logging enabled
                    if (stateStoreFactory.loggingEnabled() && !storeToChangelogTopic.containsKey(stateStoreName)) {
                        final String changelogTopic = ProcessorStateManager.storeChangelogTopic(applicationId, stateStoreName);
                        storeToChangelogTopic.put(stateStoreName, changelogTopic);
                    }
                    stateStoreMap.put(stateStoreName, stateStoreFactory.build());
                } else {
                    stateStoreMap.put(stateStoreName, globalStateStores.get(stateStoreName));
                }
            }
        }
    }

    /**
     * Get any global {@link StateStore}s that are part of the
     * topology
     * @return map containing all global {@link StateStore}s
     */
    public Map<String, StateStore> globalStateStores() {
        return Collections.unmodifiableMap(globalStateStores);
    }

    public Set<String> allStateStoreName() {
        final Set<String> allNames = new HashSet<>(stateFactories.keySet());
        allNames.addAll(globalStateStores.keySet());
        return Collections.unmodifiableSet(allNames);
    }

    /**
     * Returns the map of topic groups keyed by the group id.
     * A topic group is a group of topics in the same task.
     *
     * @return groups of topic names
     */
    public synchronized Map<Integer, TopicsInfo> topicGroups() {
        final Map<Integer, TopicsInfo> topicGroups = new LinkedHashMap<>();

        if (nodeGroups == null) {
            nodeGroups = makeNodeGroups();
        }

        for (final Map.Entry<Integer, Set<String>> entry : nodeGroups.entrySet()) {
            final Set<String> sinkTopics = new HashSet<>();
            final Set<String> sourceTopics = new HashSet<>();
            final Map<String, InternalTopicConfig> repartitionTopics = new HashMap<>();
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
                        if (internalTopicNames.contains(topic)) {
                            // prefix the internal topic name with the application id
                            final String internalTopic = decorateTopic(topic);
                            repartitionTopics.put(internalTopic, new RepartitionTopicConfig(internalTopic, Collections.<String, String>emptyMap()));
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

                // if the node is connected to a state store whose changelog topics are not predefined, add to the changelog topics
                for (final StateStoreFactory stateFactory : stateFactories.values()) {
                    if (stateFactory.loggingEnabled() && stateFactory.users().contains(node)) {
                        final String topicName = storeToChangelogTopic.containsKey(stateFactory.name()) ?
                                storeToChangelogTopic.get(stateFactory.name()) :
                                ProcessorStateManager.storeChangelogTopic(applicationId, stateFactory.name());
                        if (!stateChangelogTopics.containsKey(topicName)) {
                            final InternalTopicConfig internalTopicConfig = createChangelogTopicConfig(stateFactory, topicName);
                            stateChangelogTopics.put(topicName, internalTopicConfig);
                        }
                    }
                }
            }
            if (!sourceTopics.isEmpty()) {
                topicGroups.put(entry.getKey(), new TopicsInfo(
                        Collections.unmodifiableSet(sinkTopics),
                        Collections.unmodifiableSet(sourceTopics),
                        Collections.unmodifiableMap(repartitionTopics),
                        Collections.unmodifiableMap(stateChangelogTopics)));
            }
        }

        return Collections.unmodifiableMap(topicGroups);
    }

    // Adjust the generated topology based on the configs.
    // Not exposed as public API and should be removed post 2.0
    public void adjust(final StreamsConfig config) {
        final boolean enableOptimization20 = config.getString(StreamsConfig.TOPOLOGY_OPTIMIZATION).equals(StreamsConfig.OPTIMIZE);

        if (enableOptimization20) {
            for (final Map.Entry<StoreBuilder, String> entry : storeToSourceChangelogTopic.entrySet()) {
                final StoreBuilder storeBuilder = entry.getKey();
                final String topicName = entry.getValue();

                // update store map to disable logging for this store
                storeBuilder.withLoggingDisabled();
                addStateStore(storeBuilder, true);
                connectSourceStoreAndTopic(storeBuilder.name(), topicName);
            }
        }
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

    private InternalTopicConfig createChangelogTopicConfig(final StateStoreFactory factory,
                                                           final String name) {
        if (!factory.isWindowStore()) {
            return new UnwindowedChangelogTopicConfig(name, factory.logConfig());
        } else {
            final WindowedChangelogTopicConfig config = new WindowedChangelogTopicConfig(name, factory.logConfig());
            config.setRetentionMs(factory.retentionPeriod());
            return config;
        }
    }

    public synchronized Pattern earliestResetTopicsPattern() {
        return resetTopicsPattern(earliestResetTopics, earliestResetPatterns);
    }

    public synchronized Pattern latestResetTopicsPattern() {
        return resetTopicsPattern(latestResetTopics, latestResetPatterns);
    }

    private Pattern resetTopicsPattern(final Set<String> resetTopics,
                                       final Set<Pattern> resetPatterns) {
        final List<String> topics = maybeDecorateInternalSourceTopics(resetTopics);

        return buildPatternForOffsetResetTopics(topics, resetPatterns);
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
            throw new TopologyException("there are internal topics and "
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

    // package-private for testing only
    synchronized void updateSubscriptions(final SubscriptionUpdates subscriptionUpdates,
                                          final String logPrefix) {
        log.debug("{}updating builder with {} topic(s) with possible matching regex subscription(s)",
                logPrefix, subscriptionUpdates);
        this.subscriptionUpdates = subscriptionUpdates;
        setRegexMatchedTopicsToSourceNodes();
        setRegexMatchedTopicToStateStore();
    }

    private boolean isGlobalSource(final String nodeName) {
        final NodeFactory nodeFactory = nodeFactories.get(nodeName);

        if (nodeFactory instanceof SourceNodeFactory) {
            final List<String> topics = ((SourceNodeFactory) nodeFactory).topics;
            return topics != null && topics.size() == 1 && globalTopics.contains(topics.get(0));
        }
        return false;
    }

    public TopologyDescription describe() {
        final TopologyDescription description = new TopologyDescription();

        for (final Map.Entry<Integer, Set<String>> nodeGroup : makeNodeGroups().entrySet()) {

            final Set<String> allNodesOfGroups = nodeGroup.getValue();
            final boolean isNodeGroupOfGlobalStores = nodeGroupContainsGlobalSourceNode(allNodesOfGroups);

            if (!isNodeGroupOfGlobalStores) {
                describeSubtopology(description, nodeGroup.getKey(), allNodesOfGroups);
            } else {
                describeGlobalStore(description, allNodesOfGroups, nodeGroup.getKey());
            }
        }

        return description;
    }

    private void describeGlobalStore(final TopologyDescription description, final Set<String> nodes, int id) {
        final Iterator<String> it = nodes.iterator();
        while (it.hasNext()) {
            final String node = it.next();

            if (isGlobalSource(node)) {
                // we found a GlobalStore node group; those contain exactly two node: {sourceNode,processorNode}
                it.remove(); // remove sourceNode from group
                final String processorNode = nodes.iterator().next(); // get remaining processorNode

                description.addGlobalStore(new GlobalStore(
                    node,
                    processorNode,
                    ((ProcessorNodeFactory) nodeFactories.get(processorNode)).stateStoreNames.iterator().next(),
                    nodeToSourceTopics.get(node).get(0),
                    id
                ));
                break;
            }
        }
    }

    private boolean nodeGroupContainsGlobalSourceNode(final Set<String> allNodesOfGroups) {
        for (final String node : allNodesOfGroups) {
            if (isGlobalSource(node)) {
                return true;
            }
        }
        return false;
    }

    private static class NodeComparator implements Comparator<TopologyDescription.Node>, Serializable {

        @Override
        public int compare(final TopologyDescription.Node node1,
                           final TopologyDescription.Node node2) {
            final int size1 = ((AbstractNode) node1).size;
            final int size2 = ((AbstractNode) node2).size;

            // it is possible that two nodes have the same sub-tree size (think two nodes connected via state stores)
            // in this case default to processor name string
            if (size1 != size2) {
                return size2 - size1;
            } else {
                return node1.name().compareTo(node2.name());
            }
        }
    }

    private final static NodeComparator NODE_COMPARATOR = new NodeComparator();

    private static void updateSize(final AbstractNode node, final int delta) {
        node.size += delta;

        for (final TopologyDescription.Node predecessor : node.predecessors()) {
            updateSize((AbstractNode) predecessor, delta);
        }
    }

    private void describeSubtopology(final TopologyDescription description,
                                     final Integer subtopologyId,
                                     final Set<String> nodeNames) {

        final Map<String, AbstractNode> nodesByName = new HashMap<>();

        // add all nodes
        for (final String nodeName : nodeNames) {
            nodesByName.put(nodeName, nodeFactories.get(nodeName).describe());
        }

        // connect each node to its predecessors and successors
        for (final AbstractNode node : nodesByName.values()) {
            for (final String predecessorName : nodeFactories.get(node.name()).predecessors) {
                final AbstractNode predecessor = nodesByName.get(predecessorName);
                node.addPredecessor(predecessor);
                predecessor.addSuccessor(node);
                updateSize(predecessor, node.size);
            }
        }

        description.addSubtopology(new Subtopology(
                subtopologyId,
                new HashSet<TopologyDescription.Node>(nodesByName.values())));
    }

    public final static class GlobalStore implements TopologyDescription.GlobalStore {
        private final Source source;
        private final Processor processor;
        private final int id;

        public GlobalStore(final String sourceName,
                           final String processorName,
                           final String storeName,
                           final String topicName,
                           final int id) {
            source = new Source(sourceName, topicName);
            processor = new Processor(processorName, Collections.singleton(storeName));
            source.successors.add(processor);
            processor.predecessors.add(source);
            this.id = id;
        }

        @Override
        public int id() {
            return id;
        }

        @Override
        public TopologyDescription.Source source() {
            return source;
        }

        @Override
        public TopologyDescription.Processor processor() {
            return processor;
        }

        @Override
        public String toString() {
            return "Sub-topology: " + id + " for global store (will not generate tasks)\n"
                    + "    " + source.toString() + "\n"
                    + "    " + processor.toString() + "\n";
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final GlobalStore that = (GlobalStore) o;
            return source.equals(that.source)
                && processor.equals(that.processor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(source, processor);
        }
    }

    public abstract static class AbstractNode implements TopologyDescription.Node {
        final String name;
        final Set<TopologyDescription.Node> predecessors = new TreeSet<>(NODE_COMPARATOR);
        final Set<TopologyDescription.Node> successors = new TreeSet<>(NODE_COMPARATOR);

        // size of the sub-topology rooted at this node, including the node itself
        int size;

        AbstractNode(final String name) {
            this.name = name;
            this.size = 1;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Set<TopologyDescription.Node> predecessors() {
            return Collections.unmodifiableSet(predecessors);
        }

        @Override
        public Set<TopologyDescription.Node> successors() {
            return Collections.unmodifiableSet(successors);
        }

        public void addPredecessor(final TopologyDescription.Node predecessor) {
            predecessors.add(predecessor);
        }

        public void addSuccessor(final TopologyDescription.Node successor) {
            successors.add(successor);
        }
    }

    public final static class Source extends AbstractNode implements TopologyDescription.Source {
        private final String topics;

        public Source(final String name,
                      final String topics) {
            super(name);
            this.topics = topics;
        }

        @Override
        public String topics() {
            return topics;
        }

        @Override
        public void addPredecessor(final TopologyDescription.Node predecessor) {
            throw new UnsupportedOperationException("Sources don't have predecessors.");
        }

        @Override
        public String toString() {
            return "Source: " + name + " (topics: " + topics + ")\n      --> " + nodeNames(successors);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Source source = (Source) o;
            // omit successor to avoid infinite loops
            return name.equals(source.name)
                && topics.equals(source.topics);
        }

        @Override
        public int hashCode() {
            // omit successor as it might change and alter the hash code
            return Objects.hash(name, topics);
        }
    }

    public final static class Processor extends AbstractNode implements TopologyDescription.Processor {
        private final Set<String> stores;

        public Processor(final String name,
                         final Set<String> stores) {
            super(name);
            this.stores = stores;
        }

        @Override
        public Set<String> stores() {
            return Collections.unmodifiableSet(stores);
        }

        @Override
        public String toString() {
            return "Processor: " + name + " (stores: " + stores + ")\n      --> " + nodeNames(successors) + "\n      <-- " + nodeNames(predecessors);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Processor processor = (Processor) o;
            // omit successor to avoid infinite loops
            return name.equals(processor.name)
                && stores.equals(processor.stores)
                && predecessors.equals(processor.predecessors);
        }

        @Override
        public int hashCode() {
            // omit successor as it might change and alter the hash code
            return Objects.hash(name, stores);
        }
    }

    public final static class Sink extends AbstractNode implements TopologyDescription.Sink {
        private final TopicNameExtractor topicNameExtractor;

        public Sink(final String name,
                    final TopicNameExtractor topicNameExtractor) {
            super(name);
            this.topicNameExtractor = topicNameExtractor;
        }

        public Sink(final String name,
                    final String topic) {
            super(name);
            this.topicNameExtractor = new StaticTopicNameExtractor(topic);
        }

        @Override
        public String topic() {
            if (topicNameExtractor instanceof StaticTopicNameExtractor)
                return ((StaticTopicNameExtractor) topicNameExtractor).topicName;
            else
                return null;
        }

        @Override
        public void addSuccessor(final TopologyDescription.Node successor) {
            throw new UnsupportedOperationException("Sinks don't have successors.");
        }

        @Override
        public String toString() {
            return "Sink: " + name + " (topic: " + topic() + ")\n      <-- " + nodeNames(predecessors);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Sink sink = (Sink) o;
            return name.equals(sink.name)
                && topicNameExtractor.equals(sink.topicNameExtractor)
                && predecessors.equals(sink.predecessors);
        }

        @Override
        public int hashCode() {
            // omit predecessors as it might change and alter the hash code
            return Objects.hash(name, topicNameExtractor);
        }
    }

    public final static class Subtopology implements org.apache.kafka.streams.TopologyDescription.Subtopology {
        private final int id;
        private final Set<TopologyDescription.Node> nodes;

        public Subtopology(final int id, final Set<TopologyDescription.Node> nodes) {
            this.id = id;
            this.nodes = new TreeSet<>(NODE_COMPARATOR);
            this.nodes.addAll(nodes);
        }

        @Override
        public int id() {
            return id;
        }

        @Override
        public Set<TopologyDescription.Node> nodes() {
            return Collections.unmodifiableSet(nodes);
        }

        // visible for testing
        Iterator<TopologyDescription.Node> nodesInOrder() {
            return nodes.iterator();
        }

        @Override
        public String toString() {
            return "Sub-topology: " + id + "\n" + nodesAsString() + "\n";
        }

        private String nodesAsString() {
            final StringBuilder sb = new StringBuilder();
            for (final TopologyDescription.Node node : nodes) {
                sb.append("    ");
                sb.append(node);
                sb.append('\n');
            }
            return sb.toString();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Subtopology that = (Subtopology) o;
            return id == that.id
                && nodes.equals(that.nodes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, nodes);
        }
    }

    public static class TopicsInfo {
        public Set<String> sinkTopics;
        public Set<String> sourceTopics;
        public Map<String, InternalTopicConfig> stateChangelogTopics;
        public Map<String, InternalTopicConfig> repartitionSourceTopics;

        TopicsInfo(final Set<String> sinkTopics,
                   final Set<String> sourceTopics,
                   final Map<String, InternalTopicConfig> repartitionSourceTopics,
                   final Map<String, InternalTopicConfig> stateChangelogTopics) {
            this.sinkTopics = sinkTopics;
            this.sourceTopics = sourceTopics;
            this.stateChangelogTopics = stateChangelogTopics;
            this.repartitionSourceTopics = repartitionSourceTopics;
        }

        @Override
        public boolean equals(final Object o) {
            if (o instanceof TopicsInfo) {
                final TopicsInfo other = (TopicsInfo) o;
                return other.sourceTopics.equals(sourceTopics) && other.stateChangelogTopics.equals(stateChangelogTopics);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            final long n = ((long) sourceTopics.hashCode() << 32) | (long) stateChangelogTopics.hashCode();
            return (int) (n % 0xFFFFFFFFL);
        }

        @Override
        public String toString() {
            return "TopicsInfo{" +
                "sinkTopics=" + sinkTopics +
                ", sourceTopics=" + sourceTopics +
                ", repartitionSourceTopics=" + repartitionSourceTopics +
                ", stateChangelogTopics=" + stateChangelogTopics +
                '}';
        }
    }

    private static class GlobalStoreComparator implements Comparator<TopologyDescription.GlobalStore>, Serializable {
        @Override
        public int compare(final TopologyDescription.GlobalStore globalStore1,
                           final TopologyDescription.GlobalStore globalStore2) {
            return globalStore1.id() - globalStore2.id();
        }
    }

    private final static GlobalStoreComparator GLOBALSTORE_COMPARATOR = new GlobalStoreComparator();

    private static class SubtopologyComparator implements Comparator<TopologyDescription.Subtopology>, Serializable {
        @Override
        public int compare(final TopologyDescription.Subtopology subtopology1,
                           final TopologyDescription.Subtopology subtopology2) {
            return subtopology1.id() - subtopology2.id();
        }
    }

    private final static SubtopologyComparator SUBTOPOLOGY_COMPARATOR = new SubtopologyComparator();

    public final static class TopologyDescription implements org.apache.kafka.streams.TopologyDescription {
        private final TreeSet<TopologyDescription.Subtopology> subtopologies = new TreeSet<>(SUBTOPOLOGY_COMPARATOR);
        private final TreeSet<TopologyDescription.GlobalStore> globalStores = new TreeSet<>(GLOBALSTORE_COMPARATOR);

        public void addSubtopology(final TopologyDescription.Subtopology subtopology) {
            subtopologies.add(subtopology);
        }

        public void addGlobalStore(final TopologyDescription.GlobalStore globalStore) {
            globalStores.add(globalStore);
        }

        @Override
        public Set<TopologyDescription.Subtopology> subtopologies() {
            return Collections.unmodifiableSet(subtopologies);
        }

        @Override
        public Set<TopologyDescription.GlobalStore> globalStores() {
            return Collections.unmodifiableSet(globalStores);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("Topologies:\n ");
            final TopologyDescription.Subtopology[] sortedSubtopologies = 
                subtopologies.descendingSet().toArray(new TopologyDescription.Subtopology[subtopologies.size()]);
            final TopologyDescription.GlobalStore[] sortedGlobalStores = 
                globalStores.descendingSet().toArray(new TopologyDescription.GlobalStore[globalStores.size()]);
            int expectedId = 0;
            int subtopologiesIndex = sortedSubtopologies.length - 1;
            int globalStoresIndex = sortedGlobalStores.length - 1;
            while (subtopologiesIndex != -1 && globalStoresIndex != -1) {
                sb.append("  ");
                final TopologyDescription.Subtopology subtopology = 
                    sortedSubtopologies[subtopologiesIndex];
                final TopologyDescription.GlobalStore globalStore = 
                    sortedGlobalStores[globalStoresIndex];
                if (subtopology.id() == expectedId) {
                    sb.append(subtopology);
                    subtopologiesIndex--;
                } else {
                    sb.append(globalStore);
                    globalStoresIndex--;
                }
                expectedId++;
            }
            while (subtopologiesIndex != -1) {
                final TopologyDescription.Subtopology subtopology = 
                    sortedSubtopologies[subtopologiesIndex];
                sb.append("  ");
                sb.append(subtopology);
                subtopologiesIndex--;
            }
            while (globalStoresIndex != -1) {
                final TopologyDescription.GlobalStore globalStore = 
                    sortedGlobalStores[globalStoresIndex];
                sb.append("  ");
                sb.append(globalStore);
                globalStoresIndex--;
            }
            return sb.toString();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final TopologyDescription that = (TopologyDescription) o;
            return subtopologies.equals(that.subtopologies)
                && globalStores.equals(that.globalStores);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subtopologies, globalStores);
        }

    }

    private static String nodeNames(final Set<TopologyDescription.Node> nodes) {
        final StringBuilder sb = new StringBuilder();
        if (!nodes.isEmpty()) {
            for (final TopologyDescription.Node n : nodes) {
                sb.append(n.name());
                sb.append(", ");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
        } else {
            return "none";
        }
        return sb.toString();
    }

    /**
     * Used to capture subscribed topic via Patterns discovered during the
     * partition assignment process.
     */
    public static class SubscriptionUpdates {

        private final Set<String> updatedTopicSubscriptions = new HashSet<>();

        private void updateTopics(final Collection<String> topicNames) {
            updatedTopicSubscriptions.clear();
            updatedTopicSubscriptions.addAll(topicNames);
        }

        public Collection<String> getUpdates() {
            return Collections.unmodifiableSet(updatedTopicSubscriptions);
        }

        boolean hasUpdates() {
            return !updatedTopicSubscriptions.isEmpty();
        }

        @Override
        public String toString() {
            return String.format("SubscriptionUpdates{updatedTopicSubscriptions=%s}", updatedTopicSubscriptions);
        }
    }

    public void updateSubscribedTopics(final Set<String> topics, final String logPrefix) {
        final SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();
        log.debug("{}found {} topics possibly matching regex", topics, logPrefix);
        // update the topic groups with the returned subscription set for regex pattern subscriptions
        subscriptionUpdates.updateTopics(topics);
        updateSubscriptions(subscriptionUpdates, logPrefix);
    }


    // following functions are for test only

    public synchronized Set<String> getSourceTopicNames() {
        return sourceTopicNames;
    }

    public synchronized Map<String, StateStoreFactory> getStateStores() {
        return stateFactories;
    }
}
