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

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.apache.kafka.streams.state.StoreBuilder;
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
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.LATEST;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.NONE;

public class InternalTopologyBuilder {

    public InternalTopologyBuilder() {
        this.topologyName = null;
    }

    public InternalTopologyBuilder(final TopologyConfig topologyConfigs) {
        this.topologyConfigs = topologyConfigs;
        this.topologyName = topologyConfigs.topologyName;
    }

    private static final Logger log = LoggerFactory.getLogger(InternalTopologyBuilder.class);
    private static final String[] NO_PREDECESSORS = {};

    // node factories in a topological order
    private final Map<String, NodeFactory<?, ?, ?, ?>> nodeFactories = new LinkedHashMap<>();

    private final Map<String, StoreFactory> stateFactories = new HashMap<>();

    private final Map<String, StoreFactory> globalStateBuilders = new LinkedHashMap<>();

    // built global state stores
    private final Map<String, StateStore> globalStateStores = new LinkedHashMap<>();

    // Raw names of all source topics, without the application id/named topology prefix for repartition sources
    private final Set<String> rawSourceTopicNames = new HashSet<>();

    // Full names of all source topics, including the application id/named topology prefix for repartition sources
    private List<String> fullSourceTopicNames = null;

    // String representing pattern that matches all subscribed topics, including patterns and full source topic names
    private String sourceTopicPatternString = null;

    // all internal topics with their corresponding properties auto-created by the topology builder and used in source / sink processors
    private final Map<String, InternalTopicProperties> internalTopicNamesWithProperties = new HashMap<>();

    // groups of source processors that need to be copartitioned
    private final List<Set<String>> copartitionSourceGroups = new ArrayList<>();

    // map from source processor names to subscribed topics (without application-id prefix for internal topics)
    private final Map<String, List<String>> nodeToSourceTopics = new HashMap<>();

    // map from source processor names to regex subscription patterns
    private final Map<String, Pattern> nodeToSourcePatterns = new LinkedHashMap<>();

    // map from sink processor names to sink topic (without application-id prefix for internal topics)
    private final Map<String, String> nodeToSinkTopic = new HashMap<>();

    // map from state store names to raw name (without application id/topology name prefix) of all topics subscribed
    // from source processors that are connected to these state stores
    private final Map<String, Set<String>> stateStoreNameToRawSourceTopicNames = new HashMap<>();

    // map from state store names to all the regex subscribed topics from source processors that
    // are connected to these state stores
    private final Map<String, Set<Pattern>> stateStoreNameToSourceRegex = new HashMap<>();

    // map from state store names to this state store's corresponding changelog topic if possible
    private final Map<String, String> storeToChangelogTopic = new HashMap<>();

    // map from changelog topic name to its corresponding state store.
    private final Map<String, String> changelogTopicToStore = new HashMap<>();

    // all global topics
    private final Set<String> globalTopics = new HashSet<>();

    private final Set<String> earliestResetTopics = new HashSet<>();

    private final Set<String> latestResetTopics = new HashSet<>();

    private final Set<Pattern> earliestResetPatterns = new HashSet<>();

    private final Set<Pattern> latestResetPatterns = new HashSet<>();

    private final QuickUnion<String> nodeGrouper = new QuickUnion<>();

    // Used to capture subscribed topics via Patterns discovered during the partition assignment process.
    private final Set<String> subscriptionUpdates = new HashSet<>();

    private String applicationId = null;

    private Map<Integer, Set<String>> nodeGroups = null;

    // The name of the topology this builder belongs to, or null if this is not a NamedTopology
    private final String topologyName;
    // TODO KAFKA-13336: we can remove this reference once we make the Topology/NamedTopology class into an interface and implement it
    private NamedTopology namedTopology;

    // TODO KAFKA-13283: once we enforce all configs be passed in when constructing the topology builder then we can set
    //  this up front and make it final, but for now we have to wait for the global app configs passed in to rewriteTopology
    private TopologyConfig topologyConfigs;  // the configs for this topology, including overrides and global defaults

    private boolean hasPersistentStores = false;

    private static abstract class NodeFactory<KIn, VIn, KOut, VOut> {
        final String name;
        final String[] predecessors;

        NodeFactory(final String name,
                    final String[] predecessors) {
            this.name = name;
            this.predecessors = predecessors;
        }

        public abstract ProcessorNode<KIn, VIn, KOut, VOut> build();

        abstract AbstractNode describe();
    }

    private static class ProcessorNodeFactory<KIn, VIn, KOut, VOut> extends NodeFactory<KIn, VIn, KOut, VOut> {
        private final ProcessorSupplier<KIn, VIn, KOut, VOut> supplier;
        final Set<String> stateStoreNames = new HashSet<>();

        ProcessorNodeFactory(final String name,
                             final String[] predecessors,
                             final ProcessorSupplier<KIn, VIn, KOut, VOut> supplier) {
            super(name, predecessors.clone());
            this.supplier = supplier;
        }

        public void addStateStore(final String stateStoreName) {
            stateStoreNames.add(stateStoreName);
        }

        @Override
        public ProcessorNode<KIn, VIn, KOut, VOut> build() {
            return new ProcessorNode<>(name, supplier.get(), stateStoreNames);
        }

        @Override
        Processor describe() {
            return new Processor(name, new HashSet<>(stateStoreNames));
        }
    }

    private static class FixedKeyProcessorNodeFactory<KIn, VIn, VOut> extends ProcessorNodeFactory<KIn, VIn, KIn, VOut> {
        private final FixedKeyProcessorSupplier<KIn, VIn, VOut> supplier;

        FixedKeyProcessorNodeFactory(final String name,
                             final String[] predecessors,
                             final FixedKeyProcessorSupplier<KIn, VIn, VOut> supplier) {
            super(name, predecessors.clone(), null);
            this.supplier = supplier;
        }

        @Override
        public ProcessorNode<KIn, VIn, KIn, VOut> build() {
            return new ProcessorNode<>(name, supplier.get(), stateStoreNames);
        }

        @Override
        Processor describe() {
            return new Processor(name, new HashSet<>(stateStoreNames));
        }
    }

    // Map from topics to their matched regex patterns, this is to ensure one topic is passed through on source node
    // even if it can be matched by multiple regex patterns. Only used by SourceNodeFactory
    private final Map<String, Pattern> topicToPatterns = new HashMap<>();

    private class SourceNodeFactory<KIn, VIn> extends NodeFactory<KIn, VIn, KIn, VIn> {
        private final List<String> topics;
        private final Pattern pattern;
        private final Deserializer<KIn> keyDeserializer;
        private final Deserializer<VIn> valDeserializer;
        private final TimestampExtractor timestampExtractor;

        private SourceNodeFactory(final String name,
                                  final String[] topics,
                                  final Pattern pattern,
                                  final TimestampExtractor timestampExtractor,
                                  final Deserializer<KIn> keyDeserializer,
                                  final Deserializer<VIn> valDeserializer) {
            super(name, NO_PREDECESSORS);
            this.topics = topics != null ? Arrays.asList(topics) : new ArrayList<>();
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
        public ProcessorNode<KIn, VIn, KIn, VIn> build() {
            return new SourceNode<>(name, timestampExtractor, keyDeserializer, valDeserializer);
        }

        private boolean isMatch(final String topic) {
            return pattern.matcher(topic).matches();
        }

        @Override
        Source describe() {
            return new Source(name, topics.size() == 0 ? null : new HashSet<>(topics), pattern);
        }
    }

    private class SinkNodeFactory<KIn, VIn> extends NodeFactory<KIn, VIn, Void, Void> {
        private final Serializer<KIn> keySerializer;
        private final Serializer<VIn> valSerializer;
        private final StreamPartitioner<? super KIn, ? super VIn> partitioner;
        private final TopicNameExtractor<KIn, VIn> topicExtractor;

        private SinkNodeFactory(final String name,
                                final String[] predecessors,
                                final TopicNameExtractor<KIn, VIn> topicExtractor,
                                final Serializer<KIn> keySerializer,
                                final Serializer<VIn> valSerializer,
                                final StreamPartitioner<? super KIn, ? super VIn> partitioner) {
            super(name, predecessors.clone());
            this.topicExtractor = topicExtractor;
            this.keySerializer = keySerializer;
            this.valSerializer = valSerializer;
            this.partitioner = partitioner;
        }

        @Override
        public ProcessorNode<KIn, VIn, Void, Void> build() {
            if (topicExtractor instanceof StaticTopicNameExtractor) {
                final String topic = ((StaticTopicNameExtractor<KIn, VIn>) topicExtractor).topicName;
                if (internalTopicNamesWithProperties.containsKey(topic)) {
                    // prefix the internal topic name with the application id
                    return new SinkNode<>(name, new StaticTopicNameExtractor<>(decorateTopic(topic)), keySerializer, valSerializer, partitioner);
                } else {
                    return new SinkNode<>(name, topicExtractor, keySerializer, valSerializer, partitioner);
                }
            } else {
                return new SinkNode<>(name, topicExtractor, keySerializer, valSerializer, partitioner);
            }
        }

        @Override
        Sink<KIn, VIn> describe() {
            return new Sink<>(name, topicExtractor);
        }
    }

    // public for testing only
    public final InternalTopologyBuilder setApplicationId(final String applicationId) {
        Objects.requireNonNull(applicationId, "applicationId can't be null");
        this.applicationId = applicationId;

        return this;
    }

    public synchronized final void setStreamsConfig(final StreamsConfig applicationConfig) {
        Objects.requireNonNull(applicationConfig, "config can't be null");
        topologyConfigs = new TopologyConfig(applicationConfig);
    }

    public synchronized  final void setNamedTopology(final NamedTopology namedTopology) {
        this.namedTopology = namedTopology;
    }

    public synchronized TopologyConfig topologyConfigs() {
        return topologyConfigs;
    }

    public String topologyName() {
        return topologyName;
    }

    public NamedTopology namedTopology() {
        return namedTopology;
    }

    public synchronized final InternalTopologyBuilder rewriteTopology(final StreamsConfig config) {
        Objects.requireNonNull(config, "config can't be null");

        setApplicationId(config.getString(StreamsConfig.APPLICATION_ID_CONFIG));
        setStreamsConfig(config);

        // maybe strip out caching layers
        if (topologyConfigs.cacheSize == 0L) {
            for (final StoreFactory storeFactory : stateFactories.values()) {
                storeFactory.withCachingDisabled();
            }

            for (final StoreFactory storeFactory : globalStateBuilders.values()) {
                storeFactory.withCachingDisabled();
            }
        }

        // build global state stores
        for (final StoreFactory storeFactory : globalStateBuilders.values()) {
            storeFactory.configure(config);
            globalStateStores.put(storeFactory.name(), storeFactory.build());
        }

        return this;
    }

    public final void addSource(final Topology.AutoOffsetReset offsetReset,
                                final String name,
                                final TimestampExtractor timestampExtractor,
                                final Deserializer<?> keyDeserializer,
                                final Deserializer<?> valDeserializer,
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
            rawSourceTopicNames.add(topic);
        }

        nodeFactories.put(name, new SourceNodeFactory<>(name, topics, null, timestampExtractor, keyDeserializer, valDeserializer));
        nodeToSourceTopics.put(name, Arrays.asList(topics));
        nodeGrouper.add(name);
        nodeGroups = null;
    }

    public final void addSource(final Topology.AutoOffsetReset offsetReset,
                                final String name,
                                final TimestampExtractor timestampExtractor,
                                final Deserializer<?> keyDeserializer,
                                final Deserializer<?> valDeserializer,
                                final Pattern topicPattern) {
        Objects.requireNonNull(topicPattern, "topicPattern can't be null");
        Objects.requireNonNull(name, "name can't be null");

        if (nodeFactories.containsKey(name)) {
            throw new TopologyException("Processor " + name + " is already added.");
        }

        for (final String sourceTopicName : rawSourceTopicNames) {
            if (topicPattern.matcher(sourceTopicName).matches()) {
                throw new TopologyException("Pattern " + topicPattern + " will match a topic that has already been registered by another source.");
            }
        }

        maybeAddToResetList(earliestResetPatterns, latestResetPatterns, offsetReset, topicPattern);

        nodeFactories.put(name, new SourceNodeFactory<>(name, null, topicPattern, timestampExtractor, keyDeserializer, valDeserializer));
        nodeToSourcePatterns.put(name, topicPattern);
        nodeGrouper.add(name);
        nodeGroups = null;
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

        addSink(name, new StaticTopicNameExtractor<>(topic), keySerializer, valSerializer, partitioner, predecessorNames);
        nodeToSinkTopic.put(name, topic);
        nodeGroups = null;
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
        nodeGroups = null;
    }

    public final <KIn, VIn, KOut, VOut> void addProcessor(final String name,
                                                          final ProcessorSupplier<KIn, VIn, KOut, VOut> supplier,
                                                          final String... predecessorNames) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        Objects.requireNonNull(predecessorNames, "predecessor names must not be null");
        ApiUtils.checkSupplier(supplier);
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
                throw new TopologyException("Predecessor processor " + predecessor + " is not added yet for " + name);
            }
        }

        nodeFactories.put(name, new ProcessorNodeFactory<>(name, predecessorNames, supplier));
        nodeGrouper.add(name);
        nodeGrouper.unite(name, predecessorNames);
        nodeGroups = null;
    }

    public final <KIn, VIn, VOut> void addProcessor(final String name,
                                                    final FixedKeyProcessorSupplier<KIn, VIn, VOut> supplier,
                                                    final String... predecessorNames) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        Objects.requireNonNull(predecessorNames, "predecessor names must not be null");
        ApiUtils.checkSupplier(supplier);
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
                throw new TopologyException("Predecessor processor " + predecessor + " is not added yet for " + name);
            }
        }

        nodeFactories.put(name, new FixedKeyProcessorNodeFactory<>(name, predecessorNames, supplier));
        nodeGrouper.add(name);
        nodeGrouper.unite(name, predecessorNames);
        nodeGroups = null;
    }

    public final void addStateStore(final StoreBuilder<?> storeBuilder,
                                    final String... processorNames) {
        addStateStore(new StoreBuilderWrapper(storeBuilder), false, processorNames);
    }

    public final void addStateStore(final StoreFactory storeFactory,
                                    final String... processorNames) {
        addStateStore(storeFactory, false, processorNames);
    }

    public final void addStateStore(final StoreFactory storeFactory,
                                    final boolean allowOverride,
                                    final String... processorNames) {
        Objects.requireNonNull(storeFactory, "stateStoreFactory can't be null");
        final StoreFactory stateFactory = stateFactories.get(storeFactory.name());
        if (!allowOverride && stateFactory != null && !stateFactory.isCompatibleWith(storeFactory)) {
            throw new TopologyException("A different StateStore has already been added with the name " + storeFactory.name());
        }
        if (globalStateBuilders.containsKey(storeFactory.name())) {
            throw new TopologyException("A different GlobalStateStore has already been added with the name " + storeFactory.name());
        }

        stateFactories.put(storeFactory.name(), storeFactory);

        if (processorNames != null) {
            for (final String processorName : processorNames) {
                Objects.requireNonNull(processorName, "processor name must not be null");
                connectProcessorAndStateStore(processorName, storeFactory.name());
            }
        }
        nodeGroups = null;
    }

    public final <KIn, VIn> void addGlobalStore(final StoreFactory storeFactory,
                                                final String sourceName,
                                                final TimestampExtractor timestampExtractor,
                                                final Deserializer<KIn> keyDeserializer,
                                                final Deserializer<VIn> valueDeserializer,
                                                final String topic,
                                                final String processorName,
                                                final ProcessorSupplier<KIn, VIn, Void, Void> stateUpdateSupplier) {
        Objects.requireNonNull(storeFactory, "store builder must not be null");
        ApiUtils.checkSupplier(stateUpdateSupplier);
        validateGlobalStoreArguments(sourceName,
                                     topic,
                                     processorName,
                                     stateUpdateSupplier,
                                     storeFactory.name(),
                                     storeFactory.loggingEnabled());
        validateTopicNotAlreadyRegistered(topic);

        final String[] topics = {topic};
        final String[] predecessors = {sourceName};

        final ProcessorNodeFactory<KIn, VIn, Void, Void> nodeFactory = new ProcessorNodeFactory<>(
            processorName,
            predecessors,
            stateUpdateSupplier
        );

        globalTopics.add(topic);
        nodeFactories.put(sourceName, new SourceNodeFactory<>(
            sourceName,
            topics,
            null,
            timestampExtractor,
            keyDeserializer,
            valueDeserializer)
        );
        nodeToSourceTopics.put(sourceName, Arrays.asList(topics));
        nodeGrouper.add(sourceName);
        nodeFactory.addStateStore(storeFactory.name());
        nodeFactories.put(processorName, nodeFactory);
        nodeGrouper.add(processorName);
        nodeGrouper.unite(processorName, predecessors);
        globalStateBuilders.put(storeFactory.name(), storeFactory);
        connectSourceStoreAndTopic(storeFactory.name(), topic);
        nodeGroups = null;
    }

    private void validateTopicNotAlreadyRegistered(final String topic) {
        if (rawSourceTopicNames.contains(topic) || globalTopics.contains(topic)) {
            throw new TopologyException("Topic " + topic + " has already been registered by another source.");
        }

        for (final Pattern pattern : nodeToSourcePatterns.values()) {
            if (pattern.matcher(topic).matches()) {
                throw new TopologyException("Topic " + topic + " matches a Pattern already registered by another source.");
            }
        }
    }

    public Long getHistoryRetention(final String storeName) {
        return stateFactories.get(storeName).historyRetention();
    }

    public boolean isStoreVersioned(final String storeName) {
        return stateFactories.get(storeName).isVersionedStore();
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
        nodeGroups = null;
    }

    public String getStoreForChangelogTopic(final String topicName) {
        return changelogTopicToStore.get(topicName);
    }

    public void connectSourceStoreAndTopic(final String sourceStoreName,
                                           final String topic) {
        if (storeToChangelogTopic.containsKey(sourceStoreName)) {
            throw new TopologyException("Source store " + sourceStoreName + " is already added.");
        }
        storeToChangelogTopic.put(sourceStoreName, topic);
        changelogTopicToStore.put(topic, sourceStoreName);
    }

    public final void addInternalTopic(final String topicName,
                                       final InternalTopicProperties internalTopicProperties) {
        Objects.requireNonNull(topicName, "topicName can't be null");
        Objects.requireNonNull(internalTopicProperties, "internalTopicProperties can't be null");

        internalTopicNamesWithProperties.put(topicName, internalTopicProperties);
    }

    public final void copartitionSources(final Collection<String> sourceNodes) {
        copartitionSourceGroups.add(new HashSet<>(sourceNodes));
    }

    public final void maybeUpdateCopartitionSourceGroups(final String replacedNodeName,
                                                         final String optimizedNodeName) {
        for (final Set<String> copartitionSourceGroup : copartitionSourceGroups) {
            if (copartitionSourceGroup.contains(replacedNodeName)) {
                copartitionSourceGroup.remove(replacedNodeName);
                copartitionSourceGroup.add(optimizedNodeName);
            }
        }
    }

    public void validateCopartition() {
        // allCopartitionedSourceTopics take the list of co-partitioned nodes and
        // replaces each processor name with the corresponding source topic name
        final List<Set<String>> allCopartitionedSourceTopics =
                copartitionSourceGroups
                        .stream()
                        .map(sourceGroup -> sourceGroup
                                .stream()
                                .flatMap(sourceNodeName -> nodeToSourceTopics.getOrDefault(sourceNodeName,
                                        Collections.emptyList()).stream())
                                .collect(Collectors.toSet())
                        ).collect(Collectors.toList());
        for (final Set<String> copartition : allCopartitionedSourceTopics) {
            final Map<String, Integer> numberOfPartitionsPerTopic = new HashMap<>();
            copartition.forEach(topic -> {
                final InternalTopicProperties prop = internalTopicNamesWithProperties.get(topic);
                if (prop != null && prop.getNumberOfPartitions().isPresent()) {
                    numberOfPartitionsPerTopic.put(topic, prop.getNumberOfPartitions().get());
                }
            });
            if (!numberOfPartitionsPerTopic.isEmpty() && copartition.equals(numberOfPartitionsPerTopic.keySet())) {
                final Collection<Integer> partitionNumbers = numberOfPartitionsPerTopic.values();
                final Integer first = partitionNumbers.iterator().next();
                for (final Integer partitionNumber : partitionNumbers) {
                    if (!partitionNumber.equals(first)) {
                        final String msg = String.format("Following topics do not have the same number of " +
                                "partitions: [%s]", new TreeMap<>(numberOfPartitionsPerTopic));
                        throw new TopologyException(msg);

                    }
                }
            }
        }
    }

    private void validateGlobalStoreArguments(final String sourceName,
                                              final String topic,
                                              final String processorName,
                                              final ProcessorSupplier<?, ?, Void, Void> stateUpdateSupplier,
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
        if (stateFactories.containsKey(storeName)) {
            throw new TopologyException("A different StateStore has already been added with the name " + storeName);
        }
        if (globalStateBuilders.containsKey(storeName)) {
            throw new TopologyException("A different GlobalStateStore has already been added with the name " + storeName);
        }
        if (loggingEnabled) {
            throw new TopologyException("StateStore " + storeName + " for global table must not have logging enabled.");
        }
        if (sourceName.equals(processorName)) {
            throw new TopologyException("sourceName and processorName must be different.");
        }
    }

    private void connectProcessorAndStateStore(final String processorName,
                                               final String stateStoreName) {
        if (globalStateBuilders.containsKey(stateStoreName)) {
            throw new TopologyException("Global StateStore " + stateStoreName +
                    " can be used by a Processor without being specified; it should not be explicitly passed.");
        }
        if (!stateFactories.containsKey(stateStoreName)) {
            throw new TopologyException("StateStore " + stateStoreName + " is not added yet.");
        }
        if (!nodeFactories.containsKey(processorName)) {
            throw new TopologyException("Processor " + processorName + " is not added yet.");
        }

        final StoreFactory storeFactory = stateFactories.get(stateStoreName);
        final Iterator<String> iter = storeFactory.connectedProcessorNames().iterator();
        if (iter.hasNext()) {
            final String user = iter.next();
            nodeGrouper.unite(user, processorName);
        }
        storeFactory.connectedProcessorNames().add(processorName);

        final NodeFactory<?, ?, ?, ?> nodeFactory = nodeFactories.get(processorName);
        if (nodeFactory instanceof ProcessorNodeFactory) {
            final ProcessorNodeFactory<?, ?, ?, ?> processorNodeFactory = (ProcessorNodeFactory<?, ?, ?, ?>) nodeFactory;
            processorNodeFactory.addStateStore(stateStoreName);
            connectStateStoreNameToSourceTopicsOrPattern(stateStoreName, processorNodeFactory);
        } else {
            throw new TopologyException("cannot connect a state store " + stateStoreName + " to a source node or a sink node.");
        }
    }

    private Set<SourceNodeFactory<?, ?>> findSourcesForProcessorPredecessors(final String[] predecessors) {
        final Set<SourceNodeFactory<?, ?>> sourceNodes = new HashSet<>();
        for (final String predecessor : predecessors) {
            final NodeFactory<?, ?, ?, ?> nodeFactory = nodeFactories.get(predecessor);
            if (nodeFactory instanceof SourceNodeFactory) {
                sourceNodes.add((SourceNodeFactory<?, ?>) nodeFactory);
            } else if (nodeFactory instanceof ProcessorNodeFactory) {
                sourceNodes.addAll(findSourcesForProcessorPredecessors(((ProcessorNodeFactory<?, ?, ?, ?>) nodeFactory).predecessors));
            }
        }
        return sourceNodes;
    }

    private <KIn, VIn, KOut, VOut> void connectStateStoreNameToSourceTopicsOrPattern(final String stateStoreName,
                                                                                     final ProcessorNodeFactory<KIn, VIn, KOut, VOut> processorNodeFactory) {
        // we should never update the mapping from state store names to source topics if the store name already exists
        // in the map; this scenario is possible, for example, that a state store underlying a source KTable is
        // connecting to a join operator whose source topic is not the original KTable's source topic but an internal repartition topic.

        if (stateStoreNameToRawSourceTopicNames.containsKey(stateStoreName)
            || stateStoreNameToSourceRegex.containsKey(stateStoreName)) {
            return;
        }

        final Set<String> sourceTopics = new HashSet<>();
        final Set<Pattern> sourcePatterns = new HashSet<>();
        final Set<SourceNodeFactory<?, ?>> sourceNodesForPredecessor =
            findSourcesForProcessorPredecessors(processorNodeFactory.predecessors);

        for (final SourceNodeFactory<?, ?> sourceNodeFactory : sourceNodesForPredecessor) {
            if (sourceNodeFactory.pattern != null) {
                sourcePatterns.add(sourceNodeFactory.pattern);
            } else {
                sourceTopics.addAll(sourceNodeFactory.topics);
            }
        }

        if (!sourceTopics.isEmpty()) {
            stateStoreNameToRawSourceTopicNames.put(
                stateStoreName,
                Collections.unmodifiableSet(sourceTopics)
            );
        }

        if (!sourcePatterns.isEmpty()) {
            stateStoreNameToSourceRegex.put(
                stateStoreName,
                Collections.unmodifiableSet(sourcePatterns)
            );
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

    // Order node groups by their position in the actual topology, ie upstream subtopologies come before downstream
    private Map<Integer, Set<String>> makeNodeGroups() {
        final Map<Integer, Set<String>> nodeGroups = new LinkedHashMap<>();
        final Map<String, Set<String>> rootToNodeGroup = new HashMap<>();

        int nodeGroupId = 0;

        // Traverse in topological order
        for (final String nodeName : nodeFactories.keySet()) {
            nodeGroupId = putNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
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

    /**
     * @return the full topology minus any global state
     */
    public synchronized ProcessorTopology buildTopology() {
        final Set<String> nodeGroup = new HashSet<>();
        for (final Set<String> value : nodeGroups().values()) {
            nodeGroup.addAll(value);
        }
        nodeGroup.removeAll(globalNodeGroups());

        initializeSubscription();
        return build(nodeGroup);
    }

    /**
     * @param topicGroupId group of topics corresponding to a single subtopology
     * @return subset of the full topology
     */
    public synchronized ProcessorTopology buildSubtopology(final int topicGroupId) {
        final Set<String> nodeGroup = nodeGroups().get(topicGroupId);
        return build(nodeGroup);
    }

    /**
     * Builds the topology for any global state stores
     * @return ProcessorTopology of global state
     */
    public synchronized ProcessorTopology buildGlobalStateTopology() {
        Objects.requireNonNull(applicationId, "topology has not completed optimization");

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

    @SuppressWarnings("unchecked")
    private ProcessorTopology build(final Set<String> nodeGroup) {
        Objects.requireNonNull(applicationId, "topology has not completed optimization");

        final Map<String, ProcessorNode<?, ?, ?, ?>> processorMap = new LinkedHashMap<>();
        final Map<String, SourceNode<?, ?>> topicSourceMap = new HashMap<>();
        final Map<String, SinkNode<?, ?>> topicSinkMap = new HashMap<>();
        final Map<String, StateStore> stateStoreMap = new LinkedHashMap<>();
        final Set<String> repartitionTopics = new HashSet<>();

        // create processor nodes in a topological order ("nodeFactories" is already topologically sorted)
        // also make sure the state store map values following the insertion ordering
        for (final NodeFactory<?, ?, ?, ?> factory : nodeFactories.values()) {
            if (nodeGroup == null || nodeGroup.contains(factory.name)) {
                final ProcessorNode<?, ?, ?, ?> node = factory.build();
                processorMap.put(node.name(), node);

                if (factory instanceof ProcessorNodeFactory) {
                    buildProcessorNode(processorMap,
                                       stateStoreMap,
                                       (ProcessorNodeFactory<?, ?, ?, ?>) factory,
                                       (ProcessorNode<Object, Object, Object, Object>) node);

                } else if (factory instanceof SourceNodeFactory) {
                    buildSourceNode(topicSourceMap,
                                    repartitionTopics,
                                    (SourceNodeFactory<?, ?>) factory,
                                    (SourceNode<?, ?>) node);

                } else if (factory instanceof SinkNodeFactory) {
                    buildSinkNode(processorMap,
                                  topicSinkMap,
                                  repartitionTopics,
                                  (SinkNodeFactory<?, ?>) factory,
                                  (SinkNode<?, ?>) node);
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

    private void buildSinkNode(final Map<String, ProcessorNode<?, ?, ?, ?>> processorMap,
                               final Map<String, SinkNode<?, ?>> topicSinkMap,
                               final Set<String> repartitionTopics,
                               final SinkNodeFactory<?, ?> sinkNodeFactory,
                               final SinkNode<?, ?> node) {
        @SuppressWarnings("unchecked") final ProcessorNode<Object, Object, ?, ?> sinkNode =
            (ProcessorNode<Object, Object, ?, ?>) node;

        for (final String predecessorName : sinkNodeFactory.predecessors) {
            final ProcessorNode<Object, Object, Object, Object> processor = getProcessor(processorMap, predecessorName);
            processor.addChild(sinkNode);
            if (sinkNodeFactory.topicExtractor instanceof StaticTopicNameExtractor) {
                final String topic = ((StaticTopicNameExtractor<?, ?>) sinkNodeFactory.topicExtractor).topicName;

                if (internalTopicNamesWithProperties.containsKey(topic)) {
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

    @SuppressWarnings("unchecked")
    private static <KIn, VIn, KOut, VOut> ProcessorNode<KIn, VIn, KOut, VOut> getProcessor(
        final Map<String, ProcessorNode<?, ?, ?, ?>> processorMap,
        final String predecessor) {

        return (ProcessorNode<KIn, VIn, KOut, VOut>) processorMap.get(predecessor);
    }

    private void buildSourceNode(final Map<String, SourceNode<?, ?>> topicSourceMap,
                                 final Set<String> repartitionTopics,
                                 final SourceNodeFactory<?, ?> sourceNodeFactory,
                                 final SourceNode<?, ?> node) {

        final List<String> topics = (sourceNodeFactory.pattern != null) ?
            sourceNodeFactory.getTopics(subscriptionUpdates()) :
            sourceNodeFactory.topics;

        for (final String topic : topics) {
            if (internalTopicNamesWithProperties.containsKey(topic)) {
                // prefix the internal topic name with the application id
                final String decoratedTopic = decorateTopic(topic);
                topicSourceMap.put(decoratedTopic, node);
                repartitionTopics.add(decoratedTopic);
            } else {
                topicSourceMap.put(topic, node);
            }
        }
    }

    private void buildProcessorNode(final Map<String, ProcessorNode<?, ?, ?, ?>> processorMap,
                                    final Map<String, StateStore> stateStoreMap,
                                    final ProcessorNodeFactory<?, ?, ?, ?> factory,
                                    final ProcessorNode<Object, Object, Object, Object> node) {

        for (final String predecessor : factory.predecessors) {
            final ProcessorNode<Object, Object, Object, Object> predecessorNode = getProcessor(processorMap, predecessor);
            predecessorNode.addChild(node);
        }
        for (final String stateStoreName : factory.stateStoreNames) {
            if (!stateStoreMap.containsKey(stateStoreName)) {
                final StateStore store;
                if (stateFactories.containsKey(stateStoreName)) {
                    final StoreFactory storeFactory = stateFactories.get(stateStoreName);

                    // remember the changelog topic if this state store is change-logging enabled
                    if (storeFactory.loggingEnabled() && !storeToChangelogTopic.containsKey(stateStoreName)) {
                        final String prefix = topologyConfigs == null ?
                                applicationId :
                                ProcessorContextUtils.getPrefix(topologyConfigs.applicationConfigs.originals(), applicationId);
                        final String changelogTopic =
                            ProcessorStateManager.storeChangelogTopic(prefix, stateStoreName, topologyName);
                        storeToChangelogTopic.put(stateStoreName, changelogTopic);
                        changelogTopicToStore.put(changelogTopic, stateStoreName);
                    }
                    if (topologyConfigs != null) {
                        storeFactory.configure(topologyConfigs.applicationConfigs);
                    }
                    store = storeFactory.build();
                    stateStoreMap.put(stateStoreName, store);
                } else {
                    store = globalStateStores.get(stateStoreName);
                    stateStoreMap.put(stateStoreName, store);
                }

                if (store.persistent()) {
                    hasPersistentStores = true;
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
        Objects.requireNonNull(applicationId, "topology has not completed optimization");

        return Collections.unmodifiableMap(globalStateStores);
    }

    public Set<String> allStateStoreNames() {
        Objects.requireNonNull(applicationId, "topology has not completed optimization");

        final Set<String> allNames = new HashSet<>(stateFactories.keySet());
        allNames.addAll(globalStateStores.keySet());
        return Collections.unmodifiableSet(allNames);
    }

    public boolean hasStore(final String name) {
        return stateFactories.containsKey(name) || globalStateStores.containsKey(name);
    }

    public boolean hasPersistentStores() {
        return hasPersistentStores;
    }

    /**
     * Returns the map of topic groups keyed by the group id.
     * A topic group is a group of topics in the same task.
     *
     * @return groups of topic names
     */
    public synchronized Map<Subtopology, TopicsInfo> subtopologyToTopicsInfo() {
        final Map<Subtopology, TopicsInfo> topicGroups = new LinkedHashMap<>();

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
                        if (internalTopicNamesWithProperties.containsKey(topic)) {
                            // prefix the internal topic name with the application id
                            final String internalTopic = decorateTopic(topic);

                            final RepartitionTopicConfig repartitionTopicConfig = buildRepartitionTopicConfig(
                                internalTopic,
                                internalTopicNamesWithProperties.get(topic).getNumberOfPartitions()
                            );

                            repartitionTopics.put(repartitionTopicConfig.name(), repartitionTopicConfig);
                            sourceTopics.add(repartitionTopicConfig.name());
                        } else {
                            sourceTopics.add(topic);
                        }
                    }
                }

                // if the node is a sink node, add to the sink topics
                final String topic = nodeToSinkTopic.get(node);
                if (topic != null) {
                    if (internalTopicNamesWithProperties.containsKey(topic)) {
                        // prefix the change log topic name with the application id
                        sinkTopics.add(decorateTopic(topic));
                    } else {
                        sinkTopics.add(topic);
                    }
                }

                // if the node is connected to a state store whose changelog topics are not predefined,
                // add to the changelog topics
                for (final StoreFactory stateFactory : stateFactories.values()) {
                    if (stateFactory.connectedProcessorNames().contains(node) && storeToChangelogTopic.containsKey(stateFactory.name())) {
                        final String topicName = storeToChangelogTopic.get(stateFactory.name());
                        if (!stateChangelogTopics.containsKey(topicName)) {
                            final InternalTopicConfig internalTopicConfig =
                                createChangelogTopicConfig(stateFactory, topicName);
                            stateChangelogTopics.put(topicName, internalTopicConfig);
                        }
                    }
                }
            }
            if (!sourceTopics.isEmpty()) {
                topicGroups.put(new Subtopology(entry.getKey(), topologyName), new TopicsInfo(
                        Collections.unmodifiableSet(sinkTopics),
                        Collections.unmodifiableSet(sourceTopics),
                        Collections.unmodifiableMap(repartitionTopics),
                        Collections.unmodifiableMap(stateChangelogTopics)));
            }
        }

        return Collections.unmodifiableMap(topicGroups);
    }

    public Map<String, List<String>> nodeToSourceTopics() {
        return Collections.unmodifiableMap(nodeToSourceTopics);
    }

    private RepartitionTopicConfig buildRepartitionTopicConfig(final String internalTopic,
                                                               final Optional<Integer> numberOfPartitions) {
        return numberOfPartitions
            .map(partitions -> new RepartitionTopicConfig(internalTopic,
                                                          Collections.emptyMap(),
                                                          partitions,
                                                          true))
            .orElse(new RepartitionTopicConfig(internalTopic, Collections.emptyMap()));
    }

    private void setRegexMatchedTopicsToSourceNodes() {
        if (hasSubscriptionUpdates()) {
            for (final String nodeName : nodeToSourcePatterns.keySet()) {
                final SourceNodeFactory<?, ?> sourceNode = (SourceNodeFactory<?, ?>) nodeFactories.get(nodeName);
                final List<String> sourceTopics = sourceNode.getTopics(subscriptionUpdates);
                //need to update nodeToSourceTopics and sourceTopicNames with topics matched from given regex
                nodeToSourceTopics.put(nodeName, sourceTopics);
                rawSourceTopicNames.addAll(sourceTopics);
            }
            log.debug("Updated nodeToSourceTopics: {}", nodeToSourceTopics);
        }
    }

    private void setRegexMatchedTopicToStateStore() {
        if (hasSubscriptionUpdates()) {
            for (final Map.Entry<String, Set<Pattern>> storePattern : stateStoreNameToSourceRegex.entrySet()) {
                final Set<String> updatedTopicsForStateStore = new HashSet<>();
                for (final String subscriptionUpdateTopic : subscriptionUpdates()) {
                    for (final Pattern pattern : storePattern.getValue()) {
                        if (pattern.matcher(subscriptionUpdateTopic).matches()) {
                            updatedTopicsForStateStore.add(subscriptionUpdateTopic);
                        }
                    }
                }
                if (!updatedTopicsForStateStore.isEmpty()) {
                    final Collection<String> storeTopics = stateStoreNameToRawSourceTopicNames.get(storePattern.getKey());
                    if (storeTopics != null) {
                        updatedTopicsForStateStore.addAll(storeTopics);
                    }
                    stateStoreNameToRawSourceTopicNames.put(
                        storePattern.getKey(),
                        Collections.unmodifiableSet(updatedTopicsForStateStore));
                }
            }
        }
    }

    private <S extends StateStore> InternalTopicConfig createChangelogTopicConfig(final StoreFactory factory,
                                                                                  final String name) {
        if (factory.isVersionedStore()) {
            final VersionedChangelogTopicConfig config = new VersionedChangelogTopicConfig(name, factory.logConfig(), factory.historyRetention());
            return config;
        } else if (factory.isWindowStore()) {
            final WindowedChangelogTopicConfig config = new WindowedChangelogTopicConfig(name, factory.logConfig(), factory.retentionPeriod());
            return config;
        } else {
            return new UnwindowedUnversionedChangelogTopicConfig(name, factory.logConfig());
        }
    }

    public boolean hasOffsetResetOverrides() {
        return !(earliestResetTopics.isEmpty() && earliestResetPatterns.isEmpty()
            && latestResetTopics.isEmpty() && latestResetPatterns.isEmpty());
    }

    public OffsetResetStrategy offsetResetStrategy(final String topic) {
        if (maybeDecorateInternalSourceTopics(earliestResetTopics).contains(topic) ||
            earliestResetPatterns.stream().anyMatch(p -> p.matcher(topic).matches())) {
            return EARLIEST;
        } else if (maybeDecorateInternalSourceTopics(latestResetTopics).contains(topic) ||
            latestResetPatterns.stream().anyMatch(p -> p.matcher(topic).matches())) {
            return LATEST;
        } else if (containsTopic(topic)) {
            return NONE;
        } else {
            throw new IllegalStateException(String.format(
                "Unable to lookup offset reset strategy for the following topic as it does not exist in the topology%s: %s",
                hasNamedTopology() ? topologyName : "",
                topic)
            );
        }
    }

    /**
     * @return  map from state store name to full names (including application id/topology name prefix)
     *          of all source topics whose processors are connected to it
     */
    public Map<String, List<String>> stateStoreNameToFullSourceTopicNames() {
        final Map<String, List<String>> results = new HashMap<>();
        for (final Map.Entry<String, Set<String>> entry : stateStoreNameToRawSourceTopicNames.entrySet()) {
            results.put(entry.getKey(), maybeDecorateInternalSourceTopics(entry.getValue()));
        }
        return results;
    }

    /**
     * @return  the full names (including application id/topology name prefix) of all source topics whose
     *          processors are connected to the given state store
     */
    public Collection<String> sourceTopicsForStore(final String storeName) {
        return maybeDecorateInternalSourceTopics(stateStoreNameToRawSourceTopicNames.get(storeName));
    }

    public synchronized Collection<Set<String>> copartitionGroups() {
        // compute transitive closures of copartitionGroups to relieve registering code to know all members
        // of a copartitionGroup at the same time
        final List<Set<String>> copartitionSourceTopics =
            copartitionSourceGroups
                .stream()
                .map(sourceGroup ->
                         sourceGroup
                             .stream()
                             .flatMap(node -> maybeDecorateInternalSourceTopics(nodeToSourceTopics.get(node)).stream())
                             .collect(Collectors.toSet())
                ).collect(Collectors.toList());

        final Map<String, Set<String>> topicsToCopartitionGroup = new LinkedHashMap<>();
        for (final Set<String> topics : copartitionSourceTopics) {
            if (topics != null) {
                Set<String> coparititonGroup = null;
                for (final String topic : topics) {
                    coparititonGroup = topicsToCopartitionGroup.get(topic);
                    if (coparititonGroup != null) {
                        break;
                    }
                }
                if (coparititonGroup == null) {
                    coparititonGroup = new HashSet<>();
                }
                coparititonGroup.addAll(maybeDecorateInternalSourceTopics(topics));
                for (final String topic : topics) {
                    topicsToCopartitionGroup.put(topic, coparititonGroup);
                }
            }
        }
        final Set<Set<String>> uniqueCopartitionGroups = new HashSet<>(topicsToCopartitionGroup.values());
        return Collections.unmodifiableList(new ArrayList<>(uniqueCopartitionGroups));
    }

    private List<String> maybeDecorateInternalSourceTopics(final Collection<String> sourceTopics) {
        if (sourceTopics == null) {
            return Collections.emptyList();
        }
        final List<String> decoratedTopics = new ArrayList<>();
        for (final String topic : sourceTopics) {
            if (internalTopicNamesWithProperties.containsKey(topic)) {
                decoratedTopics.add(decorateTopic(topic));
            } else {
                decoratedTopics.add(topic);
            }
        }
        return decoratedTopics;
    }

    public String decoratePseudoTopic(final String topic) {
        return decorateTopic(topic);
    }

    private String decorateTopic(final String topic) {
        if (applicationId == null) {
            throw new TopologyException("there are internal topics and "
                                            + "applicationId hasn't been set. Call "
                                            + "setApplicationId first");
        }
        final String prefix = topologyConfigs == null ?
                                applicationId :
                                ProcessorContextUtils.getPrefix(topologyConfigs.applicationConfigs.originals(), applicationId);

        if (hasNamedTopology()) {
            return prefix + "-" + topologyName + "-" + topic;
        } else {
            return prefix + "-" + topic;
        }
    }


    void initializeSubscription() {
        if (usesPatternSubscription()) {
            log.debug("Found pattern subscribed source topics, initializing consumer's subscription pattern.");
            sourceTopicPatternString = buildSourceTopicsPatternString();
        } else {
            log.debug("No source topics using pattern subscription found, initializing consumer's subscription collection.");
            fullSourceTopicNames = maybeDecorateInternalSourceTopics(rawSourceTopicNames);
            Collections.sort(fullSourceTopicNames);
        }
    }

    private String buildSourceTopicsPatternString() {
        final List<String> allSourceTopics = maybeDecorateInternalSourceTopics(rawSourceTopicNames);
        Collections.sort(allSourceTopics);

        final StringBuilder builder = new StringBuilder();

        for (final String topic : allSourceTopics) {
            builder.append(topic).append("|");
        }

        for (final Pattern sourcePattern : nodeToSourcePatterns.values()) {
            builder.append(sourcePattern.pattern()).append("|");
        }

        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }

        return builder.toString();
    }

    boolean usesPatternSubscription() {
        return !nodeToSourcePatterns.isEmpty();
    }

    /**
     * @return names of all source topics, including the application id/named topology prefix for repartition sources
     */
    public synchronized List<String> fullSourceTopicNames() {
        if (fullSourceTopicNames == null) {
            fullSourceTopicNames = maybeDecorateInternalSourceTopics(rawSourceTopicNames);
            Collections.sort(fullSourceTopicNames);
        }
        return fullSourceTopicNames;
    }

    synchronized String sourceTopicPatternString() {
        // With a NamedTopology, it may be that this topology does not use pattern subscription but another one does
        // in which case we would need to initialize the pattern string where we would otherwise have not
        if (sourceTopicPatternString == null) {
            sourceTopicPatternString = buildSourceTopicsPatternString();
        }
        return sourceTopicPatternString;
    }

    public boolean containsTopic(final String topic) {
        return fullSourceTopicNames().contains(topic)
            || (usesPatternSubscription() && Pattern.compile(sourceTopicPatternString()).matcher(topic).matches())
            || changelogTopicToStore.containsKey(topic);
    }

    public boolean hasNoLocalTopology() {
        return nodeToSourcePatterns.isEmpty() && rawSourceTopicNames.isEmpty();
    }

    public boolean hasGlobalStores() {
        return !globalStateStores.isEmpty();
    }

    private boolean isGlobalSource(final String nodeName) {
        final NodeFactory<?, ?, ?, ?> nodeFactory = nodeFactories.get(nodeName);

        if (nodeFactory instanceof SourceNodeFactory) {
            final List<String> topics = ((SourceNodeFactory<?, ?>) nodeFactory).topics;
            return topics != null && topics.size() == 1 && globalTopics.contains(topics.get(0));
        }
        return false;
    }

    public TopologyDescription describe() {
        final TopologyDescription description = new TopologyDescription(topologyName);

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

    private void describeGlobalStore(final TopologyDescription description,
                                     final Set<String> nodes,
                                     final int id) {
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
                    ((ProcessorNodeFactory<?, ?, ?, ?>) nodeFactories.get(processorNode)).stateStoreNames.iterator().next(),
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
            if (node1.equals(node2)) {
                return 0;
            }
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

    private static void updateSize(final AbstractNode node,
                                   final int delta) {
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

        description.addSubtopology(new SubtopologyDescription(
                subtopologyId,
                new HashSet<>(nodesByName.values())));
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
            source = new Source(sourceName, Collections.singleton(topicName), null);
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
            Objects.requireNonNull(name, "name cannot be null");
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
        private final Set<String> topics;
        private final Pattern topicPattern;

        public Source(final String name,
                      final Set<String> topics,
                      final Pattern pattern) {
            super(name);
            if (topics == null && pattern == null) {
                throw new IllegalArgumentException("Either topics or pattern must be not-null, but both are null.");
            }
            if (topics != null && pattern != null) {
                throw new IllegalArgumentException("Either topics or pattern must be null, but both are not null.");
            }

            this.topics = topics;
            this.topicPattern = pattern;
        }

        @Override
        public Set<String> topicSet() {
            return topics;
        }

        @Override
        public Pattern topicPattern() {
            return topicPattern;
        }

        @Override
        public void addPredecessor(final TopologyDescription.Node predecessor) {
            throw new UnsupportedOperationException("Sources don't have predecessors.");
        }

        @Override
        public String toString() {
            final String topicsString = topics == null ? topicPattern.toString() : topics.toString();

            return "Source: " + name + " (topics: " + topicsString + ")\n      --> " + nodeNames(successors);
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
                && Objects.equals(topics, source.topics)
                && (topicPattern == null ?
                        source.topicPattern == null :
                        topicPattern.pattern().equals(source.topicPattern.pattern()));
        }

        @Override
        public int hashCode() {
            // omit successor as it might change and alter the hash code
            return Objects.hash(name, topics, topicPattern);
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
            return "Processor: " + name + " (stores: " + stores + ")\n      --> "
                + nodeNames(successors) + "\n      <-- " + nodeNames(predecessors);
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

    public final static class Sink<K, V> extends AbstractNode implements TopologyDescription.Sink {
        private final TopicNameExtractor<K, V> topicNameExtractor;
        public Sink(final String name,
                    final TopicNameExtractor<K, V> topicNameExtractor) {
            super(name);
            this.topicNameExtractor = topicNameExtractor;
        }

        public Sink(final String name,
                    final String topic) {
            super(name);
            this.topicNameExtractor = new StaticTopicNameExtractor<>(topic);
        }

        @Override
        public String topic() {
            if (topicNameExtractor instanceof StaticTopicNameExtractor) {
                return ((StaticTopicNameExtractor<K, V>) topicNameExtractor).topicName;
            } else {
                return null;
            }
        }

        @Override
        public TopicNameExtractor<K, V> topicNameExtractor() {
            if (topicNameExtractor instanceof StaticTopicNameExtractor) {
                return null;
            } else {
                return topicNameExtractor;
            }
        }

        @Override
        public void addSuccessor(final TopologyDescription.Node successor) {
            throw new UnsupportedOperationException("Sinks don't have successors.");
        }

        @Override
        public String toString() {
            if (topicNameExtractor instanceof StaticTopicNameExtractor) {
                return "Sink: " + name + " (topic: " + topic() + ")\n      <-- " + nodeNames(predecessors);
            }
            return "Sink: " + name + " (extractor class: " + topicNameExtractor + ")\n      <-- "
                + nodeNames(predecessors);
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Sink<K, V> sink = (Sink<K, V>) o;
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

    public final static class SubtopologyDescription implements TopologyDescription.Subtopology {
        private final int id;
        private final Set<TopologyDescription.Node> nodes;

        public SubtopologyDescription(final int id, final Set<TopologyDescription.Node> nodes) {
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

            final SubtopologyDescription that = (SubtopologyDescription) o;
            return id == that.id
                && nodes.equals(that.nodes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, nodes);
        }
    }

    public static class TopicsInfo {
        public final Set<String> sinkTopics;
        public final Set<String> sourceTopics;
        public final Map<String, InternalTopicConfig> stateChangelogTopics;
        public final Map<String, InternalTopicConfig> repartitionSourceTopics;

        TopicsInfo(final Set<String> sinkTopics,
                   final Set<String> sourceTopics,
                   final Map<String, InternalTopicConfig> repartitionSourceTopics,
                   final Map<String, InternalTopicConfig> stateChangelogTopics) {
            this.sinkTopics = sinkTopics;
            this.sourceTopics = sourceTopics;
            this.stateChangelogTopics = stateChangelogTopics;
            this.repartitionSourceTopics = repartitionSourceTopics;
        }

        /**
         * Returns the config for any changelogs that must be prepared for this topic group, ie excluding any source
         * topics that are reused as a changelog
         */
        public Set<InternalTopicConfig> nonSourceChangelogTopics() {
            final Set<InternalTopicConfig> topicConfigs = new HashSet<>();
            for (final Map.Entry<String, InternalTopicConfig> changelogTopicEntry : stateChangelogTopics.entrySet()) {
                if (!sourceTopics.contains(changelogTopicEntry.getKey())) {
                    topicConfigs.add(changelogTopicEntry.getValue());
                }
            }
            return topicConfigs;
        }

        /**
         * Returns the topic names for any optimized source changelogs
         */
        public Set<String> sourceTopicChangelogs() {
            return sourceTopics.stream().filter(stateChangelogTopics::containsKey).collect(Collectors.toSet());
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
            if (globalStore1.equals(globalStore2)) {
                return 0;
            }
            return globalStore1.id() - globalStore2.id();
        }
    }

    private final static GlobalStoreComparator GLOBALSTORE_COMPARATOR = new GlobalStoreComparator();

    private static class SubtopologyComparator implements Comparator<TopologyDescription.Subtopology>, Serializable {
        @Override
        public int compare(final TopologyDescription.Subtopology subtopology1,
                           final TopologyDescription.Subtopology subtopology2) {
            if (subtopology1.equals(subtopology2)) {
                return 0;
            }
            return subtopology1.id() - subtopology2.id();
        }
    }

    private final static SubtopologyComparator SUBTOPOLOGY_COMPARATOR = new SubtopologyComparator();

    public final static class TopologyDescription implements org.apache.kafka.streams.TopologyDescription {
        private final TreeSet<TopologyDescription.Subtopology> subtopologies = new TreeSet<>(SUBTOPOLOGY_COMPARATOR);
        private final TreeSet<TopologyDescription.GlobalStore> globalStores = new TreeSet<>(GLOBALSTORE_COMPARATOR);
        private final String namedTopology;

        public TopologyDescription() {
            this(null);
        }

        public TopologyDescription(final String namedTopology) {
            this.namedTopology = namedTopology;
        }

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

            if (namedTopology == null) {
                sb.append("Topologies:\n ");
            } else {
                sb.append("Topology: ").append(namedTopology).append(":\n ");
            }
            final TopologyDescription.Subtopology[] sortedSubtopologies =
                subtopologies.descendingSet().toArray(new TopologyDescription.Subtopology[0]);
            final TopologyDescription.GlobalStore[] sortedGlobalStores =
                globalStores.descendingSet().toArray(new GlobalStore[0]);
            int expectedId = 0;
            int subtopologiesIndex = sortedSubtopologies.length - 1;
            int globalStoresIndex = sortedGlobalStores.length - 1;
            while (subtopologiesIndex != -1 && globalStoresIndex != -1) {
                sb.append("  ");
                final TopologyDescription.Subtopology subtopology = sortedSubtopologies[subtopologiesIndex];
                final TopologyDescription.GlobalStore globalStore = sortedGlobalStores[globalStoresIndex];
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
                final TopologyDescription.Subtopology subtopology = sortedSubtopologies[subtopologiesIndex];
                sb.append("  ");
                sb.append(subtopology);
                subtopologiesIndex--;
            }
            while (globalStoresIndex != -1) {
                final TopologyDescription.GlobalStore globalStore = sortedGlobalStores[globalStoresIndex];
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

    private Set<String> subscriptionUpdates() {
        return Collections.unmodifiableSet(subscriptionUpdates);
    }

    private boolean hasSubscriptionUpdates() {
        return !subscriptionUpdates.isEmpty();
    }

    synchronized void addSubscribedTopicsFromAssignment(final Set<TopicPartition> partitions, final String logPrefix) {
        if (usesPatternSubscription()) {
            final Set<String> assignedTopics = new HashSet<>();
            for (final TopicPartition topicPartition : partitions) {
                assignedTopics.add(topicPartition.topic());
            }

            final Collection<String> existingTopics = subscriptionUpdates();

            if  (!existingTopics.equals(assignedTopics)) {
                assignedTopics.addAll(existingTopics);
                updateSubscribedTopics(assignedTopics, logPrefix);
            }
        }
    }

    synchronized void addSubscribedTopicsFromMetadata(final Set<String> topics, final String logPrefix) {
        if (usesPatternSubscription() && !subscriptionUpdates().equals(topics)) {
            updateSubscribedTopics(topics, logPrefix);
        }
    }

    private void updateSubscribedTopics(final Set<String> topics, final String logPrefix) {
        subscriptionUpdates.clear();
        subscriptionUpdates.addAll(topics);

        log.debug("{}found {} topics possibly matching subscription", logPrefix, topics.size());

        setRegexMatchedTopicsToSourceNodes();
        setRegexMatchedTopicToStateStore();
    }

    /**
     * @return a copy of the string representation of any pattern subscribed source nodes
     */
    public synchronized List<String> allSourcePatternStrings() {
        return nodeToSourcePatterns.values().stream().map(Pattern::pattern).collect(Collectors.toList());
    }

    public boolean hasNamedTopology() {
        return topologyName != null;
    }

    public synchronized Map<String, StoreFactory> stateStores() {
        return stateFactories;
    }
}
