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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.SessionStoreBuilder;
import org.apache.kafka.streams.state.internals.TimestampedWindowStoreBuilder;
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
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class InternalTopologyBuilder {

    private static final Logger log = LoggerFactory.getLogger(InternalTopologyBuilder.class);
    private static final Pattern EMPTY_ZERO_LENGTH_PATTERN = Pattern.compile("");
    private static final String[] NO_PREDECESSORS = {};

    // node factories in a topological order
    private final Map<String, NodeFactory<?, ?>> nodeFactories = new LinkedHashMap<>();

    private final Map<String, StateStoreFactory<?>> stateFactories = new HashMap<>();

    private final Map<String, StoreBuilder<?>> globalStateBuilders = new LinkedHashMap<>();

    // built global state stores
    private final Map<String, StateStore> globalStateStores = new LinkedHashMap<>();

    // all topics subscribed from source processors (without application-id prefix for internal topics)
    private final Set<String> sourceTopicNames = new HashSet<>();

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

    // map from state store names to all the topics subscribed from source processors that
    // are connected to these state stores
    private final Map<String, Set<String>> stateStoreNameToSourceTopics = new HashMap<>();

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

    private Pattern sourceTopicPattern = null;

    private List<String> sourceTopicCollection = null;

    private Map<Integer, Set<String>> nodeGroups = null;

    public static class StateStoreFactory<S extends StateStore> {
        private final StoreBuilder<S> builder;
        private final Set<String> users = new HashSet<>();

        private StateStoreFactory(final StoreBuilder<S> builder) {
            this.builder = builder;
        }

        public S build() {
            return builder.build();
        }

        long retentionPeriod() {
            if (builder instanceof WindowStoreBuilder) {
                return ((WindowStoreBuilder<?, ?>) builder).retentionPeriod();
            } else if (builder instanceof TimestampedWindowStoreBuilder) {
                return ((TimestampedWindowStoreBuilder<?, ?>) builder).retentionPeriod();
            } else if (builder instanceof SessionStoreBuilder) {
                return ((SessionStoreBuilder<?, ?>) builder).retentionPeriod();
            } else {
                throw new IllegalStateException("retentionPeriod is not supported when not a window store");
            }
        }

        private Set<String> users() {
            return users;
        }

        public boolean loggingEnabled() {
            return builder.loggingEnabled();
        }

        private String name() {
            return builder.name();
        }

        private boolean isWindowStore() {
            return builder instanceof WindowStoreBuilder
                || builder instanceof TimestampedWindowStoreBuilder
                || builder instanceof SessionStoreBuilder;
        }

        // Apparently Java strips the generics from this method because we're using the raw type for builder,
        // even though this method doesn't use builder's (missing) type parameter. Our usage seems obviously
        // correct, though, hence the suppression.
        private Map<String, String> logConfig() {
            return builder.logConfig();
        }
    }

    private static abstract class NodeFactory<K, V> {
        final String name;
        final String[] predecessors;

        NodeFactory(final String name,
                    final String[] predecessors) {
            this.name = name;
            this.predecessors = predecessors;
        }

        public abstract ProcessorNode<K, V> build();

        abstract AbstractNode describe();
    }

    private static class ProcessorNodeFactory<K, V> extends NodeFactory<K, V> {
        private final ProcessorSupplier<K, V> supplier;
        private final Set<StoreBuilder> stateStoreBuilders = new HashSet<>();

        ProcessorNodeFactory(final String name,
                             final String[] predecessors,
                             final ProcessorSupplier<K, V> supplier) {
            super(name, predecessors.clone());
            this.supplier = supplier;
        }

        public void addStateStore(final StoreBuilder storeBuilder) {
            stateStoreBuilders.add(storeBuilder);
        }

        public Set<String> connectedStoreNames() {
            return stateStoreBuilders.stream().map(StoreBuilder::name).collect(Collectors.toSet());
        }

        @Override
        public ProcessorNode<K, V> build() {
            return new ProcessorNode<>(name, supplier.get(), connectedStoreNames());
        }

        @Override
        Processor describe() {
            final HashSet<Store> stores = new HashSet<>();
            for (final StoreBuilder<?> storeBuilder : stateStoreBuilders) {
                final List<String> serdeNames = storeBuilder.serdes() != null ?
                    storeBuilder.serdes().stream().map(s -> s != null ? s.getClass().getSimpleName() : "null").collect(Collectors.toList()) :
                    Collections.emptyList();
                final Store store = new Store(storeBuilder.name(), serdeNames);
                stores.add(store);
            }

            return new Processor(name, stores);
        }
    }

    // Map from topics to their matched regex patterns, this is to ensure one topic is passed through on source node
    // even if it can be matched by multiple regex patterns. Only used by SourceNodeFactory
    private final Map<String, Pattern> topicToPatterns = new HashMap<>();

    private class SourceNodeFactory<K, V> extends NodeFactory<K, V> {
        private final List<String> topics;
        private final Pattern pattern;
        private final Deserializer<K> keyDeserializer;
        private final Deserializer<V> valDeserializer;
        private final TimestampExtractor timestampExtractor;

        private SourceNodeFactory(final String name,
                                  final String[] topics,
                                  final Pattern pattern,
                                  final TimestampExtractor timestampExtractor,
                                  final Deserializer<K> keyDeserializer,
                                  final Deserializer<V> valDeserializer) {
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
        public ProcessorNode<K, V> build() {
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
            return new Source(name, new HashSet<>(topics), pattern,
                keyDeserializer != null ? keyDeserializer.getClass().getSimpleName() : "null",
                valDeserializer != null ? valDeserializer.getClass().getSimpleName() : "null");
        }
    }

    private class SinkNodeFactory<K, V> extends NodeFactory<K, V> {
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
        public ProcessorNode<K, V> build() {
            if (topicExtractor instanceof StaticTopicNameExtractor) {
                final String topic = ((StaticTopicNameExtractor<K, V>) topicExtractor).topicName;
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
        Sink<K, V> describe() {
            return new Sink<>(name, topicExtractor,
                keySerializer != null ? keySerializer.getClass().getSimpleName() : "null",
                valSerializer != null ? valSerializer.getClass().getSimpleName() : "null");
        }
    }

    // public for testing only
    public synchronized final InternalTopologyBuilder setApplicationId(final String applicationId) {
        Objects.requireNonNull(applicationId, "applicationId can't be null");
        this.applicationId = applicationId;

        return this;
    }

    public synchronized final InternalTopologyBuilder rewriteTopology(final StreamsConfig config) {
        Objects.requireNonNull(config, "config can't be null");

        // set application id
        setApplicationId(config.getString(StreamsConfig.APPLICATION_ID_CONFIG));

        // maybe strip out caching layers
        if (config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG) == 0L) {
            for (final StateStoreFactory<?> storeFactory : stateFactories.values()) {
                storeFactory.builder.withCachingDisabled();
            }

            for (final StoreBuilder<?> storeBuilder : globalStateBuilders.values()) {
                storeBuilder.withCachingDisabled();
            }
        }

        // build global state stores
        for (final StoreBuilder<?> storeBuilder : globalStateBuilders.values()) {
            globalStateStores.put(storeBuilder.name(), storeBuilder.build());
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
            sourceTopicNames.add(topic);
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

    public final void addProcessor(final String name,
                                   final ProcessorSupplier<?, ?> supplier,
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
                throw new TopologyException("Predecessor processor " + predecessor + " is not added yet for " + name);
            }
        }

        nodeFactories.put(name, new ProcessorNodeFactory<>(name, predecessorNames, supplier));
        nodeGrouper.add(name);
        nodeGrouper.unite(name, predecessorNames);
        nodeGroups = null;
    }

    public final void addStateStore(final StoreBuilder<?> storeBuilder,
                                    final String... processorNames) {
        addStateStore(storeBuilder, false, processorNames);
    }

    public final void addStateStore(final StoreBuilder<?> storeBuilder,
                                    final boolean allowOverride,
                                    final String... processorNames) {
        Objects.requireNonNull(storeBuilder, "storeBuilder can't be null");
        if (!allowOverride && stateFactories.containsKey(storeBuilder.name())) {
            throw new TopologyException("StateStore " + storeBuilder.name() + " is already added.");
        }

        stateFactories.put(storeBuilder.name(), new StateStoreFactory<>(storeBuilder));

        if (processorNames != null) {
            for (final String processorName : processorNames) {
                Objects.requireNonNull(processorName, "processor name must not be null");
                connectProcessorAndStateStore(processorName, storeBuilder.name());
            }
        }
        nodeGroups = null;
    }

    public final <K, V> void addGlobalStore(final StoreBuilder<?> storeBuilder,
                                            final String sourceName,
                                            final TimestampExtractor timestampExtractor,
                                            final Deserializer<K> keyDeserializer,
                                            final Deserializer<V> valueDeserializer,
                                            final String topic,
                                            final String processorName,
                                            final ProcessorSupplier<K, V> stateUpdateSupplier) {
        Objects.requireNonNull(storeBuilder, "store builder must not be null");
        validateGlobalStoreArguments(sourceName,
                                     topic,
                                     processorName,
                                     stateUpdateSupplier,
                                     storeBuilder.name(),
                                     storeBuilder.loggingEnabled());
        validateTopicNotAlreadyRegistered(topic);

        final String[] topics = {topic};
        final String[] predecessors = {sourceName};

        final ProcessorNodeFactory<K, V> nodeFactory = new ProcessorNodeFactory<>(
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
        nodeFactory.addStateStore(storeBuilder);
        nodeFactories.put(processorName, nodeFactory);
        nodeGrouper.add(processorName);
        nodeGrouper.unite(processorName, predecessors);
        globalStateBuilders.put(storeBuilder.name(), storeBuilder);
        connectSourceStoreAndTopic(storeBuilder.name(), topic);
        nodeGroups = null;
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
        nodeGroups = null;
    }

    public Map<String, String> getChangelogTopicToStore() {
        return changelogTopicToStore;
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
        copartitionSourceGroups.add(Collections.unmodifiableSet(new HashSet<>(sourceNodes)));
    }

    private void validateGlobalStoreArguments(final String sourceName,
                                              final String topic,
                                              final String processorName,
                                              final ProcessorSupplier<?, ?> stateUpdateSupplier,
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
        if (stateFactories.containsKey(storeName) || globalStateBuilders.containsKey(storeName)) {
            throw new TopologyException("StateStore " + storeName + " is already added.");
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

        final StateStoreFactory<?> stateStoreFactory = stateFactories.get(stateStoreName);
        final Iterator<String> iter = stateStoreFactory.users().iterator();
        if (iter.hasNext()) {
            final String user = iter.next();
            nodeGrouper.unite(user, processorName);
        }
        stateStoreFactory.users().add(processorName);

        final NodeFactory<?, ?> nodeFactory = nodeFactories.get(processorName);
        if (nodeFactory instanceof ProcessorNodeFactory) {
            final ProcessorNodeFactory<?, ?> processorNodeFactory = (ProcessorNodeFactory<?, ?>) nodeFactory;
            processorNodeFactory.addStateStore(stateStoreFactory.builder);
            connectStateStoreNameToSourceTopicsOrPattern(stateStoreName, processorNodeFactory);
        } else {
            throw new TopologyException("cannot connect a state store " + stateStoreName + " to a source node or a sink node.");
        }
    }

    private Set<SourceNodeFactory<?, ?>> findSourcesForProcessorPredecessors(final String[] predecessors) {
        final Set<SourceNodeFactory<?, ?>> sourceNodes = new HashSet<>();
        for (final String predecessor : predecessors) {
            final NodeFactory<?, ?> nodeFactory = nodeFactories.get(predecessor);
            if (nodeFactory instanceof SourceNodeFactory) {
                sourceNodes.add((SourceNodeFactory<?, ?>) nodeFactory);
            } else if (nodeFactory instanceof ProcessorNodeFactory) {
                sourceNodes.addAll(findSourcesForProcessorPredecessors(((ProcessorNodeFactory<?, ?>) nodeFactory).predecessors));
            }
        }
        return sourceNodes;
    }

    private <K, V> void connectStateStoreNameToSourceTopicsOrPattern(final String stateStoreName,
                                                                     final ProcessorNodeFactory<K, V> processorNodeFactory) {
        // we should never update the mapping from state store names to source topics if the store name already exists
        // in the map; this scenario is possible, for example, that a state store underlying a source KTable is
        // connecting to a join operator whose source topic is not the original KTable's source topic but an internal repartition topic.

        if (stateStoreNameToSourceTopics.containsKey(stateStoreName)
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

    private ProcessorTopology build(final Set<String> nodeGroup) {
        Objects.requireNonNull(applicationId, "topology has not completed optimization");

        final Map<String, ProcessorNode<?, ?>> processorMap = new LinkedHashMap<>();
        final Map<String, SourceNode<?, ?>> topicSourceMap = new HashMap<>();
        final Map<String, SinkNode<?, ?>> topicSinkMap = new HashMap<>();
        final Map<String, StateStore> stateStoreMap = new LinkedHashMap<>();
        final Set<String> repartitionTopics = new HashSet<>();

        // create processor nodes in a topological order ("nodeFactories" is already topologically sorted)
        // also make sure the state store map values following the insertion ordering
        for (final NodeFactory<?, ?> factory : nodeFactories.values()) {
            if (nodeGroup == null || nodeGroup.contains(factory.name)) {
                final ProcessorNode<?, ?> node = factory.build();
                processorMap.put(node.name(), node);

                if (factory instanceof ProcessorNodeFactory) {
                    buildProcessorNode(processorMap,
                                       stateStoreMap,
                                       (ProcessorNodeFactory<?, ?>) factory,
                                       node);

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

    private void buildSinkNode(final Map<String, ProcessorNode<?, ?>> processorMap,
                               final Map<String, SinkNode<?, ?>> topicSinkMap,
                               final Set<String> repartitionTopics,
                               final SinkNodeFactory<?, ?> sinkNodeFactory,
                               final SinkNode<?, ?> node) {

        for (final String predecessor : sinkNodeFactory.predecessors) {
            processorMap.get(predecessor).addChild(node);
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

    private void buildProcessorNode(final Map<String, ProcessorNode<?, ?>> processorMap,
                                    final Map<String, StateStore> stateStoreMap,
                                    final ProcessorNodeFactory<?, ?> factory,
                                    final ProcessorNode<?, ?> node) {

        for (final String predecessor : factory.predecessors) {
            final ProcessorNode<?, ?> predecessorNode = processorMap.get(predecessor);
            predecessorNode.addChild(node);
        }
        for (final StoreBuilder storeBuilder : factory.stateStoreBuilders) {
            final String stateStoreName = storeBuilder.name();
            if (!stateStoreMap.containsKey(stateStoreName)) {
                if (stateFactories.containsKey(stateStoreName)) {
                    final StateStoreFactory<?> stateStoreFactory = stateFactories.get(stateStoreName);

                    // remember the changelog topic if this state store is change-logging enabled
                    if (stateStoreFactory.loggingEnabled() && !storeToChangelogTopic.containsKey(stateStoreName)) {
                        final String changelogTopic = ProcessorStateManager.storeChangelogTopic(applicationId, stateStoreName);
                        storeToChangelogTopic.put(stateStoreName, changelogTopic);
                        changelogTopicToStore.put(changelogTopic, stateStoreName);
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
        Objects.requireNonNull(applicationId, "topology has not completed optimization");

        return Collections.unmodifiableMap(globalStateStores);
    }

    public Set<String> allStateStoreName() {
        Objects.requireNonNull(applicationId, "topology has not completed optimization");

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
                for (final StateStoreFactory<?> stateFactory : stateFactories.values()) {
                    if (stateFactory.users().contains(node) && storeToChangelogTopic.containsKey(stateFactory.name())) {
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
                topicGroups.put(entry.getKey(), new TopicsInfo(
                        Collections.unmodifiableSet(sinkTopics),
                        Collections.unmodifiableSet(sourceTopics),
                        Collections.unmodifiableMap(repartitionTopics),
                        Collections.unmodifiableMap(stateChangelogTopics)));
            }
        }

        return Collections.unmodifiableMap(topicGroups);
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
                sourceTopicNames.addAll(sourceTopics);
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
                    final Collection<String> storeTopics = stateStoreNameToSourceTopics.get(storePattern.getKey());
                    if (storeTopics != null) {
                        updatedTopicsForStateStore.addAll(storeTopics);
                    }
                    stateStoreNameToSourceTopics.put(
                        storePattern.getKey(),
                        Collections.unmodifiableSet(updatedTopicsForStateStore));
                }
            }
        }
    }

    private <S extends StateStore> InternalTopicConfig createChangelogTopicConfig(final StateStoreFactory<S> factory,
                                                                                  final String name) {
        if (factory.isWindowStore()) {
            final WindowedChangelogTopicConfig config = new WindowedChangelogTopicConfig(name, factory.logConfig());
            config.setRetentionMs(factory.retentionPeriod());
            return config;
        } else {
            return new UnwindowedChangelogTopicConfig(name, factory.logConfig());
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

        return buildPattern(topics, resetPatterns);
    }

    private static Pattern buildPattern(final Collection<String> sourceTopics,
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

    public Collection<String> sourceTopicsForStore(final String storeName) {
        return maybeDecorateInternalSourceTopics(stateStoreNameToSourceTopics.get(storeName));
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

    private String decorateTopic(final String topic) {
        if (applicationId == null) {
            throw new TopologyException("there are internal topics and "
                    + "applicationId hasn't been set. Call "
                    + "setApplicationId first");
        }

        return applicationId + "-" + topic;
    }

    void initializeSubscription() {
        if (usesPatternSubscription()) {
            log.debug("Found pattern subscribed source topics, initializing consumer's subscription pattern.");
            final List<String> allSourceTopics = maybeDecorateInternalSourceTopics(sourceTopicNames);
            Collections.sort(allSourceTopics);
            sourceTopicPattern = buildPattern(allSourceTopics, nodeToSourcePatterns.values());
        } else {
            log.debug("No source topics using pattern subscription found, initializing consumer's subscription collection.");
            sourceTopicCollection = maybeDecorateInternalSourceTopics(sourceTopicNames);
            Collections.sort(sourceTopicCollection);
        }
    }

    boolean usesPatternSubscription() {
        return !nodeToSourcePatterns.isEmpty();
    }

    synchronized Collection<String> sourceTopicCollection() {
        return sourceTopicCollection;
    }

    synchronized Pattern sourceTopicPattern() {
        return sourceTopicPattern;
    }

    private boolean isGlobalSource(final String nodeName) {
        final NodeFactory<?, ?> nodeFactory = nodeFactories.get(nodeName);

        if (nodeFactory instanceof SourceNodeFactory) {
            final List<String> topics = ((SourceNodeFactory<?, ?>) nodeFactory).topics;
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

    private void describeGlobalStore(final TopologyDescription description,
                                     final Set<String> nodes,
                                     final int id) {
        final Iterator<String> it = nodes.iterator();
        while (it.hasNext()) {
            final String node = it.next();

            if (isGlobalSource(node)) {
                it.remove(); // remove sourceNode from group
                final String processorNode = nodes.iterator().next(); // get remaining processorNode

                description.addGlobalStore(new GlobalStore(
                    node,
                    processorNode,
                    ((ProcessorNodeFactory<?, ?>) nodeFactories.get(processorNode)).connectedStoreNames().iterator().next(),
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
            final NodeFactory node = nodeFactories.get(nodeName);
            nodesByName.put(nodeName, node.describe());
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
                new HashSet<>(nodesByName.values())));
    }

    public final static class GlobalStore implements TopologyDescription.GlobalStore {
        private final Source source;
        private final Processor processor;
        private final int id;

        public GlobalStore(final String sourceNodeName,
                           final String processorNodeName,
                           final String sourceTopicName,
                           final String storeName,
                           final int id) {
            source = new Source(sourceNodeName,
                Collections.singleton(sourceTopicName),
                null,
                "null",
                "null");

            processor = new Processor(processorNodeName, Collections.singleton(new Store(storeName, Arrays.asList("null", "null"))));
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
        private final String keySerdeName;
        private final String valueSerdeName;

        public Source(final String name,
                      final Set<String> topics,
                      final Pattern pattern) {
            this(name, topics, pattern, null, null);
        }

        public Source(final String name,
                      final Set<String> topics,
                      final Pattern pattern,
                      final String keySerdeName,
                      final String valueSerdeName) {
            super(name);
            if (topics == null && pattern == null) {
                throw new IllegalArgumentException("Either topics or pattern must be not-null, but both are null.");
            }
            if (topics != null && pattern != null) {
                throw new IllegalArgumentException("Either topics or pattern must be null, but both are not null.");
            }

            this.topics = topics;
            this.topicPattern = pattern;
            this.keySerdeName = keySerdeName;
            this.valueSerdeName = valueSerdeName;
        }

        @Deprecated
        @Override
        public String topics() {
            return topics.toString();
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
        public String keySerdeName() {
            return keySerdeName;
        }

        @Override
        public String valueSerdeName() {
            return valueSerdeName;
        }

        @Override
        public String toString() {
            final String topicsString = topics == null ? topicPattern.toString() : topics.toString();

            return "Source: " + name + " (topics: " + topicsString + ", keySerde: " + keySerdeName + ", valueSerde: " + valueSerdeName + ")\n      --> " + nodeNames(successors);
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

    public final static class Store implements TopologyDescription.Store {
        private final String name;
        private final List<String> serdeNames;

        public Store(final String name, final List<String> serdeNames) {
            this.name = name;
            this.serdeNames = serdeNames;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public List<String> serdeNames() {
            return serdeNames;
        }

        @Override
        public String toString() {
            return "(" + name + ", serdes: " + serdeNames + ")";
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Store store = (Store) o;
            return name.equals(store.name) && serdeNames.equals(store.serdeNames);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, serdeNames);
        }
    }

    public final static class Processor extends AbstractNode implements TopologyDescription.Processor {
        private final Set<Store> stores;

        public Processor(final String name,
                         final Set<Store> stores) {
            super(name);
            this.stores = stores;
        }

        @Deprecated
        @Override
        public Set<String> stores() {
            return Collections.unmodifiableSet(stores.stream().map(Store::name).collect(Collectors.toSet()));
        }

        @Override
        public Set<org.apache.kafka.streams.TopologyDescription.Store> storeSet() {
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
        private final TopicNameExtractor topicNameExtractor;
        private final String keySerdeName;
        private final String valueSerdeName;

        public Sink(final String name,
                    final TopicNameExtractor topicNameExtractor) {
            this(name, topicNameExtractor, null, null);
        }

        public Sink(final String name,
                    final String topic) {
            this(name, topic, null, null);
        }

        public Sink(final String name,
                    final TopicNameExtractor topicNameExtractor,
                    final String keySerdeName,
                    final String valueSerdeName) {
            super(name);
            this.topicNameExtractor = topicNameExtractor;
            this.keySerdeName = keySerdeName;
            this.valueSerdeName = valueSerdeName;
        }

        public Sink(final String name,
                    final String topic,
                    final String keySerdeName,
                    final String valueSerdeName) {
            super(name);
            this.topicNameExtractor = new StaticTopicNameExtractor<>(topic);
            this.keySerdeName = keySerdeName;
            this.valueSerdeName = valueSerdeName;
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
        public String keySerdeName() {
            return keySerdeName;
        }

        @Override
        public String valueSerdeName() {
            return valueSerdeName;
        }

        @Override
        public String toString() {
            if (topicNameExtractor instanceof StaticTopicNameExtractor) {
                return "Sink: " + name + " (topic: " + topic() + ", keySerde: " + keySerdeName + ", valueSerde: " + valueSerdeName + ")\n      <-- " + nodeNames(predecessors);
            }
            return "Sink: " + name + " (extractor class: " + topicNameExtractor + ", keySerde: " + keySerdeName + ", valueSerde: " + valueSerdeName + ")\n      <-- "
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
        final Set<String> sinkTopics;
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
                subtopologies.descendingSet().toArray(new Subtopology[0]);
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

    synchronized void addSubscribedTopicsFromAssignment(final List<TopicPartition> partitions, final String logPrefix) {
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

    // following functions are for test only
    public synchronized Set<String> sourceTopicNames() {
        return sourceTopicNames;
    }

    public synchronized Map<String, StateStoreFactory<?>> stateStores() {
        return stateFactories;
    }
}
