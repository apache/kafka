/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.internals.InternalTopicConfig;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.QuickUnion;
import org.apache.kafka.streams.processor.internals.SinkNode;
import org.apache.kafka.streams.processor.internals.SourceNode;
import org.apache.kafka.streams.processor.internals.StreamPartitionAssignor.SubscriptionUpdates;
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


/**
 * A component that is used to build a {@link ProcessorTopology}. A topology contains an acyclic graph of sources, processors,
 * and sinks. A {@link SourceNode source} is a node in the graph that consumes one or more Kafka topics and forwards them to
 * its child nodes. A {@link Processor processor} is a node in the graph that receives input records from upstream nodes,
 * processes that records, and optionally forwarding new records to one or all of its children. Finally, a {@link SinkNode sink}
 * is a node in the graph that receives records from upstream nodes and writes them to a Kafka topic. This builder allows you
 * to construct an acyclic graph of these nodes, and the builder is then passed into a new {@link org.apache.kafka.streams.KafkaStreams}
 * instance that will then {@link org.apache.kafka.streams.KafkaStreams#start() begin consuming, processing, and producing records}.
 */
public class TopologyBuilder {

    private static final Logger log = LoggerFactory.getLogger(TopologyBuilder.class);

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

        StateStoreFactory(StateStoreSupplier supplier) {
            this.supplier = supplier;
            this.users = new HashSet<>();
        }
    }

    private static abstract class NodeFactory {
        public final String name;

        NodeFactory(String name) {
            this.name = name;
        }

        public abstract ProcessorNode build();
    }

    private static class ProcessorNodeFactory extends NodeFactory {
        private final String[] parents;
        private final ProcessorSupplier<?, ?> supplier;
        private final Set<String> stateStoreNames = new HashSet<>();

        ProcessorNodeFactory(String name, String[] parents, ProcessorSupplier<?, ?> supplier) {
            super(name);
            this.parents = parents.clone();
            this.supplier = supplier;
        }

        public void addStateStore(String stateStoreName) {
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

        private SourceNodeFactory(String name, String[] topics, Pattern pattern, Deserializer<?> keyDeserializer, Deserializer<?> valDeserializer) {
            super(name);
            this.topics = topics != null ? Arrays.asList(topics) : null;
            this.pattern = pattern;
            this.keyDeserializer = keyDeserializer;
            this.valDeserializer = valDeserializer;
        }

        List<String> getTopics(Collection<String> subscribedTopics) {
            // if it is subscribed via patterns, it is possible that the topic metadata has not been updated
            // yet and hence the map from source node to topics is stale, in this case we put the pattern as a place holder;
            // this should only happen for debugging since during runtime this function should always be called after the metadata has updated.
            if (subscribedTopics.isEmpty())
                return Collections.singletonList("Pattern[" + pattern + "]");

            List<String> matchedTopics = new ArrayList<>();
            for (String update : subscribedTopics) {
                if (this.pattern == topicToPatterns.get(update)) {
                    matchedTopics.add(update);
                } else if (topicToPatterns.containsKey(update) && isMatch(update)) {
                    // the same topic cannot be matched to more than one pattern
                    // TODO: we should lift this requirement in the future
                    throw new TopologyBuilderException("Topic " + update +
                            " is already matched for another regex pattern " + topicToPatterns.get(update) +
                            " and hence cannot be matched to this regex pattern " + pattern + " any more.");
                } else if (isMatch(update)) {
                    topicToPatterns.put(update, this.pattern);
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
            if (sourceTopics == null)
                return new SourceNode<>(name, Collections.singletonList("Pattern[" + pattern + "]"), keyDeserializer, valDeserializer);
            else
                return new SourceNode<>(name, maybeDecorateInternalSourceTopics(sourceTopics), keyDeserializer, valDeserializer);
        }

        private boolean isMatch(String topic) {
            return this.pattern.matcher(topic).matches();
        }
    }

    private class SinkNodeFactory<K, V> extends NodeFactory {
        private final String[] parents;
        private final String topic;
        private final Serializer<K> keySerializer;
        private final Serializer<V> valSerializer;
        private final StreamPartitioner<? super K, ? super V> partitioner;

        private SinkNodeFactory(String name, String[] parents, String topic, Serializer<K> keySerializer, Serializer<V> valSerializer, StreamPartitioner<? super K, ? super V> partitioner) {
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

    public static class TopicsInfo {
        public Set<String> sinkTopics;
        public Set<String> sourceTopics;
        public Map<String, InternalTopicConfig> stateChangelogTopics;
        public Map<String, InternalTopicConfig> repartitionSourceTopics;

        TopicsInfo(Set<String> sinkTopics, Set<String> sourceTopics, Map<String, InternalTopicConfig> repartitionSourceTopics, Map<String, InternalTopicConfig> stateChangelogTopics) {
            this.sinkTopics = sinkTopics;
            this.sourceTopics = sourceTopics;
            this.stateChangelogTopics = stateChangelogTopics;
            this.repartitionSourceTopics = repartitionSourceTopics;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof TopicsInfo) {
                TopicsInfo other = (TopicsInfo) o;
                return other.sourceTopics.equals(this.sourceTopics) && other.stateChangelogTopics.equals(this.stateChangelogTopics);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            long n = ((long) sourceTopics.hashCode() << 32) | (long) stateChangelogTopics.hashCode();
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

    /**
     * Enum used to define auto offset reset policy when creating {@link KStream} or {@link KTable}
     */
    public enum AutoOffsetReset {
        EARLIEST , LATEST
    }

    /**
     * Create a new builder.
     */
    public TopologyBuilder() {}

    /**
     * Set the applicationId to be used for auto-generated internal topics.
     *
     * This is required before calling {@link #topicGroups}, {@link #copartitionSources},
     * {@link #stateStoreNameToSourceTopics} and {@link #build(Integer)}.
     *
     * @param applicationId the streams applicationId. Should be the same as set by
     * {@link org.apache.kafka.streams.StreamsConfig#APPLICATION_ID_CONFIG}
     */
    public synchronized final TopologyBuilder setApplicationId(String applicationId) {
        Objects.requireNonNull(applicationId, "applicationId can't be null");
        this.applicationId = applicationId;

        return this;
    }

    /**
     * Add a new source that consumes the named topics and forward the records to child processor and/or sink nodes.
     * The source will use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     *
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topics the name of one or more Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder addSource(String name, String... topics) {
        return addSource(null, name, null, null, topics);
    }

    /**
     * Add a new source that consumes the named topics and forward the records to child processor and/or sink nodes.
     * The source will use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     *
     * @param offsetReset the auto offset reset policy to use for this source if no committed offsets found; acceptable values earliest or latest
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topics the name of one or more Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder addSource(AutoOffsetReset offsetReset, String name,  String... topics) {
        return addSource(offsetReset, name, null, null, topics);
    }


    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forward the records to child processor and/or sink nodes.
     * The source will use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     *
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topicPattern regular expression pattern to match Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder addSource(String name, Pattern topicPattern) {
        return addSource(null, name, null, null, topicPattern);
    }

    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forward the records to child processor and/or sink nodes.
     * The source will use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     *
     * @param offsetReset the auto offset reset policy value for this source if no committed offsets found; acceptable values earliest or latest.
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topicPattern regular expression pattern to match Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder addSource(AutoOffsetReset offsetReset, String name,  Pattern topicPattern) {
        return addSource(offsetReset, name, null, null, topicPattern);
    }


    /**
     * Add a new source that consumes the named topics and forwards the records to child processor and/or sink nodes.
     * The source will use the specified key and value deserializers.
     *
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param keyDeserializer the {@link Deserializer key deserializer} used when consuming records; may be null if the source
     * should use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param valDeserializer the {@link Deserializer value deserializer} used when consuming records; may be null if the source
     * should use the {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param topics the name of one or more Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if processor is already added or if topics have already been registered by another source
     */
    public synchronized final TopologyBuilder addSource(String name, Deserializer keyDeserializer, Deserializer valDeserializer, String... topics) {
        return addSource(null, name, keyDeserializer, valDeserializer, topics);

    }

    /**
     * Add a new source that consumes the named topics and forwards the records to child processor and/or sink nodes.
     * The source will use the specified key and value deserializers.
     *
     * @param offsetReset the auto offset reset policy to use for this stream if no committed offsets found; acceptable values are earliest or latest.
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param keyDeserializer the {@link Deserializer key deserializer} used when consuming records; may be null if the source
     * should use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param valDeserializer the {@link Deserializer value deserializer} used when consuming records; may be null if the source
     * should use the {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param topics the name of one or more Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if processor is already added or if topics have already been registered by another source
     */
    public synchronized final TopologyBuilder addSource(AutoOffsetReset offsetReset, String name, Deserializer keyDeserializer, Deserializer valDeserializer, String... topics) {
        if (topics.length == 0) {
            throw new TopologyBuilderException("You must provide at least one topic");
        }
        Objects.requireNonNull(name, "name must not be null");
        if (nodeFactories.containsKey(name))
            throw new TopologyBuilderException("Processor " + name + " is already added.");

        for (String topic : topics) {
            Objects.requireNonNull(topic, "topic names cannot be null");
            validateTopicNotAlreadyRegistered(topic);
            maybeAddToResetList(earliestResetTopics, latestResetTopics, offsetReset, topic);
            sourceTopicNames.add(topic);
        }

        nodeFactories.put(name, new SourceNodeFactory(name, topics, null, keyDeserializer, valDeserializer));
        nodeToSourceTopics.put(name, Arrays.asList(topics));
        nodeGrouper.add(name);

        return this;
    }

    /**
     * Adds a global {@link StateStore} to the topology. The {@link StateStore} sources its data
     * from all partitions of the provided input topic. There will be exactly one instance of this
     * {@link StateStore} per Kafka Streams instance.
     * <p>
     * A {@link SourceNode} with the provided sourceName will be added to consume the data arriving
     * from the partitions of the input topic.
     * <p>
     * The provided {@link ProcessorSupplier} will be used to create an {@link ProcessorNode} that will
     * receive all records forwarded from the {@link SourceNode}. This
     * {@link ProcessorNode} should be used to keep the {@link StateStore} up-to-date.
     *
     * @param store                 the instance of {@link StateStore}
     * @param sourceName            name of the {@link SourceNode} that will be automatically added
     * @param keyDeserializer       the {@link Deserializer} to deserialize keys with
     * @param valueDeserializer     the {@link Deserializer} to deserialize values with
     * @param topic                 the topic to source the data from
     * @param processorName         the name of the {@link ProcessorSupplier}
     * @param stateUpdateSupplier   the instance of {@link ProcessorSupplier}
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized TopologyBuilder addGlobalStore(final StateStore store,
                                                       final String sourceName,
                                                       final Deserializer keyDeserializer,
                                                       final Deserializer valueDeserializer,
                                                       final String topic,
                                                       final String processorName,
                                                       final ProcessorSupplier stateUpdateSupplier) {
        Objects.requireNonNull(store, "store must not be null");
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
        if (stateFactories.containsKey(store.name()) || globalStateStores.containsKey(store.name())) {
            throw new TopologyBuilderException("StateStore " + store.name() + " is already added.");
        }

        validateTopicNotAlreadyRegistered(topic);

        globalTopics.add(topic);
        final String[] topics = {topic};
        nodeFactories.put(sourceName, new SourceNodeFactory(sourceName, topics, null, keyDeserializer, valueDeserializer));
        nodeToSourceTopics.put(sourceName, Arrays.asList(topics));
        nodeGrouper.add(sourceName);

        final String[] parents = {sourceName};
        final ProcessorNodeFactory nodeFactory = new ProcessorNodeFactory(processorName, parents, stateUpdateSupplier);
        nodeFactory.addStateStore(store.name());
        nodeFactories.put(processorName, nodeFactory);
        nodeGrouper.add(processorName);
        nodeGrouper.unite(processorName, parents);

        globalStateStores.put(store.name(), store);
        connectSourceStoreAndTopic(store.name(), topic);
        return this;

    }

    private void validateTopicNotAlreadyRegistered(final String topic) {
        if (sourceTopicNames.contains(topic) || globalTopics.contains(topic)) {
            throw new TopologyBuilderException("Topic " + topic + " has already been registered by another source.");
        }

        for (Pattern pattern : nodeToSourcePatterns.values()) {
            if (pattern.matcher(topic).matches()) {
                throw new TopologyBuilderException("Topic " + topic + " matches a Pattern already registered by another source.");
            }
        }
    }

    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forwards the records to child processor and/or sink nodes.
     * The source will use the specified key and value deserializers. The provided
     * de-/serializers will be used for all matched topics, so care should be taken to specify patterns for
     * topics that share the same key-value data format.
     *
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param keyDeserializer the {@link Deserializer key deserializer} used when consuming records; may be null if the source
     * should use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param valDeserializer the {@link Deserializer value deserializer} used when consuming records; may be null if the source
     * should use the {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param topicPattern regular expression pattern to match Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if processor is already added or if topics have already been registered by name
     */

    public synchronized final TopologyBuilder addSource(String name, Deserializer keyDeserializer, Deserializer valDeserializer, Pattern topicPattern) {
        return addSource(null, name,  keyDeserializer, valDeserializer, topicPattern);

    }

    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forwards the records to child processor and/or sink nodes.
     * The source will use the specified key and value deserializers. The provided
     * de-/serializers will be used for all matched topics, so care should be taken to specify patterns for
     * topics that share the same key-value data format.
     *
     * @param offsetReset  the auto offset reset policy to use for this stream if no committed offsets found; acceptable values are earliest or latest
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param keyDeserializer the {@link Deserializer key deserializer} used when consuming records; may be null if the source
     * should use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param valDeserializer the {@link Deserializer value deserializer} used when consuming records; may be null if the source
     * should use the {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param topicPattern regular expression pattern to match Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if processor is already added or if topics have already been registered by name
     */

    public synchronized final TopologyBuilder addSource(AutoOffsetReset offsetReset, String name,  Deserializer keyDeserializer, Deserializer valDeserializer, Pattern topicPattern) {
        Objects.requireNonNull(topicPattern, "topicPattern can't be null");
        Objects.requireNonNull(name, "name can't be null");

        if (nodeFactories.containsKey(name)) {
            throw new TopologyBuilderException("Processor " + name + " is already added.");
        }

        for (String sourceTopicName : sourceTopicNames) {
            if (topicPattern.matcher(sourceTopicName).matches()) {
                throw new TopologyBuilderException("Pattern  " + topicPattern + " will match a topic that has already been registered by another source.");
            }
        }

        maybeAddToResetList(earliestResetPatterns, latestResetPatterns, offsetReset, topicPattern);

        nodeFactories.put(name, new SourceNodeFactory(name, null, topicPattern, keyDeserializer, valDeserializer));
        nodeToSourcePatterns.put(name, topicPattern);
        nodeGrouper.add(name);

        return this;
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic.
     * The sink will use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key serializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its records
     * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
     * and write to its topic
     * @return this builder instance so methods can be chained together; never null
     * @see #addSink(String, String, StreamPartitioner, String...)
     * @see #addSink(String, String, Serializer, Serializer, String...)
     * @see #addSink(String, String, Serializer, Serializer, StreamPartitioner, String...)
     */
    public synchronized final TopologyBuilder addSink(String name, String topic, String... parentNames) {
        return addSink(name, topic, null, null, parentNames);
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic, using
     * the supplied partitioner.
     * The sink will use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key serializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     * <p>
     * The sink will also use the specified {@link StreamPartitioner} to determine how records are distributed among
     * the named Kafka topic's partitions. Such control is often useful with topologies that use
     * {@link #addStateStore(StateStoreSupplier, String...) state stores}
     * in its processors. In most other cases, however, a partitioner needs not be specified and Kafka will automatically distribute
     * records among partitions using Kafka's default partitioning logic.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its records
     * @param partitioner the function that should be used to determine the partition for each record processed by the sink
     * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
     * and write to its topic
     * @return this builder instance so methods can be chained together; never null
     * @see #addSink(String, String, String...)
     * @see #addSink(String, String, Serializer, Serializer, String...)
     * @see #addSink(String, String, Serializer, Serializer, StreamPartitioner, String...)
     */
    public synchronized final TopologyBuilder addSink(String name, String topic, StreamPartitioner partitioner, String... parentNames) {
        return addSink(name, topic, null, null, partitioner, parentNames);
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic.
     * The sink will use the specified key and value serializers.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its records
     * @param keySerializer the {@link Serializer key serializer} used when consuming records; may be null if the sink
     * should use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param valSerializer the {@link Serializer value serializer} used when consuming records; may be null if the sink
     * should use the {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
     * and write to its topic
     * @return this builder instance so methods can be chained together; never null
     * @see #addSink(String, String, String...)
     * @see #addSink(String, String, StreamPartitioner, String...)
     * @see #addSink(String, String, Serializer, Serializer, StreamPartitioner, String...)
     */
    public synchronized final TopologyBuilder addSink(String name, String topic, Serializer keySerializer, Serializer valSerializer, String... parentNames) {
        return addSink(name, topic, keySerializer, valSerializer, null, parentNames);
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic.
     * The sink will use the specified key and value serializers, and the supplied partitioner.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its records
     * @param keySerializer the {@link Serializer key serializer} used when consuming records; may be null if the sink
     * should use the {@link org.apache.kafka.streams.StreamsConfig#KEY_SERDE_CLASS_CONFIG default key serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param valSerializer the {@link Serializer value serializer} used when consuming records; may be null if the sink
     * should use the {@link org.apache.kafka.streams.StreamsConfig#VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param partitioner the function that should be used to determine the partition for each record processed by the sink
     * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
     * and write to its topic
     * @return this builder instance so methods can be chained together; never null
     * @see #addSink(String, String, String...)
     * @see #addSink(String, String, StreamPartitioner, String...)
     * @see #addSink(String, String, Serializer, Serializer, String...)
     * @throws TopologyBuilderException if parent processor is not added yet, or if this processor's name is equal to the parent's name
     */
    public synchronized final <K, V> TopologyBuilder addSink(String name, String topic, Serializer<K> keySerializer, Serializer<V> valSerializer, StreamPartitioner<? super K, ? super V> partitioner, String... parentNames) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(topic, "topic must not be null");
        if (nodeFactories.containsKey(name))
            throw new TopologyBuilderException("Processor " + name + " is already added.");

        if (parentNames != null) {
            for (String parent : parentNames) {
                if (parent.equals(name)) {
                    throw new TopologyBuilderException("Processor " + name + " cannot be a parent of itself.");
                }
                if (!nodeFactories.containsKey(parent)) {
                    throw new TopologyBuilderException("Parent processor " + parent + " is not added yet.");
                }
            }
        }

        nodeFactories.put(name, new SinkNodeFactory<>(name, parentNames, topic, keySerializer, valSerializer, partitioner));
        nodeToSinkTopic.put(name, topic);
        nodeGrouper.add(name);
        nodeGrouper.unite(name, parentNames);
        return this;
    }

    /**
     * Add a new processor node that receives and processes records output by one or more parent source or processor node.
     * Any new record output by this processor will be forwarded to its child processor or sink nodes.
     * @param name the unique name of the processor node
     * @param supplier the supplier used to obtain this node's {@link Processor} instance
     * @param parentNames the name of one or more source or processor nodes whose output records this processor should receive
     * and process
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if parent processor is not added yet, or if this processor's name is equal to the parent's name
     */
    public synchronized final TopologyBuilder addProcessor(String name, ProcessorSupplier supplier, String... parentNames) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        if (nodeFactories.containsKey(name))
            throw new TopologyBuilderException("Processor " + name + " is already added.");

        if (parentNames != null) {
            for (String parent : parentNames) {
                if (parent.equals(name)) {
                    throw new TopologyBuilderException("Processor " + name + " cannot be a parent of itself.");
                }
                if (!nodeFactories.containsKey(parent)) {
                    throw new TopologyBuilderException("Parent processor " + parent + " is not added yet.");
                }
            }
        }

        nodeFactories.put(name, new ProcessorNodeFactory(name, parentNames, supplier));
        nodeGrouper.add(name);
        nodeGrouper.unite(name, parentNames);
        return this;
    }
    /**
     * Adds a state store
     *
     * @param supplier the supplier used to obtain this state store {@link StateStore} instance
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if state store supplier is already added
     */
    public synchronized final TopologyBuilder addStateStore(StateStoreSupplier supplier, String... processorNames) {
        Objects.requireNonNull(supplier, "supplier can't be null");
        if (stateFactories.containsKey(supplier.name())) {
            throw new TopologyBuilderException("StateStore " + supplier.name() + " is already added.");
        }

        stateFactories.put(supplier.name(), new StateStoreFactory(supplier));

        if (processorNames != null) {
            for (String processorName : processorNames) {
                connectProcessorAndStateStore(processorName, supplier.name());
            }
        }

        return this;
    }

    /**
     * Connects the processor and the state stores
     *
     * @param processorName the name of the processor
     * @param stateStoreNames the names of state stores that the processor uses
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder connectProcessorAndStateStores(String processorName, String... stateStoreNames) {
        Objects.requireNonNull(processorName, "processorName can't be null");
        if (stateStoreNames != null) {
            for (String stateStoreName : stateStoreNames) {
                connectProcessorAndStateStore(processorName, stateStoreName);
            }
        }

        return this;
    }

    /**
     * This is used only for KStreamBuilder: when adding a KTable from a source topic,
     * we need to add the topic as the KTable's materialized state store's changelog.
     */
    protected synchronized final TopologyBuilder connectSourceStoreAndTopic(String sourceStoreName, String topic) {
        if (storeToChangelogTopic.containsKey(sourceStoreName)) {
            throw new TopologyBuilderException("Source store " + sourceStoreName + " is already added.");
        }
        storeToChangelogTopic.put(sourceStoreName, topic);
        return this;
    }

    /**
     * Connects a list of processors.
     *
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @param processorNames the name of the processors
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if less than two processors are specified, or if one of the processors is not added yet
     */
    public synchronized final TopologyBuilder connectProcessors(String... processorNames) {
        if (processorNames.length < 2)
            throw new TopologyBuilderException("At least two processors need to participate in the connection.");

        for (String processorName : processorNames) {
            if (!nodeFactories.containsKey(processorName))
                throw new TopologyBuilderException("Processor " + processorName + " is not added yet.");

        }

        String firstProcessorName = processorNames[0];

        nodeGrouper.unite(firstProcessorName, Arrays.copyOfRange(processorNames, 1, processorNames.length));

        return this;
    }

    /**
     * Adds an internal topic
     *
     * @param topicName the name of the topic
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder addInternalTopic(String topicName) {
        Objects.requireNonNull(topicName, "topicName can't be null");
        this.internalTopicNames.add(topicName);

        return this;
    }

    /**
     * Asserts that the streams of the specified source nodes must be copartitioned.
     *
     * @param sourceNodes a set of source node names
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder copartitionSources(Collection<String> sourceNodes) {
        copartitionSourceGroups.add(Collections.unmodifiableSet(new HashSet<>(sourceNodes)));
        return this;
    }

    private void connectProcessorAndStateStore(String processorName, String stateStoreName) {
        if (!stateFactories.containsKey(stateStoreName))
            throw new TopologyBuilderException("StateStore " + stateStoreName + " is not added yet.");
        if (!nodeFactories.containsKey(processorName))
            throw new TopologyBuilderException("Processor " + processorName + " is not added yet.");

        StateStoreFactory stateStoreFactory = stateFactories.get(stateStoreName);
        Iterator<String> iter = stateStoreFactory.users.iterator();
        if (iter.hasNext()) {
            String user = iter.next();
            nodeGrouper.unite(user, processorName);
        }
        stateStoreFactory.users.add(processorName);

        NodeFactory nodeFactory = nodeFactories.get(processorName);
        if (nodeFactory instanceof ProcessorNodeFactory) {
            ProcessorNodeFactory processorNodeFactory = (ProcessorNodeFactory) nodeFactory;
            processorNodeFactory.addStateStore(stateStoreName);
            connectStateStoreNameToSourceTopics(stateStoreName, processorNodeFactory);
        } else {
            throw new TopologyBuilderException("cannot connect a state store " + stateStoreName + " to a source node or a sink node.");
        }
    }

    private Set<String> findSourceTopicsForProcessorParents(String [] parents) {
        final Set<String> sourceTopics = new HashSet<>();
        for (String parent : parents) {
            NodeFactory nodeFactory = nodeFactories.get(parent);
            if (nodeFactory instanceof SourceNodeFactory) {
                sourceTopics.addAll(((SourceNodeFactory) nodeFactory).topics);
            } else if (nodeFactory instanceof ProcessorNodeFactory) {
                sourceTopics.addAll(findSourceTopicsForProcessorParents(((ProcessorNodeFactory) nodeFactory).parents));
            }
        }
        return sourceTopics;
    }

    private void connectStateStoreNameToSourceTopics(final String stateStoreName,
                                                     final ProcessorNodeFactory processorNodeFactory) {

        // we should never update the mapping from state store names to source topics if the store name already exists
        // in the map; this scenario is possible, for example, that a state store underlying a source KTable is
        // connecting to a join operator whose source topic is not the original KTable's source topic but an internal repartition topic.
        if (stateStoreNameToSourceTopics.containsKey(stateStoreName)) {
            return;
        }

        final Set<String> sourceTopics = findSourceTopicsForProcessorParents(processorNodeFactory.parents);
        if (sourceTopics.isEmpty()) {
            throw new TopologyBuilderException("can't find source topic for state store " +
                    stateStoreName);
        }
        stateStoreNameToSourceTopics.put(stateStoreName,
                Collections.unmodifiableSet(sourceTopics));
    }


    private <T> void maybeAddToResetList(Collection<T> earliestResets, Collection<T> latestResets, AutoOffsetReset offsetReset, T item) {
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

    /**
     * Returns the map of node groups keyed by the topic group id.
     *
     * @return groups of node names
     */
    public synchronized Map<Integer, Set<String>> nodeGroups() {
        if (nodeGroups == null)
            nodeGroups = makeNodeGroups();

        return nodeGroups;
    }

    private Map<Integer, Set<String>> makeNodeGroups() {
        HashMap<Integer, Set<String>> nodeGroups = new LinkedHashMap<>();
        HashMap<String, Set<String>> rootToNodeGroup = new HashMap<>();

        int nodeGroupId = 0;

        // Go through source nodes first. This makes the group id assignment easy to predict in tests
        final HashSet<String> allSourceNodes = new HashSet<>(nodeToSourceTopics.keySet());
        allSourceNodes.addAll(nodeToSourcePatterns.keySet());

        for (String nodeName : Utils.sorted(allSourceNodes)) {
            String root = nodeGrouper.root(nodeName);
            Set<String> nodeGroup = rootToNodeGroup.get(root);
            if (nodeGroup == null) {
                nodeGroup = new HashSet<>();
                rootToNodeGroup.put(root, nodeGroup);
                nodeGroups.put(nodeGroupId++, nodeGroup);
            }
            nodeGroup.add(nodeName);
        }

        // Go through non-source nodes
        for (String nodeName : Utils.sorted(nodeFactories.keySet())) {
            if (!nodeToSourceTopics.containsKey(nodeName)) {
                String root = nodeGrouper.root(nodeName);
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

    /**
     * Build the topology for the specified topic group. This is called automatically when passing this builder into the
     * {@link org.apache.kafka.streams.KafkaStreams#KafkaStreams(TopologyBuilder, org.apache.kafka.streams.StreamsConfig)} constructor.
     *
     * @see org.apache.kafka.streams.KafkaStreams#KafkaStreams(TopologyBuilder, org.apache.kafka.streams.StreamsConfig)
     */
    public synchronized ProcessorTopology build(Integer topicGroupId) {
        Set<String> nodeGroup;
        if (topicGroupId != null) {
            nodeGroup = nodeGroups().get(topicGroupId);
        } else {
            // when topicGroupId is null, we build the full topology minus the global groups
            final Set<String> globalNodeGroups = globalNodeGroups();
            final Collection<Set<String>> values = nodeGroups().values();
            nodeGroup = new HashSet<>();
            for (Set<String> value : values) {
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
            for (String node : nodes) {
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

    private ProcessorTopology build(Set<String> nodeGroup) {
        List<ProcessorNode> processorNodes = new ArrayList<>(nodeFactories.size());
        Map<String, ProcessorNode> processorMap = new HashMap<>();
        Map<String, SourceNode> topicSourceMap = new HashMap<>();
        Map<String, SinkNode> topicSinkMap = new HashMap<>();
        Map<String, StateStore> stateStoreMap = new LinkedHashMap<>();

        // create processor nodes in a topological order ("nodeFactories" is already topologically sorted)
        for (NodeFactory factory : nodeFactories.values()) {
            if (nodeGroup == null || nodeGroup.contains(factory.name)) {
                final ProcessorNode node = factory.build();
                processorNodes.add(node);
                processorMap.put(node.name(), node);

                if (factory instanceof ProcessorNodeFactory) {
                    for (String parent : ((ProcessorNodeFactory) factory).parents) {
                        ProcessorNode<?, ?> parentNode = processorMap.get(parent);
                        parentNode.addChild(node);
                    }
                    for (String stateStoreName : ((ProcessorNodeFactory) factory).stateStoreNames) {
                        if (!stateStoreMap.containsKey(stateStoreName)) {
                            StateStore stateStore;

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

                    for (String topic : topics) {
                        if (internalTopicNames.contains(topic)) {
                            // prefix the internal topic name with the application id
                            topicSourceMap.put(decorateTopic(topic), (SourceNode) node);
                        } else {
                            topicSourceMap.put(topic, (SourceNode) node);
                        }
                    }
                } else if (factory instanceof SinkNodeFactory) {
                    final SinkNodeFactory sinkNodeFactory = (SinkNodeFactory) factory;

                    for (String parent : sinkNodeFactory.parents) {
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
    public synchronized Map<Integer, TopicsInfo> topicGroups() {
        Map<Integer, TopicsInfo> topicGroups = new LinkedHashMap<>();

        if (nodeGroups == null)
            nodeGroups = makeNodeGroups();

        for (Map.Entry<Integer, Set<String>> entry : nodeGroups.entrySet()) {
            Set<String> sinkTopics = new HashSet<>();
            Set<String> sourceTopics = new HashSet<>();
            Map<String, InternalTopicConfig> internalSourceTopics = new HashMap<>();
            Map<String, InternalTopicConfig> stateChangelogTopics = new HashMap<>();
            for (String node : entry.getValue()) {
                // if the node is a source node, add to the source topics
                List<String> topics = nodeToSourceTopics.get(node);
                if (topics != null) {
                    // if some of the topics are internal, add them to the internal topics
                    for (String topic : topics) {
                        // skip global topic as they don't need partition assignment
                        if (globalTopics.contains(topic)) {
                            continue;
                        }
                        if (this.internalTopicNames.contains(topic)) {
                            // prefix the internal topic name with the application id
                            String internalTopic = decorateTopic(topic);
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
                String topic = nodeToSinkTopic.get(node);
                if (topic != null) {
                    if (internalTopicNames.contains(topic)) {
                        // prefix the change log topic name with the application id
                        sinkTopics.add(decorateTopic(topic));
                    } else {
                        sinkTopics.add(topic);
                    }
                }

                // if the node is connected to a state, add to the state topics
                for (StateStoreFactory stateFactory : stateFactories.values()) {
                    final StateStoreSupplier supplier = stateFactory.supplier;
                    if (supplier.loggingEnabled() && stateFactory.users.contains(node)) {
                        final String name = ProcessorStateManager.storeChangelogTopic(applicationId, supplier.name());
                        final InternalTopicConfig internalTopicConfig = createInternalTopicConfig(supplier, name);
                        stateChangelogTopics.put(name, internalTopicConfig);
                    }
                }
            }
            if (!sourceTopics.isEmpty()) {
                topicGroups.put(entry.getKey(), new TopicsInfo(
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
            for (Map.Entry<String, Pattern> stringPatternEntry : nodeToSourcePatterns.entrySet()) {
                SourceNodeFactory sourceNode = (SourceNodeFactory) nodeFactories.get(stringPatternEntry.getKey());
                //need to update nodeToSourceTopics with topics matched from given regex
                nodeToSourceTopics.put(stringPatternEntry.getKey(), sourceNode.getTopics(subscriptionUpdates.getUpdates()));
                log.debug("nodeToSourceTopics {}", nodeToSourceTopics);
            }
        }
    }

    private InternalTopicConfig createInternalTopicConfig(final StateStoreSupplier<?> supplier, final String name) {
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

    /**
     * Get the Pattern to match all topics requiring to start reading from earliest available offset
     * @return the Pattern for matching all topics reading from earliest offset, never null
     */
    public synchronized Pattern earliestResetTopicsPattern() {
        final List<String> topics = maybeDecorateInternalSourceTopics(earliestResetTopics);
        final Pattern earliestPattern =  buildPatternForOffsetResetTopics(topics, earliestResetPatterns);

        ensureNoRegexOverlap(earliestPattern, latestResetPatterns, latestResetTopics);

        return earliestPattern;
    }

    /**
     * Get the Pattern to match all topics requiring to start reading from latest available offset
     * @return the Pattern for matching all topics reading from latest offset, never null
     */
    public synchronized Pattern latestResetTopicsPattern() {
        final List<String> topics = maybeDecorateInternalSourceTopics(latestResetTopics);
        final Pattern latestPattern = buildPatternForOffsetResetTopics(topics, latestResetPatterns);

        ensureNoRegexOverlap(latestPattern, earliestResetPatterns, earliestResetTopics);

        return  latestPattern;
    }

    private void ensureNoRegexOverlap(Pattern builtPattern, Set<Pattern> otherPatterns, Set<String> otherTopics) {

        for (Pattern otherPattern : otherPatterns) {
            if (builtPattern.pattern().contains(otherPattern.pattern())) {
                throw new TopologyBuilderException(String.format("Found overlapping regex [%s] against [%s] for a KStream with auto offset resets", otherPattern.pattern(), builtPattern.pattern()));
            }
        }

        for (String otherTopic : otherTopics) {
            if (builtPattern.matcher(otherTopic).matches()) {
                throw new TopologyBuilderException(String.format("Found overlapping regex [%s] matching topic [%s] for a KStream with auto offset resets", builtPattern.pattern(), otherTopic));
            }
        }
    }

    /**
     * Builds a composite pattern out of topic names and Pattern object for matching topic names.  If the provided
     * arrays are empty a Pattern.compile("") instance is returned.
     *
     * @param sourceTopics  the name of source topics to add to a composite pattern
     * @param sourcePatterns Patterns for matching source topics to add to a composite pattern
     * @return a Pattern that is composed of the literal source topic names and any Patterns for matching source topics
     */
    private static synchronized Pattern buildPatternForOffsetResetTopics(Collection<String> sourceTopics, Collection<Pattern> sourcePatterns) {
        StringBuilder builder = new StringBuilder();

        for (String topic : sourceTopics) {
            builder.append(topic).append("|");
        }

        for (Pattern sourcePattern : sourcePatterns) {
            builder.append(sourcePattern.pattern()).append("|");
        }

        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
            return Pattern.compile(builder.toString());
        }

        return EMPTY_ZERO_LENGTH_PATTERN;
    }

    /**
     * @return a mapping from state store name to a Set of source Topics.
     */
    public Map<String, List<String>> stateStoreNameToSourceTopics() {
        final Map<String, List<String>> results = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : stateStoreNameToSourceTopics.entrySet()) {
            results.put(entry.getKey(), maybeDecorateInternalSourceTopics(entry.getValue()));
        }
        return results;
    }

    /**
     * Returns the copartition groups.
     * A copartition group is a group of source topics that are required to be copartitioned.
     *
     * @return groups of topic names
     */
    public synchronized Collection<Set<String>> copartitionGroups() {
        List<Set<String>> list = new ArrayList<>(copartitionSourceGroups.size());
        for (Set<String> nodeNames : copartitionSourceGroups) {
            Set<String> copartitionGroup = new HashSet<>();
            for (String node : nodeNames) {
                final List<String> topics = nodeToSourceTopics.get(node);
                if (topics != null)
                    copartitionGroup.addAll(maybeDecorateInternalSourceTopics(topics));
            }
            list.add(Collections.unmodifiableSet(copartitionGroup));
        }
        return Collections.unmodifiableList(list);
    }

    private List<String> maybeDecorateInternalSourceTopics(final Collection<String> sourceTopics) {
        final List<String> decoratedTopics = new ArrayList<>();
        for (String topic : sourceTopics) {
            if (internalTopicNames.contains(topic)) {
                decoratedTopics.add(decorateTopic(topic));
            } else {
                decoratedTopics.add(topic);
            }
        }
        return decoratedTopics;
    }

    private String decorateTopic(String topic) {
        if (applicationId == null) {
            throw new TopologyBuilderException("there are internal topics and "
                    + "applicationId hasn't been set. Call "
                    + "setApplicationId first");
        }

        return applicationId + "-" + topic;
    }

    public synchronized Pattern sourceTopicPattern() {
        if (this.topicPattern == null) {
            List<String> allSourceTopics = new ArrayList<>();
            if (!nodeToSourceTopics.isEmpty()) {
                for (List<String> topics : nodeToSourceTopics.values()) {
                    allSourceTopics.addAll(maybeDecorateInternalSourceTopics(topics));
                }
            }
            Collections.sort(allSourceTopics);

            this.topicPattern = buildPatternForOffsetResetTopics(allSourceTopics, nodeToSourcePatterns.values());
        }

        return this.topicPattern;
    }

    public synchronized void updateSubscriptions(SubscriptionUpdates subscriptionUpdates, String threadId) {
        log.debug("stream-thread [{}] updating builder with {} topic(s) with possible matching regex subscription(s)", threadId, subscriptionUpdates);
        this.subscriptionUpdates = subscriptionUpdates;
        setRegexMatchedTopicsToSourceNodes();
    }
}
