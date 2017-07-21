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
package org.apache.kafka.streams.processor;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.internals.InternalTopicConfig;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.SinkNode;
import org.apache.kafka.streams.processor.internals.SourceNode;
import org.apache.kafka.streams.processor.internals.StreamPartitionAssignor.SubscriptionUpdates;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collection;
import java.util.List;
import java.util.Map;
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
@InterfaceStability.Evolving
public class TopologyBuilder {

    private static abstract class NodeFactory {
        final String name;
        final String[] parents;

        NodeFactory(final String name, final String[] parents) {
            this.name = name;
            this.parents = parents;
        }

        public abstract ProcessorNode build();

        abstract TopologyDescription.AbstractNode describe();
    }

    private static class ProcessorNodeFactory extends NodeFactory {
        private final ProcessorSupplier<?, ?> supplier;
        private final Set<String> stateStoreNames = new HashSet<>();

        ProcessorNodeFactory(String name, String[] parents, ProcessorSupplier<?, ?> supplier) {
            super(name, parents.clone());
            this.supplier = supplier;
        }

        public void addStateStore(String stateStoreName) {
            stateStoreNames.add(stateStoreName);
        }

        @Override
        public ProcessorNode build() {
            return new ProcessorNode<>(name, supplier.get(), stateStoreNames);
        }

        @Override
        TopologyDescription.Processor describe() {
            return new TopologyDescription.Processor(name, new HashSet<>(stateStoreNames));
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
            super(name, new String[0]);
            this.topics = topics != null ? Arrays.asList(topics) : new ArrayList<String>();
            this.pattern = pattern;
            this.keyDeserializer = keyDeserializer;
            this.valDeserializer = valDeserializer;
            this.timestampExtractor = timestampExtractor;
        }

        List<String> getTopics(Collection<String> subscribedTopics) {
            // if it is subscribed via patterns, it is possible that the topic metadata has not been updated
            // yet and hence the map from source node to topics is stale, in this case we put the pattern as a place holder;
            // this should only happen for debugging since during runtime this function should always be called after the metadata has updated.
            if (subscribedTopics.isEmpty())
                return Collections.singletonList("" + pattern + "");

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
                return new SourceNode<>(name, Collections.singletonList("" + pattern + ""), timestampExtractor, keyDeserializer, valDeserializer);
            else
                return new SourceNode<>(name, maybeDecorateInternalSourceTopics(sourceTopics), timestampExtractor, keyDeserializer, valDeserializer);
        }

        private boolean isMatch(String topic) {
            return this.pattern.matcher(topic).matches();
        }

        @Override
        TopologyDescription.Source describe() {
            String sourceTopics;

            if (pattern == null) {
                sourceTopics = topics.toString();
                sourceTopics = sourceTopics.substring(1, sourceTopics.length() - 1); // trim first and last, ie. []
            } else {
                sourceTopics = pattern.toString();
            }

            return new TopologyDescription.Source(name, sourceTopics);
        }
    }

    private class SinkNodeFactory<K, V> extends NodeFactory {
        private final String topic;
        private final Serializer<K> keySerializer;
        private final Serializer<V> valSerializer;
        private final StreamPartitioner<? super K, ? super V> partitioner;

        private SinkNodeFactory(String name, String[] parents, String topic, Serializer<K> keySerializer, Serializer<V> valSerializer, StreamPartitioner<? super K, ? super V> partitioner) {
            super(name, parents.clone());
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

        @Override
        TopologyDescription.Sink describe() {
            return new TopologyDescription.Sink(name, topic);
        }
    }

    /**
     * NOTE this member would not needed by developers working with the processor APIs, but only used
     * for internal functionalities.
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public final InternalTopologyBuilder internalTopologyBuilder = new InternalTopologyBuilder();

    /**
     * NOTE this class would not needed by developers working with the processor APIs, but only used
     * for internal functionalities.
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public static class TopicsInfo {
        public Set<String> sinkTopics;
        public Set<String> sourceTopics;
        public Map<String, InternalTopicConfig> stateChangelogTopics;
        public Map<String, InternalTopicConfig> repartitionSourceTopics;

        public TopicsInfo(final Set<String> sinkTopics,
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

    /**
     * Enum used to define auto offset reset policy when creating {@link KStream} or {@link KTable}
     */
    public enum AutoOffsetReset {
        EARLIEST, LATEST
    }

    /**
     * Create a new builder.
     */
    public TopologyBuilder() {}

    /** @deprecated This class is not part of public API and should never be used by a developer. */
    @Deprecated
    public synchronized final TopologyBuilder setApplicationId(final String applicationId) {
        internalTopologyBuilder.setApplicationId(applicationId);
        return this;
    }

    /**
     * Add a new source that consumes the named topics and forward the records to child processor and/or sink nodes.
     * The source will use the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topics the name of one or more Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder addSource(final String name,
                                                        final String... topics) {
        internalTopologyBuilder.addSource(null, name, null, null, null, topics);
        return this;
    }

    /**
     * Add a new source that consumes the named topics and forward the records to child processor and/or sink nodes.
     * The source will use the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param offsetReset the auto offset reset policy to use for this source if no committed offsets found; acceptable values earliest or latest
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topics the name of one or more Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder addSource(final AutoOffsetReset offsetReset,
                                                        final String name,
                                                        final String... topics) {
        internalTopologyBuilder.addSource(offsetReset, name, null, null, null, topics);
        return this;
    }

    /**
     * Add a new source that consumes the named topics and forward the records to child processor and/or sink nodes.
     * The source will use the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     *
     * @param timestampExtractor the stateless timestamp extractor used for this source,
     *                           if not specified the default extractor defined in the configs will be used
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topics             the name of one or more Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder addSource(final TimestampExtractor timestampExtractor,
                                                        final String name, final String... topics) {
        internalTopologyBuilder.addSource(null, name, timestampExtractor, null, null, topics);
        return this;
    }

    /**
     * Add a new source that consumes the named topics and forward the records to child processor and/or sink nodes.
     * The source will use the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     *
     * @param offsetReset        the auto offset reset policy to use for this source if no committed offsets found;
     *                           acceptable values earliest or latest
     * @param timestampExtractor the stateless timestamp extractor used for this source,
     *                           if not specified the default extractor defined in the configs will be used
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topics             the name of one or more Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder addSource(final AutoOffsetReset offsetReset,
                                                        final TimestampExtractor timestampExtractor, final String name, final String... topics) {
        internalTopologyBuilder.addSource(offsetReset, name, timestampExtractor, null, null, topics);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forward the records to child processor and/or sink nodes.
     * The source will use the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topicPattern regular expression pattern to match Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder addSource(final String name,
                                                        final Pattern topicPattern) {
        internalTopologyBuilder.addSource(null, name, null, null, null, topicPattern);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forward the records to child processor and/or sink nodes.
     * The source will use the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param offsetReset the auto offset reset policy value for this source if no committed offsets found; acceptable values earliest or latest.
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topicPattern regular expression pattern to match Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder addSource(final AutoOffsetReset offsetReset,
                                                        final String name,
                                                        final Pattern topicPattern) {
        internalTopologyBuilder.addSource(offsetReset, name, null, null, null, topicPattern);
        return this;
    }


    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forward the records to child processor and/or sink nodes.
     * The source will use the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     *
     * @param timestampExtractor the stateless timestamp extractor used for this source,
     *                           if not specified the default extractor defined in the configs will be used
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder addSource(final TimestampExtractor timestampExtractor,
                                                        final String name,
                                                        final Pattern topicPattern) {
        internalTopologyBuilder.addSource(null, name, timestampExtractor, null, null, topicPattern);
        return this;
    }


    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forward the records to child processor and/or sink nodes.
     * The source will use the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}.
     *
     * @param offsetReset        the auto offset reset policy value for this source if no committed offsets found;
     *                           acceptable values earliest or latest.
     * @param timestampExtractor the stateless timestamp extractor used for this source,
     *                           if not specified the default extractor defined in the configs will be used
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder addSource(final AutoOffsetReset offsetReset,
                                                        final TimestampExtractor timestampExtractor,
                                                        final String name,
                                                        final Pattern topicPattern) {
        internalTopologyBuilder.addSource(offsetReset, name, timestampExtractor, null, null, topicPattern);
        return this;
    }


    /**
     * Add a new source that consumes the named topics and forwards the records to child processor and/or sink nodes.
     * The source will use the specified key and value deserializers.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}
     * @param keyDeserializer    key deserializer used to read this source, if not specified the default
     *                           key deserializer defined in the configs will be used
     * @param valDeserializer    value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topics             the name of one or more Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if processor is already added or if topics have already been registered by another source
     */

    public synchronized final TopologyBuilder addSource(final String name,
                                                        final Deserializer keyDeserializer,
                                                        final Deserializer valDeserializer,
                                                        final String... topics) {
        internalTopologyBuilder.addSource(null, name, null, keyDeserializer, valDeserializer, topics);
        return this;
    }

    /**
     * Add a new source that consumes the named topics and forwards the records to child processor and/or sink nodes.
     * The source will use the specified key and value deserializers.
     *
     * @param offsetReset        the auto offset reset policy to use for this stream if no committed offsets found;
     *                           acceptable values are earliest or latest.
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param timestampExtractor the stateless timestamp extractor used for this source,
     *                           if not specified the default extractor defined in the configs will be used
     * @param keyDeserializer    key deserializer used to read this source, if not specified the default
     *                           key deserializer defined in the configs will be used
     * @param valDeserializer    value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topics             the name of one or more Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if processor is already added or if topics have already been registered by another source
     */

    public synchronized final TopologyBuilder addSource(final AutoOffsetReset offsetReset,
                                                        final String name,
                                                        final TimestampExtractor timestampExtractor,
                                                        final Deserializer keyDeserializer,
                                                        final Deserializer valDeserializer,
                                                        final String... topics) {
        internalTopologyBuilder.addSource(offsetReset, name, timestampExtractor, keyDeserializer, valDeserializer, topics);
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
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param storeSupplier         user defined state store supplier
     * @param sourceName            name of the {@link SourceNode} that will be automatically added
     * @param keyDeserializer       the {@link Deserializer} to deserialize keys with
     * @param valueDeserializer     the {@link Deserializer} to deserialize values with
     * @param topic                 the topic to source the data from
     * @param processorName         the name of the {@link ProcessorSupplier}
     * @param stateUpdateSupplier   the instance of {@link ProcessorSupplier}
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized TopologyBuilder addGlobalStore(final StateStoreSupplier<KeyValueStore> storeSupplier,
                                                       final String sourceName,
                                                       final Deserializer keyDeserializer,
                                                       final Deserializer valueDeserializer,
                                                       final String topic,
                                                       final String processorName,
                                                       final ProcessorSupplier stateUpdateSupplier) {
        internalTopologyBuilder.addGlobalStore(storeSupplier, sourceName, null, keyDeserializer, valueDeserializer, topic, processorName, stateUpdateSupplier);
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
     * @param storeSupplier         user defined state store supplier
     * @param sourceName            name of the {@link SourceNode} that will be automatically added
     * @param timestampExtractor    the stateless timestamp extractor used for this source,
     *                              if not specified the default extractor defined in the configs will be used
     * @param keyDeserializer       the {@link Deserializer} to deserialize keys with
     * @param valueDeserializer     the {@link Deserializer} to deserialize values with
     * @param topic                 the topic to source the data from
     * @param processorName         the name of the {@link ProcessorSupplier}
     * @param stateUpdateSupplier   the instance of {@link ProcessorSupplier}
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized TopologyBuilder addGlobalStore(final StateStoreSupplier<KeyValueStore> storeSupplier,
                                                       final String sourceName,
                                                       final TimestampExtractor timestampExtractor,
                                                       final Deserializer keyDeserializer,
                                                       final Deserializer valueDeserializer,
                                                       final String topic,
                                                       final String processorName,
                                                       final ProcessorSupplier stateUpdateSupplier) {
        internalTopologyBuilder.addGlobalStore(storeSupplier, sourceName, timestampExtractor, keyDeserializer, valueDeserializer, topic, processorName, stateUpdateSupplier);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forwards the records to child processor and/or sink nodes.
     * The source will use the specified key and value deserializers. The provided
     * de-/serializers will be used for all matched topics, so care should be taken to specify patterns for
     * topics that share the same key-value data format.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}
     * @param keyDeserializer    key deserializer used to read this source, if not specified the default
     *                           key deserializer defined in the configs will be used
     * @param valDeserializer    value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if processor is already added or if topics have already been registered by name
     */
    public synchronized final TopologyBuilder addSource(final String name,
                                                        final Deserializer keyDeserializer,
                                                        final Deserializer valDeserializer,
                                                        final Pattern topicPattern) {
        internalTopologyBuilder.addSource(null, name, null, keyDeserializer, valDeserializer, topicPattern);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forwards the records to child processor and/or sink nodes.
     * The source will use the specified key and value deserializers. The provided
     * de-/serializers will be used for all matched topics, so care should be taken to specify patterns for
     * topics that share the same key-value data format.
     *
     * @param offsetReset        the auto offset reset policy to use for this stream if no committed offsets found;
     *                           acceptable values are earliest or latest
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param timestampExtractor the stateless timestamp extractor used for this source,
     *                           if not specified the default extractor defined in the configs will be used
     * @param keyDeserializer    key deserializer used to read this source, if not specified the default
     *                           key deserializer defined in the configs will be used
     * @param valDeserializer    value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if processor is already added or if topics have already been registered by name
     */
    public synchronized final TopologyBuilder addSource(final AutoOffsetReset offsetReset,
                                                        final String name,
                                                        final TimestampExtractor timestampExtractor,
                                                        final Deserializer keyDeserializer,
                                                        final Deserializer valDeserializer,
                                                        final Pattern topicPattern) {
        internalTopologyBuilder.addSource(offsetReset, name, timestampExtractor, keyDeserializer, valDeserializer, topicPattern);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forwards the records to child processor and/or sink nodes.
     * The source will use the specified key and value deserializers. The provided
     * de-/serializers will be used for all matched topics, so care should be taken to specify patterns for
     * topics that share the same key-value data format.
     *
     * @param offsetReset        the auto offset reset policy to use for this stream if no committed offsets found;
     *                           acceptable values are earliest or latest
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}
     * @param keyDeserializer    key deserializer used to read this source, if not specified the default
     *                           key deserializer defined in the configs will be used
     * @param valDeserializer    value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if processor is already added or if topics have already been registered by name
     */
    public synchronized final TopologyBuilder addSource(final AutoOffsetReset offsetReset,
                                                        final String name,
                                                        final Deserializer keyDeserializer,
                                                        final Deserializer valDeserializer,
                                                        final Pattern topicPattern) {
        internalTopologyBuilder.addSource(offsetReset, name, null, keyDeserializer, valDeserializer, topicPattern);
        return this;
    }


    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic.
     * The sink will use the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
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
    public synchronized final TopologyBuilder addSink(final String name,
                                                      final String topic,
                                                      final String... parentNames) {
        internalTopologyBuilder.addSink(name, topic, null, null, null, parentNames);
        return this;
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic, using
     * the supplied partitioner.
     * The sink will use the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} and
     * {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
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
    public synchronized final TopologyBuilder addSink(final String name,
                                                      final String topic,
                                                      final StreamPartitioner partitioner,
                                                      final String... parentNames) {
        internalTopologyBuilder.addSink(name, topic, null, null, partitioner, parentNames);
        return this;
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic.
     * The sink will use the specified key and value serializers.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its records
     * @param keySerializer the {@link Serializer key serializer} used when consuming records; may be null if the sink
     * should use the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param valSerializer the {@link Serializer value serializer} used when consuming records; may be null if the sink
     * should use the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
     * and write to its topic
     * @return this builder instance so methods can be chained together; never null
     * @see #addSink(String, String, String...)
     * @see #addSink(String, String, StreamPartitioner, String...)
     * @see #addSink(String, String, Serializer, Serializer, StreamPartitioner, String...)
     */
    public synchronized final TopologyBuilder addSink(final String name,
                                                      final String topic,
                                                      final Serializer keySerializer,
                                                      final Serializer valSerializer,
                                                      final String... parentNames) {
        internalTopologyBuilder.addSink(name, topic, keySerializer, valSerializer, null, parentNames);
        return this;
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic.
     * The sink will use the specified key and value serializers, and the supplied partitioner.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its records
     * @param keySerializer the {@link Serializer key serializer} used when consuming records; may be null if the sink
     * should use the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} specified in the
     * {@link org.apache.kafka.streams.StreamsConfig stream configuration}
     * @param valSerializer the {@link Serializer value serializer} used when consuming records; may be null if the sink
     * should use the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
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
    public synchronized final <K, V> TopologyBuilder addSink(final String name,
                                                             final String topic,
                                                             final Serializer<K> keySerializer,
                                                             final Serializer<V> valSerializer,
                                                             final StreamPartitioner<? super K, ? super V> partitioner,
                                                             final String... parentNames) {
        internalTopologyBuilder.addSink(name, topic, keySerializer, valSerializer, partitioner, parentNames);
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
    public synchronized final TopologyBuilder addProcessor(final String name,
                                                           final ProcessorSupplier supplier,
                                                           final String... parentNames) {
        internalTopologyBuilder.addProcessor(name, supplier, parentNames);
        return this;
    }

    /**
     * Adds a state store
     *
     * @param supplier the supplier used to obtain this state store {@link StateStore} instance
     * @return this builder instance so methods can be chained together; never null
     * @throws TopologyBuilderException if state store supplier is already added
     */
    public synchronized final TopologyBuilder addStateStore(final StateStoreSupplier supplier,
                                                            final String... processorNames) {
        internalTopologyBuilder.addStateStore(supplier, processorNames);
        return this;
    }

    /**
     * Connects the processor and the state stores
     *
     * @param processorName the name of the processor
     * @param stateStoreNames the names of state stores that the processor uses
     * @return this builder instance so methods can be chained together; never null
     */
    public synchronized final TopologyBuilder connectProcessorAndStateStores(final String processorName,
                                                                             final String... stateStoreNames) {
        internalTopologyBuilder.connectProcessorAndStateStores(processorName, stateStoreNames);
        return this;
    }

    /**
     * This is used only for KStreamBuilder: when adding a KTable from a source topic,
     * we need to add the topic as the KTable's materialized state store's changelog.
     *
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    protected synchronized final TopologyBuilder connectSourceStoreAndTopic(final String sourceStoreName,
                                                                            final String topic) {
        internalTopologyBuilder.connectSourceStoreAndTopic(sourceStoreName, topic);
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
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public synchronized final TopologyBuilder connectProcessors(final String... processorNames) {
        internalTopologyBuilder.connectProcessors(processorNames);
        return this;
    }

    /**
     * Adds an internal topic
     *
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @param topicName the name of the topic
     * @return this builder instance so methods can be chained together; never null
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public synchronized final TopologyBuilder addInternalTopic(final String topicName) {
        internalTopologyBuilder.addInternalTopic(topicName);
        return this;
    }

    /**
     * Asserts that the streams of the specified source nodes must be copartitioned.
     *
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @param sourceNodes a set of source node names
     * @return this builder instance so methods can be chained together; never null
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public synchronized final TopologyBuilder copartitionSources(final Collection<String> sourceNodes) {
        internalTopologyBuilder.copartitionSources(sourceNodes);
        return this;
    }

    /**
     * Returns the map of node groups keyed by the topic group id.
     *
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @return groups of node names
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public synchronized Map<Integer, Set<String>> nodeGroups() {
        return internalTopologyBuilder.nodeGroups();
    }

    /**
     * Build the topology for the specified topic group. This is called automatically when passing this builder into the
     * {@link org.apache.kafka.streams.KafkaStreams#KafkaStreams(TopologyBuilder, org.apache.kafka.streams.StreamsConfig)} constructor.
     *
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @see org.apache.kafka.streams.KafkaStreams#KafkaStreams(TopologyBuilder, org.apache.kafka.streams.StreamsConfig)
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public synchronized ProcessorTopology build(final Integer topicGroupId) {
        return internalTopologyBuilder.build(topicGroupId);
    }

    /**
     * Builds the topology for any global state stores
     *
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @return ProcessorTopology
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public synchronized ProcessorTopology buildGlobalStateTopology() {
        return internalTopologyBuilder.buildGlobalStateTopology();
    }

    /**
     * Get any global {@link StateStore}s that are part of the
     * topology
     *
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @return map containing all global {@link StateStore}s
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public Map<String, StateStore> globalStateStores() {
        return internalTopologyBuilder.globalStateStores();
    }

    /**
     * Returns the map of topic groups keyed by the group id.
     * A topic group is a group of topics in the same task.
     *
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @return groups of topic names
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public synchronized Map<Integer, TopicsInfo> topicGroups() {
        return internalTopologyBuilder.topicGroups();
    }

    /**
     * Get the Pattern to match all topics requiring to start reading from earliest available offset
     *
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @return the Pattern for matching all topics reading from earliest offset, never null
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public synchronized Pattern earliestResetTopicsPattern() {
        return internalTopologyBuilder.earliestResetTopicsPattern();
    }

    /**
     * Get the Pattern to match all topics requiring to start reading from latest available offset
     *
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @return the Pattern for matching all topics reading from latest offset, never null
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public synchronized Pattern latestResetTopicsPattern() {
        return internalTopologyBuilder.latestResetTopicsPattern();
    }

    /**
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @return a mapping from state store name to a Set of source Topics.
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public Map<String, List<String>> stateStoreNameToSourceTopics() {
        return internalTopologyBuilder.stateStoreNameToSourceTopics();
    }

    /**
     * Returns the copartition groups.
     * A copartition group is a group of source topics that are required to be copartitioned.
     *
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @return groups of topic names
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public synchronized Collection<Set<String>> copartitionGroups() {
        return internalTopologyBuilder.copartitionGroups();
    }

    /**
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public SubscriptionUpdates subscriptionUpdates() {
        return internalTopologyBuilder.subscriptionUpdates();
    }

    /**
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public synchronized Pattern sourceTopicPattern() {
        return internalTopologyBuilder.sourceTopicPattern();
    }

    /**
     * NOTE this function would not needed by developers working with the processor APIs, but only used
     * for the high-level DSL parsing functionalities.
     *
     * @deprecated not part of public API and for internal usage only
     */
    @Deprecated
    public synchronized void updateSubscriptions(final SubscriptionUpdates subscriptionUpdates,
                                                 final String threadId) {
        internalTopologyBuilder.updateSubscriptions(subscriptionUpdates, threadId);
    }

    private boolean isGlobalSource(final String nodeName) {
        final NodeFactory nodeFactory = nodeFactories.get(nodeName);

        if (nodeFactory instanceof SourceNodeFactory) {
            final List<String> topics = ((SourceNodeFactory) nodeFactory).topics;
            if (topics != null && topics.size() == 1 && globalTopics.contains(topics.get(0))) {
                return true;
            }
        }

        return false;
    }

    TopologyDescription describe() {
        final TopologyDescription description = new TopologyDescription();

        describeSubtopologies(description);
        describeGlobalStores(description);

        return description;
    }

    private void describeSubtopologies(final TopologyDescription description) {
        for (final Map.Entry<Integer, Set<String>> nodeGroup : makeNodeGroups().entrySet()) {

            final Set<String> allNodesOfGroups = nodeGroup.getValue();
            final boolean isNodeGroupOfGlobalStores = nodeGroupContainsGlobalSourceNode(allNodesOfGroups);

            if (!isNodeGroupOfGlobalStores) {
                describeSubtopology(description, nodeGroup.getKey(), allNodesOfGroups);
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

    private void describeSubtopology(final TopologyDescription description,
                                     final Integer subtopologyId,
                                     final Set<String> nodeNames) {

        final HashMap<String, TopologyDescription.AbstractNode> nodesByName = new HashMap<>();

        // add all nodes
        for (final String nodeName : nodeNames) {
            nodesByName.put(nodeName, nodeFactories.get(nodeName).describe());
        }

        // connect each node to its predecessors and successors
        for (final TopologyDescription.AbstractNode node : nodesByName.values()) {
            for (final String predecessorName : nodeFactories.get(node.name()).parents) {
                final TopologyDescription.AbstractNode predecessor = nodesByName.get(predecessorName);
                node.addPredecessor(predecessor);
                predecessor.addSuccessor(node);
            }
        }

        description.addSubtopology(new TopologyDescription.Subtopology(
            subtopologyId,
            new HashSet<TopologyDescription.Node>(nodesByName.values())));
    }

    private void describeGlobalStores(final TopologyDescription description) {
        for (final Map.Entry<Integer, Set<String>> nodeGroup : makeNodeGroups().entrySet()) {
            final Set<String> nodes = nodeGroup.getValue();

            final Iterator<String> it = nodes.iterator();
            while (it.hasNext()) {
                final String node = it.next();

                if (isGlobalSource(node)) {
                    // we found a GlobalStore node group; those contain exactly two node: {sourceNode,processorNode}
                    it.remove(); // remove sourceNode from group
                    final String processorNode = nodes.iterator().next(); // get remaining processorNode

                    description.addGlobalStore(new TopologyDescription.GlobalStore(
                        node,
                        processorNode,
                        ((ProcessorNodeFactory) nodeFactories.get(processorNode)).stateStoreNames.iterator().next(),
                        nodeToSourceTopics.get(node).get(0)
                    ));
                    break;
                }
            }
        }
    }

}
