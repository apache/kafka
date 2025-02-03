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
package org.apache.kafka.streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.internals.AutoOffsetResetInternal;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.ConnectedStoreProvider;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.SinkNode;
import org.apache.kafka.streams.processor.internals.SourceNode;
import org.apache.kafka.streams.processor.internals.StoreDelegatingProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * A logical representation of a {@link ProcessorTopology}.
 * A topology is an acyclic graph of sources, processors, and sinks.
 * A {@link SourceNode source} is a node in the graph that consumes one or more Kafka topics and forwards them to its
 * successor nodes.
 * A {@link Processor processor} is a node in the graph that receives input records from upstream nodes, processes the
 * records, and optionally forwarding new records to one or all of its downstream nodes.
 * Finally, a {@link SinkNode sink} is a node in the graph that receives records from upstream nodes and writes them to
 * a Kafka topic.
 * A {@code Topology} allows you to construct an acyclic graph of these nodes, and then passed into a new
 * {@link KafkaStreams} instance that will then {@link KafkaStreams#start() begin consuming, processing, and producing
 * records}.
 */
public class Topology {

    protected final InternalTopologyBuilder internalTopologyBuilder;

    public Topology() {
        this(new InternalTopologyBuilder());
    }

    public Topology(final TopologyConfig topologyConfigs) {
        this(new InternalTopologyBuilder(topologyConfigs));
    }

    protected Topology(final InternalTopologyBuilder internalTopologyBuilder) {
        this.internalTopologyBuilder = internalTopologyBuilder;
    }

    /**
     * Sets the {@code auto.offset.reset} configuration when
     * {@link #addSource(AutoOffsetReset, String, String...) adding a source processor} or when creating {@link KStream}
     * or {@link KTable} via {@link StreamsBuilder}.
     *
     * @deprecated Since 4.0. Use {@link org.apache.kafka.streams.AutoOffsetReset} instead.
     */
    @Deprecated
    public enum AutoOffsetReset {
        EARLIEST, LATEST
    }

    @Deprecated
    private static AutoOffsetResetInternal convertOldToNew(final Topology.AutoOffsetReset resetPolicy) {
        if (resetPolicy == null) {
            return null;
        }

        return new AutoOffsetResetInternal(
            resetPolicy == org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST
                ? org.apache.kafka.streams.AutoOffsetReset.earliest()
                : org.apache.kafka.streams.AutoOffsetReset.latest()
        );
    }

    /**
     * Add a new source that consumes the named topics and forward the records to child processor and/or sink nodes.
     * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link StreamsConfig stream configuration}.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topics the name of one or more Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by another source
     */
    public synchronized Topology addSource(final String name,
                                           final String... topics) {
        internalTopologyBuilder.addSource(null, name, null, null, null, topics);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forward the records to child processor and/or sink nodes.
     * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link StreamsConfig stream configuration}.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topicPattern regular expression pattern to match Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by another source
     */
    public synchronized Topology addSource(final String name,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(null, name, null, null, null, topicPattern);
        return this;
    }

    /**
     * Add a new source that consumes the named topics and forward the records to child processor and/or sink nodes.
     * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link StreamsConfig stream configuration}.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param offsetReset the auto offset reset policy to use for this source if no committed offsets found; acceptable values earliest or latest
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topics the name of one or more Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by another source
     * @deprecated Since 4.0. Use {@link #addSource(org.apache.kafka.streams.AutoOffsetReset, String, String...)} instead.
     */
    @Deprecated
    public synchronized Topology addSource(final AutoOffsetReset offsetReset,
                                           final String name,
                                           final String... topics) {
        internalTopologyBuilder.addSource(convertOldToNew(offsetReset), name, null, null, null, topics);
        return this;
    }

    /**
     * Adds a new source that consumes the specified topics and forwards the records to child processor and/or sink nodes.
     * The source will use the specified {@link org.apache.kafka.streams.AutoOffsetReset offset reset policy} if no committed offsets are found.
     *
     * @param offsetReset the auto offset reset policy to use for this source if no committed offsets are found
     * @param name the unique name of the source used to reference this node when {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}
     * @param topics the name of one or more Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if a processor is already added or if topics have already been registered by another source
     */
    public synchronized Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                           final String name,
                                           final String... topics) {
        internalTopologyBuilder.addSource(new AutoOffsetResetInternal(offsetReset), name, null, null, null, topics);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forward the records to child processor and/or sink nodes.
     * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link StreamsConfig stream configuration}.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param offsetReset the auto offset reset policy value for this source if no committed offsets found; acceptable values earliest or latest.
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topicPattern regular expression pattern to match Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by another source
     * @deprecated Since 4.0. Use {@link #addSource(org.apache.kafka.streams.AutoOffsetReset, String, Pattern)} instead.
     */
    @Deprecated
    public synchronized Topology addSource(final AutoOffsetReset offsetReset,
                                           final String name,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(convertOldToNew(offsetReset), name, null, null, null, topicPattern);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forward the records to child processor and/or sink nodes.
     * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link StreamsConfig stream configuration}.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param offsetReset the auto offset reset policy value for this source if no committed offsets found
     * @param name the unique name of the source used to reference this node when
     * {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topicPattern regular expression pattern to match Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by another source
     */
    public synchronized Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                           final String name,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(new AutoOffsetResetInternal(offsetReset), name, null, null, null, topicPattern);
        return this;
    }

    /**
     * Add a new source that consumes the named topics and forward the records to child processor and/or sink nodes.
     * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link StreamsConfig stream configuration}.
     *
     * @param timestampExtractor the stateless timestamp extractor used for this source,
     *                           if not specified the default extractor defined in the configs will be used
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topics             the name of one or more Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by another source
     */
    public synchronized Topology addSource(final TimestampExtractor timestampExtractor,
                                           final String name,
                                           final String... topics) {
        internalTopologyBuilder.addSource(null, name, timestampExtractor, null, null, topics);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern
     * and forward the records to child processor and/or sink nodes.
     * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link StreamsConfig stream configuration}.
     *
     * @param timestampExtractor the stateless timestamp extractor used for this source,
     *                           if not specified the default extractor defined in the configs will be used
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by another source
     */
    public synchronized Topology addSource(final TimestampExtractor timestampExtractor,
                                           final String name,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(null, name, timestampExtractor, null, null, topicPattern);
        return this;
    }

    /**
     * Add a new source that consumes the named topics and forward the records to child processor and/or sink nodes.
     * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link StreamsConfig stream configuration}.
     *
     * @param offsetReset        the auto offset reset policy to use for this source if no committed offsets found;
     *                           acceptable values earliest or latest
     * @param timestampExtractor the stateless timestamp extractor used for this source,
     *                           if not specified the default extractor defined in the configs will be used
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topics             the name of one or more Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by another source
     * @deprecated Since 4.0. Use {@link #addSource(org.apache.kafka.streams.AutoOffsetReset, TimestampExtractor, String, String...)} instead.
     */
    @Deprecated
    public synchronized Topology addSource(final AutoOffsetReset offsetReset,
                                           final TimestampExtractor timestampExtractor,
                                           final String name,
                                           final String... topics) {
        internalTopologyBuilder.addSource(convertOldToNew(offsetReset), name, timestampExtractor, null, null, topics);
        return this;
    }

    /**
     * Adds a new source that consumes the specified topics with a specified {@link TimestampExtractor}
     * and forwards the records to child processor and/or sink nodes.
     * The source will use the provided timestamp extractor to determine the timestamp of each record.
     *
     * @param offsetReset the auto offset reset policy to use if no committed offsets are found
     * @param timestampExtractor the timestamp extractor to use for this source
     * @param name the unique name of the source used to reference this node when {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}
     * @param topics the name of one or more Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if a processor is already added or if topics have already been registered by another source
     */
    public synchronized Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                           final TimestampExtractor timestampExtractor,
                                           final String name,
                                           final String... topics) {
        internalTopologyBuilder.addSource(new AutoOffsetResetInternal(offsetReset), name, timestampExtractor, null, null, topics);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern and forward the records to child processor
     * and/or sink nodes.
     * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
     * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
     * {@link StreamsConfig stream configuration}.
     *
     * @param offsetReset        the auto offset reset policy value for this source if no committed offsets found;
     *                           acceptable values earliest or latest.
     * @param timestampExtractor the stateless timestamp extractor used for this source,
     *                           if not specified the default extractor defined in the configs will be used
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by another source
     * @deprecated Since 4.0. Use {@link #addSource(org.apache.kafka.streams.AutoOffsetReset, TimestampExtractor, String, Pattern)} instead.
     */
    @Deprecated
    public synchronized Topology addSource(final AutoOffsetReset offsetReset,
                                           final TimestampExtractor timestampExtractor,
                                           final String name,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(convertOldToNew(offsetReset), name, timestampExtractor, null, null, topicPattern);
        return this;
    }

    /**
     * Adds a new source that consumes from topics matching the given pattern with a specified {@link TimestampExtractor}
     * and forwards the records to child processor and/or sink nodes.
     * The source will use the provided timestamp extractor to determine the timestamp of each record.
     *
     * @param offsetReset the auto offset reset policy to use if no committed offsets are found
     * @param timestampExtractor the timestamp extractor to use for this source
     * @param name the unique name of the source used to reference this node when {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}
     * @param topicPattern the regular expression pattern to match Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if a processor is already added or if topics have already been registered by another source
     */
    public synchronized Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                           final TimestampExtractor timestampExtractor,
                                           final String name,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(new AutoOffsetResetInternal(offsetReset), name, timestampExtractor, null, null, topicPattern);
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
     * @param valueDeserializer  value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topics             the name of one or more Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by another source
     */
    public synchronized Topology addSource(final String name,
                                           final Deserializer<?> keyDeserializer,
                                           final Deserializer<?> valueDeserializer,
                                           final String... topics) {
        internalTopologyBuilder.addSource(null, name, null, keyDeserializer, valueDeserializer, topics);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern and forwards the records to child processor
     * and/or sink nodes.
     * The source will use the specified key and value deserializers.
     * The provided de-/serializers will be used for all matched topics, so care should be taken to specify patterns for
     * topics that share the same key-value data format.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}
     * @param keyDeserializer    key deserializer used to read this source, if not specified the default
     *                           key deserializer defined in the configs will be used
     * @param valueDeserializer  value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by name
     */
    public synchronized Topology addSource(final String name,
                                           final Deserializer<?> keyDeserializer,
                                           final Deserializer<?> valueDeserializer,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(null, name, null, keyDeserializer, valueDeserializer, topicPattern);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern and forwards the records to child processor
     * and/or sink nodes.
     * The source will use the specified key and value deserializers.
     * The provided de-/serializers will be used for all the specified topics, so care should be taken when specifying
     * topics that share the same key-value data format.
     *
     * @param offsetReset        the auto offset reset policy to use for this stream if no committed offsets found;
     *                           acceptable values are earliest or latest
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}
     * @param keyDeserializer    key deserializer used to read this source, if not specified the default
     *                           key deserializer defined in the configs will be used
     * @param valueDeserializer  value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topics             the name of one or more Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by name
     * @deprecated Since 4.0. Use {@link #addSource(org.apache.kafka.streams.AutoOffsetReset, String, Deserializer, Deserializer, String...)} instead.
     */
    @Deprecated
    public synchronized Topology addSource(final AutoOffsetReset offsetReset,
                                           final String name,
                                           final Deserializer<?> keyDeserializer,
                                           final Deserializer<?> valueDeserializer,
                                           final String... topics) {
        internalTopologyBuilder.addSource(convertOldToNew(offsetReset), name, null, keyDeserializer, valueDeserializer, topics);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern and forwards the records to child processor
     * and/or sink nodes.
     * The source will use the specified key and value deserializers.
     * The provided de-/serializers will be used for all the specified topics, so care should be taken when specifying
     * topics that share the same key-value data format.
     *
     * @param offsetReset        the auto offset reset policy to use for this stream if no committed offsets found
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}
     * @param keyDeserializer    key deserializer used to read this source, if not specified the default
     *                           key deserializer defined in the configs will be used
     * @param valueDeserializer  value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topics             the name of one or more Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by name
     */
    public synchronized Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                           final String name,
                                           final Deserializer<?> keyDeserializer,
                                           final Deserializer<?> valueDeserializer,
                                           final String... topics) {
        internalTopologyBuilder.addSource(new AutoOffsetResetInternal(offsetReset), name, null, keyDeserializer, valueDeserializer, topics);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern and forwards the records to child processor
     * and/or sink nodes.
     * The source will use the specified key and value deserializers.
     * The provided de-/serializers will be used for all matched topics, so care should be taken to specify patterns for
     * topics that share the same key-value data format.
     *
     * @param offsetReset        the auto offset reset policy to use for this stream if no committed offsets found;
     *                           acceptable values are earliest or latest
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}
     * @param keyDeserializer    key deserializer used to read this source, if not specified the default
     *                           key deserializer defined in the configs will be used
     * @param valueDeserializer  value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by name
     * @deprecated Since 4.0. Use {@link #addSource(org.apache.kafka.streams.AutoOffsetReset, String, Deserializer, Deserializer, Pattern)} instead.
     */
    @Deprecated
    public synchronized Topology addSource(final AutoOffsetReset offsetReset,
                                           final String name,
                                           final Deserializer<?> keyDeserializer,
                                           final Deserializer<?> valueDeserializer,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(convertOldToNew(offsetReset), name, null, keyDeserializer, valueDeserializer, topicPattern);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern and forwards the records to child processor
     * and/or sink nodes.
     * The source will use the specified key and value deserializers.
     * The provided de-/serializers will be used for all matched topics, so care should be taken to specify patterns for
     * topics that share the same key-value data format.
     *
     * @param offsetReset        the auto offset reset policy to use for this stream if no committed offsets found
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}
     * @param keyDeserializer    key deserializer used to read this source, if not specified the default
     *                           key deserializer defined in the configs will be used
     * @param valueDeserializer  value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by name
     */
    public synchronized Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                           final String name,
                                           final Deserializer<?> keyDeserializer,
                                           final Deserializer<?> valueDeserializer,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(new AutoOffsetResetInternal(offsetReset), name, null, keyDeserializer, valueDeserializer, topicPattern);
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
     * @param valueDeserializer  value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topics             the name of one or more Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by another source
     * @deprecated Since 4.0. Use {@link #addSource(org.apache.kafka.streams.AutoOffsetReset, String, TimestampExtractor, Deserializer, Deserializer, String...)} instead.
     */
    @Deprecated
    public synchronized Topology addSource(final AutoOffsetReset offsetReset,
                                           final String name,
                                           final TimestampExtractor timestampExtractor,
                                           final Deserializer<?> keyDeserializer,
                                           final Deserializer<?> valueDeserializer,
                                           final String... topics) {
        internalTopologyBuilder.addSource(convertOldToNew(offsetReset), name, timestampExtractor, keyDeserializer, valueDeserializer, topics);
        return this;
    }

    /**
     * Add a new source that consumes the named topics and forwards the records to child processor and/or sink nodes.
     * The source will use the specified key and value deserializers.
     *
     * @param offsetReset        the auto offset reset policy to use for this stream if no committed offsets found
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param timestampExtractor the stateless timestamp extractor used for this source,
     *                           if not specified the default extractor defined in the configs will be used
     * @param keyDeserializer    key deserializer used to read this source, if not specified the default
     *                           key deserializer defined in the configs will be used
     * @param valueDeserializer  value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topics             the name of one or more Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by another source
     */
    public synchronized Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                           final String name,
                                           final TimestampExtractor timestampExtractor,
                                           final Deserializer<?> keyDeserializer,
                                           final Deserializer<?> valueDeserializer,
                                           final String... topics) {
        internalTopologyBuilder.addSource(new AutoOffsetResetInternal(offsetReset), name, timestampExtractor, keyDeserializer, valueDeserializer, topics);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern and forwards the records to child processor
     * and/or sink nodes.
     * The source will use the specified key and value deserializers.
     * The provided de-/serializers will be used for all matched topics, so care should be taken to specify patterns for
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
     * @param valueDeserializer  value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by name
     * @deprecated Since 4.0. Use {@link #addSource(org.apache.kafka.streams.AutoOffsetReset, String, TimestampExtractor, Deserializer, Deserializer, Pattern)} instead.
     */
    @Deprecated
    public synchronized Topology addSource(final AutoOffsetReset offsetReset,
                                           final String name,
                                           final TimestampExtractor timestampExtractor,
                                           final Deserializer<?> keyDeserializer,
                                           final Deserializer<?> valueDeserializer,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(convertOldToNew(offsetReset), name, timestampExtractor, keyDeserializer, valueDeserializer, topicPattern);
        return this;
    }

    /**
     * Add a new source that consumes from topics matching the given pattern and forwards the records to child processor
     * and/or sink nodes.
     * The source will use the specified key and value deserializers.
     * The provided de-/serializers will be used for all matched topics, so care should be taken to specify patterns for
     * topics that share the same key-value data format.
     *
     * @param offsetReset        the auto offset reset policy to use for this stream if no committed offsets found
     * @param name               the unique name of the source used to reference this node when
     *                           {@link #addProcessor(String, ProcessorSupplier, String...) adding processor children}.
     * @param timestampExtractor the stateless timestamp extractor used for this source,
     *                           if not specified the default extractor defined in the configs will be used
     * @param keyDeserializer    key deserializer used to read this source, if not specified the default
     *                           key deserializer defined in the configs will be used
     * @param valueDeserializer  value deserializer used to read this source,
     *                           if not specified the default value deserializer defined in the configs will be used
     * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
     * @return itself
     * @throws TopologyException if processor is already added or if topics have already been registered by name
     */
    public synchronized Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                           final String name,
                                           final TimestampExtractor timestampExtractor,
                                           final Deserializer<?> keyDeserializer,
                                           final Deserializer<?> valueDeserializer,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(new AutoOffsetResetInternal(offsetReset), name, timestampExtractor, keyDeserializer, valueDeserializer, topicPattern);
        return this;
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic.
     * The sink will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} and
     * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link StreamsConfig stream configuration}.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its records
     * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
     * and write to its topic
     * @return itself
     * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
     * @see #addSink(String, String, StreamPartitioner, String...)
     * @see #addSink(String, String, Serializer, Serializer, String...)
     * @see #addSink(String, String, Serializer, Serializer, StreamPartitioner, String...)
     */
    public synchronized Topology addSink(final String name,
                                         final String topic,
                                         final String... parentNames) {
        internalTopologyBuilder.addSink(name, topic, null, null, null, parentNames);
        return this;
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic,
     * using the supplied partitioner.
     * The sink will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} and
     * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link StreamsConfig stream configuration}.
     * <p>
     * The sink will also use the specified {@link StreamPartitioner} to determine how records are distributed among
     * the named Kafka topic's partitions.
     * Such control is often useful with topologies that use {@link #addStateStore(StoreBuilder, String...) state
     * stores} in its processors.
     * In most other cases, however, a partitioner needs not be specified and Kafka will automatically distribute
     * records among partitions using Kafka's default partitioning logic.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its records
     * @param partitioner the function that should be used to determine the partition for each record processed by the sink
     * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
     * and write to its topic
     * @return itself
     * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
     * @see #addSink(String, String, String...)
     * @see #addSink(String, String, Serializer, Serializer, String...)
     * @see #addSink(String, String, Serializer, Serializer, StreamPartitioner, String...)
     */
    public synchronized <K, V> Topology addSink(final String name,
                                                final String topic,
                                                final StreamPartitioner<? super K, ? super V> partitioner,
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
     * should use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} specified in the
     * {@link StreamsConfig stream configuration}
     * @param valueSerializer the {@link Serializer value serializer} used when consuming records; may be null if the sink
     * should use the {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link StreamsConfig stream configuration}
     * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
     * and write to its topic
     * @return itself
     * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
     * @see #addSink(String, String, String...)
     * @see #addSink(String, String, StreamPartitioner, String...)
     * @see #addSink(String, String, Serializer, Serializer, StreamPartitioner, String...)
     */
    public synchronized <K, V> Topology addSink(final String name,
                                                final String topic,
                                                final Serializer<K> keySerializer,
                                                final Serializer<V> valueSerializer,
                                                final String... parentNames) {
        internalTopologyBuilder.addSink(name, topic, keySerializer, valueSerializer, null, parentNames);
        return this;
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic.
     * The sink will use the specified key and value serializers, and the supplied partitioner.
     *
     * @param name the unique name of the sink
     * @param topic the name of the Kafka topic to which this sink should write its records
     * @param keySerializer the {@link Serializer key serializer} used when consuming records; may be null if the sink
     * should use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} specified in the
     * {@link StreamsConfig stream configuration}
     * @param valueSerializer the {@link Serializer value serializer} used when consuming records; may be null if the sink
     * should use the {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link StreamsConfig stream configuration}
     * @param partitioner the function that should be used to determine the partition for each record processed by the sink
     * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
     * and write to its topic
     * @return itself
     * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
     * @see #addSink(String, String, String...)
     * @see #addSink(String, String, StreamPartitioner, String...)
     * @see #addSink(String, String, Serializer, Serializer, String...)
     */
    public synchronized <K, V> Topology addSink(final String name,
                                                final String topic,
                                                final Serializer<K> keySerializer,
                                                final Serializer<V> valueSerializer,
                                                final StreamPartitioner<? super K, ? super V> partitioner,
                                                final String... parentNames) {
        internalTopologyBuilder.addSink(name, topic, keySerializer, valueSerializer, partitioner, parentNames);
        return this;
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to Kafka topics based on {@code topicExtractor}.
     * The topics that it may ever send to should be pre-created.
     * The sink will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} and
     * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link StreamsConfig stream configuration}.
     *
     * @param name              the unique name of the sink
     * @param topicExtractor    the extractor to determine the name of the Kafka topic to which this sink should write for each record
     * @param parentNames       the name of one or more source or processor nodes whose output records this sink should consume
     *                          and dynamically write to topics
     * @return                  itself
     * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
     * @see #addSink(String, String, StreamPartitioner, String...)
     * @see #addSink(String, String, Serializer, Serializer, String...)
     * @see #addSink(String, String, Serializer, Serializer, StreamPartitioner, String...)
     */
    public synchronized <K, V> Topology addSink(final String name,
                                                final TopicNameExtractor<K, V> topicExtractor,
                                                final String... parentNames) {
        internalTopologyBuilder.addSink(name, topicExtractor, null, null, null, parentNames);
        return this;
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to Kafka topics based on {@code topicExtractor},
     * using the supplied partitioner.
     * The topics that it may ever send to should be pre-created.
     * The sink will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} and
     * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     * {@link StreamsConfig stream configuration}.
     * <p>
     * The sink will also use the specified {@link StreamPartitioner} to determine how records are distributed among
     * the named Kafka topic's partitions.
     * Such control is often useful with topologies that use {@link #addStateStore(StoreBuilder, String...) state
     * stores} in its processors.
     * In most other cases, however, a partitioner needs not be specified and Kafka will automatically distribute
     * records among partitions using Kafka's default partitioning logic.
     *
     * @param name              the unique name of the sink
     * @param topicExtractor    the extractor to determine the name of the Kafka topic to which this sink should write for each record
     * @param partitioner       the function that should be used to determine the partition for each record processed by the sink
     * @param parentNames       the name of one or more source or processor nodes whose output records this sink should consume
     *                          and dynamically write to topics
     * @return                  itself
     * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
     * @see #addSink(String, String, String...)
     * @see #addSink(String, String, Serializer, Serializer, String...)
     * @see #addSink(String, String, Serializer, Serializer, StreamPartitioner, String...)
     */
    public synchronized <K, V> Topology addSink(final String name,
                                                final TopicNameExtractor<K, V> topicExtractor,
                                                final StreamPartitioner<? super K, ? super V> partitioner,
                                                final String... parentNames) {
        internalTopologyBuilder.addSink(name, topicExtractor, null, null, partitioner, parentNames);
        return this;
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to Kafka topics based on {@code topicExtractor}.
     * The topics that it may ever send to should be pre-created.
     * The sink will use the specified key and value serializers.
     *
     * @param name              the unique name of the sink
     * @param topicExtractor    the extractor to determine the name of the Kafka topic to which this sink should write for each record
     * @param keySerializer     the {@link Serializer key serializer} used when consuming records; may be null if the sink
     *                          should use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} specified in the
     *                          {@link StreamsConfig stream configuration}
     * @param valueSerializer   the {@link Serializer value serializer} used when consuming records; may be null if the sink
     *                          should use the {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     *                          {@link StreamsConfig stream configuration}
     * @param parentNames       the name of one or more source or processor nodes whose output records this sink should consume
     *                          and dynamically write to topics
     * @return                  itself
     * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
     * @see #addSink(String, String, String...)
     * @see #addSink(String, String, StreamPartitioner, String...)
     * @see #addSink(String, String, Serializer, Serializer, StreamPartitioner, String...)
     */
    public synchronized <K, V> Topology addSink(final String name,
                                                final TopicNameExtractor<K, V> topicExtractor,
                                                final Serializer<K> keySerializer,
                                                final Serializer<V> valueSerializer,
                                                final String... parentNames) {
        internalTopologyBuilder.addSink(name, topicExtractor, keySerializer, valueSerializer, null, parentNames);
        return this;
    }

    /**
     * Add a new sink that forwards records from upstream parent processor and/or source nodes to Kafka topics based on {@code topicExtractor}.
     * The topics that it may ever send to should be pre-created.
     * The sink will use the specified key and value serializers, and the supplied partitioner.
     *
     * @param name              the unique name of the sink
     * @param topicExtractor    the extractor to determine the name of the Kafka topic to which this sink should write for each record
     * @param keySerializer     the {@link Serializer key serializer} used when consuming records; may be null if the sink
     *                          should use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} specified in the
     *                          {@link StreamsConfig stream configuration}
     * @param valueSerializer   the {@link Serializer value serializer} used when consuming records; may be null if the sink
     *                          should use the {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
     *                          {@link StreamsConfig stream configuration}
     * @param partitioner       the function that should be used to determine the partition for each record processed by the sink
     * @param parentNames       the name of one or more source or processor nodes whose output records this sink should consume
     *                          and dynamically write to topics
     * @return                  itself
     * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
     * @see #addSink(String, String, String...)
     * @see #addSink(String, String, StreamPartitioner, String...)
     * @see #addSink(String, String, Serializer, Serializer, String...)
     */
    public synchronized <K, V> Topology addSink(final String name,
                                                final TopicNameExtractor<K, V> topicExtractor,
                                                final Serializer<K> keySerializer,
                                                final Serializer<V> valueSerializer,
                                                final StreamPartitioner<? super K, ? super V> partitioner,
                                                final String... parentNames) {
        internalTopologyBuilder.addSink(name, topicExtractor, keySerializer, valueSerializer, partitioner, parentNames);
        return this;
    }

    /**
     * Add a new processor node that receives and processes records output by one or more parent source or processor
     * node.
     * Any new record output by this processor will be forwarded to its child processor or sink nodes.
     * If {@code supplier} provides stores via {@link ConnectedStoreProvider#stores()}, the provided {@link StoreBuilder}s
     * will be added to the topology and connected to this processor automatically.
     *
     * @param name the unique name of the processor node
     * @param supplier the supplier used to obtain this node's {@link Processor} instance
     * @param parentNames the name of one or more source or processor nodes whose output records this processor should receive
     * and process
     * @return itself
     * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
     */
    public synchronized <KIn, VIn, KOut, VOut> Topology addProcessor(final String name,
                                                                     final ProcessorSupplier<KIn, VIn, KOut, VOut> supplier,
                                                                     final String... parentNames) {
        final ProcessorSupplier<KIn, VIn, KOut, VOut> wrapped = internalTopologyBuilder.wrapProcessorSupplier(name, supplier);
        internalTopologyBuilder.addProcessor(name, wrapped, parentNames);
        final Set<StoreBuilder<?>> stores = wrapped.stores();

        if (stores != null) {
            for (final StoreBuilder<?> storeBuilder : stores) {
                internalTopologyBuilder.addStateStore(storeBuilder, name);
            }
        }
        return this;
    }

    /**
     * Adds a state store.
     *
     * @param storeBuilder the storeBuilder used to obtain this state store {@link StateStore} instance
     * @param processorNames the names of the processors that should be able to access the provided store
     * @return itself
     * @throws TopologyException if state store supplier is already added
     */
    public synchronized Topology addStateStore(final StoreBuilder<?> storeBuilder,
                                               final String... processorNames) {
        internalTopologyBuilder.addStateStore(storeBuilder, processorNames);
        return this;
    }

    /**
     * Adds a read-only {@link StateStore} to the topology.
     * <p>
     * A read-only {@link StateStore} does not create a dedicated changelog topic but uses it's input topic as
     * changelog; thus, the used topic should be configured with log compaction.
     * <p>
     * The <code>auto.offset.reset</code> property will be set to <code>earliest</code> for this topic.
     * <p>
     * The provided {@link ProcessorSupplier} will be used to create a processor for all messages received
     * from the given topic. This processor should contain logic to keep the {@link StateStore} up-to-date.
     *
     * @param storeBuilder          user defined store builder
     * @param sourceName            name of the {@link SourceNode} that will be automatically added
     * @param timestampExtractor    the stateless timestamp extractor used for this source,
     *                              if not specified the default extractor defined in the configs will be used
     * @param keyDeserializer       the {@link Deserializer} to deserialize keys with
     * @param valueDeserializer     the {@link Deserializer} to deserialize values with
     * @param topic                 the topic to source the data from
     * @param processorName         the name of the {@link ProcessorSupplier}
     * @param stateUpdateSupplier   the instance of {@link ProcessorSupplier}
     * @return itself
     * @throws TopologyException if the processor of state is already registered
     */
    public synchronized <KIn, VIn> Topology addReadOnlyStateStore(final StoreBuilder<?> storeBuilder,
                                                                  final String sourceName,
                                                                  final TimestampExtractor timestampExtractor,
                                                                  final Deserializer<KIn> keyDeserializer,
                                                                  final Deserializer<VIn> valueDeserializer,
                                                                  final String topic,
                                                                  final String processorName,
                                                                  final ProcessorSupplier<KIn, VIn, Void, Void> stateUpdateSupplier) {
        storeBuilder.withLoggingDisabled();

        internalTopologyBuilder.addSource(
            new AutoOffsetResetInternal(org.apache.kafka.streams.AutoOffsetReset.earliest()),
            sourceName,
            timestampExtractor,
            keyDeserializer,
            valueDeserializer,
            topic
        );
        internalTopologyBuilder.addProcessor(processorName, stateUpdateSupplier, sourceName);
        internalTopologyBuilder.addStateStore(storeBuilder, processorName);
        internalTopologyBuilder.connectSourceStoreAndTopic(storeBuilder.name(), topic);

        return this;
    }

    /**
     * Adds a read-only {@link StateStore} to the topology.
     * <p>
     * A read-only {@link StateStore} does not create a dedicated changelog topic but uses it's input topic as
     * changelog; thus, the used topic should be configured with log compaction.
     * <p>
     * The <code>auto.offset.reset</code> property will be set to <code>earliest</code> for this topic.
     * <p>
     * The provided {@link ProcessorSupplier} will be used to create a processor for all messages received
     * from the given topic. This processor should contain logic to keep the {@link StateStore} up-to-date.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param storeBuilder          user defined store builder
     * @param sourceName            name of the {@link SourceNode} that will be automatically added
     * @param keyDeserializer       the {@link Deserializer} to deserialize keys with
     * @param valueDeserializer     the {@link Deserializer} to deserialize values with
     * @param topic                 the topic to source the data from
     * @param processorName         the name of the {@link ProcessorSupplier}
     * @param stateUpdateSupplier   the instance of {@link ProcessorSupplier}
     * @return itself
     * @throws TopologyException if the processor of state is already registered
     */
    public synchronized <KIn, VIn> Topology addReadOnlyStateStore(final StoreBuilder<?> storeBuilder,
                                                                  final String sourceName,
                                                                  final Deserializer<KIn> keyDeserializer,
                                                                  final Deserializer<VIn> valueDeserializer,
                                                                  final String topic,
                                                                  final String processorName,
                                                                  final ProcessorSupplier<KIn, VIn, Void, Void> stateUpdateSupplier) {
        return addReadOnlyStateStore(
                storeBuilder,
                sourceName,
                null,
                keyDeserializer,
                valueDeserializer,
                topic,
                processorName,
                stateUpdateSupplier
        );
    }

    /**
     * Adds a global {@link StateStore} to the topology.
     * The {@link StateStore} sources its data from all partitions of the provided input topic.
     * There will be exactly one instance of this {@link StateStore} per Kafka Streams instance.
     * <p>
     * A {@link SourceNode} with the provided sourceName will be added to consume the data arriving from the partitions
     * of the input topic.
     * <p>
     * The provided {@link ProcessorSupplier} will be used to create an {@link ProcessorNode} that will receive all
     * records forwarded from the {@link SourceNode}.
     * This {@link ProcessorNode} should be used to keep the {@link StateStore} up-to-date.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param storeBuilder          user defined state store builder
     * @param sourceName            name of the {@link SourceNode} that will be automatically added
     * @param keyDeserializer       the {@link Deserializer} to deserialize keys with
     * @param valueDeserializer     the {@link Deserializer} to deserialize values with
     * @param topic                 the topic to source the data from
     * @param processorName         the name of the {@link ProcessorSupplier}
     * @param stateUpdateSupplier   the instance of {@link ProcessorSupplier}
     * @return itself
     * @throws TopologyException if the processor of state is already registered
     */
    public synchronized <KIn, VIn> Topology addGlobalStore(final StoreBuilder<?> storeBuilder,
                                                           final String sourceName,
                                                           final Deserializer<KIn> keyDeserializer,
                                                           final Deserializer<VIn> valueDeserializer,
                                                           final String topic,
                                                           final String processorName,
                                                           final ProcessorSupplier<KIn, VIn, Void, Void> stateUpdateSupplier) {
        internalTopologyBuilder.addGlobalStore(
            sourceName,
            null,
            keyDeserializer,
            valueDeserializer,
            topic,
            processorName,
            new StoreDelegatingProcessorSupplier<>(stateUpdateSupplier, Set.of(storeBuilder)),
            true
        );
        return this;
    }

    /**
     * Adds a global {@link StateStore} to the topology.
     * The {@link StateStore} sources its data from all partitions of the provided input topic.
     * There will be exactly one instance of this {@link StateStore} per Kafka Streams instance.
     * <p>
     * A {@link SourceNode} with the provided sourceName will be added to consume the data arriving from the partitions
     * of the input topic.
     * <p>
     * The provided {@link ProcessorSupplier} will be used to create an {@link ProcessorNode} that will receive all
     * records forwarded from the {@link SourceNode}.
     * This {@link ProcessorNode} should be used to keep the {@link StateStore} up-to-date.
     *
     * @param storeBuilder          user defined key value store builder
     * @param sourceName            name of the {@link SourceNode} that will be automatically added
     * @param timestampExtractor    the stateless timestamp extractor used for this source,
     *                              if not specified the default extractor defined in the configs will be used
     * @param keyDeserializer       the {@link Deserializer} to deserialize keys with
     * @param valueDeserializer     the {@link Deserializer} to deserialize values with
     * @param topic                 the topic to source the data from
     * @param processorName         the name of the {@link ProcessorSupplier}
     * @param stateUpdateSupplier   the instance of {@link ProcessorSupplier}
     * @return itself
     * @throws TopologyException if the processor of state is already registered
     */
    public synchronized <KIn, VIn> Topology addGlobalStore(final StoreBuilder<?> storeBuilder,
                                                           final String sourceName,
                                                           final TimestampExtractor timestampExtractor,
                                                           final Deserializer<KIn> keyDeserializer,
                                                           final Deserializer<VIn> valueDeserializer,
                                                           final String topic,
                                                           final String processorName,
                                                           final ProcessorSupplier<KIn, VIn, Void, Void> stateUpdateSupplier) {
        internalTopologyBuilder.addGlobalStore(
            sourceName,
            timestampExtractor,
            keyDeserializer,
            valueDeserializer,
            topic,
            processorName,
            new StoreDelegatingProcessorSupplier<>(stateUpdateSupplier, Set.of(storeBuilder)),
            true
        );
        return this;
    }

    /**
     * Connects the processor and the state stores.
     *
     * @param processorName the name of the processor
     * @param stateStoreNames the names of state stores that the processor uses
     * @return itself
     * @throws TopologyException if the processor or a state store is unknown
     */
    public synchronized Topology connectProcessorAndStateStores(final String processorName,
                                                                final String... stateStoreNames) {
        internalTopologyBuilder.connectProcessorAndStateStores(processorName, stateStoreNames);
        return this;
    }

    /**
     * Returns a description of the specified {@code Topology}.
     *
     * @return a description of the topology.
     */

    public synchronized TopologyDescription describe() {
        return internalTopologyBuilder.describe();
    }
}
