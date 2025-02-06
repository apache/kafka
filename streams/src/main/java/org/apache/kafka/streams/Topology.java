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
import org.apache.kafka.streams.processor.internals.StoreDelegatingProcessorSupplier;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A logical representation of a {@code ProcessorTopology}.
 * A topology is a graph of sources, processors, and sinks.
 * A {@code SourceNode} is a node in the graph that consumes one or more Kafka topics and forwards them to its
 * successor nodes.
 * A {@link Processor} is a node in the graph that receives input records from upstream nodes, processes the
 * records, and optionally forwarding new records to one, multiple, or all of its downstream nodes.
 * Finally, a {@code SinkNode} is a node in the graph that receives records from upstream nodes and writes them to
 * a Kafka topic.
 * A {@code Topology} allows you to construct a graph of these nodes, and then passed into a new
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
     * Add a source that consumes the named topics and forwards the records to child
     * {@link #addProcessor(String, ProcessorSupplier, String...) processors} and
     * {@link #addSink(String, String, String...) sinks}.
     *
     * <p>The source will use the default values from {@link StreamsConfig} for
     * <ul>
     *   <li>{@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG key deserializer}</li>
     *   <li>{@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG value deserializer}</li>
     *   <li>{@link org.apache.kafka.clients.consumer.ConsumerConfig#AUTO_OFFSET_RESET_CONFIG auto.offset.reset}</li>
     *   <li>{@link StreamsConfig#DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG timestamp extractor}</li>
     * </ul>
     *
     * If you want to specify a source specific {@link org.apache.kafka.streams.AutoOffsetReset auto.offset.reset
     * strategy}, {@link TimestampExtractor}, or key/value {@link Deserializer}, use the corresponding overloaded
     * {@code addSource(...)} method.
     *
     * @param name
     *        the unique name of the source used to reference this node when adding
     *        {@link #addProcessor(String, ProcessorSupplier, String...) processor} or
     *        {@link #addSink(String, String, String...) sink} children
     * @param topics
     *        the name of one or more Kafka topics that this source is to consume
     *
     * @return itself
     *
     * @throws TopologyException
     *         if the provided source name is not unique,
     *         no topics are specified, or
     *         a topic has already been registered by another source,
     *         {@link #addReadOnlyStateStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier) read-only state store}, or
     *         {@link #addGlobalStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier) global state store}
     * @throws NullPointerException
     *         if {@code name} or {@code topics} is {@code null}, or
     *         {@code topics} contains a {@code null} topic
     *
     * @see #addSource(String, Pattern)
     */
    public synchronized Topology addSource(final String name,
                                           final String... topics) {
        internalTopologyBuilder.addSource(null, name, null, null, null, topics);
        return this;
    }

    /**
     * See {@link #addSource(String, String...)}.
     *
     * <p>Takes a {@link Pattern} (cannot be {@code null}) to match topics to consumes from, instead of a list of topic names.
     */
    public synchronized Topology addSource(final String name,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(null, name, null, null, null, topicPattern);
        return this;
    }

    /**
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
     * See {@link #addSource(String, String...)}.
     */
    public synchronized Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                           final String name,
                                           final String... topics) {
        internalTopologyBuilder.addSource(
            offsetReset == null ? null : new AutoOffsetResetInternal(offsetReset),
            name,
            null,
            null,
            null,
            topics
        );
        return this;
    }

    /**
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
     * See {@link #addSource(String, Pattern)}.
     */
    public synchronized Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                           final String name,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(
            offsetReset == null ? null : new AutoOffsetResetInternal(offsetReset),
            name,
            null,
            null,
            null,
            topicPattern
        );
        return this;
    }

    /**
     * See {@link #addSource(String, String...)}.
     */
    public synchronized Topology addSource(final TimestampExtractor timestampExtractor,
                                           final String name,
                                           final String... topics) {
        internalTopologyBuilder.addSource(null, name, timestampExtractor, null, null, topics);
        return this;
    }

    /**
     * See {@link #addSource(String, Pattern)}.
     */
    public synchronized Topology addSource(final TimestampExtractor timestampExtractor,
                                           final String name,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(null, name, timestampExtractor, null, null, topicPattern);
        return this;
    }

    /**
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
     * See {@link #addSource(String, String...)}.
     */
    public synchronized Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                           final TimestampExtractor timestampExtractor,
                                           final String name,
                                           final String... topics) {
        internalTopologyBuilder.addSource(
            offsetReset == null ? null : new AutoOffsetResetInternal(offsetReset),
            name,
            timestampExtractor,
            null,
            null,
            topics
        );
        return this;
    }

    /**
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
     * See {@link #addSource(String, Pattern)}.
     */
    public synchronized Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                           final TimestampExtractor timestampExtractor,
                                           final String name,
                                           final Pattern topicPattern) {
        internalTopologyBuilder.addSource(
            offsetReset == null ? null : new AutoOffsetResetInternal(offsetReset),
            name,
            timestampExtractor,
            null,
            null,
            topicPattern
        );
        return this;
    }

    /**
     * See {@link #addSource(String, String...)}.
     */
    public synchronized <K, V> Topology addSource(final String name,
                                                  final Deserializer<K> keyDeserializer,
                                                  final Deserializer<V> valueDeserializer,
                                                  final String... topics) {
        internalTopologyBuilder.addSource(null, name, null, keyDeserializer, valueDeserializer, topics);
        return this;
    }

    /**
     * See {@link #addSource(String, Pattern)}.
     */
    public synchronized <K, V> Topology addSource(final String name,
                                                  final Deserializer<K> keyDeserializer,
                                                  final Deserializer<V> valueDeserializer,
                                                  final Pattern topicPattern) {
        internalTopologyBuilder.addSource(null, name, null, keyDeserializer, valueDeserializer, topicPattern);
        return this;
    }

    /**
     * @deprecated Since 4.0. Use {@link #addSource(org.apache.kafka.streams.AutoOffsetReset, String, Deserializer, Deserializer, String...)} instead.
     */
    @Deprecated
    public synchronized <K, V> Topology addSource(final AutoOffsetReset offsetReset,
                                                  final String name,
                                                  final Deserializer<K> keyDeserializer,
                                                  final Deserializer<V> valueDeserializer,
                                                  final String... topics) {
        internalTopologyBuilder.addSource(convertOldToNew(offsetReset), name, null, keyDeserializer, valueDeserializer, topics);
        return this;
    }

    /**
     * See {@link #addSource(String, String...)}.
     */
    public synchronized <K, V> Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                                  final String name,
                                                  final Deserializer<K> keyDeserializer,
                                                  final Deserializer<V> valueDeserializer,
                                                  final String... topics) {
        internalTopologyBuilder.addSource(
            offsetReset == null ? null : new AutoOffsetResetInternal(offsetReset),
            name,
            null,
            keyDeserializer,
            valueDeserializer,
            topics
        );
        return this;
    }

    /**
     * @deprecated Since 4.0. Use {@link #addSource(org.apache.kafka.streams.AutoOffsetReset, String, Deserializer, Deserializer, Pattern)} instead.
     */
    @Deprecated
    public synchronized <K, V> Topology addSource(final AutoOffsetReset offsetReset,
                                                  final String name,
                                                  final Deserializer<K> keyDeserializer,
                                                  final Deserializer<V> valueDeserializer,
                                                  final Pattern topicPattern) {
        internalTopologyBuilder.addSource(convertOldToNew(offsetReset), name, null, keyDeserializer, valueDeserializer, topicPattern);
        return this;
    }

    /**
     * See {@link #addSource(String, Pattern)}.
     */
    public synchronized <K, V> Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                                  final String name,
                                                  final Deserializer<K> keyDeserializer,
                                                  final Deserializer<V> valueDeserializer,
                                                  final Pattern topicPattern) {
        internalTopologyBuilder.addSource(
            offsetReset == null ? null : new AutoOffsetResetInternal(offsetReset),
            name,
            null,
            keyDeserializer,
            valueDeserializer,
            topicPattern
        );
        return this;
    }

    /**
     * @deprecated Since 4.0. Use {@link #addSource(org.apache.kafka.streams.AutoOffsetReset, String, TimestampExtractor, Deserializer, Deserializer, String...)} instead.
     */
    @Deprecated
    public synchronized <K, V> Topology addSource(final AutoOffsetReset offsetReset,
                                                  final String name,
                                                  final TimestampExtractor timestampExtractor,
                                                  final Deserializer<K> keyDeserializer,
                                                  final Deserializer<V> valueDeserializer,
                                                  final String... topics) {
        internalTopologyBuilder.addSource(convertOldToNew(offsetReset), name, timestampExtractor, keyDeserializer, valueDeserializer, topics);
        return this;
    }

    /**
     * See {@link #addSource(String, String...)}.
     */
    public synchronized <K, V> Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                                  final String name,
                                                  final TimestampExtractor timestampExtractor,
                                                  final Deserializer<K> keyDeserializer,
                                                  final Deserializer<V> valueDeserializer,
                                                  final String... topics) {
        internalTopologyBuilder.addSource(
            offsetReset == null ? null : new AutoOffsetResetInternal(offsetReset),
            name,
            timestampExtractor,
            keyDeserializer,
            valueDeserializer,
            topics
        );
        return this;
    }

    /**
     * @deprecated Since 4.0. Use {@link #addSource(org.apache.kafka.streams.AutoOffsetReset, String, TimestampExtractor, Deserializer, Deserializer, Pattern)} instead.
     */
    @Deprecated
    public synchronized <K, V> Topology addSource(final AutoOffsetReset offsetReset,
                                                  final String name,
                                                  final TimestampExtractor timestampExtractor,
                                                  final Deserializer<K> keyDeserializer,
                                                  final Deserializer<V> valueDeserializer,
                                                  final Pattern topicPattern) {
        internalTopologyBuilder.addSource(convertOldToNew(offsetReset), name, timestampExtractor, keyDeserializer, valueDeserializer, topicPattern);
        return this;
    }

    /**
     * See {@link #addSource(String, Pattern)}.
     */
    public synchronized <K, V> Topology addSource(final org.apache.kafka.streams.AutoOffsetReset offsetReset,
                                                  final String name,
                                                  final TimestampExtractor timestampExtractor,
                                                  final Deserializer<K> keyDeserializer,
                                                  final Deserializer<V> valueDeserializer,
                                                  final Pattern topicPattern) {
        internalTopologyBuilder.addSource(
            offsetReset == null ? null : new AutoOffsetResetInternal(offsetReset),
            name,
            timestampExtractor,
            keyDeserializer,
            valueDeserializer,
            topicPattern
        );
        return this;
    }

    /**
     * Add a sink that sends records from upstream
     * {@link #addProcessor(String, ProcessorSupplier, String...) processors} or
     * {@link #addSource(String, String...) sources} to the named Kafka topic.
     * The specified topic should be created before the {@link KafkaStreams} instance is started.
     *
     * <p>The sink will use the default values from {@link StreamsConfig} for
     * <ul>
     *   <li>{@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG key serializer}</li>
     *   <li>{@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG value serializer}</li>
     * </ul>
     *
     * Furthermore, the producer's configured partitioner is used to write into the topic.
     * If you want to specify a sink specific key or value {@link Serializer}, or use a different
     * {@link StreamPartitioner partitioner}, use the corresponding overloaded {@code addSink(...)} method.
     *
     * @param name
     *        the unique name of the sink
     * @param topic
     *        the name of the Kafka topic to which this sink should write its records
     * @param parentNames
     *        the name of one or more {@link #addProcessor(String, ProcessorSupplier, String...) processors} or
     *        {@link #addSource(String, String...) sources}, whose output records this sink should consume and write
     *        to the specified output topic
     *
     * @return itself
     *
     * @throws TopologyException
     *         if the provided sink name is not unique, or
     *         if a parent processor/source name is unknown or specifies a sink
     * @throws NullPointerException
     *         if {@code name}, {@code topic}, or {@code parentNames} is {@code null}, or
     *         {@code parentNames} contains a {@code null} parent name
     *
     * @see #addSink(String, TopicNameExtractor, String...)
     */
    public synchronized Topology addSink(final String name,
                                         final String topic,
                                         final String... parentNames) {
        internalTopologyBuilder.addSink(name, topic, null, null, null, parentNames);
        return this;
    }

    /**
     * See {@link #addSink(String, String, String...)}.
     */
    public synchronized <K, V> Topology addSink(final String name,
                                                final String topic,
                                                final StreamPartitioner<? super K, ? super V> partitioner,
                                                final String... parentNames) {
        internalTopologyBuilder.addSink(name, topic, null, null, partitioner, parentNames);
        return this;
    }

    /**
     * See {@link #addSink(String, String, String...)}.
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
     * See {@link #addSink(String, String, String...)}.
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
     * See {@link #addSink(String, String, String...)}.
     *
     * <p>Takes a {@link TopicNameExtractor} (cannot be {@code null}) that computes topic names to send records into,
     * instead of a single topic name.
     * The topic name extractor is called for every result record and may compute a different topic name each time.
     * All topics, that the topic name extractor may compute, should be created before the {@link KafkaStreams}
     * instance is started.
     * Returning {@code null} as topic name is invalid and will result in a runtime exception.
     */
    public synchronized <K, V> Topology addSink(final String name,
                                                final TopicNameExtractor<? super K, ? super V> topicExtractor,
                                                final String... parentNames) {
        internalTopologyBuilder.addSink(name, topicExtractor, null, null, null, parentNames);
        return this;
    }

    /**
     * See {@link #addSink(String, String, String...)}.
     */
    public synchronized <K, V> Topology addSink(final String name,
                                                final TopicNameExtractor<? super K, ? super V> topicExtractor,
                                                final StreamPartitioner<? super K, ? super V> partitioner,
                                                final String... parentNames) {
        internalTopologyBuilder.addSink(name, topicExtractor, null, null, partitioner, parentNames);
        return this;
    }

    /**
     * See {@link #addSink(String, String, String...)}.
     */
    public synchronized <K, V> Topology addSink(final String name,
                                                final TopicNameExtractor<? super K, ? super V> topicExtractor,
                                                final Serializer<K> keySerializer,
                                                final Serializer<V> valueSerializer,
                                                final String... parentNames) {
        internalTopologyBuilder.addSink(name, topicExtractor, keySerializer, valueSerializer, null, parentNames);
        return this;
    }

    /**
     * See {@link #addSink(String, String, String...)}.
     */
    public synchronized <K, V> Topology addSink(final String name,
                                                final TopicNameExtractor<? super K, ? super V> topicExtractor,
                                                final Serializer<K> keySerializer,
                                                final Serializer<V> valueSerializer,
                                                final StreamPartitioner<? super K, ? super V> partitioner,
                                                final String... parentNames) {
        internalTopologyBuilder.addSink(name, topicExtractor, keySerializer, valueSerializer, partitioner, parentNames);
        return this;
    }

    /**
     * Add a {@link Processor processor} that receives and processed records from one or more parent processors or
     * {@link #addSource(String, String...) sources}.
     * Any record output by this processor will be forwarded to its child processors and
     * {@link #addSink(String, String, String...) sinks}.
     *
     * <p>By default, the processor is stateless.
     * There is three different {@link StateStore state stores}, which can be connected to a processor:
     * <ul>
     *   <li>{@link #addStateStore(StoreBuilder, String...) state stores} for processing (i.e., read/write access)</li>
     *   <li>{@link #addReadOnlyStateStore(StoreBuilder, String, TimestampExtractor, Deserializer, Deserializer, String, String, ProcessorSupplier) read-only state stores}</li>
     *   <li>{@link #addGlobalStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier) global state stores} (read-only)</li>
     * </ul>
     *
     * If the {@code supplier} provides state stores via {@link ConnectedStoreProvider#stores()}, the corresponding
     * {@link StoreBuilder StoreBuilders} will be {@link #addStateStore(StoreBuilder, String...) added to the topology
     * and connected} to this processor automatically.
     *
     * @param name
     *        the unique name of the processor used to reference this node when adding other processor or
     *        {@link #addSink(String, String, String...) sink} children
     * @param supplier
     *        the supplier used to obtain {@link Processor} instances
     * @param parentNames
     *        the name of one or more processors or {@link #addSource(String, String...) sources},
     *        whose output records this processor should receive and process
     *
     * @return itself
     *
     * @throws TopologyException
     *         if the provided processor name is not unique, or
     *         if a parent processor/source name is unknown or specifies a sink
     *
     * @see org.apache.kafka.streams.processor.api.ContextualProcessor ContextualProcessor
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
     * Add a {@link StateStore state store} to the topology, and optionally connect it to one or more
     * {@link #addProcessor(String, ProcessorSupplier, String...) processors}.
     * State stores are sharded and the number of shards is determined at runtime by the number of input topic
     * partitions and the structure of the topology.
     * Each connected {@link Processor} instance in the topology has access to a single shard of the state store.
     * Additionally, the state store can be accessed from "outside" using "Interactive Queries" (cf.,
     * {@link KafkaStreams#store(StoreQueryParameters)} and {@link KafkaStreams#query(StateQueryRequest)}).
     * If you need access to all data in a state store inside a {@link Processor}, you can use a (read-only)
     * {@link #addGlobalStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier)
     * global state store}.
     *
     * <p>If no {@code processorNames} is specified, the state store can be
     * {@link #connectProcessorAndStateStores(String, String...) connected} to one or more
     * {@link #addProcessor(String, ProcessorSupplier, String...) processors} later.
     *
     * <p>Note, if a state store is never connected to any
     * {@link #addProcessor(String, ProcessorSupplier, String...) processor}, the state store is "dangling" and would
     * not be added to the created {@code ProcessorTopology}, when {@link KafkaStreams} is started.
     * For this case, the state store is not available for "Interactive Queries".
     * If you want to add a state store only for "Interactive Queries", you can use a
     * {@link #addReadOnlyStateStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier) read-only state store}.
     *
     * <p>For failure and recovery, a state store {@link StoreBuilder#loggingEnabled() may be backed} by an internal
     * changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is provided by the
     * {@link StoreBuilder#name() store builder}, and "-changelog" is a fixed suffix.
     *
     * <p>You can verify the created {@code ProcessorTopology} and added state stores, and retrieve all generated
     * internal topic names, via {@link Topology#describe()}.
     *
     * @param storeBuilder
     *        the {@link StoreBuilder} used to obtain {@link StateStore state store} instances (one per shard)
     * @param processorNames
     *        the names of the {@link #addProcessor(String, ProcessorSupplier, String...) processors} that should be
     *        able to access the provided state store
     *
     * @return itself
     *
     * @throws TopologyException
     *         if the {@link StoreBuilder#name() state store} was already added, or
     *         if a processor name is unknown or specifies a source or sink
     */
    public synchronized <S extends StateStore> Topology addStateStore(final StoreBuilder<S> storeBuilder,
                                                                      final String... processorNames) {
        internalTopologyBuilder.addStateStore(storeBuilder, processorNames);
        return this;
    }

    /**
     * Adds a read-only {@link StateStore state store} to the topology.
     * The state store will be populated with data from the named source topic.
     * State stores are sharded and the number of shards is determined at runtime by the number of input topic
     * partitions for the source topic <em>and</em> the connected processors (if any).
     * Read-only state stores can be accessed from "outside" using "Interactive Queries" (cf.,
     * {@link KafkaStreams#store(StoreQueryParameters)} and {@link KafkaStreams#query(StateQueryRequest)}).
     *
     * <p>The {@code auto.offset.reset} property will be set to {@code "earliest"} for the source topic.
     * If you want to specify a source specific {@link TimestampExtractor} you can use
     * {@link #addReadOnlyStateStore(StoreBuilder, String, TimestampExtractor, Deserializer, Deserializer, String, String, ProcessorSupplier)}.
     *
     * <p>{@link #connectProcessorAndStateStores(String, String...) Connecting} a read-only state store to
     * {@link #addProcessor(String, ProcessorSupplier, String...) processors} is optional.
     * If not connected to any processor, the state store will still be created and can be queried via
     * {@link KafkaStreams#store(StoreQueryParameters)} or {@link KafkaStreams#query(StateQueryRequest)}.
     * If the state store is connected to another processor, each corresponding {@link Processor} instance in the
     * topology has <em>read-only</em> access to a single shard of the state store.
     * If you need write access to a state store, you can use a
     * {@link #addStateStore(StoreBuilder, String...) "regular" state store} instead.
     * If you need access to all data in a state store inside a {@link Processor}, you can use a (read-only)
     * {@link #addGlobalStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier)
     * global state store}.
     *
     * <p>The provided {@link ProcessorSupplier} will be used to create {@link Processor} instances which will be used
     * to process the records from the source topic.
     * These {@link Processor processors} are the only ones with <em>write</em> access to the state store,
     * and should contain logic to keep the {@link StateStore} up-to-date.
     *
     * <p>Read-only state stores are always enabled for fault-tolerance and recovery.
     * In contrast to {@link #addStateStore(StoreBuilder, String...) "regular" state stores} no dedicated changelog
     * topic will be created in Kafka though, but the source topic is used for recovery.
     * Thus, the source topic should be configured with log compaction.
     *
     * @param storeBuilder
     *        the {@link StoreBuilder} used to obtain {@link StateStore state store} instances (one per shard)
     * @param sourceName
     *        the unique name of the internally added {@link #addSource(String, String...) source}
     * @param keyDeserializer
     *        the {@link Deserializer} for record keys
     *        (can be {@code null} to use the default key deserializer from {@link StreamsConfig})
     * @param valueDeserializer
     *        the {@link Deserializer} for record values
     *        (can be {@code null} to use the default value deserializer from {@link StreamsConfig})
     * @param topic
     *        the source topic to read the data from
     * @param processorName
     *        the unique name of the internally added
     *        {@link #addProcessor(String, ProcessorSupplier, String...) processor} which maintains the state store
     * @param stateUpdateSupplier
     *        the supplier used to obtain {@link Processor} instances, which maintain the state store
     *
     * @return itself
     *
     * @throws TopologyException
     *         if the {@link StoreBuilder#name() state store} was already added, or
     *         if the source or processor names are not unique, or
     *         if the source topic has already been registered by another
     *         {@link #addSink(String, String, String...) source}, read-only state store, or
     *         {@link #addGlobalStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier) global state store}
     */
    public synchronized <K, V, S extends StateStore> Topology addReadOnlyStateStore(
        final StoreBuilder<S> storeBuilder,
        final String sourceName,
        final Deserializer<K> keyDeserializer,
        final Deserializer<V> valueDeserializer,
        final String topic,
        final String processorName,
        final ProcessorSupplier<K, V, Void, Void> stateUpdateSupplier
    ) {
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
     * See {@link #addReadOnlyStateStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier)}.
     */
    public synchronized <K, V, S extends StateStore> Topology addReadOnlyStateStore(
        final StoreBuilder<S> storeBuilder,
        final String sourceName,
        final TimestampExtractor timestampExtractor,
        final Deserializer<K> keyDeserializer,
        final Deserializer<V> valueDeserializer,
        final String topic,
        final String processorName,
        final ProcessorSupplier<K, V, Void, Void> stateUpdateSupplier
    ) {
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

        // connect the source topic as (read-only) changelog topic for fault-tolerance
        storeBuilder.withLoggingDisabled();
        internalTopologyBuilder.connectSourceStoreAndTopic(storeBuilder.name(), topic);

        return this;
    }


    /**
     * Adds a global {@link StateStore state store} to the topology.
     * The state store will be populated with data from the named source topic.
     * Global state stores are read-only, and contain data from all partitions of the specified source topic.
     * Thus, each {@link KafkaStreams} instance has a full copy to the data; the source topic records are effectively
     * broadcast to all instances.
     * In contrast to
     * {@link #addReadOnlyStateStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier) read-only state stores}
     * global state stores are "bootstrapped" on startup, and are maintained by a separate thread.
     * Thus, updates to a global store are not "stream-time synchronized" what may lead to non-deterministic results.
     * Like all other stores, global state stores can be accessed from "outside" using "Interactive Queries" (cf.,
     * {@link KafkaStreams#store(StoreQueryParameters)} and {@link KafkaStreams#query(StateQueryRequest)}).
     *
     * <p>The {@code auto.offset.reset} property will be set to {@code "earliest"} for the source topic.
     * If you want to specify a source specific {@link TimestampExtractor} you can use
     * {@link #addGlobalStore(StoreBuilder, String, TimestampExtractor, Deserializer, Deserializer, String, String, ProcessorSupplier)}.
     *
     * <p>All {@link #addProcessor(String, ProcessorSupplier, String...) processors} of the topology automatically
     * have read-only access to the global store; it is not necessary to connect them.
     * If you need write access to a state store, you can use a
     * {@link #addStateStore(StoreBuilder, String...) "regular" state store} instead.
     *
     * <p>The provided {@link ProcessorSupplier} will be used to create {@link Processor} instances which will be used
     * to process the records from the source topic.
     * These {@link Processor processors} are the only ones with <em>write</em> access to the state store,
     * and should contain logic to keep the {@link StateStore} up-to-date.
     *
     * <p>Global state stores are always enabled for fault-tolerance and recovery.
     * In contrast to {@link #addStateStore(StoreBuilder, String...) "regular" state stores} no dedicated changelog
     * topic will be created in Kafka though, but the source topic is used for recovery.
     * Thus, the source topic should be configured with log compaction.
     *
     * @param storeBuilder
     *        the {@link StoreBuilder} used to obtain the {@link StateStore state store} (one per {@link KafkaStreams} instance)
     * @param sourceName
     *        the unique name of the internally added source
     * @param keyDeserializer
     *        the {@link Deserializer} for record keys
     *        (can be {@code null} to use the default key deserializer from {@link StreamsConfig})
     * @param valueDeserializer
     *        the {@link Deserializer} for record values
     *        (can be {@code null} to use the default value deserializer from {@link StreamsConfig})
     * @param topic
     *        the source topic to read the data from
     * @param processorName
     *        the unique name of the internally added processor which maintains the state store
     * @param stateUpdateSupplier
     *        the supplier used to obtain {@link Processor} instances, which maintain the state store
     *
     * @return itself
     *
     * @throws TopologyException
     *         if the {@link StoreBuilder#name() state store} was already added, or
     *         if the source or processor names are not unique, or
     *         if the source topic has already been registered by another
     *         {@link #addSink(String, String, String...) source},
     *         {@link #addReadOnlyStateStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier) read-only state store}, or
     *         global state store
     */
    public synchronized <K, V, S extends StateStore> Topology addGlobalStore(
        final StoreBuilder<S> storeBuilder,
        final String sourceName,
        final Deserializer<K> keyDeserializer,
        final Deserializer<V> valueDeserializer,
        final String topic,
        final String processorName,
        final ProcessorSupplier<K, V, Void, Void> stateUpdateSupplier
    ) {
        Objects.requireNonNull(storeBuilder, "storeBuilder cannot be null");
        Objects.requireNonNull(stateUpdateSupplier, "stateUpdateSupplier cannot be null");

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
     * See {@link #addGlobalStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier)}.
     */
    public synchronized <K, V, S extends StateStore> Topology addGlobalStore(
        final StoreBuilder<S> storeBuilder,
        final String sourceName,
        final TimestampExtractor timestampExtractor,
        final Deserializer<K> keyDeserializer,
        final Deserializer<V> valueDeserializer,
        final String topic,
        final String processorName,
        final ProcessorSupplier<K, V, Void, Void> stateUpdateSupplier
    ) {
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
     * Connect a {@link #addProcessor(String, ProcessorSupplier, String...) processor} to one or more
     * {@link StateStore state stores}.
     * The state stores must have been previously added to the topology via
     * {@link #addStateStore(StoreBuilder, String...)}, or
     * {@link #addReadOnlyStateStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier)}.
     *
     * @param processorName
     *        the name of the processor
     * @param stateStoreNames
     *        the names of state stores that the processor should be able to access
     *
     * @return itself
     *
     * @throws TopologyException
     *         if the processor name or a state store name is unknown, or
     *         if the processor name specifies a source or sink
     */
    public synchronized Topology connectProcessorAndStateStores(final String processorName,
                                                                final String... stateStoreNames) {
        internalTopologyBuilder.connectProcessorAndStateStores(processorName, stateStoreNames);
        return this;
    }

    /**
     * Returns a description of the specified {@code Topology}.
     *
     * @return A description of the topology.
     */
    public synchronized TopologyDescription describe() {
        return internalTopologyBuilder.describe();
    }
}
