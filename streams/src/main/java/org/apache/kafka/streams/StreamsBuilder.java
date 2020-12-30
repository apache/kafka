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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorAdapter;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.SourceNode;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * {@code StreamsBuilder} provide the high-level Kafka Streams DSL to specify a Kafka Streams topology.
 *
 * <p>
 * It is a requirement that the processing logic ({@link Topology}) be defined in a deterministic way,
 * as in, the order in which all operators are added must be predictable and the same across all application
 * instances.
 * Topologies are only identical if all operators are added in the same order.
 * If different {@link KafkaStreams} instances of the same application build different topologies the result may be
 * incompatible runtime code and unexpected results or errors
 *
 * @see Topology
 * @see KStream
 * @see KTable
 * @see GlobalKTable
 */
public class StreamsBuilder {

    /** The actual topology that is constructed by this StreamsBuilder. */
    private final Topology topology = new Topology();

    /** The topology's internal builder. */
    final InternalTopologyBuilder internalTopologyBuilder = topology.internalTopologyBuilder;

    private final InternalStreamsBuilder internalStreamsBuilder = new InternalStreamsBuilder(internalTopologyBuilder);

    /**
     * Create a {@link KStream} from the specified topic.
     * The default {@code "auto.offset.reset"} strategy, default {@link TimestampExtractor}, and default key and value
     * deserializers as specified in the {@link StreamsConfig config} are used.
     * <p>
     * If multiple topics are specified there is no ordering guarantee for records from different topics.
     * <p>
     * Note that the specified input topic must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the data before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param topic the topic name; cannot be {@code null}
     * @return a {@link KStream} for the specified topic
     */
    public synchronized <K, V> KStream<K, V> stream(final String topic) {
        return stream(Collections.singleton(topic));
    }

    /**
     * Create a {@link KStream} from the specified topic.
     * The {@code "auto.offset.reset"} strategy, {@link TimestampExtractor}, key and value deserializers
     * are defined by the options in {@link Consumed} are used.
     * <p>
     * Note that the specified input topic must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the data before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param topic the topic names; cannot be {@code null}
     * @param consumed      the instance of {@link Consumed} used to define optional parameters
     * @return a {@link KStream} for the specified topic
     */
    public synchronized <K, V> KStream<K, V> stream(final String topic,
                                                    final Consumed<K, V> consumed) {
        return stream(Collections.singleton(topic), consumed);
    }

    /**
     * Create a {@link KStream} from the specified topics.
     * The default {@code "auto.offset.reset"} strategy, default {@link TimestampExtractor}, and default key and value
     * deserializers as specified in the {@link StreamsConfig config} are used.
     * <p>
     * If multiple topics are specified there is no ordering guarantee for records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the data before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param topics the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public synchronized <K, V> KStream<K, V> stream(final Collection<String> topics) {
        return stream(topics, Consumed.with(null, null, null, null));
    }

    /**
     * Create a {@link KStream} from the specified topics.
     * The {@code "auto.offset.reset"} strategy, {@link TimestampExtractor}, key and value deserializers
     * are defined by the options in {@link Consumed} are used.
     * <p>
     * If multiple topics are specified there is no ordering guarantee for records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the data before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param topics the topic names; must contain at least one topic name
     * @param consumed      the instance of {@link Consumed} used to define optional parameters
     * @return a {@link KStream} for the specified topics
     */
    public synchronized <K, V> KStream<K, V> stream(final Collection<String> topics,
                                                    final Consumed<K, V> consumed) {
        Objects.requireNonNull(topics, "topics can't be null");
        Objects.requireNonNull(consumed, "consumed can't be null");
        return internalStreamsBuilder.stream(topics, new ConsumedInternal<>(consumed));
    }


    /**
     * Create a {@link KStream} from the specified topic pattern.
     * The default {@code "auto.offset.reset"} strategy, default {@link TimestampExtractor}, and default key and value
     * deserializers as specified in the {@link StreamsConfig config} are used.
     * <p>
     * If multiple topics are matched by the specified pattern, the created {@link KStream} will read data from all of
     * them and there is no ordering guarantee between records from different topics. This also means that the work
     * will not be parallelized for multiple topics, and the number of tasks will scale with the maximum partition
     * count of any matching topic rather than the total number of partitions across all topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the data before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param topicPattern the pattern to match for topic names
     * @return a {@link KStream} for topics matching the regex pattern.
     */
    public synchronized <K, V> KStream<K, V> stream(final Pattern topicPattern) {
        return stream(topicPattern, Consumed.with(null, null));
    }

    /**
     * Create a {@link KStream} from the specified topic pattern.
     * The {@code "auto.offset.reset"} strategy, {@link TimestampExtractor}, key and value deserializers
     * are defined by the options in {@link Consumed} are used.
     * <p>
     * If multiple topics are matched by the specified pattern, the created {@link KStream} will read data from all of
     * them and there is no ordering guarantee between records from different topics. This also means that the work
     * will not be parallelized for multiple topics, and the number of tasks will scale with the maximum partition
     * count of any matching topic rather than the total number of partitions across all topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the data before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param topicPattern  the pattern to match for topic names
     * @param consumed      the instance of {@link Consumed} used to define optional parameters
     * @return a {@link KStream} for topics matching the regex pattern.
     */
    public synchronized <K, V> KStream<K, V> stream(final Pattern topicPattern,
                                                    final Consumed<K, V> consumed) {
        Objects.requireNonNull(topicPattern, "topicPattern can't be null");
        Objects.requireNonNull(consumed, "consumed can't be null");
        return internalStreamsBuilder.stream(topicPattern, new ConsumedInternal<>(consumed));
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The {@code "auto.offset.reset"} strategy, {@link TimestampExtractor}, key and value deserializers
     * are defined by the options in {@link Consumed} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topic must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} using the given
     * {@code Materialized} instance.
     * An internal changelog topic is created by default. Because the source topic can
     * be used for recovery, you can avoid creating the changelog topic by setting
     * the {@code "topology.optimization"} to {@code "all"} in the {@link StreamsConfig}.
     * <p>
     * You should only specify serdes in the {@link Consumed} instance as these will also be used to overwrite the
     * serdes in {@link Materialized}, i.e.,
     * <pre> {@code
     * streamBuilder.table(topic, Consumed.with(Serde.String(), Serde.String()), Materialized.<String, String, KeyValueStore<Bytes, byte[]>as(storeName))
     * }
     * </pre>
     * To query the local {@link ReadOnlyKeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<K, ValueAndTimestamp<V>>timestampedKeyValueStore());
     * K key = "some-key";
     * ValueAndTimestamp<V> valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param topic              the topic name; cannot be {@code null}
     * @param consumed           the instance of {@link Consumed} used to define optional parameters; cannot be {@code null}
     * @param materialized       the instance of {@link Materialized} used to materialize a state store; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final String topic,
                                                  final Consumed<K, V> consumed,
                                                  final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(topic, "topic can't be null");
        Objects.requireNonNull(consumed, "consumed can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final ConsumedInternal<K, V> consumedInternal = new ConsumedInternal<>(consumed);
        materialized.withKeySerde(consumedInternal.keySerde()).withValueSerde(consumedInternal.valueSerde());

        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, internalStreamsBuilder, topic + "-");

        return internalStreamsBuilder.table(topic, consumedInternal, materializedInternal);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The default {@code "auto.offset.reset"} strategy and default key and value deserializers as specified in the
     * {@link StreamsConfig config} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with an internal
     * store name. Note that store name may not be queryable through Interactive Queries.
     * An internal changelog topic is created by default. Because the source topic can
     * be used for recovery, you can avoid creating the changelog topic by setting
     * the {@code "topology.optimization"} to {@code "all"} in the {@link StreamsConfig}.
     *
     * @param topic the topic name; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final String topic) {
        return table(topic, new ConsumedInternal<>());
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The {@code "auto.offset.reset"} strategy, {@link TimestampExtractor}, key and value deserializers
     * are defined by the options in {@link Consumed} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with an internal
     * store name. Note that store name may not be queryable through Interactive Queries.
     * An internal changelog topic is created by default. Because the source topic can
     * be used for recovery, you can avoid creating the changelog topic by setting
     * the {@code "topology.optimization"} to {@code "all"} in the {@link StreamsConfig}.
     *
     * @param topic     the topic name; cannot be {@code null}
     * @param consumed  the instance of {@link Consumed} used to define optional parameters; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final String topic,
                                                  final Consumed<K, V> consumed) {
        Objects.requireNonNull(topic, "topic can't be null");
        Objects.requireNonNull(consumed, "consumed can't be null");
        final ConsumedInternal<K, V> consumedInternal = new ConsumedInternal<>(consumed);

        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(
                Materialized.with(consumedInternal.keySerde(), consumedInternal.valueSerde()),
                internalStreamsBuilder,
                topic + "-");

        return internalStreamsBuilder.table(topic, consumedInternal, materializedInternal);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The default {@code "auto.offset.reset"} strategy as specified in the {@link StreamsConfig config} are used.
     * Key and value deserializers as defined by the options in {@link Materialized} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} using the {@link Materialized} instance.
     * An internal changelog topic is created by default. Because the source topic can
     * be used for recovery, you can avoid creating the changelog topic by setting
     * the {@code "topology.optimization"} to {@code "all"} in the {@link StreamsConfig}.
     *
     * @param topic         the topic name; cannot be {@code null}
     * @param materialized  the instance of {@link Materialized} used to materialize a state store; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final String topic,
                                                  final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(topic, "topic can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, internalStreamsBuilder, topic + "-");

        final ConsumedInternal<K, V> consumedInternal =
                new ConsumedInternal<>(Consumed.with(materializedInternal.keySerde(), materializedInternal.valueSerde()));

        return internalStreamsBuilder.table(topic, consumedInternal, materializedInternal);
    }

    /**
     * Create a {@link GlobalKTable} for the specified topic.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} with an internal
     * store name. Note that store name may not be queryable through Interactive Queries.
     * No internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
     * regardless of the specified value in {@link StreamsConfig} or {@link Consumed}.
     *
     * @param topic the topic name; cannot be {@code null}
     * @param consumed  the instance of {@link Consumed} used to define optional parameters
     * @return a {@link GlobalKTable} for the specified topic
     */
    public synchronized <K, V> GlobalKTable<K, V> globalTable(final String topic,
                                                              final Consumed<K, V> consumed) {
        Objects.requireNonNull(topic, "topic can't be null");
        Objects.requireNonNull(consumed, "consumed can't be null");
        final ConsumedInternal<K, V> consumedInternal = new ConsumedInternal<>(consumed);

        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(
                Materialized.with(consumedInternal.keySerde(), consumedInternal.valueSerde()),
                internalStreamsBuilder, topic + "-");

        return internalStreamsBuilder.globalTable(topic, consumedInternal, materializedInternal);
    }

    /**
     * Create a {@link GlobalKTable} for the specified topic.
     * The default key and value deserializers as specified in the {@link StreamsConfig config} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} with an internal
     * store name. Note that store name may not be queryable through Interactive Queries.
     * No internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
     * regardless of the specified value in {@link StreamsConfig}.
     *
     * @param topic the topic name; cannot be {@code null}
     * @return a {@link GlobalKTable} for the specified topic
     */
    public synchronized <K, V> GlobalKTable<K, V> globalTable(final String topic) {
        return globalTable(topic, Consumed.with(null, null));
    }

    /**
     * Create a {@link GlobalKTable} for the specified topic.
     *
     * Input {@link KeyValue} pairs with {@code null} key will be dropped.
     * <p>
     * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} configured with
     * the provided instance of {@link Materialized}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * You should only specify serdes in the {@link Consumed} instance as these will also be used to overwrite the
     * serdes in {@link Materialized}, i.e.,
     * <pre> {@code
     * streamBuilder.globalTable(topic, Consumed.with(Serde.String(), Serde.String()), Materialized.<String, String, KeyValueStore<Bytes, byte[]>as(storeName))
     * }
     * </pre>
     * To query the local {@link ReadOnlyKeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<K, ValueAndTimestamp<V>>timestampedKeyValueStore());
     * K key = "some-key";
     * ValueAndTimestamp<V> valueForKey = localStore.get(key);
     * }</pre>
     * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
     * regardless of the specified value in {@link StreamsConfig} or {@link Consumed}.
     *
     * @param topic         the topic name; cannot be {@code null}
     * @param consumed      the instance of {@link Consumed} used to define optional parameters; can't be {@code null}
     * @param materialized   the instance of {@link Materialized} used to materialize a state store; cannot be {@code null}
     * @return a {@link GlobalKTable} for the specified topic
     */
    public synchronized <K, V> GlobalKTable<K, V> globalTable(final String topic,
                                                              final Consumed<K, V> consumed,
                                                              final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(topic, "topic can't be null");
        Objects.requireNonNull(consumed, "consumed can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final ConsumedInternal<K, V> consumedInternal = new ConsumedInternal<>(consumed);
        // always use the serdes from consumed
        materialized.withKeySerde(consumedInternal.keySerde()).withValueSerde(consumedInternal.valueSerde());

        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, internalStreamsBuilder, topic + "-");

        return internalStreamsBuilder.globalTable(topic, consumedInternal, materializedInternal);
    }

    /**
     * Create a {@link GlobalKTable} for the specified topic.
     *
     * Input {@link KeyValue} pairs with {@code null} key will be dropped.
     * <p>
     * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} configured with
     * the provided instance of {@link Materialized}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link ReadOnlyKeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<K, ValueAndTimestamp<V>>timestampedKeyValueStore());
     * K key = "some-key";
     * ValueAndTimestamp<V> valueForKey = localStore.get(key);
     * }</pre>
     * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
     * regardless of the specified value in {@link StreamsConfig}.
     *
     * @param topic         the topic name; cannot be {@code null}
     * @param materialized   the instance of {@link Materialized} used to materialize a state store; cannot be {@code null}
     * @return a {@link GlobalKTable} for the specified topic
     */
    public synchronized <K, V> GlobalKTable<K, V> globalTable(final String topic,
                                                              final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(topic, "topic can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, internalStreamsBuilder, topic + "-");

        return internalStreamsBuilder.globalTable(topic,
                                                  new ConsumedInternal<>(Consumed.with(materializedInternal.keySerde(),
                                                                                       materializedInternal.valueSerde())),
                                                  materializedInternal);
    }


    /**
     * Adds a state store to the underlying {@link Topology}.
     * <p>
     * It is required to connect state stores to {@link org.apache.kafka.streams.processor.Processor Processors}, {@link Transformer Transformers},
     * or {@link ValueTransformer ValueTransformers} before they can be used.
     *
     * @param builder the builder used to obtain this state store {@link StateStore} instance
     * @return itself
     * @throws TopologyException if state store supplier is already added
     */
    public synchronized StreamsBuilder addStateStore(final StoreBuilder<?> builder) {
        Objects.requireNonNull(builder, "builder can't be null");
        internalStreamsBuilder.addStateStore(builder);
        return this;
    }

    /**
     * @deprecated Use {@link #addGlobalStore(StoreBuilder, String, Consumed, ProcessorSupplier)} instead.
     */
    @Deprecated
    public synchronized <K, V>  StreamsBuilder addGlobalStore(final StoreBuilder<?> storeBuilder,
                                                              final String topic,
                                                              final String sourceName,
                                                              final Consumed<K, V> consumed,
                                                              final String processorName,
                                                              final org.apache.kafka.streams.processor.ProcessorSupplier<K, V> stateUpdateSupplier) {
        Objects.requireNonNull(storeBuilder, "storeBuilder can't be null");
        Objects.requireNonNull(consumed, "consumed can't be null");
        internalStreamsBuilder.addGlobalStore(
            storeBuilder,
            sourceName,
            topic,
            new ConsumedInternal<>(consumed),
            processorName,
            () -> ProcessorAdapter.adapt(stateUpdateSupplier.get())
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
     * The provided {@link org.apache.kafka.streams.processor.ProcessorSupplier} will be used to create an {@link ProcessorNode} that will receive all
     * records forwarded from the {@link SourceNode}. NOTE: you should not use the {@code Processor} to insert transformed records into
     * the global state store. This store uses the source topic as changelog and during restore will insert records directly
     * from the source.
     * This {@link ProcessorNode} should be used to keep the {@link StateStore} up-to-date.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     * <p>
     * It is not required to connect a global store to {@link org.apache.kafka.streams.processor.Processor Processors}, {@link Transformer Transformers},
     * or {@link ValueTransformer ValueTransformer}; those have read-only access to all global stores by default.
     * <p>
     * The supplier should always generate a new instance each time {@link  ProcessorSupplier#get()} gets called. Creating
     * a single {@link Processor} object and returning the same object reference in {@link ProcessorSupplier#get()} would be
     * a violation of the supplier pattern and leads to runtime exceptions.
     *
     * @param storeBuilder          user defined {@link StoreBuilder}; can't be {@code null}
     * @param topic                 the topic to source the data from
     * @param consumed              the instance of {@link Consumed} used to define optional parameters; can't be {@code null}
     * @param stateUpdateSupplier   the instance of {@link org.apache.kafka.streams.processor.ProcessorSupplier}
     * @return itself
     * @throws TopologyException if the processor of state is already registered
     * @deprecated Since 2.7.0; use {@link #addGlobalStore(StoreBuilder, String, Consumed, ProcessorSupplier)} instead.
     */
    @Deprecated
    public synchronized <K, V> StreamsBuilder addGlobalStore(final StoreBuilder<?> storeBuilder,
                                                             final String topic,
                                                             final Consumed<K, V> consumed,
                                                             final org.apache.kafka.streams.processor.ProcessorSupplier<K, V> stateUpdateSupplier) {
        Objects.requireNonNull(storeBuilder, "storeBuilder can't be null");
        Objects.requireNonNull(consumed, "consumed can't be null");
        internalStreamsBuilder.addGlobalStore(
            storeBuilder,
            topic,
            new ConsumedInternal<>(consumed),
            () -> ProcessorAdapter.adapt(stateUpdateSupplier.get())
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
     * The provided {@link ProcessorSupplier}} will be used to create an
     * {@link Processor} that will receive all records forwarded from the {@link SourceNode}.
     * The supplier should always generate a new instance. Creating a single {@link Processor} object
     * and returning the same object reference in {@link ProcessorSupplier#get()} is a
     * violation of the supplier pattern and leads to runtime exceptions.
     * NOTE: you should not use the {@link Processor} to insert transformed records into
     * the global state store. This store uses the source topic as changelog and during restore will insert records directly
     * from the source.
     * This {@link Processor} should be used to keep the {@link StateStore} up-to-date.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     * <p>
     * It is not required to connect a global store to the {@link Processor Processors},
     * {@link Transformer Transformers}, or {@link ValueTransformer ValueTransformer}; those have read-only access to all global stores by default.
     *
     * @param storeBuilder          user defined {@link StoreBuilder}; can't be {@code null}
     * @param topic                 the topic to source the data from
     * @param consumed              the instance of {@link Consumed} used to define optional parameters; can't be {@code null}
     * @param stateUpdateSupplier   the instance of {@link ProcessorSupplier}
     * @return itself
     * @throws TopologyException if the processor of state is already registered
     */
    public synchronized <KIn, VIn> StreamsBuilder addGlobalStore(final StoreBuilder<?> storeBuilder,
                                                                 final String topic,
                                                                 final Consumed<KIn, VIn> consumed,
                                                                 final ProcessorSupplier<KIn, VIn, Void, Void> stateUpdateSupplier) {
        Objects.requireNonNull(storeBuilder, "storeBuilder can't be null");
        Objects.requireNonNull(consumed, "consumed can't be null");
        internalStreamsBuilder.addGlobalStore(
            storeBuilder,
            topic,
            new ConsumedInternal<>(consumed),
            stateUpdateSupplier
        );
        return this;
    }

    /**
     * Returns the {@link Topology} that represents the specified processing logic.
     * Note that using this method means no optimizations are performed.
     *
     * @return the {@link Topology} that represents the specified processing logic
     */
    public synchronized Topology build() {
        return build(null);
    }
    
    /**
     * Returns the {@link Topology} that represents the specified processing logic and accepts
     * a {@link Properties} instance used to indicate whether to optimize topology or not.
     *
     * @param props the {@link Properties} used for building possibly optimized topology
     * @return the {@link Topology} that represents the specified processing logic
     */
    public synchronized Topology build(final Properties props) {
        internalStreamsBuilder.buildAndOptimizeTopology(props);
        return topology;
    }
}
