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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.internals.GlobalKTableImpl;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.kstream.internals.KTableSourceValueGetterSupplier;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.RocksDBKeyValueStoreSupplier;

import java.util.Collections;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * {@code KStreamBuilder} provide the high-level Kafka Streams DSL to specify a Kafka Streams topology.
 *
 * @see org.apache.kafka.streams.processor.TopologyBuilder
 * @see KStream
 * @see KTable
 * @see GlobalKTable
 * @deprecated Use {@link org.apache.kafka.streams.StreamsBuilder StreamsBuilder} instead
 */
@Deprecated
public class KStreamBuilder extends org.apache.kafka.streams.processor.TopologyBuilder {

    private final InternalStreamsBuilder internalStreamsBuilder = new InternalStreamsBuilder(super.internalTopologyBuilder);

    private Topology.AutoOffsetReset translateAutoOffsetReset(final org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset resetPolicy) {
        if (resetPolicy == null) {
            return null;
        }
        return resetPolicy == org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset.EARLIEST ? Topology.AutoOffsetReset.EARLIEST : Topology.AutoOffsetReset.LATEST;
    }

    /**
     * Create a {@link KStream} from the specified topics.
     * The default {@code "auto.offset.reset"} strategy, default {@link TimestampExtractor}, and default key and value
     * deserializers as specified in the {@link StreamsConfig config} are used.
     * <p>
     * If multiple topics are specified there is no ordering guarantee for records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the date before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param topics the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public <K, V> KStream<K, V> stream(final String... topics) {
        return stream(null, null, null, null, topics);
    }

    /**
     * Create a {@link KStream} from the specified topics.
     * The default {@link TimestampExtractor} and default key and value deserializers as specified in the
     * {@link StreamsConfig config} are used.
     * <p>
     * If multiple topics are specified there is no ordering guarantee for records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the date before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param offsetReset the {@code "auto.offset.reset"} policy to use for the specified topics if no valid committed
     *                    offsets are available
     * @param topics      the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public <K, V> KStream<K, V> stream(final AutoOffsetReset offsetReset,
                                       final String... topics) {
        return stream(offsetReset, null, null, null, topics);
    }

    /**
     * Create a {@link KStream} from the specified topic pattern.
     * The default {@code "auto.offset.reset"} strategy, default {@link TimestampExtractor}, and default key and value
     * deserializers as specified in the {@link StreamsConfig config} are used.
     * <p>
     * If multiple topics are matched by the specified pattern, the created {@link KStream} will read data from all of
     * them and there is no ordering guarantee between records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the date before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param topicPattern the pattern to match for topic names
     * @return a {@link KStream} for topics matching the regex pattern.
     */
    public <K, V> KStream<K, V> stream(final Pattern topicPattern) {
        return stream(null, null,  null, null, topicPattern);
    }

    /**
     * Create a {@link KStream} from the specified topic pattern.
     * The default {@link TimestampExtractor} and default key and value deserializers as specified in the
     * {@link StreamsConfig config} are used.
     * <p>
     * If multiple topics are matched by the specified pattern, the created {@link KStream} will read data from all of
     * them and there is no ordering guarantee between records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the date before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param offsetReset  the {@code "auto.offset.reset"} policy to use for the matched topics if no valid committed
     *                     offsets are available
     * @param topicPattern the pattern to match for topic names
     * @return a {@link KStream} for topics matching the regex pattern.
     */
    public <K, V> KStream<K, V> stream(final AutoOffsetReset offsetReset, final Pattern topicPattern) {
        return stream(offsetReset, null, null, null, topicPattern);
    }

    /**
     * Create a {@link KStream} from the specified topics.
     * The default {@code "auto.offset.reset"} strategy and default {@link TimestampExtractor} as specified in the
     * {@link StreamsConfig config} are used.
     * <p>
     * If multiple topics are specified there is no ordering guarantee for records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the date before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param keySerde key serde used to read this source {@link KStream},
     *                 if not specified the default serde defined in the configs will be used
     * @param valSerde value serde used to read this source {@link KStream},
     *                 if not specified the default serde defined in the configs will be used
     * @param topics   the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public <K, V> KStream<K, V> stream(final Serde<K> keySerde, final Serde<V> valSerde, final String... topics) {
        return stream(null, null, keySerde, valSerde, topics);
    }

    /**
     * Create a {@link KStream} from the specified topics.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     * <p>
     * If multiple topics are specified there is no ordering guarantee for records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the date before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param offsetReset the {@code "auto.offset.reset"} policy to use for the specified topics if no valid committed
     *                    offsets are available
     * @param keySerde    key serde used to read this source {@link KStream},
     *                    if not specified the default serde defined in the configs will be used
     * @param valSerde    value serde used to read this source {@link KStream},
     *                    if not specified the default serde defined in the configs will be used
     * @param topics      the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public <K, V> KStream<K, V> stream(final AutoOffsetReset offsetReset,
                                       final Serde<K> keySerde,
                                       final Serde<V> valSerde,
                                       final String... topics) {
        return stream(offsetReset, null, keySerde, valSerde, topics);
    }

    /**
     * Create a {@link KStream} from the specified topics.
     * The default {@code "auto.offset.reset"} strategy as specified in the {@link StreamsConfig config} is used.
     * <p>
     * If multiple topics are specified there is no ordering guarantee for records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the date before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param timestampExtractor the stateless timestamp extractor used for this source {@link KStream},
     *                           if not specified the default extractor defined in the configs will be used
     * @param keySerde           key serde used to read this source {@link KStream}, if not specified the default
     *                           serde defined in the configs will be used
     * @param valSerde           value serde used to read this source {@link KStream},
     *                           if not specified the default serde defined in the configs will be used
     * @param topics             the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public <K, V> KStream<K, V> stream(final TimestampExtractor timestampExtractor,
                                       final Serde<K> keySerde,
                                       final Serde<V> valSerde,
                                       final String... topics) {
        return stream(null, timestampExtractor, keySerde, valSerde, topics);
    }

    /**
     * Create a {@link KStream} from the specified topics.
     * <p>
     * If multiple topics are specified there is no ordering guarantee for records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the date before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param offsetReset        the {@code "auto.offset.reset"} policy to use for the specified topics
     *                           if no valid committed offsets are available
     * @param timestampExtractor the stateless timestamp extractor used for this source {@link KStream},
     *                           if not specified the default extractor defined in the configs will be used
     * @param keySerde           key serde used to read this source {@link KStream},
     *                           if not specified the default serde defined in the configs will be used
     * @param valSerde           value serde used to read this source {@link KStream},
     *                           if not specified the default serde defined in the configs will be used
     * @param topics             the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public <K, V> KStream<K, V> stream(final AutoOffsetReset offsetReset,
                                       final TimestampExtractor timestampExtractor,
                                       final Serde<K> keySerde,
                                       final Serde<V> valSerde,
                                       final String... topics) {
        try {
            final String name = newName(KStreamImpl.SOURCE_NAME);

            internalTopologyBuilder.addSource(translateAutoOffsetReset(offsetReset), name, timestampExtractor,
                keySerde == null ? null : keySerde.deserializer(), valSerde == null ? null : valSerde.deserializer(), topics);

            return new KStreamImpl<>(internalStreamsBuilder, name, Collections.singleton(name), false);
        } catch (final org.apache.kafka.streams.errors.TopologyException e) {
            throw new org.apache.kafka.streams.errors.TopologyBuilderException(e);
        }
    }

    /**
     * Create a {@link KStream} from the specified topic pattern.
     * The default {@code "auto.offset.reset"} strategy and default {@link TimestampExtractor}
     * as specified in the {@link StreamsConfig config} are used.
     * <p>
     * If multiple topics are matched by the specified pattern, the created {@link KStream} will read data from all of
     * them and there is no ordering guarantee between records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the date before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param keySerde     key serde used to read this source {@link KStream},
     *                     if not specified the default serde defined in the configs will be used
     * @param valSerde     value serde used to read this source {@link KStream},
     *                     if not specified the default serde defined in the configs will be used
     * @param topicPattern the pattern to match for topic names
     * @return a {@link KStream} for topics matching the regex pattern.
     */
    public <K, V> KStream<K, V> stream(final Serde<K> keySerde,
                                       final Serde<V> valSerde,
                                       final Pattern topicPattern) {
        return stream(null, null, keySerde, valSerde, topicPattern);
    }

    /**
     * Create a {@link KStream} from the specified topic pattern.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     * <p>
     * If multiple topics are matched by the specified pattern, the created {@link KStream} will read data from all of
     * them and there is no ordering guarantee between records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the date before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param offsetReset  the {@code "auto.offset.reset"} policy to use for the matched topics if no valid committed
     *                     offsets are available
     * @param keySerde     key serde used to read this source {@link KStream},
     *                     if not specified the default serde defined in the configs will be used
     * @param valSerde     value serde used to read this source {@link KStream},
     *                     if not specified the default serde defined in the configs will be used
     * @param topicPattern the pattern to match for topic names
     * @return a {@link KStream} for topics matching the regex pattern.
     */
    public <K, V> KStream<K, V> stream(final AutoOffsetReset offsetReset,
                                       final Serde<K> keySerde,
                                       final Serde<V> valSerde,
                                       final Pattern topicPattern) {
        return stream(offsetReset, null, keySerde, valSerde, topicPattern);
    }

    /**
     * Create a {@link KStream} from the specified topic pattern.
     * The default {@code "auto.offset.reset"} strategy as specified in the {@link StreamsConfig config} is used.
     * <p>
     * If multiple topics are matched by the specified pattern, the created {@link KStream} will read data from all of
     * them and there is no ordering guarantee between records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the date before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param timestampExtractor the stateless timestamp extractor used for this source {@link KStream},
     *                           if not specified the default extractor defined in the configs will be used
     * @param keySerde           key serde used to read this source {@link KStream},
     *                           if not specified the default serde defined in the configs will be used
     * @param valSerde           value serde used to read this source {@link KStream},
     *                           if not specified the default serde defined in the configs will be used
     * @param topicPattern       the pattern to match for topic names
     * @return a {@link KStream} for topics matching the regex pattern.
     */
    public <K, V> KStream<K, V> stream(final TimestampExtractor timestampExtractor,
                                       final Serde<K> keySerde,
                                       final Serde<V> valSerde,
                                       final Pattern topicPattern) {
        return stream(null, timestampExtractor, keySerde, valSerde, topicPattern);
    }

    /**
     * Create a {@link KStream} from the specified topic pattern.
     * <p>
     * If multiple topics are matched by the specified pattern, the created {@link KStream} will read data from all of
     * them and there is no ordering guarantee between records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the date before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param offsetReset        the {@code "auto.offset.reset"} policy to use for the matched topics if no valid
     *                           committed  offsets are available
     * @param timestampExtractor the stateless timestamp extractor used for this source {@link KStream},
     *                           if not specified the default extractor defined in the configs will be used
     * @param keySerde           key serde used to read this source {@link KStream},
     *                           if not specified the default serde defined in the configs will be used
     * @param valSerde           value serde used to read this source {@link KStream},
     *                           if not specified the default serde defined in the configs will be used
     * @param topicPattern       the pattern to match for topic names
     * @return a {@link KStream} for topics matching the regex pattern.
     */
    public <K, V> KStream<K, V> stream(final AutoOffsetReset offsetReset,
                                       final TimestampExtractor timestampExtractor,
                                       final Serde<K> keySerde,
                                       final Serde<V> valSerde,
                                       final Pattern topicPattern) {
        try {
            final String name = newName(KStreamImpl.SOURCE_NAME);

            internalTopologyBuilder.addSource(translateAutoOffsetReset(offsetReset), name, timestampExtractor,
                keySerde == null ? null : keySerde.deserializer(), valSerde == null ? null : valSerde.deserializer(), topicPattern);

            return new KStreamImpl<>(internalStreamsBuilder, name, Collections.singleton(name), false);
        } catch (final org.apache.kafka.streams.errors.TopologyException e) {
            throw new org.apache.kafka.streams.errors.TopologyBuilderException(e);
        }
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The default {@code "auto.offset.reset"} strategy, default {@link TimestampExtractor}, and
     * default key and value deserializers as specified in the {@link StreamsConfig config} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topic must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code queryableStoreName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param topic              the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; If {@code null} this is the equivalent of {@link KStreamBuilder#table(String)} ()}.
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final String topic,
                                     final String queryableStoreName) {
        return table(null, null,  null, null, topic, queryableStoreName);
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
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code queryableStoreName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param topic         the topic name; cannot be {@code null}
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final String topic,
                                     final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        return table(null, null, null, null, topic, storeSupplier);
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
     * store name. Note that that store name may not be queriable through Interactive Queries.
     * No internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * @param topic     the topic name; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final String topic) {
        return table(null, null, null, null, topic, (String) null);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The default key and value deserializers as specified in the {@link StreamsConfig config} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code queryableStoreName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param offsetReset        the {@code "auto.offset.reset"} policy to use for the specified topic if no valid committed
     *                           offsets are available
     * @param topic              the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; If {@code null} this is the equivalent of
     * {@link #table(org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset, String) table(AutoOffsetReset, String)}.
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final AutoOffsetReset offsetReset,
                                     final String topic,
                                     final String queryableStoreName) {
        return table(offsetReset, null, null, null, topic, queryableStoreName);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The default {@link TimestampExtractor} and default key and value deserializers
     * as specified in the {@link StreamsConfig config} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topic must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code queryableStoreName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param offsetReset the {@code "auto.offset.reset"} policy to use for the specified topic if no valid committed
     *                    offsets are available
     * @param topic       the topic name; cannot be {@code null}
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final AutoOffsetReset offsetReset,
                                     final String topic,
                                     final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        return table(offsetReset, null, null, null, topic, storeSupplier);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The default key and value deserializers as specified in the {@link StreamsConfig config} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with an internal
     * store name. Note that that store name may not be queriable through Interactive Queries.
     * No internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * @param offsetReset the {@code "auto.offset.reset"} policy to use for the specified topic if no valid committed
     *                    offsets are available
     * @param topic       the topic name; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final AutoOffsetReset offsetReset,
                                     final String topic) {
        return table(offsetReset, null, null, null, topic, (String) null);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The default {@code "auto.offset.reset"} strategy and default key and value deserializers
     * as specified in the {@link StreamsConfig config} are used.
     * Input {@link KeyValue} pairs with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topic must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code storeName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param timestampExtractor the stateless timestamp extractor used for this source {@link KTable},
     *                           if not specified the default extractor defined in the configs will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param storeName          the state store name; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final TimestampExtractor timestampExtractor,
                                     final String topic,
                                     final String storeName) {
        return table(null, timestampExtractor, null, null, topic, storeName);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The default key and value deserializers as specified in the {@link StreamsConfig config} are used.
     * Input {@link KeyValue} pairs with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topic must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code storeName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param offsetReset the {@code "auto.offset.reset"} policy to use for the specified topic if no valid committed
     *                    offsets are available
     * @param topic       the topic name; cannot be {@code null}
     * @param storeName   the state store name; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final AutoOffsetReset offsetReset,
                                     final TimestampExtractor timestampExtractor,
                                     final String topic,
                                     final String storeName) {
        return table(offsetReset, timestampExtractor, null, null, topic, storeName);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The default {@code "auto.offset.reset"} strategy and default {@link TimestampExtractor}
     * as specified in the {@link StreamsConfig config} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topic must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code queryableStoreName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param keySerde           key serde used to send key-value pairs,
     *                           if not specified the default key serde defined in the configuration will be used
     * @param valSerde           value serde used to send key-value pairs,
     *                           if not specified the default value serde defined in the configuration will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; If {@code null} this is the equivalent of {@link KStreamBuilder#table(Serde, Serde, String)} ()}.
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final Serde<K> keySerde,
                                     final Serde<V> valSerde,
                                     final String topic,
                                     final String queryableStoreName) {
        return table(null, null, keySerde, valSerde, topic, queryableStoreName);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     * The default {@code "auto.offset.reset"} strategy as specified in the {@link StreamsConfig config} is used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topic must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code queryableStoreName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param keySerde      key serde used to send key-value pairs,
     *                      if not specified the default key serde defined in the configuration will be used
     * @param valSerde      value serde used to send key-value pairs,
     *                      if not specified the default value serde defined in the configuration will be used
     * @param topic         the topic name; cannot be {@code null}
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final Serde<K> keySerde,
                                     final Serde<V> valSerde,
                                     final String topic,
                                     final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        return table(null, null, keySerde, valSerde, topic, storeSupplier);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The default {@code "auto.offset.reset"} strategy as specified in the {@link StreamsConfig config} is used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with an internal
     * store name. Note that that store name may not be queriable through Interactive Queries.
     * No internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * @param keySerde  key serde used to send key-value pairs,
     *                  if not specified the default key serde defined in the configuration will be used
     * @param valSerde  value serde used to send key-value pairs,
     *                  if not specified the default value serde defined in the configuration will be used
     * @param topic     the topic name; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final Serde<K> keySerde,
                                     final Serde<V> valSerde,
                                     final String topic) {
        return table(null, null, keySerde, valSerde, topic, (String) null);
    }

    private <K, V> KTable<K, V> doTable(final AutoOffsetReset offsetReset,
                                        final Serde<K> keySerde,
                                        final Serde<V> valSerde,
                                        final TimestampExtractor timestampExtractor,
                                        final String topic,
                                        final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier,
                                        final boolean isQueryable) {
        try {
            final String source = newName(KStreamImpl.SOURCE_NAME);
            final String name = newName(KTableImpl.SOURCE_NAME);
            final ProcessorSupplier<K, V> processorSupplier = new KTableSource<>(storeSupplier.name());

            internalTopologyBuilder.addSource(translateAutoOffsetReset(offsetReset), source, timestampExtractor,
                keySerde == null ? null : keySerde.deserializer(),
                valSerde == null ? null : valSerde.deserializer(),
                topic);
            internalTopologyBuilder.addProcessor(name, processorSupplier, source);

            final KTableImpl<K, ?, V> kTable = new KTableImpl<>(internalStreamsBuilder, name, processorSupplier,
                    keySerde, valSerde, Collections.singleton(source), storeSupplier.name(), isQueryable);

            addStateStore(storeSupplier, name);
            connectSourceStoreAndTopic(storeSupplier.name(), topic);

            return kTable;
        } catch (final org.apache.kafka.streams.errors.TopologyException e) {
            throw new org.apache.kafka.streams.errors.TopologyBuilderException(e);
        }
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code queryableStoreName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param offsetReset        the {@code "auto.offset.reset"} policy to use for the specified topic if no valid committed
     *                           offsets are available
     * @param keySerde           key serde used to send key-value pairs,
     *                           if not specified the default key serde defined in the configuration will be used
     * @param valSerde           value serde used to send key-value pairs,
     *                           if not specified the default value serde defined in the configuration will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; If {@code null} this is the equivalent of
     * {@link #table(org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset, Serde, Serde, String) table(AutoOffsetReset, Serde, Serde, String)}
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final AutoOffsetReset offsetReset,
                                     final Serde<K> keySerde,
                                     final Serde<V> valSerde,
                                     final String topic,
                                     final String queryableStoreName) {
        return table(offsetReset, null, keySerde, valSerde, topic, queryableStoreName);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The default {@code "auto.offset.reset"} strategy as specified in the {@link StreamsConfig config} is used.
     * Input {@link KeyValue} pairs with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topic must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code storeName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param timestampExtractor the stateless timestamp extractor used for this source {@link KTable},
     *                           if not specified the default extractor defined in the configs will be used
     * @param keySerde           key serde used to send key-value pairs,
     *                           if not specified the default key serde defined in the configuration will be used
     * @param valSerde           value serde used to send key-value pairs,
     *                           if not specified the default value serde defined in the configuration will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param storeName          the state store name; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final TimestampExtractor timestampExtractor,
                                     final Serde<K> keySerde,
                                     final Serde<V> valSerde,
                                     final String topic,
                                     final String storeName) {
        return table(null, timestampExtractor, keySerde, valSerde, topic, storeName);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * Input {@link KeyValue} pairs with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topic must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code storeName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param offsetReset        the {@code "auto.offset.reset"} policy to use for the specified topic if no valid
     *                           committed offsets are available
     * @param timestampExtractor the stateless timestamp extractor used for this source {@link KTable},
     *                           if not specified the default extractor defined in the configs will be used
     * @param keySerde           key serde used to send key-value pairs,
     *                           if not specified the default key serde defined in the configuration will be used
     * @param valSerde           value serde used to send key-value pairs,
     *                           if not specified the default value serde defined in the configuration will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; If {@code null} this is the equivalent of
     * {@link #table(org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset, Serde, Serde, String) table(AutoOffsetReset, Serde, Serde, String)}
     * @return a {@link KTable} for the specified topic
     */
    @SuppressWarnings("unchecked")
    public <K, V> KTable<K, V> table(final AutoOffsetReset offsetReset,
                                     final TimestampExtractor timestampExtractor,
                                     final Serde<K> keySerde,
                                     final Serde<V> valSerde,
                                     final String topic,
                                     final String queryableStoreName) {
        final String internalStoreName = queryableStoreName != null ? queryableStoreName : newStoreName(KTableImpl.SOURCE_NAME);
        final org.apache.kafka.streams.processor.StateStoreSupplier storeSupplier = new RocksDBKeyValueStoreSupplier<>(
            internalStoreName,
            keySerde,
            valSerde,
            false,
            Collections.<String, String>emptyMap(),
            true);
        return doTable(offsetReset, keySerde, valSerde, timestampExtractor, topic, storeSupplier, queryableStoreName != null);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The default {@code "auto.offset.reset"} strategy as specified in the {@link StreamsConfig config} is used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with an internal
     * store name. Note that that store name may not be queriable through Interactive Queries.
     * No internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * @param offsetReset the {@code "auto.offset.reset"} policy to use for the specified topic if no valid committed
     *                    offsets are available
     * @param keySerde    key serde used to send key-value pairs,
     *                    if not specified the default key serde defined in the configuration will be used
     * @param valSerde    value serde used to send key-value pairs,
     *                    if not specified the default value serde defined in the configuration will be used
     * @param topic       the topic name; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final AutoOffsetReset offsetReset,
                                     final Serde<K> keySerde,
                                     final Serde<V> valSerde,
                                     final String topic) {
        return table(offsetReset, null, keySerde, valSerde, topic, (String) null);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case the returned {@link KTable} will be corrupted.
     * <p>
     * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code queryableStoreName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param offsetReset the {@code "auto.offset.reset"} policy to use for the specified topic if no valid committed
     *                    offsets are available
     * @param keySerde    key serde used to send key-value pairs,
     *                    if not specified the default key serde defined in the configuration will be used
     * @param valSerde    value serde used to send key-value pairs,
     *                    if not specified the default value serde defined in the configuration will be used
     * @param topic       the topic name; cannot be {@code null}
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final AutoOffsetReset offsetReset,
                                     final TimestampExtractor timestampExtractor,
                                     final Serde<K> keySerde,
                                     final Serde<V> valSerde,
                                     final String topic,
                                     final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doTable(offsetReset, keySerde, valSerde, timestampExtractor, topic, storeSupplier, true);
    }

    /**
     * Create a {@link GlobalKTable} for the specified topic.
     * The default key and value deserializers as specified in the {@link StreamsConfig config} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code queryableStoreName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key);
     * }</pre>
     * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
     * regardless of the specified value in {@link StreamsConfig}.
     *
     * @param topic     the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; If {@code null} this is the equivalent of {@link KStreamBuilder#globalTable(String)}
     * @return a {@link GlobalKTable} for the specified topic
     */
    public <K, V> GlobalKTable<K, V> globalTable(final String topic,
                                                 final String queryableStoreName) {
        return globalTable(null, null, null,  topic, queryableStoreName);
    }

    /**
     * Create a {@link GlobalKTable} for the specified topic.
     * The default key and value deserializers as specified in the {@link StreamsConfig config} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} with an internal
     * store name. Note that that store name may not be queriable through Interactive Queries.
     * No internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
     * regardless of the specified value in {@link StreamsConfig}.
     *
     * @param topic     the topic name; cannot be {@code null}
     * @return a {@link GlobalKTable} for the specified topic
     */
    public <K, V> GlobalKTable<K, V> globalTable(final String topic) {
        return globalTable(null, null, null, topic, null);
    }

    /**
     * Create a {@link GlobalKTable} for the specified topic.
     * The default {@link TimestampExtractor} and default key and value deserializers as specified in
     * the {@link StreamsConfig config} are used.
     * Input {@link KeyValue} pairs with {@code null} key will be dropped.
     * <p>
     * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code queryableStoreName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key);
     * }</pre>
     * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
     * regardless of the specified value in {@link StreamsConfig}.
     *
     * @param keySerde  key serde used to send key-value pairs,
     *                  if not specified the default key serde defined in the configuration will be used
     * @param valSerde  value serde used to send key-value pairs,
     *                  if not specified the default value serde defined in the configuration will be used
     * @param topic     the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; If {@code null} this is the equivalent of {@link KStreamBuilder#globalTable(Serde, Serde, String)} ()}
     * @return a {@link GlobalKTable} for the specified topic
     */
    public <K, V> GlobalKTable<K, V> globalTable(final Serde<K> keySerde,
                                                 final Serde<V> valSerde,
                                                 final TimestampExtractor timestampExtractor,
                                                 final String topic,
                                                 final String queryableStoreName) {
        final String internalStoreName = queryableStoreName != null ? queryableStoreName : newStoreName(KTableImpl.SOURCE_NAME);
        return doGlobalTable(keySerde, valSerde, timestampExtractor, topic, new RocksDBKeyValueStoreSupplier<>(internalStoreName,
                            keySerde,
                            valSerde,
                    false,
                            Collections.<String, String>emptyMap(),
                    true));
    }

    /**
     * Create a {@link GlobalKTable} for the specified topic.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     * Input {@link KeyValue} pairs with {@code null} key will be dropped.
     * <p>
     * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code queryableStoreName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key);
     * }</pre>
     * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
     * regardless of the specified value in {@link StreamsConfig}.
     *
     * @param keySerde  key serde used to send key-value pairs,
     *                  if not specified the default key serde defined in the configuration will be used
     * @param valSerde  value serde used to send key-value pairs,
     *                  if not specified the default value serde defined in the configuration will be used
     * @param topic     the topic name; cannot be {@code null}
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @return a {@link GlobalKTable} for the specified topic
     */
    public <K, V> GlobalKTable<K, V> globalTable(final Serde<K> keySerde,
                                                 final Serde<V> valSerde,
                                                 final String topic,
                                                 final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        return doGlobalTable(keySerde, valSerde, null, topic, storeSupplier);
    }

    /**
     * Create a {@link GlobalKTable} for the specified topic.
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     * Input {@link KeyValue} pairs with {@code null} key will be dropped.
     * <p>
     * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code queryableStoreName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key);
     * }</pre>
     * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
     * regardless of the specified value in {@link StreamsConfig}.
     *
     * @param keySerde           key serde used to send key-value pairs,
     *                           if not specified the default key serde defined in the configuration will be used
     * @param valSerde           value serde used to send key-value pairs,
     *                           if not specified the default value serde defined in the configuration will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; If {@code null} this is the equivalent of
     *                           {@link KStreamBuilder#globalTable(Serde, Serde, String)} ()}
     * @return a {@link GlobalKTable} for the specified topic
     */
    public <K, V> GlobalKTable<K, V> globalTable(final Serde<K> keySerde,
                                                 final Serde<V> valSerde,
                                                 final String topic,
                                                 final String queryableStoreName) {
        return globalTable(keySerde, valSerde, null, topic, queryableStoreName);
    }

    /**
     * Create a {@link GlobalKTable} for the specified topic.
     * Input {@link KeyValue} pairs with {@code null} key will be dropped.
     * <p>
     * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} with the given
     * {@code storeName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key);
     * }</pre>
     * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
     * regardless of the specified value in {@link StreamsConfig}.
     *
     * @param timestampExtractor the stateless timestamp extractor used for this source {@link GlobalKTable},
     *                           if not specified the default extractor defined in the configs will be used
     * @param keySerde           key serde used to send key-value pairs,
     *                           if not specified the default key serde defined in the configuration will be used
     * @param valSerde           value serde used to send key-value pairs,
     *                           if not specified the default value serde defined in the configuration will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param storeSupplier      user defined state store supplier. Cannot be {@code null}.
     * @return a {@link GlobalKTable} for the specified topic
     */
    @SuppressWarnings("unchecked")
    private <K, V> GlobalKTable<K, V> doGlobalTable(final Serde<K> keySerde,
                                                    final Serde<V> valSerde,
                                                    final TimestampExtractor timestampExtractor,
                                                    final String topic,
                                                    final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier) {
        try {
            Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
            final String sourceName = newName(KStreamImpl.SOURCE_NAME);
            final String processorName = newName(KTableImpl.SOURCE_NAME);
            final KTableSource<K, V> tableSource = new KTableSource<>(storeSupplier.name());

            final Deserializer<K> keyDeserializer = keySerde == null ? null : keySerde.deserializer();
            final Deserializer<V> valueDeserializer = valSerde == null ? null : valSerde.deserializer();

            internalTopologyBuilder.addGlobalStore(storeSupplier, sourceName, timestampExtractor, keyDeserializer, valueDeserializer, topic, processorName, tableSource);
            return new GlobalKTableImpl(new KTableSourceValueGetterSupplier<>(storeSupplier.name()));
        } catch (final org.apache.kafka.streams.errors.TopologyException e) {
            throw new org.apache.kafka.streams.errors.TopologyBuilderException(e);
        }
    }

    /**
     * Create a {@link GlobalKTable} for the specified topic.
     * The default key and value deserializers as specified in the {@link StreamsConfig config} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} with an internal
     * store name. Note that that store name may not be queriable through Interactive Queries.
     * No internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
     * regardless of the specified value in {@link StreamsConfig}.
     *
     * @param keySerde  key serde used to send key-value pairs,
     *                  if not specified the default key serde defined in the configuration will be used
     * @param valSerde  value serde used to send key-value pairs,
     *                  if not specified the default value serde defined in the configuration will be used
     * @param topic     the topic name; cannot be {@code null}
     * @return a {@link GlobalKTable} for the specified topic
     */
    public <K, V> GlobalKTable<K, V> globalTable(final Serde<K> keySerde,
                                                 final Serde<V> valSerde,
                                                 final String topic) {

        return globalTable(keySerde, valSerde, null, topic, (String) null);
    }

    /**
     * Create a new instance of {@link KStream} by merging the given {@link KStream}s.
     * <p>
     * There is no ordering guarantee for records from different {@link KStream}s.
     *
     * @param streams the {@link KStream}s to be merged
     * @return a {@link KStream} containing all records of the given streams
     */
    @SuppressWarnings("unchecked")
    public <K, V> KStream<K, V> merge(final KStream<K, V>... streams) {
        Objects.requireNonNull(streams, "streams can't be null");
        if (streams.length <= 1) {
            throw new IllegalArgumentException("Number of arguments required needs to be greater than one.");
        }
        try {
            KStream<K, V> mergedStream = streams[0];
            for (int i = 1; i < streams.length; i++) {
                mergedStream = mergedStream.merge(streams[i]);
            }
            return mergedStream;
        } catch (final org.apache.kafka.streams.errors.TopologyException e) {
            throw new org.apache.kafka.streams.errors.TopologyBuilderException(e);
        }
    }

    /**
     * <strong>This function is only for internal usage only and should not be called.</strong>
     * <p>
     * Create a unique processor name used for translation into the processor topology.
     *
     * @param prefix processor name prefix
     * @return a new unique name
     */
    public String newName(final String prefix) {
        return internalStreamsBuilder.newProcessorName(prefix);
    }

    /**
     * <strong>This function is only for internal usage only and should not be called.</strong>
     * <p>
     * Create a unique state store name.
     *
     * @param prefix processor name prefix
     * @return a new unique name
     */
    public String newStoreName(final String prefix) {
        return internalStreamsBuilder.newStoreName(prefix);
    }

}
