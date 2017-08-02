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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.SourceNode;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;

import java.util.regex.Pattern;

/**
 * {@code StreamsBuilder} provide the high-level Kafka Streams DSL to specify a Kafka Streams topology.
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
    public synchronized <K, V> KStream<K, V> stream(final String... topics) {
        return internalStreamsBuilder.stream(null, null, null, null, topics);
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
    public synchronized <K, V> KStream<K, V> stream(final Topology.AutoOffsetReset offsetReset,
                                                    final String... topics) {
        return internalStreamsBuilder.stream(offsetReset, null, null, null, topics);
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
    public synchronized <K, V> KStream<K, V> stream(final Pattern topicPattern) {
        return internalStreamsBuilder.stream(null, null,  null, null, topicPattern);
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
    public synchronized <K, V> KStream<K, V> stream(final Topology.AutoOffsetReset offsetReset,
                                                    final Pattern topicPattern) {
        return internalStreamsBuilder.stream(offsetReset, null, null, null, topicPattern);
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
     * @param keySerde   key serde used to read this source {@link KStream},
     *                   if not specified the default serde defined in the configs will be used
     * @param valueSerde value serde used to read this source {@link KStream},
     *                   if not specified the default serde defined in the configs will be used
     * @param topics     the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public synchronized <K, V> KStream<K, V> stream(final Serde<K> keySerde,
                                                    final Serde<V> valueSerde,
                                                    final String... topics) {
        return internalStreamsBuilder.stream(null, null, keySerde, valueSerde, topics);
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
     * @param valueSerde  value serde used to read this source {@link KStream},
     *                    if not specified the default serde defined in the configs will be used
     * @param topics      the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public synchronized <K, V> KStream<K, V> stream(final Topology.AutoOffsetReset offsetReset,
                                                    final Serde<K> keySerde,
                                                    final Serde<V> valueSerde,
                                                    final String... topics) {
        return internalStreamsBuilder.stream(offsetReset, null, keySerde, valueSerde, topics);
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
     * @param valueSerde         value serde used to read this source {@link KStream},
     *                           if not specified the default serde defined in the configs will be used
     * @param topics             the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public synchronized <K, V> KStream<K, V> stream(final TimestampExtractor timestampExtractor,
                                                    final Serde<K> keySerde,
                                                    final Serde<V> valueSerde,
                                                    final String... topics) {
        return internalStreamsBuilder.stream(null, timestampExtractor, keySerde, valueSerde, topics);
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
     * @param valueSerde         value serde used to read this source {@link KStream},
     *                           if not specified the default serde defined in the configs will be used
     * @param topics             the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public synchronized <K, V> KStream<K, V> stream(final Topology.AutoOffsetReset offsetReset,
                                                    final TimestampExtractor timestampExtractor,
                                                    final Serde<K> keySerde,
                                                    final Serde<V> valueSerde,
                                                    final String... topics) {
        return internalStreamsBuilder.stream(offsetReset, timestampExtractor, keySerde, valueSerde, topics);
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
     * @param valueSerde   value serde used to read this source {@link KStream},
     *                     if not specified the default serde defined in the configs will be used
     * @param topicPattern the pattern to match for topic names
     * @return a {@link KStream} for topics matching the regex pattern.
     */
    public synchronized <K, V> KStream<K, V> stream(final Serde<K> keySerde,
                                                    final Serde<V> valueSerde,
                                                    final Pattern topicPattern) {
        return internalStreamsBuilder.stream(null, null, keySerde, valueSerde, topicPattern);
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
     * @param valueSerde   value serde used to read this source {@link KStream},
     *                     if not specified the default serde defined in the configs will be used
     * @param topicPattern the pattern to match for topic names
     * @return a {@link KStream} for topics matching the regex pattern.
     */
    public synchronized <K, V> KStream<K, V> stream(final Topology.AutoOffsetReset offsetReset,
                                                    final Serde<K> keySerde,
                                                    final Serde<V> valueSerde,
                                                    final Pattern topicPattern) {
        return internalStreamsBuilder.stream(offsetReset, null, keySerde, valueSerde, topicPattern);
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
     * @param valueSerde         value serde used to read this source {@link KStream},
     *                           if not specified the default serde defined in the configs will be used
     * @param topicPattern       the pattern to match for topic names
     * @return a {@link KStream} for topics matching the regex pattern.
     */
    public synchronized <K, V> KStream<K, V> stream(final TimestampExtractor timestampExtractor,
                                                    final Serde<K> keySerde,
                                                    final Serde<V> valueSerde,
                                                    final Pattern topicPattern) {
        return internalStreamsBuilder.stream(null, timestampExtractor, keySerde, valueSerde, topicPattern);
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
     * @param valueSerde         value serde used to read this source {@link KStream},
     *                           if not specified the default serde defined in the configs will be used
     * @param topicPattern       the pattern to match for topic names
     * @return a {@link KStream} for topics matching the regex pattern.
     */
    public synchronized <K, V> KStream<K, V> stream(final Topology.AutoOffsetReset offsetReset,
                                                    final TimestampExtractor timestampExtractor,
                                                    final Serde<K> keySerde,
                                                    final Serde<V> valueSerde,
                                                    final Pattern topicPattern) {
        return internalStreamsBuilder.stream(offsetReset, timestampExtractor, keySerde, valueSerde, topicPattern);
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
     * @param queryableStoreName the state store name; if {@code null} this is the equivalent of {@link #table(String)}
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final String topic,
                                                  final String queryableStoreName) {
        return internalStreamsBuilder.table(null, null,  null, null, topic, queryableStoreName);
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
     * @param storeSupplier user defined state store supplier; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final String topic,
                                                  final StateStoreSupplier<KeyValueStore> storeSupplier) {
        return internalStreamsBuilder.table(null, null, null, null, topic, storeSupplier);
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
     *
     * @param topic the topic name; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final String topic) {
        return internalStreamsBuilder.table(null, null, null, null, topic, (String) null);
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
     * @param queryableStoreName the state store name; if {@code null} this is the equivalent of
     *                           {@link #table(Topology.AutoOffsetReset, String)}
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final Topology.AutoOffsetReset offsetReset,
                                                  final String topic,
                                                  final String queryableStoreName) {
        return internalStreamsBuilder.table(offsetReset, null, null, null, topic, queryableStoreName);
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
     * @param offsetReset   the {@code "auto.offset.reset"} policy to use for the specified topic if no valid committed
     *                      offsets are available
     * @param topic         the topic name; cannot be {@code null}
     * @param storeSupplier user defined state store supplier; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final Topology.AutoOffsetReset offsetReset,
                                                  final String topic,
                                                  final StateStoreSupplier<KeyValueStore> storeSupplier) {
        return internalStreamsBuilder.table(offsetReset, null, null, null, topic, storeSupplier);
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
    public synchronized <K, V> KTable<K, V> table(final Topology.AutoOffsetReset offsetReset,
                                                  final String topic) {
        return internalStreamsBuilder.table(offsetReset, null, null, null, topic, (String) null);
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
     * @param queryableStoreName the state store name; if {@code null} an internal store name will be automatically given
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final TimestampExtractor timestampExtractor,
                                                  final String topic,
                                                  final String queryableStoreName) {
        return internalStreamsBuilder.table(null, timestampExtractor, null, null, topic, queryableStoreName);
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
     * @param offsetReset        the {@code "auto.offset.reset"} policy to use for the specified topic if no valid committed
     *                           offsets are available
     * @param timestampExtractor the stateless timestamp extractor used for this source {@link KTable},
     *                           if not specified the default extractor defined in the configs will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; if {@code null} an internal store name will be automatically given
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final Topology.AutoOffsetReset offsetReset,
                                                  final TimestampExtractor timestampExtractor,
                                                  final String topic,
                                                  final String queryableStoreName) {
        return internalStreamsBuilder.table(offsetReset, timestampExtractor, null, null, topic, queryableStoreName);
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
     * @param valueSerde         value serde used to send key-value pairs,
     *                           if not specified the default value serde defined in the configuration will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; if {@code null} an internal store name will be automatically given
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final Serde<K> keySerde,
                                                  final Serde<V> valueSerde,
                                                  final String topic,
                                                  final String queryableStoreName) {
        return internalStreamsBuilder.table(null, null, keySerde, valueSerde, topic, queryableStoreName);
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
     * @param valueSerde    value serde used to send key-value pairs,
     *                      if not specified the default value serde defined in the configuration will be used
     * @param topic         the topic name; cannot be {@code null}
     * @param storeSupplier user defined state store supplier; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final Serde<K> keySerde,
                                                  final Serde<V> valueSerde,
                                                  final String topic,
                                                  final StateStoreSupplier<KeyValueStore> storeSupplier) {
        return internalStreamsBuilder.table(null, null, keySerde, valueSerde, topic, storeSupplier);
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
     * @param keySerde   key serde used to send key-value pairs,
     *                   if not specified the default key serde defined in the configuration will be used
     * @param valueSerde value serde used to send key-value pairs,
     *                   if not specified the default value serde defined in the configuration will be used
     * @param topic      the topic name; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final Serde<K> keySerde,
                                                  final Serde<V> valueSerde,
                                                  final String topic) {
        return internalStreamsBuilder.table(null, null, keySerde, valueSerde, topic, (String) null);
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
     * @param offsetReset        the {@code "auto.offset.reset"} policy to use for the specified topic if no valid
     *                           committed offsets are available
     * @param keySerde           key serde used to send key-value pairs,
     *                           if not specified the default key serde defined in the configuration will be used
     * @param valueSerde         value serde used to send key-value pairs,
     *                           if not specified the default value serde defined in the configuration will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; if {@code null} an internal store name will be automatically given
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final Topology.AutoOffsetReset offsetReset,
                                                  final Serde<K> keySerde,
                                                  final Serde<V> valueSerde,
                                                  final String topic,
                                                  final String queryableStoreName) {
        return internalStreamsBuilder.table(offsetReset, null, keySerde, valueSerde, topic, queryableStoreName);
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
     * @param valueSerde         value serde used to send key-value pairs,
     *                           if not specified the default value serde defined in the configuration will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; if {@code null} an internal store name will be automatically given
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final TimestampExtractor timestampExtractor,
                                                  final Serde<K> keySerde,
                                                  final Serde<V> valueSerde,
                                                  final String topic,
                                                  final String queryableStoreName) {
        return internalStreamsBuilder.table(null, timestampExtractor, keySerde, valueSerde, topic, queryableStoreName);
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
     * @param valueSerde         value serde used to send key-value pairs,
     *                           if not specified the default value serde defined in the configuration will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; if {@code null} an internal store name will be automatically given
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final Topology.AutoOffsetReset offsetReset,
                                                  final TimestampExtractor timestampExtractor,
                                                  final Serde<K> keySerde,
                                                  final Serde<V> valueSerde,
                                                  final String topic,
                                                  final String queryableStoreName) {
        return internalStreamsBuilder.table(offsetReset, timestampExtractor, keySerde, valueSerde, topic, queryableStoreName);
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
     * @param valueSerde  value serde used to send key-value pairs,
     *                    if not specified the default value serde defined in the configuration will be used
     * @param topic       the topic name; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final Topology.AutoOffsetReset offsetReset,
                                                  final Serde<K> keySerde,
                                                  final Serde<V> valueSerde,
                                                  final String topic) {
        return internalStreamsBuilder.table(offsetReset, null, keySerde, valueSerde, topic, (String) null);
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
     * @param timestampExtractor the stateless timestamp extractor used for this source {@link KTable},
     *                           if not specified the default extractor defined in the configs will be used
     * @param keySerde           key serde used to send key-value pairs,
     *                           if not specified the default key serde defined in the configuration will be used
     * @param valueSerde         value serde used to send key-value pairs,
     *                           if not specified the default value serde defined in the configuration will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param storeSupplier      user defined state store supplier; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public synchronized <K, V> KTable<K, V> table(final Topology.AutoOffsetReset offsetReset,
                                                  final TimestampExtractor timestampExtractor,
                                                  final Serde<K> keySerde,
                                                  final Serde<V> valueSerde,
                                                  final String topic,
                                                  final StateStoreSupplier<KeyValueStore> storeSupplier) {
        return internalStreamsBuilder.table(offsetReset, timestampExtractor, keySerde, valueSerde, topic, storeSupplier);
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
     * @param topic              the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; if {@code null} an internal store name will be automatically given
     * @return a {@link GlobalKTable} for the specified topic
     */
    public synchronized <K, V> GlobalKTable<K, V> globalTable(final String topic,
                                                              final String queryableStoreName) {
        return internalStreamsBuilder.globalTable(null, null, null,  topic, queryableStoreName);
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
     * @param topic the topic name; cannot be {@code null}
     * @return a {@link GlobalKTable} for the specified topic
     */
    public synchronized <K, V> GlobalKTable<K, V> globalTable(final String topic) {
        return internalStreamsBuilder.globalTable(null, null, null, topic, null);
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
     * @param keySerde           key serde used to send key-value pairs,
     *                           if not specified the default key serde defined in the configuration will be used
     * @param valueSerde         value serde used to send key-value pairs,
     *                           if not specified the default value serde defined in the configuration will be used
     * @param timestampExtractor the stateless timestamp extractor used for this source {@link KTable},
     *                           if not specified the default extractor defined in the configs will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; if {@code null} an internal store name will be automatically given
     * @return a {@link GlobalKTable} for the specified topic
     */
    public synchronized <K, V> GlobalKTable<K, V> globalTable(final Serde<K> keySerde,
                                                              final Serde<V> valueSerde,
                                                              final TimestampExtractor timestampExtractor,
                                                              final String topic,
                                                              final String queryableStoreName) {
        return internalStreamsBuilder.globalTable(keySerde, valueSerde, timestampExtractor, topic, queryableStoreName);
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
     * @param keySerde      key serde used to send key-value pairs,
     *                      if not specified the default key serde defined in the configuration will be used
     * @param valueSerde    value serde used to send key-value pairs,
     *                      if not specified the default value serde defined in the configuration will be used
     * @param topic         the topic name; cannot be {@code null}
     * @param storeSupplier user defined state store supplier; Cannot be {@code null}
     * @return a {@link GlobalKTable} for the specified topic
     */
    public synchronized <K, V> GlobalKTable<K, V> globalTable(final Serde<K> keySerde,
                                                              final Serde<V> valueSerde,
                                                              final String topic,
                                                              final StateStoreSupplier<KeyValueStore> storeSupplier) {
        return internalStreamsBuilder.globalTable(keySerde, valueSerde, topic, storeSupplier);
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
     * @param valueSerde         value serde used to send key-value pairs,
     *                           if not specified the default value serde defined in the configuration will be used
     * @param topic              the topic name; cannot be {@code null}
     * @param queryableStoreName the state store name; if {@code null} an internal store name will be automatically given
     * @return a {@link GlobalKTable} for the specified topic
     */
    public synchronized <K, V> GlobalKTable<K, V> globalTable(final Serde<K> keySerde,
                                                              final Serde<V> valueSerde,
                                                              final String topic,
                                                              final String queryableStoreName) {
        return internalStreamsBuilder.globalTable(keySerde, valueSerde, null, topic, queryableStoreName);
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
     * @param keySerde   key serde used to send key-value pairs,
     *                   if not specified the default key serde defined in the configuration will be used
     * @param valueSerde value serde used to send key-value pairs,
     *                   if not specified the default value serde defined in the configuration will be used
     * @param topic      the topic name; cannot be {@code null}
     * @return a {@link GlobalKTable} for the specified topic
     */
    public synchronized <K, V> GlobalKTable<K, V> globalTable(final Serde<K> keySerde,
                                                              final Serde<V> valueSerde,
                                                              final String topic) {
        return internalStreamsBuilder.globalTable(keySerde, valueSerde, null, topic, null);
    }

    /**
     * Adds a state store to the underlying {@link Topology}.
     *
     * @param supplier the supplier used to obtain this state store {@link StateStore} instance
     * @param processorNames the names of the processors that should be able to access the provided store
     * @return itself
     * @throws TopologyException if state store supplier is already added
     */
    public synchronized StreamsBuilder addStateStore(final StateStoreSupplier supplier,
                                                     final String... processorNames) {
        internalStreamsBuilder.addStateStore(supplier, processorNames);
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
     * The default {@link TimestampExtractor} as specified in the {@link StreamsConfig config} is used.
     *
     * @param storeSupplier         user defined state store supplier
     * @param sourceName            name of the {@link SourceNode} that will be automatically added
     * @param keyDeserializer       the {@link Deserializer} to deserialize keys with
     * @param valueDeserializer     the {@link Deserializer} to deserialize values with
     * @param topic                 the topic to source the data from
     * @param processorName         the name of the {@link ProcessorSupplier}
     * @param stateUpdateSupplier   the instance of {@link ProcessorSupplier}
     * @return itself
     * @throws TopologyException if the processor of state is already registered
     */
    public synchronized StreamsBuilder addGlobalStore(final StateStoreSupplier<KeyValueStore> storeSupplier,
                                                      final String sourceName,
                                                      final Deserializer keyDeserializer,
                                                      final Deserializer valueDeserializer,
                                                      final String topic,
                                                      final String processorName,
                                                      final ProcessorSupplier stateUpdateSupplier) {
        internalStreamsBuilder.addGlobalStore(storeSupplier, sourceName, null, keyDeserializer,
            valueDeserializer, topic, processorName, stateUpdateSupplier);
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
     * @param storeSupplier         user defined state store supplier
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
    public synchronized StreamsBuilder addGlobalStore(final StateStoreSupplier<KeyValueStore> storeSupplier,
                                                      final String sourceName,
                                                      final TimestampExtractor timestampExtractor,
                                                      final Deserializer keyDeserializer,
                                                      final Deserializer valueDeserializer,
                                                      final String topic,
                                                      final String processorName,
                                                      final ProcessorSupplier stateUpdateSupplier) {
        internalStreamsBuilder.addGlobalStore(storeSupplier, sourceName, timestampExtractor, keyDeserializer,
            valueDeserializer, topic, processorName, stateUpdateSupplier);
        return this;
    }

    /**
     * Create a new instance of {@link KStream} by merging the given {@link KStream}s.
     * <p>
     * There is no ordering guarantee for records from different {@link KStream}s.
     *
     * @param streams the {@link KStream}s to be merged
     * @return a {@link KStream} containing all records of the given streams
     */
    public synchronized <K, V> KStream<K, V> merge(final KStream<K, V>... streams) {
        return internalStreamsBuilder.merge(streams);
    }

    /**
     * Returns the {@link Topology} that represents the specified processing logic.
     *
     * @return the {@link Topology} that represents the specified processing logic
     */
    public synchronized Topology build() {
        return topology;
    }
}
