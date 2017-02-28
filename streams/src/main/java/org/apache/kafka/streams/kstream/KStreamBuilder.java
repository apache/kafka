/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.streams.kstream.internals.GlobalKTableImpl;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.kstream.internals.KTableSourceValueGetterSupplier;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.RocksDBKeyValueStoreSupplier;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * {@code KStreamBuilder} provide the high-level Kafka Streams DSL to specify a Kafka Streams topology.
 *
 * @see TopologyBuilder
 * @see KStream
 * @see KTable
 * @see GlobalKTable
 */
public class KStreamBuilder extends TopologyBuilder {

    private final AtomicInteger index = new AtomicInteger(0);

    /**
     * Create a {@link KStream} from the specified topics.
     * The default {@code "auto.offset.reset"} strategy and default key and value deserializers as specified in the
     * {@link StreamsConfig config} are used.
     * <p>
     * If multiple topics are specified there are no ordering guaranteed for records from different topics.
     * <p>
     * Note that the specified input topics must be partitioned by key.
     * If this is not the case it is the user's responsibility to repartition the date before any key based operation
     * (like aggregation or join) is applied to the returned {@link KStream}.
     *
     * @param topics the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public <K, V> KStream<K, V> stream(final String... topics) {
        return stream(null, null, null, topics);
    }

    /**
     * Create a {@link KStream} from the specified topics.
     * The default key and value deserializers as specified in the {@link StreamsConfig config} are used.
     * <p>
     * If multiple topics are specified there are no ordering guaranteed for records from different topics.
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
        return stream(offsetReset, null, null, topics);
    }

    /**
     * Create a {@link KStream} from the specified topic pattern.
     * The default {@code "auto.offset.reset"} strategy and default key and value deserializers as specified in the
     * {@link StreamsConfig config} are used.
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
        return stream(null, null, null, topicPattern);
    }

    /**
     * Create a {@link KStream} from the specified topic pattern.
     * The default key and value deserializers as specified in the {@link StreamsConfig config} are used.
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
        return stream(offsetReset, null, null, topicPattern);
    }

    /**
     * Create a {@link KStream} from the specified topics.
     * The default {@code "auto.offset.reset"} strategy as specified in the {@link StreamsConfig config} is used.
     * <p>
     * If multiple topics are specified there are no ordering guaranteed for records from different topics.
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
        return stream(null, keySerde, valSerde, topics);
    }


    /**
     * Create a {@link KStream} from the specified topics.
     * <p>
     * If multiple topics are specified there are no ordering guaranteed for records from different topics.
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
        final String name = newName(KStreamImpl.SOURCE_NAME);

        addSource(offsetReset, name,  keySerde == null ? null : keySerde.deserializer(), valSerde == null ? null : valSerde.deserializer(), topics);

        return new KStreamImpl<>(this, name, Collections.singleton(name), false);
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
        return stream(null, keySerde, valSerde, topicPattern);
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
        final String name = newName(KStreamImpl.SOURCE_NAME);

        addSource(offsetReset, name, keySerde == null ? null : keySerde.deserializer(), valSerde == null ? null : valSerde.deserializer(), topicPattern);

        return new KStreamImpl<>(this, name, Collections.singleton(name), false);
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
     * {@code storeName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String,Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param topic     the topic name; cannot be {@code null}
     * @param storeName the state store name; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final String topic,
                                     final String storeName) {
        return table(null, null, null, topic, storeName);
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
     * {@code storeName}.
     * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
     * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ...
     * ReadOnlyKeyValueStore<String,Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
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
                                     final String topic,
                                     final String storeName) {
        return table(offsetReset, null, null, topic, storeName);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * The default {@code "auto.offset.reset"} strategy as specified in the {@link StreamsConfig config} is used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topics must be partitioned by key.
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
     * ReadOnlyKeyValueStore<String,Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param keySerde  key serde used to send key-value pairs,
     *                  if not specified the default key serde defined in the configuration will be used
     * @param valSerde  value serde used to send key-value pairs,
     *                  if not specified the default value serde defined in the configuration will be used
     * @param topic     the topic name; cannot be {@code null}
     * @param storeName the state store name; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final Serde<K> keySerde,
                                     final Serde<V> valSerde,
                                     final String topic,
                                     final String storeName) {
        return table(null, keySerde, valSerde, topic, storeName);
    }

    /**
     * Create a {@link KTable} for the specified topic.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
     * <p>
     * Note that the specified input topics must be partitioned by key.
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
     * ReadOnlyKeyValueStore<String,Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
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
     * @param storeName   the state store name; cannot be {@code null}
     * @return a {@link KTable} for the specified topic
     */
    public <K, V> KTable<K, V> table(final AutoOffsetReset offsetReset,
                                     final Serde<K> keySerde,
                                     final Serde<V> valSerde,
                                     final String topic,
                                     final String storeName) {
        final String source = newName(KStreamImpl.SOURCE_NAME);
        final String name = newName(KTableImpl.SOURCE_NAME);
        final ProcessorSupplier<K, V> processorSupplier = new KTableSource<>(storeName);

        addSource(offsetReset, source, keySerde == null ? null : keySerde.deserializer(), valSerde == null ? null : valSerde.deserializer(), topic);
        addProcessor(name, processorSupplier, source);

        final KTableImpl<K, ?, V> kTable = new KTableImpl<>(this, name, processorSupplier, Collections.singleton(source), storeName);

        // only materialize the KTable into a state store if the storeName is not null
        if (storeName != null) {
            final StateStoreSupplier storeSupplier = new RocksDBKeyValueStoreSupplier<>(storeName,
                    keySerde,
                    valSerde,
                    false,
                    Collections.<String, String>emptyMap(),
                    true);

            addStateStore(storeSupplier, name);
            connectSourceStoreAndTopic(storeName, topic);
        }

        return kTable;
    }

    /**
     * Create a {@link GlobalKTable} for the specified topic.
     * The default key and value deserializers as specified in the {@link StreamsConfig config} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
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
     * ReadOnlyKeyValueStore<String,Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long valueForKey = localStore.get(key);
     * }</pre>
     * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
     * regardless of the specified value in {@link StreamsConfig}.
     *
     * @param topic     the topic name; cannot be {@code null}
     * @param storeName the state store name; cannot be {@code null}
     * @return a {@link GlobalKTable} for the specified topic
     */
    public <K, V> GlobalKTable<K, V> globalTable(final String topic,
                                                 final String storeName) {
        return globalTable(null, null, topic, storeName);
    }

    /**
     * Create a {@link GlobalKTable} for the specified topic.
     * The default key and value deserializers as specified in the {@link StreamsConfig config} are used.
     * Input {@link KeyValue records} with {@code null} key will be dropped.
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
     * ReadOnlyKeyValueStore<String,Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
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
     * @param storeName the state store name; cannot be {@code null}
     * @return a {@link GlobalKTable} for the specified topic
     */
    @SuppressWarnings("unchecked")
    public <K, V> GlobalKTable<K, V> globalTable(final Serde<K> keySerde,
                                                 final Serde<V> valSerde,
                                                 final String topic,
                                                 final String storeName) {
        final String sourceName = newName(KStreamImpl.SOURCE_NAME);
        final String processorName = newName(KTableImpl.SOURCE_NAME);
        final KTableSource<K, V> tableSource = new KTableSource<>(storeName);


        final Deserializer<K> keyDeserializer = keySerde == null ? null : keySerde.deserializer();
        final Deserializer<V> valueDeserializer = valSerde == null ? null : valSerde.deserializer();

        final StateStore store = new RocksDBKeyValueStoreSupplier<>(storeName,
                                                                    keySerde,
                                                                    valSerde,
                                                                    false,
                                                                    Collections.<String, String>emptyMap(),
                                                                    true).get();

        addGlobalStore(store, sourceName, keyDeserializer, valueDeserializer, topic, processorName, tableSource);
        return new GlobalKTableImpl(new KTableSourceValueGetterSupplier<>(storeName));
    }

    /**
     * Create a new instance of {@link KStream} by merging the given {@link KStream}s.
     * <p>
     * There are nor ordering guaranteed for records from different {@link KStream}s.
     *
     * @param streams the {@link KStream}s to be merged
     * @return a {@link KStream} containing all records of the given streams
     */
    public <K, V> KStream<K, V> merge(final KStream<K, V>... streams) {
        return KStreamImpl.merge(this, streams);
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
        return prefix + String.format("%010d", index.getAndIncrement());
    }

}
