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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

/**
 * {@code CogroupedKStream} is an abstraction of multiple <i>grouped</i> record streams of {@link
 * KeyValue} pairs. It is an intermediate representation of one or more {@link KStream}s in order to
 * apply one or more aggregation operations on the original {@link KStream} records.
 * <p>
 * It is an intermediate representation after a grouping of {@link KStream}s, before the
 * aggregations are applied to the new partitions resulting in a {@link KTable}.
 * <p>
 * A {@code CogroupedKStream} must be obtained from a {@link KGroupedStream} via {@link
 * KGroupedStream#cogroup(Aggregator, Materialized) cogroup(...)}.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 * @param <T> Type that the aggregator creates
 */
public interface KCogroupedStream<K, V, T> {

    /**
     * {@code CogroupedKStream} is an abstraction of multiple <i>grouped</i> record streams of
     * {@link KeyValue} pairs. It is an intermediate representation of one or more {@link KStream}s
     * in order to apply one or more aggregation operations on the original {@link KStream}
     * records.
     * <p>
     * It is an intermediate representation after a grouping of {@link KStream}s, before the
     * aggregations are applied to the new partitions resulting in a {@link KTable}.
     * <p>
     * @param groupedStream a group stream object
     * @param aggregator   an {@link Aggregator} that computes a new aggregate result
     * @return a KCogroupedStream with the new grouopStream and aggregator linked
     */
    KCogroupedStream<K, V, T> cogroup(final KGroupedStream<K, V> groupedStream,
                                      final Aggregator<? super K, ? super V, T> aggregator);
    /**
     * Aggregate the values of records in this stream by the grouped key. Records with {@code null}
     * key or value are ignored. Aggregating is a generalization of Reducing combining via
     * reduce(...)} as it, for example, allows the result to have a different type than the input
     * values. The result is written into a local {@link KeyValueStore} (which is basically an
     * ever-updating materialized view) that can be queried by the given store name in {@code
     * materialized}. Furthermore, updates to the store are sent downstream into a {@link KTable}
     * changelog stream.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the
     * first record. The specified {@link Aggregator} is applied for each input record and computes
     * a new aggregate using the current aggregate (or for the very first record using the
     * intermediate aggregation result provided via the {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Aggregator, Materialized)} can be used to compute
     * aggregate functions like count.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate
     * consecutive updates to the same key. The rate of propagated updates depends on your input
     * data rate, the number of distinct keys, the number of parallel running Kafka Streams
     * instances, and the {@link StreamsConfig configuration} parameters for {@link
     * StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and {@link
     * StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via {@link
     * KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // some aggregation on value type double
     * String queryableStoreName = "storeName" // the store name should be the name of the store as defined by the Materialized instance
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long aggForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link
     * KafkaStreams#allMetadata()} to query the value of the key on a parallel running instance of
     * your Kafka Streams application.
     *
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be
     * created in Kafka. Therefore, the store name defined by the Materialized instance must be a
     * valid Kafka topic name and cannot contain characters other than ASCII alphanumerics, '.', '_'
     * and '-'. The changelog topic will be named "${applicationId}-${storeName}-changelog", where
     * "applicationId" is user-specified in {@link StreamsConfig} via parameter {@link
     * StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the provide store
     * name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer  an {@link Initializer} that computes an initial intermediate aggregation
     *                     result
     * @param materialized an instance of {@link Materialized} used to materialize a state store.
     *                     Cannot be {@code null}.
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that
     * represent the latest (rolling) aggregate for each key
     */
    KTable<K, T> aggregate(final Initializer<T> initializer,
                           final Materialized<K, T, KeyValueStore<Bytes, byte[]>> materialized);
    /**
     * Aggregate the values of records in this stream by the grouped key. Records with {@code null}
     * key or value are ignored. Aggregating is a generalization of Reducing combining via
     * reduce(...)} as it, for example, allows the result to have a different type than the input
     * values. The result is written into a local {@link KeyValueStore} (which is basically an
     * ever-updating materialized view) that can be queried by the given store name in {@code
     * materialized}. Furthermore, updates to the store are sent downstream into a {@link KTable}
     * changelog stream.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the
     * first record. The specified {@link Aggregator} is applied for each input record and computes
     * a new aggregate using the current aggregate (or for the very first record using the
     * intermediate aggregation result provided via the {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Aggregator, Materialized)} can be used to compute
     * aggregate functions like count.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate
     * consecutive updates to the same key. The rate of propagated updates depends on your input
     * data rate, the number of distinct keys, the number of parallel running Kafka Streams
     * instances, and the {@link StreamsConfig configuration} parameters for {@link
     * StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and {@link
     * StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via {@link
     * KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // some aggregation on value type double
     * String queryableStoreName = "storeName" // the store name should be the name of the store as defined by the Materialized instance
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long aggForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link
     * KafkaStreams#allMetadata()} to query the value of the key on a parallel running instance of
     * your Kafka Streams application.
     *
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be
     * created in Kafka. Therefore, the store name defined by the Materialized instance must be a
     * valid Kafka topic name and cannot contain characters other than ASCII alphanumerics, '.', '_'
     * and '-'. The changelog topic will be named "${applicationId}-${storeName}-changelog", where
     * "applicationId" is user-specified in {@link StreamsConfig} via parameter {@link
     * StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the provide store
     * name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer  an {@link Initializer} that computes an initial intermediate aggregation
     *                     result
     * @param storeSupplier an {@link StoreSupplier} that set up the state store.
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that
     * represent the latest (rolling) aggregate for each key
     */
    KTable<K, T> aggregate(final Initializer<T> initializer,
                           final StoreSupplier<KeyValueStore> storeSupplier);
    /**
     * Aggregate the values of records in this stream by the grouped key. Records with {@code null}
     * key or value are ignored. Aggregating is a generalization of Reducing combining via
     * reduce(...)} as it, for example, allows the result to have a different type than the input
     * values. The result is written into a local {@link KeyValueStore} (which is basically an
     * ever-updating materialized view) that can be queried by the given store name in {@code
     * materialized}. Furthermore, updates to the store are sent downstream into a {@link KTable}
     * changelog stream.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the
     * first record. The specified {@link Aggregator} is applied for each input record and computes
     * a new aggregate using the current aggregate (or for the very first record using the
     * intermediate aggregation result provided via the {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Aggregator, Materialized)} can be used to compute
     * aggregate functions like count.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate
     * consecutive updates to the same key. The rate of propagated updates depends on your input
     * data rate, the number of distinct keys, the number of parallel running Kafka Streams
     * instances, and the {@link StreamsConfig configuration} parameters for {@link
     * StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and {@link
     * StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via {@link
     * KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // some aggregation on value type double
     * String queryableStoreName = "storeName" // the store name should be the name of the store as defined by the Materialized instance
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long aggForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link
     * KafkaStreams#allMetadata()} to query the value of the key on a parallel running instance of
     * your Kafka Streams application.
     *
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be
     * created in Kafka. Therefore, the store name defined by the Materialized instance must be a
     * valid Kafka topic name and cannot contain characters other than ASCII alphanumerics, '.', '_'
     * and '-'. The changelog topic will be named "${applicationId}-${storeName}-changelog", where
     * "applicationId" is user-specified in {@link StreamsConfig} via parameter {@link
     * StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the provide store
     * name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer  an {@link Initializer} that computes an initial intermediate aggregation
     *                     result
     *
     * @param sessionMerger TODO:
     * @param sessionWindows TODO:
     * @param materialized an instance of {@link Materialized} used to materialize a state store.
     *                     Cannot be {@code null}.
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that
     * represent the latest (rolling) aggregate for each key
     */
    KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                     final Merger<? super K, V> sessionMerger,
                                     final SessionWindows sessionWindows,
                                     final Materialized<K, T, KeyValueStore<Bytes, byte[]>> materialized);
    /**
     * Aggregate the values of records in this stream by the grouped key. Records with {@code null}
     * key or value are ignored. Aggregating is a generalization of Reducing combining via
     * reduce(...)} as it, for example, allows the result to have a different type than the input
     * values. The result is written into a local {@link KeyValueStore} (which is basically an
     * ever-updating materialized view) that can be queried by the given store name in {@code
     * materialized}. Furthermore, updates to the store are sent downstream into a {@link KTable}
     * changelog stream.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the
     * first record. The specified {@link Aggregator} is applied for each input record and computes
     * a new aggregate using the current aggregate (or for the very first record using the
     * intermediate aggregation result provided via the {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Aggregator, Materialized)} can be used to compute
     * aggregate functions like count.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate
     * consecutive updates to the same key. The rate of propagated updates depends on your input
     * data rate, the number of distinct keys, the number of parallel running Kafka Streams
     * instances, and the {@link StreamsConfig configuration} parameters for {@link
     * StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and {@link
     * StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via {@link
     * KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // some aggregation on value type double
     * String queryableStoreName = "storeName" // the store name should be the name of the store as defined by the Materialized instance
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long aggForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link
     * KafkaStreams#allMetadata()} to query the value of the key on a parallel running instance of
     * your Kafka Streams application.
     *
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be
     * created in Kafka. Therefore, the store name defined by the Materialized instance must be a
     * valid Kafka topic name and cannot contain characters other than ASCII alphanumerics, '.', '_'
     * and '-'. The changelog topic will be named "${applicationId}-${storeName}-changelog", where
     * "applicationId" is user-specified in {@link StreamsConfig} via parameter {@link
     * StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the provide store
     * name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer  an {@link Initializer} that computes an initial intermediate aggregation
     *                     result
     * @param sessionMerger TODO:
     * @param sessionWindows TODO:
     * @param storeSupplier TODO:
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that
     * represent the latest (rolling) aggregate for each key
     */
    KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                     final Merger<? super K, V> sessionMerger,
                                     final SessionWindows sessionWindows,
                                     final StoreSupplier<SessionStore> storeSupplier);
    /**
     * Aggregate the values of records in this stream by the grouped key. Records with {@code null}
     * key or value are ignored. Aggregating is a generalization of Reducing combining via
     * reduce(...)} as it, for example, allows the result to have a different type than the input
     * values. The result is written into a local {@link KeyValueStore} (which is basically an
     * ever-updating materialized view) that can be queried by the given store name in {@code
     * materialized}. Furthermore, updates to the store are sent downstream into a {@link KTable}
     * changelog stream.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the
     * first record. The specified {@link Aggregator} is applied for each input record and computes
     * a new aggregate using the current aggregate (or for the very first record using the
     * intermediate aggregation result provided via the {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Aggregator, Materialized)} can be used to compute
     * aggregate functions like count.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate
     * consecutive updates to the same key. The rate of propagated updates depends on your input
     * data rate, the number of distinct keys, the number of parallel running Kafka Streams
     * instances, and the {@link StreamsConfig configuration} parameters for {@link
     * StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and {@link
     * StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via {@link
     * KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // some aggregation on value type double
     * String queryableStoreName = "storeName" // the store name should be the name of the store as defined by the Materialized instance
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long aggForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link
     * KafkaStreams#allMetadata()} to query the value of the key on a parallel running instance of
     * your Kafka Streams application.
     *
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be
     * created in Kafka. Therefore, the store name defined by the Materialized instance must be a
     * valid Kafka topic name and cannot contain characters other than ASCII alphanumerics, '.', '_'
     * and '-'. The changelog topic will be named "${applicationId}-${storeName}-changelog", where
     * "applicationId" is user-specified in {@link StreamsConfig} via parameter {@link
     * StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the provide store
     * name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer  an {@link Initializer} that computes an initial intermediate aggregation
     *                     result
     *
     * @param windows TODO:
     * @param materialized an instance of {@link Materialized} used to materialize a state store.
     *                     Cannot be {@code null}
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that
     * represent the latest (rolling) aggregate for each key
     */
    <W extends Window> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                                        final Windows<W> windows,
                                                        final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);
    /**
     * Aggregate the values of records in this stream by the grouped key. Records with {@code null}
     * key or value are ignored. Aggregating is a generalization of Reducing combining via
     * reduce(...)} as it, for example, allows the result to have a different type than the input
     * values. The result is written into a local {@link KeyValueStore} (which is basically an
     * ever-updating materialized view) that can be queried by the given store name in {@code
     * materialized}. Furthermore, updates to the store are sent downstream into a {@link KTable}
     * changelog stream.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the
     * first record. The specified {@link Aggregator} is applied for each input record and computes
     * a new aggregate using the current aggregate (or for the very first record using the
     * intermediate aggregation result provided via the {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Aggregator, Materialized)} can be used to compute
     * aggregate functions like count.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate
     * consecutive updates to the same key. The rate of propagated updates depends on your input
     * data rate, the number of distinct keys, the number of parallel running Kafka Streams
     * instances, and the {@link StreamsConfig configuration} parameters for {@link
     * StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and {@link
     * StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via {@link
     * KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // some aggregation on value type double
     * String queryableStoreName = "storeName" // the store name should be the name of the store as defined by the Materialized instance
     * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long aggForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link
     * KafkaStreams#allMetadata()} to query the value of the key on a parallel running instance of
     * your Kafka Streams application.
     *
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be
     * created in Kafka. Therefore, the store name defined by the Materialized instance must be a
     * valid Kafka topic name and cannot contain characters other than ASCII alphanumerics, '.', '_'
     * and '-'. The changelog topic will be named "${applicationId}-${storeName}-changelog", where
     * "applicationId" is user-specified in {@link StreamsConfig} via parameter {@link
     * StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the provide store
     * name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer  an {@link Initializer} that computes an initial intermediate aggregation
     *                     result
     * @param windows TODO:
     *
     * @param storeSupplier TODO:
     *
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that
     * represent the latest (rolling) aggregate for each key
     */
    <W extends Window> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                                        final Windows<W> windows,
                                                        final StoreSupplier<WindowStore> storeSupplier);
}

