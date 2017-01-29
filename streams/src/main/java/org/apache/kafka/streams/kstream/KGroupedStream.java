/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

/**
 * {@code KGroupedStream} is an abstraction of a <i>grouped</i> record stream of {@link KeyValue} pairs.
 * It is an intermediate representation of a {@link KStream} in order to apply an aggregation operation on the original
 * {@link KStream} records.
 * <p>
 * It is an intermediate representation after a grouping of a {@link KStream} before an aggregation is applied to the
 * new partitions resulting in a {@link KTable}.
 * <p>
 * A {@code KGroupedStream} must be obtained from a {@link KStream} via {@link KStream#groupByKey() groupByKey()} or
 * {@link KStream#groupBy(KeyValueMapper) groupBy(...)}.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 * @see KStream
 */
@InterfaceStability.Unstable
public interface KGroupedStream<K, V> {

    /**
     * Count the number of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * that can be queried using the provided {@code storeName}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // counting words
     * ReadOnlyKeyValueStore<String,Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-word";
     * Long countForWord = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide {@code storeName}, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     *
     * @param storeName the name of the underlying {@link KTable} state store
     * @return a {@link KTable} that contains "update" records with unmodified keys and {@link Long} values that
     * represent the latest (rolling) count (i.e., number of records) for each key
     */
    KTable<K, Long> count(final String storeName);

    /**
     * Count the number of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * provided by the given {@code storeSupplier}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}.
     * Use {@link StateStoreSupplier#name()} to get the store name:
     * <pre>{@code
     * KafkaStreams streams = ... // counting words
     * String storeName = storeSupplier.name();
     * ReadOnlyKeyValueStore<String,Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-word";
     * Long countForWord = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param storeSupplier user defined state store supplier
     * @return a {@link KTable} that contains "update" records with unmodified keys and {@link Long} values that
     * represent the latest (rolling) count (i.e., number of records) for each key
     */
    KTable<K, Long> count(final StateStoreSupplier<KeyValueStore> storeSupplier);

    /**
     * Count the number of records in this stream by the grouped key and the defined windows.
     * Records with {@code null} key or value are ignored.
     * The specified {@code windows} define either hopping time windows that can be overlapping or tumbling (c.f.
     * {@link TimeWindows}) or they define landmark windows (c.f. {@link UnlimitedWindows}).
     * The result is written into a local windowed {@link KeyValueStore} (which is basically an ever-updating
     * materialized view) that can be queried using the provided {@code storeName}.
     * Windows are retained until their retention time expires (c.f. {@link Windows#until(long)}).
     * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
     * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local windowed {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // counting words
     * ReadOnlyWindowStore<String,Long> localWindowStore = streams.store(storeName, QueryableStoreTypes.<String, Long>windowStore());
     * String key = "some-word";
     * long fromTime = ...;
     * long toTime = ...;
     * WindowStoreIterator<Long> countForWordsForWindows = localWindowStore.fetch(key, timeFrom, timeTo); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide {@code storeName}, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     *
     * @param windows   the specification of the aggregation {@link Windows}
     * @param storeName the name of the underlying {@link KTable} state store
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys and {@link Long} values
     * that represent the latest (rolling) count (i.e., number of records) for each key within a window
     */
    <W extends Window> KTable<Windowed<K>, Long> count(final Windows<W> windows,
                                                       final String storeName);

    /**
     * Count the number of records in this stream by the grouped key and the defined windows.
     * Records with {@code null} key or value are ignored.
     * The specified {@code windows} define either hopping time windows that can be overlapping or tumbling (c.f.
     * {@link TimeWindows}) or they define landmark windows (c.f. {@link UnlimitedWindows}).
     * The result is written into a local windowed {@link KeyValueStore} (which is basically an ever-updating
     * materialized view) provided by the given {@code storeSupplier}.
     * Windows are retained until their retention time expires (c.f. {@link Windows#until(long)}).
     * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
     * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local windowed {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}.
     * Use {@link StateStoreSupplier#name()} to get the store name:
     * <pre>{@code
     * KafkaStreams streams = ... // counting words
     * String storeName = storeSupplier.name();
     * ReadOnlyWindowStore<String,Long> localWindowStore = streams.store(storeName, QueryableStoreTypes.<String, Long>windowStore());
     * String key = "some-word";
     * long fromTime = ...;
     * long toTime = ...;
     * WindowStoreIterator<Long> countForWordsForWindows = localWindowStore.fetch(key, timeFrom, timeTo); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param windows       the specification of the aggregation {@link Windows}
     * @param storeSupplier user defined state store supplier
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys and {@link Long} values
     * that represent the latest (rolling) count (i.e., number of records) for each key within a window
     */
    <W extends Window> KTable<Windowed<K>, Long> count(final Windows<W> windows,
                                                       final StateStoreSupplier<WindowStore> storeSupplier);


    /**
     * Count the number of records in this stream by the grouped key into {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating
     * materialized view) that can be queried using the provided {@code storeName}.
     * SessionWindows are retained until their retention time expires (c.f. {@link SessionWindows#until(long)}).
     * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
     * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link SessionStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}.
     * Use {@link StateStoreSupplier#name()} to get the store name:
     * <pre>{@code
     * KafkaStreams streams = ... // counting words
     * String storeName = storeSupplier.name();
     * ReadOnlySessionStore<String,Long> sessionStore = streams.store(storeName, QueryableStoreTypes.<String, Long>sessionStore());
     * String key = "some-word";
     * KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     *
     * @param sessionWindows the specification of the aggregation {@link SessionWindows}
     * @param storeName      the name of the state store created from this operation.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys and {@link Long} values
     * that represent the latest (rolling) count (i.e., number of records) for each key within a window
     */
    KTable<Windowed<K>, Long> count(final SessionWindows sessionWindows, final String storeName);

    /**
     * Count the number of records in this stream by the grouped key into {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view)
     * provided by the given {@code storeSupplier}.
     * SessionWindows are retained until their retention time expires (c.f. {@link SessionWindows#until(long)}).
     * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
     * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link SessionStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}.
     * Use {@link StateStoreSupplier#name()} to get the store name:
     * <pre>{@code
     * KafkaStreams streams = ... // counting words
     * String storeName = storeSupplier.name();
     * ReadOnlySessionStore<String,Long> sessionStore = streams.store(storeName, QueryableStoreTypes.<String, Long>sessionStore());
     * String key = "some-word";
     * KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     *
     * @param sessionWindows the specification of the aggregation {@link SessionWindows}
     * @param storeSupplier  user defined state store supplier
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys and {@link Long} values
     * that represent the latest (rolling) count (i.e., number of records) for each key within a window
     */
    KTable<Windowed<K>, Long> count(final SessionWindows sessionWindows,
                                    final StateStoreSupplier<SessionStore> storeSupplier);

    /**
     * Combine the values of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator, Serde, String)}).
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * that can be queried using the provided {@code storeName}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Reducer} is applied for each input record and computes a new aggregate using the current
     * aggregate and the record's value.
     * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
     * value as-is.
     * Thus, {@code reduce(Reducer, String)} can be used to compute aggregate functions like sum, min, or max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // compute sum
     * ReadOnlyKeyValueStore<String,Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long sumForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide {@code storeName}, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     *
     * @param reducer   a {@link Reducer} that computes a new aggregate result
     * @param storeName the name of the underlying {@link KTable} state store
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     */
    KTable<K, V> reduce(final Reducer<V> reducer,
                        final String storeName);

    /**
     * Combine the value of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator, StateStoreSupplier)}).
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * provided by the given {@code storeSupplier}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Reducer} is applied for each input record and computes a new aggregate using the current
     * aggregate and the record's value.
     * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
     * value as-is.
     * Thus, {@code reduce(Reducer, StateStoreSupplier)} can be used to compute aggregate functions like sum, min, or
     * max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}.
     * Use {@link StateStoreSupplier#name()} to get the store name:
     * <pre>{@code
     * KafkaStreams streams = ... // compute sum
     * String storeName = storeSupplier.name();
     * ReadOnlyKeyValueStore<String,Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long sumForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param reducer   a {@link Reducer} that computes a new aggregate result
     * @param storeSupplier user defined state store supplier
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     */
    KTable<K, V> reduce(final Reducer<V> reducer,
                        final StateStoreSupplier<KeyValueStore> storeSupplier);

    /**
     * Combine the number of records in this stream by the grouped key and the defined windows.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator, Windows, Serde, String)}).
     * The specified {@code windows} define either hopping time windows that can be overlapping or tumbling (c.f.
     * {@link TimeWindows}) or they define landmark windows (c.f. {@link UnlimitedWindows}).
     * The result is written into a local windowed {@link KeyValueStore} (which is basically an ever-updating
     * materialized view) that can be queried using the provided {@code storeName}.
     * Windows are retained until their retention time expires (c.f. {@link Windows#until(long)}).
     * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
     * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
     * <p>
     * The specified {@link Reducer} is applied for each input record and computes a new aggregate using the current
     * aggregate and the record's value.
     * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
     * value as-is.
     * Thus, {@code reduce(Reducer, Windows, String)} can be used to compute aggregate functions like sum, min, or max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local windowed {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // compute sum
     * ReadOnlyWindowStore<String,Long> localWindowStore = streams.store(storeName, QueryableStoreTypes.<String, Long>windowStore());
     * String key = "some-key";
     * long fromTime = ...;
     * long toTime = ...;
     * WindowStoreIterator<Long> sumForKeyForWindows = localWindowStore.fetch(key, timeFrom, timeTo); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide {@code storeName}, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     *
     * @param reducer   a {@link Reducer} that computes a new aggregate result
     * @param windows   the specification of the aggregation {@link Windows}
     * @param storeName the name of the state store created from this operation
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    <W extends Window> KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                                     final Windows<W> windows,
                                                     final String storeName);

    /**
     * Combine the values of records in this stream by the grouped key and the defined windows.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator, Windows, Serde, String)}).
     * The specified {@code windows} define either hopping time windows that can be overlapping or tumbling (c.f.
     * {@link TimeWindows}) or they define landmark windows (c.f. {@link UnlimitedWindows}).
     * The result is written into a local windowed {@link KeyValueStore} (which is basically an ever-updating
     * materialized view) provided by the given {@code storeSupplier}.
     * Windows are retained until their retention time expires (c.f. {@link Windows#until(long)}).
     * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
     * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
     * <p>
     * The specified {@link Reducer} is applied for each input record and computes a new aggregate using the current
     * aggregate and the record's value.
     * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
     * value as-is.
     * Thus, {@code reduce(Reducer, Windows, StateStoreSupplier)} can be used to compute aggregate functions like sum,
     * min, or max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local windowed {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}.
     * Use {@link StateStoreSupplier#name()} to get the store name:
     * <pre>{@code
     * KafkaStreams streams = ... // compute sum
     * Sting storeName = storeSupplier.name();
     * ReadOnlyWindowStore<String,Long> localWindowStore = streams.store(storeName, QueryableStoreTypes.<String, Long>windowStore());
     * String key = "some-key";
     * long fromTime = ...;
     * long toTime = ...;
     * WindowStoreIterator<Long> sumForKeyForWindows = localWindowStore.fetch(key, timeFrom, timeTo); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param reducer       a {@link Reducer} that computes a new aggregate result
     * @param windows       the specification of the aggregation {@link Windows}
     * @param storeSupplier user defined state store supplier
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    <W extends Window> KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                                     final Windows<W> windows,
                                                     final StateStoreSupplier<WindowStore> storeSupplier);

    /**
     * Combine values of this stream by the grouped key into {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator, Merger, SessionWindows, Serde, String)}).
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating
     * materialized view) that can be queried using the provided {@code storeName}.
     * SessionWindows are retained until their retention time expires (c.f. {@link SessionWindows#until(long)}).
     * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
     * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
     * <p>
     * The specified {@link Reducer} is applied for each input record and computes a new aggregate using the current
     * aggregate and the record's value.
     * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
     * value as-is.
     * Thus, {@code reduce(Reducer, SessionWindows, String)} can be used to compute aggregate functions like sum, min,
     * or max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link SessionStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // compute sum
     * ReadOnlySessionStore<String,Long> sessionStore = streams.store(storeName, QueryableStoreTypes.<String, Long>sessionStore());
     * String key = "some-key";
     * KeyValueIterator<Windowed<String>, Long> sumForKeyForSession = localWindowStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide {@code storeName}, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * @param reducer           the instance of {@link Reducer}
     * @param sessionWindows    the specification of the aggregation {@link SessionWindows}
     * @param storeName         the name of the state store created from this operation
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                  final SessionWindows sessionWindows,
                                  final String storeName);

    /**
     * Combine values of this stream by the grouped key into {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator, Merger, SessionWindows, Serde, String)}).
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view)
     * provided by the given {@code storeSupplier}.
     * SessionWindows are retained until their retention time expires (c.f. {@link SessionWindows#until(long)}).
     * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
     * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
     * <p>
     * The specified {@link Reducer} is applied for each input record and computes a new aggregate using the current
     * aggregate and the record's value.
     * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
     * value as-is.
     * Thus, {@code reduce(Reducer, SessionWindows, StateStoreSupplier)} can be used to compute aggregate functions like
     * sum, min, or max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link SessionStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}.
     * Use {@link StateStoreSupplier#name()} to get the store name:
     * <pre>{@code
     * KafkaStreams streams = ... // compute sum
     * Sting storeName = storeSupplier.name();
     * ReadOnlySessionStore<String,Long> sessionStore = streams.store(storeName, QueryableStoreTypes.<String, Long>sessionStore());
     * String key = "some-key";
     * KeyValueIterator<Windowed<String>, Long> sumForKeyForSession = localWindowStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide {@code storeName}, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * @param reducer           the instance of {@link Reducer}
     * @param sessionWindows    the specification of the aggregation {@link SessionWindows}
     * @param storeSupplier     user defined state store supplier
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                  final SessionWindows sessionWindows,
                                  final StateStoreSupplier<SessionStore> storeSupplier);


    /**
     * Aggregate the values of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * Aggregating is a generalization of {@link #reduce(Reducer, String) combining via reduce(...)} as it, for example,
     * allows the result to have a different type than the input values.
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * that can be queried using the provided {@code storeName}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record is processed to
     * provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Aggregator, Serde, String)} can be used to compute aggregate functions like
     * count (c.f. {@link #count(String)}).
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // some aggregation on value type double
     * ReadOnlyKeyValueStore<String,Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long aggForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide {@code storeName}, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     *
     * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result
     * @param aggregator    an {@link Aggregator} that computes a new aggregate result
     * @param aggValueSerde aggregate value serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param storeName     the name of the state store created from this operation
     * @param <VR>          the value type of the resulting {@link KTable}
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     */
    <VR> KTable<K, VR> aggregate(final Initializer<VR> initializer,
                                 final Aggregator<? super K, ? super V, VR> aggregator,
                                 final Serde<VR> aggValueSerde,
                                 final String storeName);

    /**
     * Aggregate the values of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * Aggregating is a generalization of {@link #reduce(Reducer, StateStoreSupplier) combining via reduce(...)} as it,
     * for example, allows the result to have a different type than the input values.
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * provided by the given {@code storeSupplier}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record is processed to
     * provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Aggregator, StateStoreSupplier)} can be used to compute aggregate functions
     * like count (c.f. {@link #count(String)}).
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}.
     * Use {@link StateStoreSupplier#name()} to get the store name:
     * <pre>{@code
     * KafkaStreams streams = ... // some aggregation on value type double
     * Sting storeName = storeSupplier.name();
     * ReadOnlyKeyValueStore<String,Long> localStore = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
     * String key = "some-key";
     * Long aggForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result
     * @param aggregator    an {@link Aggregator} that computes a new aggregate result
     * @param storeSupplier user defined state store supplier
     * @param <VR>          the value type of the resulting {@link KTable}
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     */
    <VR> KTable<K, VR> aggregate(final Initializer<VR> initializer,
                                 final Aggregator<? super K, ? super V, VR> aggregator,
                                 final StateStoreSupplier<KeyValueStore> storeSupplier);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined windows.
     * Records with {@code null} key or value are ignored.
     * Aggregating is a generalization of {@link #reduce(Reducer, Windows, String) combining via reduce(...)} as it,
     * for example, allows the result to have a different type than the input values.
     * The specified {@code windows} define either hopping time windows that can be overlapping or tumbling (c.f.
     * {@link TimeWindows}) or they define landmark windows (c.f. {@link UnlimitedWindows}).
     * The result is written into a local windowed {@link KeyValueStore} (which is basically an ever-updating
     * materialized view) that can be queried using the provided {@code storeName}.
     * Windows are retained until their retention time expires (c.f. {@link Windows#until(long)}).
     * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
     * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
     * <p>
     * The specified {@link Initializer} is applied once per window directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Aggregator, Windows, Serde, String)} can be used to compute aggregate
     * functions like count (c.f. {@link #count(String)}).
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local windowed {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // some windowed aggregation on value type double
     * ReadOnlyWindowStore<String,Long> localWindowStore = streams.store(storeName, QueryableStoreTypes.<String, Long>windowStore());
     * String key = "some-key";
     * long fromTime = ...;
     * long toTime = ...;
     * WindowStoreIterator<Long> aggForKeyForWindows = localWindowStore.fetch(key, timeFrom, timeTo); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide {@code storeName}, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     *
     *
     * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result
     * @param aggregator    an {@link Aggregator} that computes a new aggregate result
     * @param windows       the specification of the aggregation {@link Windows}
     * @param aggValueSerde aggregate value serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param <VR>          the value type of the resulting {@link KTable}
     * @param storeName     the name of the state store created from this operation
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    <W extends Window, VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                                             final Aggregator<? super K, ? super V, VR> aggregator,
                                                             final Windows<W> windows,
                                                             final Serde<VR> aggValueSerde,
                                                             final String storeName);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined windows.
     * Records with {@code null} key or value are ignored.
     * Aggregating is a generalization of {@link #reduce(Reducer, Windows, StateStoreSupplier) combining via
     * reduce(...)} as it, for example, allows the result to have a different type than the input values.
     * The specified {@code windows} define either hopping time windows that can be overlapping or tumbling (c.f.
     * {@link TimeWindows}) or they define landmark windows (c.f. {@link UnlimitedWindows}).
     * The result is written into a local windowed {@link KeyValueStore} (which is basically an ever-updating
     * materialized view) provided by the given {@code storeSupplier}.
     * Windows are retained until their retention time expires (c.f. {@link Windows#until(long)}).
     * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
     * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
     * <p>
     * The specified {@link Initializer} is applied once per window directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Aggregator, Windows, StateStoreSupplier)} can be used to compute aggregate
     * functions like count (c.f. {@link #count(String)}).
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local windowed {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}.
     * Use {@link StateStoreSupplier#name()} to get the store name:
     * <pre>{@code
     * KafkaStreams streams = ... // some windowed aggregation on value type Long
     * Sting storeName = storeSupplier.name();
     * ReadOnlyWindowStore<String,Long> localWindowStore = streams.store(storeName, QueryableStoreTypes.<String, Long>windowStore());
     * String key = "some-key";
     * long fromTime = ...;
     * long toTime = ...;
     * WindowStoreIterator<Long> aggForKeyForWindows = localWindowStore.fetch(key, timeFrom, timeTo); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result
     * @param aggregator    an {@link Aggregator} that computes a new aggregate result
     * @param windows       the specification of the aggregation {@link Windows}
     * @param <VR>          the value type of the resulting {@link KTable}
     * @param storeSupplier user defined state store supplier
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    <W extends Window, VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                                             final Aggregator<? super K, ? super V, VR> aggregator,
                                                             final Windows<W> windows,
                                                             final StateStoreSupplier<WindowStore> storeSupplier);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * Aggregating is a generalization of {@link #reduce(Reducer, SessionWindows, String) combining via
     * reduce(...)} as it, for example, allows the result to have a different type than the input values.
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating
     * materialized view) that can be queried using the provided {@code storeName}.
     * SessionWindows are retained until their retention time expires (c.f. {@link SessionWindows#until(long)}).
     * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
     * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
     * <p>
     * The specified {@link Initializer} is applied once per session directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Aggregator, Merger, SessionWindows, Serde, String)} can be used to compute
     * aggregate functions like count (c.f. {@link #count(String)})
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link SessionStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}.
     * Use {@link StateStoreSupplier#name()} to get the store name:
     * <pre>{@code
     * KafkaStreams streams = ... // some windowed aggregation on value type double
     * Sting storeName = storeSupplier.name();
     * ReadOnlySessionStore<String, Long> sessionStore = streams.store(storeName, QueryableStoreTypes.<String, Long>sessionStore());
     * String key = "some-key";
     * KeyValueIterator<Windowed<String>, Long> aggForKeyForSession = localWindowStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param initializer    the instance of {@link Initializer}
     * @param aggregator     the instance of {@link Aggregator}
     * @param sessionMerger  the instance of {@link Merger}
     * @param sessionWindows the specification of the aggregation {@link SessionWindows}
     * @param aggValueSerde aggregate value serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param <T>           the value type of the resulting {@link KTable}
     * @param storeName     the name of the state store created from this operation
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    <T> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                         final Aggregator<? super K, ? super V, T> aggregator,
                                         final Merger<? super K, T> sessionMerger,
                                         final SessionWindows sessionWindows,
                                         final Serde<T> aggValueSerde,
                                         final String storeName);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * Aggregating is a generalization of {@link #reduce(Reducer, SessionWindows, String) combining via
     * reduce(...)} as it, for example, allows the result to have a different type than the input values.
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view)
     * provided by the given {@code storeSupplier}.
     * SessionWindows are retained until their retention time expires (c.f. {@link SessionWindows#until(long)}).
     * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
     * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
     * <p>
     * The specified {@link Initializer} is applied once per session directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * Thus, {@code #aggregate(Initializer, Aggregator, Merger, SessionWindows, Serde, StateStoreSupplier)} can be used
     * to compute aggregate functions like count (c.f. {@link #count(String)}).
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * To query the local {@link SessionStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}.
     * Use {@link StateStoreSupplier#name()} to get the store name:
     * <pre>{@code
     * KafkaStreams streams = ... // some windowed aggregation on value type double
     * Sting storeName = storeSupplier.name();
     * ReadOnlySessionStore<String, Long> sessionStore = streams.store(storeName, QueryableStoreTypes.<String, Long>sessionStore());
     * String key = "some-key";
     * KeyValueIterator<Windowed<String>, Long> aggForKeyForSession = localWindowStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     *
     * @param initializer    the instance of {@link Initializer}
     * @param aggregator     the instance of {@link Aggregator}
     * @param sessionMerger  the instance of {@link Merger}
     * @param sessionWindows the specification of the aggregation {@link SessionWindows}
     * @param aggValueSerde  aggregate value serdes for materializing the aggregated table,
     *                       if not specified the default serdes defined in the configs will be used
     * @param storeSupplier  user defined state store supplier
     * @param <T>           the value type of the resulting {@link KTable}
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    <T> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                         final Aggregator<? super K, ? super V, T> aggregator,
                                         final Merger<? super K, T> sessionMerger,
                                         final SessionWindows sessionWindows,
                                         final Serde<T> aggValueSerde,
                                         final StateStoreSupplier<SessionStore> storeSupplier);

}
