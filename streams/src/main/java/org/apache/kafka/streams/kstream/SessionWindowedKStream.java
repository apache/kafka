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
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.SessionStore;

import java.time.Duration;

/**
 * {@code SessionWindowedKStream} is an abstraction of a <i>windowed</i> record stream of {@link KeyValue} pairs.
 * It is an intermediate representation after a grouping and windowing of a {@link KStream} before an aggregation is
 * applied to the new (partitioned) windows resulting in a windowed {@link KTable} (a <emph>windowed</emph>
 * {@code KTable} is a {@link KTable} with key type {@link Windowed Windowed<K>}).
 * <p>
 * {@link SessionWindows} are dynamic data driven windows.
 * They have no fixed time boundaries, rather the size of the window is determined by the records.
 * <p>
 * The result is written into a local {@link SessionStore} (which is basically an ever-updating
 * materialized view) that can be queried using the name provided in the {@link Materialized} instance.
 * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
 * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
 * New events are added to sessions until their grace period ends (see {@link SessionWindows#grace(Duration)}).
 * <p>
 * A {@code SessionWindowedKStream} must be obtained from a {@link KGroupedStream} via
 * {@link KGroupedStream#windowedBy(SessionWindows)}.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 * @see KStream
 * @see KGroupedStream
 * @see SessionWindows
 */
public interface SessionWindowedKStream<K, V> {

    /**
     * Count the number of records in this stream by the grouped key and defined sessions.
     * Note that sessions are generated on a per-key basis and records with different keys create independent sessions.
     * Records with {@code null} key or value are ignored.
     * <p>
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view).
     * The default key serde from the config will be used for serializing the result.
     * If a different serde is required then you should use {@link #count(Materialized)}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same session and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
     * and "-changelog" is a fixed suffix.
     * Note that the internal store name may not be queryable through Interactive Queries.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys and {@link Long} values
     * that represent the latest (rolling) count (i.e., number of records) for each key per session
     */
    KTable<Windowed<K>, Long> count();

    /**
     * Count the number of records in this stream by the grouped key and defined sessions.
     * Note that sessions are generated on a per-key basis and records with different keys create independent sessions.
     * Records with {@code null} key or value are ignored.
     * <p>
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view).
     * The default key serde from the config will be used for serializing the result.
     * If a different serde is required then you should use {@link #count(Named, Materialized)}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same session and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
     * and "-changelog" is a fixed suffix.
     * Note that the internal store name may not be queryable through Interactive Queries.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param named  a {@link Named} config used to name the processor in the topology. Cannot be {@code null}.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys and {@link Long} values
     * that represent the latest (rolling) count (i.e., number of records) for each key per session
     */
    KTable<Windowed<K>, Long> count(final Named named);

    /**
     * Count the number of records in this stream by the grouped key and defined sessions.
     * Note that sessions are generated on a per-key basis and records with different keys create independent sessions.
     * Records with {@code null} key or value are ignored.
     * <p>
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view)
     * that can be queried using the name provided with {@link Materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * Not all updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates
     * to the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enabled the rate of propagated updates depends on your input data rate, the number of distinct
     * keys, the number of parallel running Kafka Streams instances, and the {@link StreamsConfig configuration}
     * parameters for {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link SessionStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // compute sum
     * Sting queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * ReadOnlySessionStore<String,Long> localWindowStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>ReadOnlySessionStore<String, Long>);
     * String key = "some-key";
     * KeyValueIterator<Windowed<String>, Long> sumForKeyForWindows = localWindowStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot
     * contain characters other than ASCII alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the provide store name defined
     * in {@code Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     *                      Note: the valueSerde will be automatically set to {@link org.apache.kafka.common.serialization.Serdes#Long() Serdes#Long()}
     *                      if there is no valueSerde provided
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys and {@link Long} values
     * that represent the latest (rolling) count (i.e., number of records) for each key per session
     */
    KTable<Windowed<K>, Long> count(final Materialized<K, Long, SessionStore<Bytes, byte[]>> materialized);

    /**
     * Count the number of records in this stream by the grouped key and defined sessions.
     * Note that sessions are generated on a per-key basis and records with different keys create independent sessions.
     * Records with {@code null} key or value are ignored.
     * <p>
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view)
     * that can be queried using the name provided with {@link Materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * Not all updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates
     * to the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enabled the rate of propagated updates depends on your input data rate, the number of distinct
     * keys, the number of parallel running Kafka Streams instances, and the {@link StreamsConfig configuration}
     * parameters for {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link SessionStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // compute sum
     * Sting queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * ReadOnlySessionStore<String,Long> localWindowStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>ReadOnlySessionStore<String, Long>);
     * String key = "some-key";
     * KeyValueIterator<Windowed<String>, Long> sumForKeyForWindows = localWindowStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot
     * contain characters other than ASCII alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the provide store name defined
     * in {@code Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param named         a {@link Named} config used to name the processor in the topology. Cannot be {@code null}.
     * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     *                      Note: the valueSerde will be automatically set to {@link org.apache.kafka.common.serialization.Serdes#Long() Serdes#Long()}
     *                      if there is no valueSerde provided
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys and {@link Long} values
     * that represent the latest (rolling) count (i.e., number of records) for each key per session
     */
    KTable<Windowed<K>, Long> count(final Named named,
                                    final Materialized<K, Long, SessionStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined sessions.
     * Note that sessions are generated on a per-key basis and records with different keys create independent sessions.
     * Records with {@code null} key or value are ignored.
     * Aggregating is a generalization of {@link #reduce(Reducer) combining via reduce(...)} as it, for example,
     * allows the result to have a different type than the input values.
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view).
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Initializer} is applied directly before the first input record per session is processed to
     * provide an initial intermediate aggregation result that is used to process the first record per session.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * The specified {@link Merger} is used to merge two existing sessions into one, i.e., when the windows overlap,
     * they are merged into a single session and the old sessions are discarded.
     * Thus, {@code aggregate()} can be used to compute aggregate functions like count (c.f. {@link #count()}).
     * <p>
     * The default key and value serde from the config will be used for serializing the result.
     * If a different serde is required then you should use
     * {@link #aggregate(Initializer, Aggregator, Merger, Materialized)}.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
     * and "-changelog" is a fixed suffix.
     * Note that the internal store name may not be queryable through Interactive Queries.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer    an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @param aggregator     an {@link Aggregator} that computes a new aggregate result. Cannot be {@code null}.
     * @param sessionMerger  a {@link Merger} that combines two aggregation results. Cannot be {@code null}.
     * @param <VR>           the value type of the resulting {@link KTable}
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key per session
     */
    <VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                           final Aggregator<? super K, ? super V, VR> aggregator,
                                           final Merger<? super K, VR> sessionMerger);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined sessions.
     * Note that sessions are generated on a per-key basis and records with different keys create independent sessions.
     * Records with {@code null} key or value are ignored.
     * Aggregating is a generalization of {@link #reduce(Reducer) combining via reduce(...)} as it, for example,
     * allows the result to have a different type than the input values.
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view).
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Initializer} is applied directly before the first input record per session is processed to
     * provide an initial intermediate aggregation result that is used to process the first record per session.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * The specified {@link Merger} is used to merge two existing sessions into one, i.e., when the windows overlap,
     * they are merged into a single session and the old sessions are discarded.
     * Thus, {@code aggregate()} can be used to compute aggregate functions like count (c.f. {@link #count()}).
     * <p>
     * The default key and value serde from the config will be used for serializing the result.
     * If a different serde is required then you should use
     * {@link #aggregate(Initializer, Aggregator, Merger, Named, Materialized)}.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct
     * keys, the number of parallel running Kafka Streams instances, and the {@link StreamsConfig configuration}
     * parameters for {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
     * and "-changelog" is a fixed suffix.
     * Note that the internal store name may not be queryable through Interactive Queries.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer    an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @param aggregator     an {@link Aggregator} that computes a new aggregate result. Cannot be {@code null}.
     * @param sessionMerger  a {@link Merger} that combines two aggregation results. Cannot be {@code null}.
     * @param named          a {@link Named} config used to name the processor in the topology. Cannot be {@code null}.
     * @param <VR>           the value type of the resulting {@link KTable}
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key per session
     */
    <VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                           final Aggregator<? super K, ? super V, VR> aggregator,
                                           final Merger<? super K, VR> sessionMerger,
                                           final Named named);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined sessions.
     * Note that sessions are generated on a per-key basis and records with different keys create independent sessions.
     * Records with {@code null} key or value are ignored.
     * Aggregating is a generalization of {@link #reduce(Reducer) combining via reduce(...)} as it, for example,
     * allows the result to have a different type than the input values.
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view)
     * that can be queried using the store name as provided with {@link Materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Initializer} is applied directly before the first input record per session is processed to
     * provide an initial intermediate aggregation result that is used to process the first record per session.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * The specified {@link Merger} is used to merge two existing sessions into one, i.e., when the windows overlap,
     * they are merged into a single session and the old sessions are discarded.
     * Thus, {@code aggregate()} can be used to compute aggregate functions like count (c.f. {@link #count()}).
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enabled the rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link SessionStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // some windowed aggregation on value type double
     * Sting queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * ReadOnlySessionStore<String, Long> sessionStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>sessionStore());
     * String key = "some-key";
     * KeyValueIterator<Windowed<String>, Long> aggForKeyForSession = localWindowStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the {@link Materialized} instance must be a valid Kafka topic name and
     * cannot contain characters other than ASCII alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide store name defined in {@link Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer    an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @param aggregator     an {@link Aggregator} that computes a new aggregate result. Cannot be {@code null}.
     * @param sessionMerger  a {@link Merger} that combines two aggregation results. Cannot be {@code null}.
     * @param materialized   a {@link Materialized} config used to materialize a state store. Cannot be {@code null}.
     * @param <VR>           the value type of the resulting {@link KTable}
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key per session
     */
    <VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                           final Aggregator<? super K, ? super V, VR> aggregator,
                                           final Merger<? super K, VR> sessionMerger,
                                           final Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined sessions.
     * Note that sessions are generated on a per-key basis and records with different keys create independent sessions.
     * Records with {@code null} key or value are ignored.
     * Aggregating is a generalization of {@link #reduce(Reducer) combining via reduce(...)} as it, for example,
     * allows the result to have a different type than the input values.
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view)
     * that can be queried using the store name as provided with {@link Materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Initializer} is applied directly before the first input record per session is processed to
     * provide an initial intermediate aggregation result that is used to process the first record per session.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * The specified {@link Merger} is used to merge two existing sessions into one, i.e., when the windows overlap,
     * they are merged into a single session and the old sessions are discarded.
     * Thus, {@code aggregate()} can be used to compute aggregate functions like count (c.f. {@link #count()}).
     * <p>
     * Not all updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates
     * to the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enabled the rate of propagated updates depends on your input data rate, the number of distinct
     * keys, the number of parallel running Kafka Streams instances, and the {@link StreamsConfig configuration}
     * parameters for {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link SessionStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // some windowed aggregation on value type double
     * Sting queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * ReadOnlySessionStore<String, Long> sessionStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>sessionStore());
     * String key = "some-key";
     * KeyValueIterator<Windowed<String>, Long> aggForKeyForSession = localWindowStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the {@link Materialized} instance must be a valid Kafka topic name and
     * cannot contain characters other than ASCII alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide store name defined in {@link Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer    an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @param aggregator     an {@link Aggregator} that computes a new aggregate result. Cannot be {@code null}.
     * @param sessionMerger  a {@link Merger} that combines two aggregation results. Cannot be {@code null}.
     * @param named          a {@link Named} config used to name the processor in the topology. Cannot be {@code null}.
     * @param materialized   a {@link Materialized} config used to materialize a state store. Cannot be {@code null}.
     * @param <VR>           the value type of the resulting {@link KTable}
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key per session
     */
    <VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                           final Aggregator<? super K, ? super V, VR> aggregator,
                                           final Merger<? super K, VR> sessionMerger,
                                           final Named named,
                                           final Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized);

    /**
     * Combine the values of records in this stream by the grouped key and defined sessions.
     * Note that sessions are generated on a per-key basis and records with different keys create independent sessions.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator, Merger)}).
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view).
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * The default key and value serde from the config will be used for serializing the result.
     * If a different serde is required then you should use {@link #reduce(Reducer, Materialized)} .
     * <p>
     * The value of the first record per session initialized the session result.
     * The specified {@link Reducer} is applied for each additional input record per session and computes a new
     * aggregate using the current aggregate (first argument) and the record's value (second argument):
     * <pre>{@code
     * // At the example of a Reducer<Long>
     * new Reducer<Long>() {
     *   public Long apply(Long aggValue, Long currValue) {
     *     return aggValue + currValue;
     *   }
     * }
     * }</pre>
     * Thus, {@code reduce()} can be used to compute aggregate functions like sum, min, or max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
     * and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param reducer  a {@link Reducer} that computes a new aggregate result. Cannot be {@code null}.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key per session
     */
    KTable<Windowed<K>, V> reduce(final Reducer<V> reducer);

    /**
     * Combine the values of records in this stream by the grouped key and defined sessions.
     * Note that sessions are generated on a per-key basis and records with different keys create independent sessions.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator, Merger)}).
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view).
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * The default key and value serde from the config will be used for serializing the result.
     * If a different serde is required then you should use {@link #reduce(Reducer, Named, Materialized)} .
     * <p>
     * The value of the first record per session initialized the session result.
     * The specified {@link Reducer} is applied for each additional input record per session and computes a new
     * aggregate using the current aggregate (first argument) and the record's value (second argument):
     * <pre>{@code
     * // At the example of a Reducer<Long>
     * new Reducer<Long>() {
     *   public Long apply(Long aggValue, Long currValue) {
     *     return aggValue + currValue;
     *   }
     * }
     * }</pre>
     * Thus, {@code reduce()} can be used to compute aggregate functions like sum, min, or max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
     * and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param reducer  a {@link Reducer} that computes a new aggregate result. Cannot be {@code null}.
     * @param named    a {@link Named} config used to name the processor in the topology. Cannot be {@code null}.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key per session
     */
    KTable<Windowed<K>, V> reduce(final Reducer<V> reducer, final Named named);

    /**
     * Combine the values of records in this stream by the grouped key and defined sessions.
     * Note that sessions are generated on a per-key basis and records with different keys create independent sessions.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator, Merger)}).
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view)
     * that can be queried using the store name as provided with {@link Materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The value of the first record per session initialized the session result.
     * The specified {@link Reducer} is applied for each additional input record per session and computes a new
     * aggregate using the current aggregate (first argument) and the record's value (second argument):
     * <pre>{@code
     * // At the example of a Reducer<Long>
     * new Reducer<Long>() {
     *   public Long apply(Long aggValue, Long currValue) {
     *     return aggValue + currValue;
     *   }
     * }
     * }</pre>
     * Thus, {@code reduce()} can be used to compute aggregate functions like sum, min, or max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates
     * to the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enabled the rate of propagated updates depends on your input data rate, the number of distinct
     * keys, the number of parallel running Kafka Streams instances, and the {@link StreamsConfig configuration}
     * parameters for {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link SessionStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // compute sum
     * Sting queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * ReadOnlySessionStore<String,Long> localWindowStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>ReadOnlySessionStore<String, Long>);
     * String key = "some-key";
     * KeyValueIterator<Windowed<String>, Long> sumForKeyForWindows = localWindowStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot
     * contain characters other than ASCII alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the provide store name defined
     * in {@code Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param reducer       a {@link Reducer} that computes a new aggregate result. Cannot be {@code null}.
     * @param materialized  a {@link Materialized} config used to materialize a state store. Cannot be {@code null}.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key per session
     */
    KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                  final Materialized<K, V, SessionStore<Bytes, byte[]>> materialized);

    /**
     * Combine the values of records in this stream by the grouped key and defined sessions.
     * Note that sessions are generated on a per-key basis and records with different keys create independent sessions.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator, Merger)}).
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view)
     * that can be queried using the store name as provided with {@link Materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The value of the first record per session initialized the session result.
     * The specified {@link Reducer} is applied for each additional input record per session and computes a new
     * aggregate using the current aggregate (first argument) and the record's value (second argument):
     * <pre>{@code
     * // At the example of a Reducer<Long>
     * new Reducer<Long>() {
     *   public Long apply(Long aggValue, Long currValue) {
     *     return aggValue + currValue;
     *   }
     * }
     * }</pre>
     * Thus, {@code reduce()} can be used to compute aggregate functions like sum, min, or max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates
     * to the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enabled the rate of propagated updates depends on your input data rate, the number of distinct
     * keys, the number of parallel running Kafka Streams instances, and the {@link StreamsConfig configuration}
     * parameters for {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link SessionStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters)}  KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // compute sum
     * Sting queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * ReadOnlySessionStore<String,Long> localWindowStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>ReadOnlySessionStore<String, Long>);
     * String key = "some-key";
     * KeyValueIterator<Windowed<String>, Long> sumForKeyForWindows = localWindowStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot
     * contain characters other than ASCII alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the provide store name defined
     * in {@link Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param reducer       a {@link Reducer} that computes a new aggregate result. Cannot be {@code null}.
     * @param named         a {@link Named} config used to name the processor in the topology. Cannot be {@code null}.
     * @param materialized  a {@link Materialized} config used to materialize a state store. Cannot be {@code null}.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key per session
     */
    KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                  final Named named,
                                  final Materialized<K, V, SessionStore<Bytes, byte[]>> materialized);
}
