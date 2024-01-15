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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

/**
 * {@code CogroupedKStream} is an abstraction of multiple <i>grouped</i> record streams of {@link KeyValue} pairs.
 * <p>
 * It is an intermediate representation after a grouping of {@link KStream}s, before the
 * aggregations are applied to the new partitions resulting in a {@link KTable}.
 * <p>
 * A {@code CogroupedKStream} must be obtained from a {@link KGroupedStream} via
 * {@link KGroupedStream#cogroup(Aggregator) cogroup(...)}.
 *
 * @param <K> Type of keys
 * @param <VAgg> Type of values after agg
 */
public interface CogroupedKStream<K, VAgg> {

    /**
     * Add an already {@link KGroupedStream grouped KStream} to this {@code CogroupedKStream}.
     * <p>
     * The added {@link KGroupedStream grouped KStream} must have the same number of partitions as all existing
     * streams of this {@code CogroupedKStream}.
     * If this is not the case, you would need to call {@link KStream#repartition(Repartitioned)} before
     * {@link KStream#groupByKey() grouping} the {@link KStream} and specify the "correct" number of
     * partitions via {@link Repartitioned} parameter.
     * <p>
     * The specified {@link Aggregator} is applied in the actual {@link #aggregate(Initializer) aggregation} step for
     * each input record and computes a new aggregate using the current aggregate (or for the very first record per key
     * using the initial intermediate aggregation result provided via the {@link Initializer} that is passed into
     * {@link #aggregate(Initializer)}) and the record's value.
     *
     * @param groupedStream
     *        a group stream
     * @param aggregator
     *        an {@link Aggregator} that computes a new aggregate result
     *
     * @param <V> Type of input values
     *
     * @return a {@code CogroupedKStream}
     */
    <V> CogroupedKStream<K, VAgg> cogroup(final KGroupedStream<K, V> groupedStream,
                                          final Aggregator<? super K, ? super V, VAgg> aggregator);

    /**
     * Aggregate the values of records in these streams by the grouped key.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * that can be queried by the given store name in {@code materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * To compute the aggregation the corresponding {@link Aggregator} as specified in
     * {@link #cogroup(KGroupedStream, Aggregator) cogroup(...)} is used per input stream.
     * The specified {@link Initializer} is applied once per key, directly before the first input record per key is
     * processed to provide an initial intermediate aggregation result that is used to process the first record.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to the
     * same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link ReadOnlyKeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // some aggregation on value type double
     * String queryableStoreName = "storeName" // the store name should be the name of the store as defined by the Materialized instance
     * StoreQueryParameters<ReadOnlyKeyValueStore<K, ValueAndTimestamp<VOut>>> storeQueryParams = StoreQueryParameters.fromNameAndType(queryableStoreName, QueryableStoreTypes.timestampedKeyValueStore());
     * ReadOnlyKeyValueStore<K, ValueAndTimestamp<VOut>> localStore = streams.store(storeQueryParams);
     * K key = "some-key";
     * ValueAndTimestamp<VOut> aggForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to query
     * the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedKeyValueStore}) will be backed by
     * an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot
     * contain characters other than ASCII alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is a generated value, and
     * "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer
     *        an {@link Initializer} that computes an initial intermediate aggregation
     *        result. Cannot be {@code null}.
     *
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that
     * represent the latest (rolling) aggregate for each key
     */
    KTable<K, VAgg> aggregate(final Initializer<VAgg> initializer);

    /**
     * Aggregate the values of records in these streams by the grouped key.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * that can be queried by the given store name in {@code materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * To compute the aggregation the corresponding {@link Aggregator} as specified in
     * {@link #cogroup(KGroupedStream, Aggregator) cogroup(...)} is used per input stream.
     * The specified {@link Initializer} is applied once per key, directly before the first input record per key is
     * processed to provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Named} is applied once to the processor combining the grouped streams.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to the
     * same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link ReadOnlyKeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // some aggregation on value type double
     * String queryableStoreName = "storeName" // the store name should be the name of the store as defined by the Materialized instance
     * StoreQueryParameters<ReadOnlyKeyValueStore<K, ValueAndTimestamp<VOut>>> storeQueryParams = StoreQueryParameters.fromNameAndType(queryableStoreName, QueryableStoreTypes.timestampedKeyValueStore());
     * ReadOnlyKeyValueStore<K, ValueAndTimestamp<VOut>> localStore = streams.store(storeQueryParams);
     * K key = "some-key";
     * ValueAndTimestamp<VOut> aggForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to query
     * the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedKeyValueStore}) will be backed by
     * an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot
     * contain characters other than ASCII alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the provide store name defined
     * in {@code Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer
     *        an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @param named
     *        name the processor. Cannot be {@code null}.
     *
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that
     * represent the latest (rolling) aggregate for each key
     */
    KTable<K, VAgg> aggregate(final Initializer<VAgg> initializer,
                              final Named named);

    /**
     * Aggregate the values of records in these streams by the grouped key.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * that can be queried by the given store name in {@code materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * To compute the aggregation the corresponding {@link Aggregator} as specified in
     * {@link #cogroup(KGroupedStream, Aggregator) cogroup(...)} is used per input stream.
     * The specified {@link Initializer} is applied once per key, directly before the first input record per key is
     * processed to provide an initial intermediate aggregation result that is used to process the first record.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to the
     * same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link ReadOnlyKeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // some aggregation on value type double
     * String queryableStoreName = "storeName" // the store name should be the name of the store as defined by the Materialized instance
     * StoreQueryParameters<ReadOnlyKeyValueStore<K, ValueAndTimestamp<VOut>>> storeQueryParams = StoreQueryParameters.fromNameAndType(queryableStoreName, QueryableStoreTypes.timestampedKeyValueStore());
     * ReadOnlyKeyValueStore<K, ValueAndTimestamp<VOut>> localStore = streams.store(storeQueryParams);
     * K key = "some-key";
     * ValueAndTimestamp<VOut> aggForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to query
     * the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedKeyValueStore} -- regardless of what
     * is specified in the parameter {@code materialized}) will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot
     * contain characters other than ASCII alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the provide store name defined
     * in {@code Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer
     *        an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @param materialized
     *        an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     *
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that
     * represent the latest (rolling) aggregate for each key
     */
    KTable<K, VAgg> aggregate(final Initializer<VAgg> initializer,
                              final Materialized<K, VAgg, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the values of records in these streams by the grouped key.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * that can be queried by the given store name in {@code materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * To compute the aggregation the corresponding {@link Aggregator} as specified in
     * {@link #cogroup(KGroupedStream, Aggregator) cogroup(...)} is used per input stream.
     * The specified {@link Initializer} is applied once per key, directly before the first input record per key is
     * processed to provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Named} is used to name the processor combining the grouped streams.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to the
     * same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // some aggregation on value type double
     * String queryableStoreName = "storeName" // the store name should be the name of the store as defined by the Materialized instance
     * StoreQueryParameters<ReadOnlyKeyValueStore<K, ValueAndTimestamp<VOut>>> storeQueryParams = StoreQueryParameters.fromNameAndType(queryableStoreName, QueryableStoreTypes.timestampedKeyValueStore());
     * ReadOnlyKeyValueStore<K, ValueAndTimestamp<VOut>> localStore = streams.store(storeQueryParams);
     * K key = "some-key";
     * ValueAndTimestamp<VOut> aggForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to query
     * the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedKeyValueStore} -- regardless of what
     * is specified in the parameter {@code materialized}) will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot
     * contain characters other than ASCII alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the provide store name defined
     * in {@code Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer
     *        an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @param materialized
     *        an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     * @param named
     *        name the processors. Cannot be {@code null}.
     *
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that
     * represent the latest (rolling) aggregate for each key
     */
    KTable<K, VAgg> aggregate(final Initializer<VAgg> initializer,
                              final Named named,
                              final Materialized<K, VAgg, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Create a new {@link TimeWindowedCogroupedKStream} instance that can be used to perform windowed
     * aggregations.
     *
     * @param windows
     *        the specification of the aggregation {@link Windows}
     *
     * @param <W>     the window type
     *
     * @return an instance of {@link TimeWindowedCogroupedKStream}
     */
    <W extends Window> TimeWindowedCogroupedKStream<K, VAgg> windowedBy(final Windows<W> windows);

    /**
     * Create a new {@link TimeWindowedCogroupedKStream} instance that can be used to perform sliding
     * windowed aggregations.
     *
     * @param windows
     *        the specification of the aggregation {@link SlidingWindows}
     *
     * @return an instance of {@link TimeWindowedCogroupedKStream}
     */
    TimeWindowedCogroupedKStream<K, VAgg> windowedBy(final SlidingWindows windows);

    /**
     * Create a new {@link SessionWindowedCogroupedKStream} instance that can be used to perform session
     * windowed aggregations.
     *
     * @param windows
     *        the specification of the aggregation {@link SessionWindows}
     *
     * @return an instance of {@link SessionWindowedCogroupedKStream}
     */
    SessionWindowedCogroupedKStream<K, VAgg> windowedBy(final SessionWindows windows);

}
