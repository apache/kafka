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
public interface KGroupedStream<K, V> {

    /**
     * Count the number of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view).
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedKeyValueStore}) will be backed by
     * an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
     * and "-changelog" is a fixed suffix.
     * Note that the internal store name may not be queryable through Interactive Queries.
     *
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @return a {@link KTable} that contains "update" records with unmodified keys and {@link Long} values that
     * represent the latest (rolling) count (i.e., number of records) for each key
     */
    KTable<K, Long> count();

    /**
     * Count the number of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view).
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedKeyValueStore}) will be backed by
     * an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
     * and "-changelog" is a fixed suffix.
     * Note that the internal store name may not be queryable through Interactive Queries.
     *
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param named  a {@link Named} config used to name the processor in the topology
     *
     * @return a {@link KTable} that contains "update" records with unmodified keys and {@link Long} values that
     * represent the latest (rolling) count (i.e., number of records) for each key
     */
    KTable<K, Long> count(final Named named);

    /**
     * Count the number of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * provided by the given store name in {@code materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link ReadOnlyKeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}.
     * <pre>{@code
     * KafkaStreams streams = ... // counting words
     * String queryableStoreName = "storeName"; // the store name should be the name of the store as defined by the Materialized instance
     * StoreQueryParameters<ReadOnlyKeyValueStore<K, ValueAndTimestamp<Long>>> storeQueryParams = StoreQueryParameters.fromNameAndType(queryableStoreName, QueryableStoreTypes.timestampedKeyValueStore());
     * ReadOnlyKeyValueStore<K, ValueAndTimestamp<Long>> localStore = streams.store(storeQueryParams);
     * K key = "some-word";
     * ValueAndTimestamp<Long> countForWord = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedKeyValueStore} -- regardless of what
     * is specified in the parameter {@code materialized}) will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot contain characters other than ASCII
     * alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide store name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
     *
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     *                      Note: the valueSerde will be automatically set to {@link org.apache.kafka.common.serialization.Serdes#Long() Serdes#Long()}
     *                      if there is no valueSerde provided
     * @return a {@link KTable} that contains "update" records with unmodified keys and {@link Long} values that
     * represent the latest (rolling) count (i.e., number of records) for each key
     */
    KTable<K, Long> count(final Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Count the number of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * provided by the given store name in {@code materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link ReadOnlyKeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}.
     * <pre>{@code
     * KafkaStreams streams = ... // counting words
     * String queryableStoreName = "storeName"; // the store name should be the name of the store as defined by the Materialized instance
     * StoreQueryParameters<ReadOnlyKeyValueStore<K, ValueAndTimestamp<Long>>> storeQueryParams = StoreQueryParameters.fromNameAndType(queryableStoreName, QueryableStoreTypes.timestampedKeyValueStore());
     * ReadOnlyKeyValueStore<K, ValueAndTimestamp<Long>> localStore = streams.store(storeQueryParams);
     * K key = "some-word";
     * ValueAndTimestamp<Long> countForWord = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedKeyValueStore} -- regardless of what
     * is specified in the parameter {@code materialized}) will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot contain characters other than ASCII
     * alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide store name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
     *
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param named         a {@link Named} config used to name the processor in the topology
     * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     *                      Note: the valueSerde will be automatically set to {@link org.apache.kafka.common.serialization.Serdes#Long() Serdes#Long()}
     *                      if there is no valueSerde provided
     * @return a {@link KTable} that contains "update" records with unmodified keys and {@link Long} values that
     * represent the latest (rolling) count (i.e., number of records) for each key
     */
    KTable<K, Long> count(final Named named,
                          final Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Combine the values of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator)}).
     * <p>
     * The specified {@link Reducer} is applied for each input record and computes a new aggregate using the current
     * aggregate and the record's value.
     * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
     * value as-is.
     * Thus, {@code reduce(Reducer)} can be used to compute aggregate functions like sum, min, or max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     *
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedKeyValueStore}) will be backed by
     * an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
     * and "-changelog" is a fixed suffix.
     * Note that the internal store name may not be queryable through Interactive Queries.
     *
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param reducer   a {@link Reducer} that computes a new aggregate result. Cannot be {@code null}.
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key. If the reduce function returns {@code null}, it is then interpreted as
     * deletion for the key, and future messages of the same key coming from upstream operators
     * will be handled as newly initialized value.
     */
    KTable<K, V> reduce(final Reducer<V> reducer);

    /**
     * Combine the value of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator, Materialized)}).
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * provided by the given store name in {@code materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Reducer} is applied for each input record and computes a new aggregate using the current
     * aggregate (first argument) and the record's value (second argument):
     * <pre>{@code
     * // At the example of a Reducer<Long>
     * new Reducer<Long>() {
     *   public Long apply(Long aggValue, Long currValue) {
     *     return aggValue + currValue;
     *   }
     * }
     * }</pre>
     * <p>
     * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
     * value as-is.
     * Thus, {@code reduce(Reducer, Materialized)} can be used to compute aggregate functions like sum, min, or
     * max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link ReadOnlyKeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}.
     * <pre>{@code
     * KafkaStreams streams = ... // compute sum
     * String queryableStoreName = "storeName" // the store name should be the name of the store as defined by the Materialized instance
     * StoreQueryParameters<ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>> storeQueryParams = StoreQueryParameters.fromNameAndType(queryableStoreName, QueryableStoreTypes.timestampedKeyValueStore());
     * ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> localStore = streams.store(storeQueryParams);
     * K key = "some-key";
     * ValueAndTimestamp<V> reduceForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedKeyValueStore} -- regardless of what
     * is specified in the parameter {@code materialized}) will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
     * and "-changelog" is a fixed suffix.
     * Note that the internal store name may not be queryable through Interactive Queries.
     *
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param reducer       a {@link Reducer} that computes a new aggregate result. Cannot be {@code null}.
     * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     */
    KTable<K, V> reduce(final Reducer<V> reducer,
                        final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);


    /**
     * Combine the value of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator, Materialized)}).
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * provided by the given store name in {@code materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Reducer} is applied for each input record and computes a new aggregate using the current
     * aggregate (first argument) and the record's value (second argument):
     * <pre>{@code
     * // At the example of a Reducer<Long>
     * new Reducer<Long>() {
     *   public Long apply(Long aggValue, Long currValue) {
     *     return aggValue + currValue;
     *   }
     * }
     * }</pre>
     * <p>
     * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
     * value as-is.
     * Thus, {@code reduce(Reducer, Materialized)} can be used to compute aggregate functions like sum, min, or
     * max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link ReadOnlyKeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}.
     * <pre>{@code
     * KafkaStreams streams = ... // compute sum
     * String queryableStoreName = "storeName" // the store name should be the name of the store as defined by the Materialized instance
     * StoreQueryParameters<ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>> storeQueryParams = StoreQueryParameters.fromNameAndType(queryableStoreName, QueryableStoreTypes.timestampedKeyValueStore());
     * ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> localStore = streams.store(storeQueryParams);
     * K key = "some-key";
     * ValueAndTimestamp<V> reduceForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedKeyValueStore} -- regardless of what
     * is specified in the parameter {@code materialized}) will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
     * and "-changelog" is a fixed suffix.
     * Note that the internal store name may not be queryable through Interactive Queries.
     *
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param reducer       a {@link Reducer} that computes a new aggregate result. Cannot be {@code null}.
     * @param named         a {@link Named} config used to name the processor in the topology.
     * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key. If the reduce function returns {@code null}, it is then interpreted as
     * deletion for the key, and future messages of the same key coming from upstream operators
     * will be handled as newly initialized value.
     */
    KTable<K, V> reduce(final Reducer<V> reducer,
                        final Named named,
                        final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the values of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * Aggregating is a generalization of {@link #reduce(Reducer) combining via reduce(...)} as it, for example,
     * allows the result to have a different type than the input values.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record is processed to
     * provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Aggregator)} can be used to compute aggregate functions like
     * count (c.f. {@link #count()}).
     * <p>
     * The default value serde from config will be used for serializing the result.
     * If a different serde is required then you should use {@link #aggregate(Initializer, Aggregator, Materialized)}.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     *
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedKeyValueStore}) will be backed by
     * an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
     * and "-changelog" is a fixed suffix.
     * Note that the internal store name may not be queryable through Interactive Queries.
     *
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result
     * @param aggregator    an {@link Aggregator} that computes a new aggregate result
     * @param <VR>          the value type of the resulting {@link KTable}
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key. If the aggregate function returns {@code null}, it is then interpreted as
     * deletion for the key, and future messages of the same key coming from upstream operators
     * will be handled as newly initialized value.
     */
    <VR> KTable<K, VR> aggregate(final Initializer<VR> initializer,
                                 final Aggregator<? super K, ? super V, VR> aggregator);

    /**
     * Aggregate the values of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * Aggregating is a generalization of {@link #reduce(Reducer) combining via reduce(...)} as it, for example,
     * allows the result to have a different type than the input values.
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * that can be queried by the given store name in {@code materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record is processed to
     * provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Aggregator, Materialized)} can be used to compute aggregate functions like
     * count (c.f. {@link #count()}).
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
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
     * StoreQueryParameters<ReadOnlyKeyValueStore<K, ValueAndTimestamp<VR>>> storeQueryParams = StoreQueryParameters.fromNameAndType(queryableStoreName, QueryableStoreTypes.timestampedKeyValueStore());
     * ReadOnlyKeyValueStore<K, ValueAndTimestamp<VR>> localStore = streams.store(storeQueryParams);
     * K key = "some-key";
     * ValueAndTimestamp<VR> aggForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedKeyValueStore} -- regardless of what
     * is specified in the parameter {@code materialized}) will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot contain characters other than ASCII
     * alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide store name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
     *
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result
     * @param aggregator    an {@link Aggregator} that computes a new aggregate result
     * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     * @param <VR>          the value type of the resulting {@link KTable}
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     */
    <VR> KTable<K, VR> aggregate(final Initializer<VR> initializer,
                                 final Aggregator<? super K, ? super V, VR> aggregator,
                                 final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the values of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * Aggregating is a generalization of {@link #reduce(Reducer) combining via reduce(...)} as it, for example,
     * allows the result to have a different type than the input values.
     * The result is written into a local {@link KeyValueStore} (which is basically an ever-updating materialized view)
     * that can be queried by the given store name in {@code materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record is processed to
     * provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Aggregator, Materialized)} can be used to compute aggregate functions like
     * count (c.f. {@link #count()}).
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same key.
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
     * StoreQueryParameters<ReadOnlyKeyValueStore<K, ValueAndTimestamp<VR>>> storeQueryParams = StoreQueryParameters.fromNameAndType(queryableStoreName, QueryableStoreTypes.timestampedKeyValueStore());
     * ReadOnlyKeyValueStore<K, ValueAndTimestamp<VR>> localStore = streams.store(storeQueryParams);
     * K key = "some-key";
     * ValueAndTimestamp<VR> aggForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedKeyValueStore} -- regardless of what
     * is specified in the parameter {@code materialized}) will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot contain characters other than ASCII
     * alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide store name defined in {@code Materialized}, and "-changelog" is a fixed suffix.
     *
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result
     * @param aggregator    an {@link Aggregator} that computes a new aggregate result
     * @param named         a {@link Named} config used to name the processor in the topology
     * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     * @param <VR>          the value type of the resulting {@link KTable}
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key. If the aggregate function returns {@code null}, it is then interpreted as
     * deletion for the key, and future messages of the same key coming from upstream operators
     * will be handled as newly initialized value.
     */
    <VR> KTable<K, VR> aggregate(final Initializer<VR> initializer,
                                 final Aggregator<? super K, ? super V, VR> aggregator,
                                 final Named named,
                                 final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Create a new {@link TimeWindowedKStream} instance that can be used to perform windowed aggregations.
     * @param windows the specification of the aggregation {@link Windows}
     * @param <W>     the window type
     * @return an instance of {@link TimeWindowedKStream}
     */
    <W extends Window> TimeWindowedKStream<K, V> windowedBy(final Windows<W> windows);

    /**
     * Create a new {@link TimeWindowedKStream} instance that can be used to perform sliding windowed aggregations.
     * @param windows the specification of the aggregation {@link SlidingWindows}
     * @return an instance of {@link TimeWindowedKStream}
     */
    TimeWindowedKStream<K, V> windowedBy(final SlidingWindows windows);

    /**
     * Create a new {@link SessionWindowedKStream} instance that can be used to perform session windowed aggregations.
     * @param windows the specification of the aggregation {@link SessionWindows}
     * @return an instance of {@link TimeWindowedKStream}
     */
    SessionWindowedKStream<K, V> windowedBy(final SessionWindows windows);

    /**
     * Create a new {@link CogroupedKStream} from this grouped KStream to allow cogrouping other
     * {@code KGroupedStream} to it.
     * {@link CogroupedKStream} is an abstraction of multiple <i>grouped</i> record streams of {@link KeyValue} pairs.
     * It is an intermediate representation after a grouping of {@link KStream}s, before the
     * aggregations are applied to the new partitions resulting in a {@link KTable}.
     * <p>
     * The specified {@link Aggregator} is applied in the actual {@link CogroupedKStream#aggregate(Initializer)
     * aggregation} step for each input record and computes a new aggregate using the current aggregate (or for the very
     * first record per key using the initial intermediate aggregation result provided via the {@link Initializer} that
     * is passed into {@link CogroupedKStream#aggregate(Initializer)}) and the record's value.
     *
     * @param aggregator an {@link Aggregator} that computes a new aggregate result
     * @param <VOut> the type of the output values
     * @return a {@link CogroupedKStream}
     */
    <VOut> CogroupedKStream<K, VOut> cogroup(final Aggregator<? super K, ? super V, VOut> aggregator);

}
