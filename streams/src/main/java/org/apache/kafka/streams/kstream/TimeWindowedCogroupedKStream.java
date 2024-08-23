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
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

/**
 * {@code TimeWindowedCogroupKStream} is an abstraction of a <i>windowed</i> record stream of {@link KeyValue} pairs.
 * It is an intermediate representation of a {@link CogroupedKStream} in order to apply a windowed aggregation operation
 * on the original {@link KGroupedStream} records resulting in a windowed {@link KTable} (a <emph>windowed</emph>
 * {@code KTable} is a {@link KTable} with key type {@link Windowed Windowed<K>}).
 * <p>
 * The specified {@code windows} define either hopping time windows that can be overlapping or tumbling (c.f.
 * {@link TimeWindows}) or they define landmark windows (c.f. {@link UnlimitedWindows}).
 * <p>
 * The result is written into a local {@link WindowStore} (which is basically an ever-updating
 * materialized view) that can be queried using the name provided in the {@link Materialized} instance.
 * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
 * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
 * New events are added to windows until their grace period ends (see {@link TimeWindows#ofSizeAndGrace(Duration, Duration)}).
 * <p>
 * A {@code TimeWindowedCogroupedKStream} must be obtained from a {@link CogroupedKStream} via
 * {@link CogroupedKStream#windowedBy(Windows)}.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 * @see KStream
 * @see KGroupedStream
 * @see CogroupedKStream
 */
public interface TimeWindowedCogroupedKStream<K, V> {

    /**
     * Aggregate the values of records in this stream by the grouped key and defined windows.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link WindowStore} (which is basically an ever-updating materialized view).
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Initializer} is applied directly before the first input record (per key) in each window is
     * processed to provide an initial intermediate aggregation result that is used to process the first record for
     * the window (per key).
     * The specified {@link Aggregator} (as specified in {@link KGroupedStream#cogroup(Aggregator)} or
     * {@link CogroupedKStream#cogroup(KGroupedStream, Aggregator)}) is applied for each input record and computes a new
     * aggregate using the current aggregate (or for the very first record using the intermediate aggregation result
     * provided via the {@link Initializer}) and the record's value.
     * Thus, {@code aggregate()} can be used to compute aggregate functions like count or sum etc.
     * <p>
     * The default key and value serde from the config will be used for serializing the result.
     * If a different serde is required then you should use {@link #aggregate(Initializer, Materialized)}.
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedWindowStore}) will be backed by
     * an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
     * and "-changelog" is a fixed suffix.
     * Note that the internal store name may not be queryable through Interactive Queries.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer  an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined windows.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link WindowStore} (which is basically an ever-updating materialized view).
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Initializer} is applied directly before the first input record (per key) in each window is
     * processed to provide an initial intermediate aggregation result that is used to process the first record for
     * the window (per key).
     * The specified {@link Aggregator} (as specified in {@link KGroupedStream#cogroup(Aggregator)} or
     * {@link CogroupedKStream#cogroup(KGroupedStream, Aggregator)}) is applied for each input record and computes a new
     * aggregate using the current aggregate (or for the very first record using the intermediate aggregation result
     * provided via the {@link Initializer}) and the record's value.
     * Thus, {@code aggregate()} can be used to compute aggregate functions like count or sum etc.
     * <p>
     * The default key and value serde from the config will be used for serializing the result.
     * If a different serde is required then you should use {@link #aggregate(Initializer, Named, Materialized)}.
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct
     * keys, the number of parallel running Kafka Streams instances, and the {@link StreamsConfig configuration}
     * parameters for {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedWindowStore}) will be backed by
     * an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-${internalStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "internalStoreName" is an internal name
     * and "-changelog" is a fixed suffix.
     * Note that the internal store name may not be queryable through Interactive Queries.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer  an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @param named        a {@link Named} config used to name the processor in the topology. Cannot be {@code null}.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                     final Named named);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined windows.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link WindowStore} (which is basically an ever-updating materialized view)
     * that can be queried using the store name as provided with {@link Materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Initializer} is applied directly before the first input record (per key) in each window is
     * processed to provide an initial intermediate aggregation result that is used to process the first record for
     * the window (per key).
     * The specified {@link Aggregator} (as specified in {@link KGroupedStream#cogroup(Aggregator)} or
     * {@link CogroupedKStream#cogroup(KGroupedStream, Aggregator)}) is applied for each input record and computes a new
     * aggregate using the current aggregate (or for the very first record using the intermediate aggregation result
     * provided via the {@link Initializer}) and the record's value.
     * Thus, {@code aggregate()} can be used to compute aggregate functions like count or sum etc.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enabled the rate of propagated updates depends on your input data rate, the number of distinct
     * keys, the number of parallel running Kafka Streams instances, and the {@link StreamsConfig configuration}
     * parameters for {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link ReadOnlyWindowStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // counting words
     * Store queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * StoreQueryParameters<ReadOnlyKeyValueStore<K, ValueAndTimestamp<VR>>> storeQueryParams = StoreQueryParameters.fromNameAndType(queryableStoreName, QueryableStoreTypes.timestampedWindowStore());
     * ReadOnlyWindowStore<K, ValueAndTimestamp<VR>> localWindowStore = streams.store(storeQueryParams);
     * K key = "some-word";
     * long fromTime = ...;
     * long toTime = ...;
     * WindowStoreIterator<ValueAndTimestamp<V>> aggregateStore = localWindowStore.fetch(key, timeFrom, timeTo); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedWindowStore} -- regardless of what
     * is specified in the parameter {@code materialized}) will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the {@link Materialized} instance must be a valid Kafka topic name and
     * cannot contain characters other than ASCII alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * name of the store defined in {@link Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @param materialized  a {@link Materialized} config used to materialize a state store. Cannot be {@code null}.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                     final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined windows.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link WindowStore} (which is basically an ever-updating materialized view)
     * that can be queried using the store name as provided with {@link Materialized}.
     * Furthermore, updates to the store are sent downstream into a {@link KTable} changelog stream.
     * <p>
     * The specified {@link Initializer} is applied directly before the first input record (per key) in each window is
     * processed to provide an initial intermediate aggregation result that is used to process the first record for
     * the window (per key).
     * The specified {@link Aggregator} (as specified in {@link KGroupedStream#cogroup(Aggregator)} or
     * {@link CogroupedKStream#cogroup(KGroupedStream, Aggregator)}) is applied for each input record and computes a new
     * aggregate using the current aggregate (or for the very first record using the intermediate aggregation result
     * provided via the {@link Initializer}) and the record's value.
     * Thus, {@code aggregate()} can be used to compute aggregate functions like count or sum etc.
     * <p>
     * Not all updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates
     * to the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enabled the rate of propagated updates depends on your input data rate, the number of distinct
     * keys, the number of parallel running Kafka Streams instances, and the {@link StreamsConfig configuration}
     * parameters for {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit interval}.
     * <p>
     * To query the local {@link ReadOnlyWindowStore} it must be obtained via
     * {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // counting words
     * Store queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * StoreQueryParameters<ReadOnlyKeyValueStore<K, ValueAndTimestamp<VR>>> storeQueryParams = StoreQueryParameters.fromNameAndType(queryableStoreName, QueryableStoreTypes.timestampedWindowStore());
     * ReadOnlyWindowStore<K, ValueAndTimestamp<VR>> localWindowStore = streams.store(storeQueryParams);
     *
     * K key = "some-word";
     * long fromTime = ...;
     * long toTime = ...;
     * WindowStoreIterator<ValueAndTimestamp<V>> aggregateStore = localWindowStore.fetch(key, timeFrom, timeTo); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#metadataForAllStreamsClients()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store (which always will be of type {@link TimestampedWindowStore} -- regardless of what
     * is specified in the parameter {@code materialized}) will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the {@link Materialized} instance must be a valid Kafka topic name and
     * cannot contain characters other than ASCII alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * name of the store defined in {@link Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @param named         a {@link Named} config used to name the processor in the topology. Cannot be {@code null}.
     * @param materialized  a {@link Materialized} config used to materialize a state store. Cannot be {@code null}.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                     final Named named,
                                     final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized);
}
