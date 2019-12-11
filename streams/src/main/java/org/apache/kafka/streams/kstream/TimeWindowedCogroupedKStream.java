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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.WindowStore;

/**
 * {@code TimeWindowedCogroupKStream} is an abstraction of a <i>windowed</i> record stream of {@link org.apache.kafka.streams.KeyValue} pairs.
 * It is an intermediate representation of a {@link CogroupedKStream} in order to apply a windowed aggregation operation on the original
 * {@link KGroupedStream} records resulting in a windowed {@link KTable}.
 * <p>
 * The specified {@code windows} define either hopping time windows that can be overlapping or tumbling (c.f.
 * {@link TimeWindows}) or they define landmark windows (c.f. {@link UnlimitedWindows}).
 * The result is written into a local {@link WindowStore} (which is basically an ever-updating
 * materialized view) that can be queried using the name provided in the {@link Materialized} instance.
 *
 * A {@code WindowedCogroupKStream} must be obtained from a {@link CogroupedKStream} via {@link CogroupedKStream#windowedBy(Windows)} .
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 * @see KStream
 * @see KGroupedStream
 * @see CogroupedKStream
 */
public interface TimeWindowedCogroupedKStream<K, V> {

    /**
     * Aggregate the values of records in this stream by the grouped key and defined window.
     * Records with {@code null} key or value are ignored.
     * <p>
     * The result is written into a local {@link WindowStore} (which is basically an ever-updating materialized view)
     * that can be queried using the store name as provided with {@link Materialized}.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record
     * for each key in each window is processed to provide an initial intermediate aggregation result
     * that is used to process the first record for each key in each window.
     * The specified {@link Aggregator} (as specified in {@link KGroupedStream#cogroup(Aggregator)} or
     * {@link CogroupedKStream#cogroup(KGroupedStream, Aggregator)}) is applied for each input record per key and per window and computes
     * a new aggregate using the current aggregate (or for the very first record using the intermediate
     * aggregation result provided via the {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer)} can be used to compute aggregate functions like
     * count or sum etc...
     * <p>
     * Not all updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates to
     * the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enable the rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}
     * <p>
     * To query the local {@link WindowStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // counting words
     * Store queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * ReadOnlyWindowStore<String,Long> localWindowStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>windowStore());
     * String key = "some-word";
     * long fromTime = ...;
     * long toTime = ...;
     * WindowStoreIterator<Long> aggregateStore = localWindowStore.fetch(key, timeFrom, timeTo); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     */
    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer);

    /**
     * Aggregate the values of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * <p>
     * The result is written into a local {@link WindowStore} (which is basically an ever-updating materialized view)
     * that can be queried using the store name as provided with {@link Materialized}.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record
     * for each key in each window is processed to provide an initial intermediate aggregation result
     * that is used to process the first record for each key in each window.
     * The specified {@link Aggregator} (as specified in {@link KGroupedStream#cogroup(Aggregator)} or
     * {@link CogroupedKStream#cogroup(KGroupedStream, Aggregator)}) is applied for each input record per key and per window and computes
     * a new aggregate using the current aggregate (or for the very first record using the intermediate
     * aggregation result provided via the {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Materialized)} can be used to compute aggregate functions like
     * count or sum etc...
     * <p>
     * Not all updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates to
     * the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enable the rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}
     * <p>
     * To query the local {@link WindowStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // counting words
     * Store queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * ReadOnlyWindowStore<String,Long> localWindowStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>windowStore());
     * String key = "some-word";
     * long fromTime = ...;
     * long toTime = ...;
     * WindowStoreIterator<Long> aggregateStore = localWindowStore.fetch(key, timeFrom, timeTo); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the {@link Materialized} instance must be a valid Kafka topic name and cannot contain characters other than ASCII
     * alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide store name defined in {@link Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     */
    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                     final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the values of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * <p>
     * The result is written into a local {@link WindowStore} (which is basically an ever-updating materialized view)
     * that can be queried using the store name as provided with {@link Materialized}.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record
     * for each key in each window is processed to provide an initial intermediate aggregation
     * result that is used to process the first record for each key in each window.
     * The specified {@link Aggregator} (as specified in {@link KGroupedStream#cogroup(Aggregator)} or
     * {@link CogroupedKStream#cogroup(KGroupedStream, Aggregator)}) is applied for each input record per key and per window and computes
     * a new aggregate using the current aggregate (or for the very first record using the intermediate
     * aggregation result provided via the {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Named)} can be used to compute aggregate functions like
     * count or sum etc...
     * <p>
     * Not all updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates to
     * the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enable the rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}
     * <p>
     * To query the local {@link WindowStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // counting words
     * Store queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * ReadOnlyWindowStore<String,Long> localWindowStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>windowStore());
     * String key = "some-word";
     * long fromTime = ...;
     * long toTime = ...;
     * WindowStoreIterator<Long> aggregateStore = localWindowStore.fetch(key, timeFrom, timeTo); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @param named  an instance of {@link Named} used to Named the processors. Cannot be {@code null}.
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     */
    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                     final Named named);

    /**
     * Aggregate the values of records in this stream by the grouped key.
     * Records with {@code null} key or value are ignored.
     * <p>
     * The result is written into a local {@link WindowStore} (which is basically an ever-updating materialized view)
     * that can be queried using the store name as provided with {@link Materialized}.
     * <p>
     * The specified {@link Initializer} is applied once directly before the first input record for each key in each window is processed to
     * provide an initial intermediate aggregation result that is used to process the first record for each key in each window.
     * The specified {@link Aggregator} (as specified in {@link KGroupedStream#cogroup(Aggregator)} or
     * {@link CogroupedKStream#cogroup(KGroupedStream, Aggregator)}) is applied for each input record per key and per window and computes
     * a new aggregate using the current aggregate (or for the very first record using the intermediate
     * aggregation result provided via the {@link Initializer}) and the record's value.
     * Thus, {@code aggregate(Initializer, Named, Materialized)} can be used to compute aggregate functions like
     * count or sum etc...
     * <p>
     * Not all updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates to
     * the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enable the rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}
     * <p>
     * To query the local {@link WindowStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // counting words
     * Store queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * ReadOnlyWindowStore<String,Long> localWindowStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>windowStore());
     * String key = "some-word";
     * long fromTime = ...;
     * long toTime = ...;
     * WindowStoreIterator<Long> aggregateStore = localWindowStore.fetch(key, timeFrom, timeTo); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name defined by the Materialized instance must be a valid Kafka topic name and cannot contain characters other than ASCII
     * alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${storeName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is the
     * provide store name defined in {@link Materialized}, and "-changelog" is a fixed suffix.
     * <p>
     * You can retrieve all generated internal topic names via {@link Topology#describe()}.
     *
     * @param initializer   an {@link Initializer} that computes an initial intermediate aggregation result. Cannot be {@code null}.
     * @param named  an instance of {@link Named} used to Named the processors. Cannot be {@code null}.
     * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     * @return a {@link KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     */
    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                     final Named named,
                                     final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized);

}
