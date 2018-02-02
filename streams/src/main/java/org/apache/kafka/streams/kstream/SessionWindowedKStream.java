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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.SessionStore;

/**
 * {@code SessionWindowedKStream} is an abstraction of a <i>windowed</i> record stream of {@link KeyValue} pairs.
 * It is an intermediate representation after a grouping and windowing of a {@link KStream} before an aggregation is applied to the
 * new (partitioned) windows resulting in a windowed {@link KTable}
 * (a <emph>windowed</emph> {@code KTable} is a {@link KTable} with key type {@link Windowed Windowed<K>}.
 * <p>
 * {@link SessionWindows} are dynamic data driven windows.
 * They have no fixed time boundaries, rather the size of the window is determined by the records.
 * Please see {@link SessionWindows} for more details.
 * <p>
 * {@link SessionWindows} are retained until their retention time expires (c.f. {@link SessionWindows#until(long)}).
 *
 * Furthermore, updates are sent downstream into a windowed {@link KTable} changelog stream, where
 * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
 * <p>
 * A {@code SessionWindowedKStream} must be obtained from a {@link KGroupedStream} via {@link KGroupedStream#windowedBy(SessionWindows)} .
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 * @see KStream
 * @see KGroupedStream
 * @see SessionWindows
 */
public interface SessionWindowedKStream<K, V> {

    /**
     * Count the number of records in this stream by the grouped key into {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     *
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys and {@link Long} values
     * that represent the latest (rolling) count (i.e., number of records) for each key within a window
     */
    KTable<Windowed<K>, Long> count();

    /**
     * Count the number of records in this stream by the grouped key into {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating
     * materialized view) that can be queried using the name provided with {@link Materialized}.
     * <p>
     * Not all updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates to
     * the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enabled the rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}
     * <p>
     * To query the local windowed {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}.
     * <pre>{@code
     * KafkaStreams streams = ... // compute sum
     * Sting queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * ReadOnlySessionStore<String,Long> localWindowStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>ReadOnlySessionStore<String, Long>);
     * String key = "some-key";
     * KeyValueIterator<Windowed<String>, Long> sumForKeyForWindows = localWindowStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     *
     * @param materialized  an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     *                      Note: the valueSerde will be automatically set to {@link Serdes#Long()} if there is no valueSerde provided
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys and {@link Long} values
     * that represent the latest (rolling) count (i.e., number of records) for each key within a window
     */
    KTable<Windowed<K>, Long> count(final Materialized<K, Long, SessionStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * Aggregating is a generalization of {@link #reduce(Reducer) combining via
     * reduce(...)} as it, for example, allows the result to have a different type than the input values.
     * <p>
     * The specified {@link Initializer} is applied once per session directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * The specified {@link Merger} is used to merge 2 existing sessions into one, i.e., when the windows overlap,
     * they are merged into a single session and the old sessions are discarded.
     * Thus, {@code aggregate(Initializer, Aggregator, Merger)} can be used to compute
     * aggregate functions like count (c.f. {@link #count()})
     * <p>
     * The default value serde from config will be used for serializing the result.
     * If a different serde is required then you should use {@link #aggregate(Initializer, Aggregator, Merger, Materialized)}.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * @param initializer    the instance of {@link Initializer}
     * @param aggregator     the instance of {@link Aggregator}
     * @param sessionMerger  the instance of {@link Merger}
     * @param <VR>            the value type of the resulting {@link KTable}
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    <VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                           final Aggregator<? super K, ? super V, VR> aggregator,
                                           final Merger<? super K, VR> sessionMerger);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating
     * materialized view) that can be queried using the name provided with {@link Materialized}.
     * Aggregating is a generalization of {@link #reduce(Reducer) combining via
     * reduce(...)} as it, for example, allows the result to have a different type than the input values.
     * <p>
     * The specified {@link Initializer} is applied once per session directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Aggregator} is applied for each input record and computes a new aggregate using the current
     * aggregate (or for the very first record using the intermediate aggregation result provided via the
     * {@link Initializer}) and the record's value.
     * * The specified {@link Merger} is used to merge 2 existing sessions into one, i.e., when the windows overlap,
     * they are merged into a single session and the old sessions are discarded.
     * Thus, {@code aggregate(Initializer, Aggregator, Merger)} can be used to compute
     * aggregate functions like count (c.f. {@link #count()})
     * <p>
     * Not all updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates to
     * the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enabled the rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}
     * <p>
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}.
     * <pre>{@code
     * KafkaStreams streams = ... // some windowed aggregation on value type double
     * Sting queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * ReadOnlySessionStore<String, Long> sessionStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>sessionStore());
     * String key = "some-key";
     * KeyValueIterator<Windowed<String>, Long> aggForKeyForSession = localWindowStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * @param initializer    the instance of {@link Initializer}
     * @param aggregator     the instance of {@link Aggregator}
     * @param sessionMerger  the instance of {@link Merger}
     * @param materialized   an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}
     * @param <VR>           the value type of the resulting {@link KTable}
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    <VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                           final Aggregator<? super K, ? super V, VR> aggregator,
                                           final Merger<? super K, VR> sessionMerger,
                                           final Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized);

    /**
     * Combine values of this stream by the grouped key into {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator, Merger)}).
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating
     * materialized view).
     * <p>
     * The specified {@link Reducer} is applied for each input record and computes a new aggregate using the current
     * aggregate and the record's value.
     * If there is no current aggregate the {@link Reducer} is not applied and the new aggregate will be the record's
     * value as-is.
     * Thus, {@code reduce(Reducer)} can be used to compute aggregate functions like sum, min,
     * or max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     *
     * @param reducer           a {@link Reducer} that computes a new aggregate result. Cannot be {@code null}.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    KTable<Windowed<K>, V> reduce(final Reducer<V> reducer);

    /**
     * Combine values of this stream by the grouped key into {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * Combining implies that the type of the aggregate result is the same as the type of the input value
     * (c.f. {@link #aggregate(Initializer, Aggregator, Merger)}).
     * The result is written into a local {@link SessionStore} (which is basically an ever-updating materialized view)
     * provided by the given {@link Materialized} instance.
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
     * Thus, {@code reduce(Reducer, SessionWindows, StateStoreSupplier)} can be used to compute aggregate functions like
     * sum, min, or max.
     * <p>
     * Not all updates might get sent downstream, as an internal cache will be used to deduplicate consecutive updates to
     * the same window and key if caching is enabled on the {@link Materialized} instance.
     * When caching is enabled the rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}
     * <p>
     * To query the local windowed {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}.
     * <pre>{@code
     * KafkaStreams streams = ... // compute sum
     * Sting queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
     * ReadOnlySessionStore<String,Long> localWindowStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>ReadOnlySessionStore<String, Long>);
     * String key = "some-key";
     * KeyValueIterator<Windowed<String>, Long> sumForKeyForWindows = localWindowStore.fetch(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * For failure and recovery the store will be backed by an internal changelog topic that will be created in Kafka.
     * Therefore, the store name must be a valid Kafka topic name and cannot contain characters other than ASCII
     * alphanumerics, '.', '_' and '-'.
     * The changelog topic will be named "${applicationId}-${queryableStoreName}-changelog", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "queryableStoreName" is the
     * provide {@code queryableStoreName}, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * @param reducer a {@link Reducer} that computes a new aggregate result. Cannot be {@code null}.
     * @param materializedAs an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                  final Materialized<K, V, SessionStore<Bytes, byte[]>> materializedAs);
}
