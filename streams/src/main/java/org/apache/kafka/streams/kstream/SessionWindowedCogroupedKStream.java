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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.SessionStore;
/**
 * {@code SessionWindowedCogroupKStream} is an abstraction of a <i>windowed</i> record stream of {@link org.apache.kafka.streams.KeyValue} pairs.
 * It is an intermediate representation of a {@link CogroupedKStream} in order to apply a windowed aggregation operation on the original
 * {@link KGroupedStream} records.
 * <p>
 * The specified {@link SessionWindows} defines how the windows are created.
 * The result is written into a local {@link SessionStore} (which is basically an ever-updating
 * materialized view) that can be queried using the name provided in the {@link Materialized} instance.
 * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
 * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.
 * A {@code SessionWindowedCogroupedKStream} must be obtained from a {@link CogroupedKStream} via {@link CogroupedKStream#windowedBy(SessionWindows)} .
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 * @see KStream
 * @see KGroupedStream
 * @see CogroupedKStream
 */

public interface SessionWindowedCogroupedKStream<K, V> {

    /**
     * Aggregate the values of records in these streams by the grouped key and defined {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * <p>
     * The specified {@link Initializer} is applied once per session directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Merger} is used to merge 2 existing sessions into one, i.e., when the windows overlap,
     * they are merged into a single session and the old sessions are discarded.
     * Thus, {@code aggregate(Initializer, Merger)} can be used to compute aggregate functions like count or sum ect...
     * <p>
     * The default key serde from config will be used for serializing the result.
     * If a different serde is required then you should use {@code #aggregate(Initializer, Merger, Materialized)}.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * @param initializer    the instance of {@link Initializer}. Cannot be {@code null}.
     * @param sessionMerger  the instance of {@link Merger}. Cannot be {@code null}.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                     final Merger<? super K, V> sessionMerger);
    /**
     * Aggregate the values of records in these streams by the grouped key and defined {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * <p>
     * The specified {@link Initializer} is applied once per session directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Merger} is used to merge 2 existing sessions into one, i.e., when the windows overlap,
     * they are merged into a single session and the old sessions are discarded.
     * Thus, {@code aggregate(Initializer, Merger, Materialized)} can be used to compute
     * aggregate functions like count or sum ect...
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * @param initializer    the instance of {@link Initializer}. Cannot be {@code null}.
     * @param sessionMerger  the instance of {@link Merger}. Cannot be {@code null}.
     * @param materialized   an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                     final Merger<? super K, V> sessionMerger,
                                     final Materialized<K, V, SessionStore<Bytes, byte[]>> materialized);
    /**
     * Aggregate the values of records in these streams by the grouped key and defined {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * <p>
     * The specified {@link Initializer} is applied once per session directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Merger} is used to merge 2 existing sessions into one, i.e., when the windows overlap,
     * they are merged into a single session and the old sessions are discarded.
     * Thus, {@code aggregate(Initializer, Named, Merger)} can be used to compute
     * aggregate functions like count or sum ect...
     * <p>
     * The default key serde from config will be used for serializing the result.
     * If a different serde is required then you should use {@code #aggregate(Initializer, Named, Merger, Materialized)}.
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * @param initializer    the instance of {@link Initializer}. Cannot be {@code null}.
     * @param sessionMerger  the instance of {@link Merger}. Cannot be {@code null}.
     * @param named  an instance of {@link Named} used to Named the processors. Cannot be {@code null}.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                     final Merger<? super K, V> sessionMerger,
                                     final Named named);
    /**
     * Aggregate the values of records in these streams by the grouped key and defined {@link SessionWindows}.
     * Records with {@code null} key or value are ignored.
     * <p>
     * The specified {@link Initializer} is applied once per session directly before the first input record is
     * processed to provide an initial intermediate aggregation result that is used to process the first record.
     * The specified {@link Merger} is used to merge 2 existing sessions into one, i.e., when the windows overlap,
     * they are merged into a single session and the old sessions are discarded.
     * Thus, {@code aggregate(Initializer, Named, Merger, Materialized)} can be used to compute
     * aggregate functions like count or sum ect...
     * <p>
     * Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
     * the same window and key.
     * The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of
     * parallel running Kafka Streams instances, and the {@link StreamsConfig configuration} parameters for
     * {@link StreamsConfig#CACHE_MAX_BYTES_BUFFERING_CONFIG cache size}, and
     * {@link StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit intervall}.
     * <p>
     * @param initializer    the instance of {@link Initializer}. Cannot be {@code null}.
     * @param sessionMerger  the instance of {@link Merger}. Cannot be {@code null}.
     * @param named  an instance of {@link Named} used to Named the processors. Cannot be {@code null}.
     * @param materialized   an instance of {@link Materialized} used to materialize a state store. Cannot be {@code null}.
     * @return a windowed {@link KTable} that contains "update" records with unmodified keys, and values that represent
     * the latest (rolling) aggregate for each key within a window
     */
    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                     final Merger<? super K, V> sessionMerger,
                                     final Named named,
                                     final Materialized<K, V, SessionStore<Bytes, byte[]>> materialized);

}
