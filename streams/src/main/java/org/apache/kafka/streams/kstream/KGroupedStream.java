/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StateStoreSupplier;

/**
 * {@link KGroupedStream} is an abstraction of a <i>grouped record stream</i> of key-value pairs
 * usually grouped on a different key than the original stream key
 *
 * <p>
 * It is an intermediate representation of a {@link KStream} before an
 * aggregation is applied to the new partitions resulting in a new {@link KTable}.
 * @param <K> Type of keys
 * @param <V> Type of values
 *
 * @see KStream
 */
@InterfaceStability.Unstable
public interface KGroupedStream<K, V> {


    /**
     * Combine values of this stream by the grouped key into a new instance of ever-updating
     * {@link KTable}.
     *
     * @param reducer           the instance of {@link Reducer}
     * @param name              the name of the resulted {@link KTable}
     *
     * @return a {@link KTable} that contains records with unmodified keys and values that represent the latest (rolling) aggregate for each key
     */
    KTable<K, V> reduce(Reducer<V> reducer,
                        String name);

    /**
     * Combine values of this stream by the grouped key into a new instance of ever-updating with user defined state store supplier
     * @param reducer           the instance of {@link Reducer}
     * @param storeSupplier     the state store supplier {@link StateStoreSupplier}
     * @param name              the name of the resulted {@link KTable}
     * @return a {@link KTable} that contains records with unmodified keys and values that represent the latest (rolling) aggregate for each key
     */
    KTable<K, V> reduce(final Reducer<V> reducer,
                        final StateStoreSupplier storeSupplier,
                        final String name);
    /**
     * Combine values of this stream by key on a window basis into a new instance of windowed {@link KTable}.
     *
     * @param reducer           the instance of {@link Reducer}
     * @param windows           the specification of the aggregation {@link Windows}
     * @return a windowed {@link KTable} which can be treated as a list of {@code KTable}s
     *         where each table contains records with unmodified keys and values
     *         that represent the latest (rolling) aggregate for each key within that window
     */
    <W extends Window> KTable<Windowed<K>, V> reduce(Reducer<V> reducer,
                                                     Windows<W> windows);
    /**
     *
     * @param reducer          the instance of {@link Reducer}
     * @param windows          the specification of the aggregation {@link Windows}
     * @param storeSupplier    the state store supplier {@link StateStoreSupplier}
     * @return a windowed {@link KTable} which can be treated as a list of {@code KTable}s
     *         where each table contains records with unmodified keys and values
     *         that represent the latest (rolling) aggregate for each key within that window
     */
    <W extends Window> KTable<Windowed<K>, V> reduce(Reducer<V> reducer,
                                                     Windows<W> windows,
                                                     StateStoreSupplier storeSupplier);
    /**
     * Aggregate values of this stream by key into a new instance of a {@link KTable}.
     *
     * @param initializer   the instance of {@link Initializer}
     * @param aggregator    the instance of {@link Aggregator}
     * @param aggValueSerde aggregate value serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param <T>           the value type of the resulted {@link KTable}
     *
     * @return a {@link KTable} that represents the latest (rolling) aggregate for each key
     */
    <T> KTable<K, T> aggregate(Initializer<T> initializer,
                               Aggregator<K, V, T> aggregator,
                               Serde<T> aggValueSerde,
                               String name);

    /**
     *
     * @param initializer   the instance of {@link Initializer}
     * @param aggregator    the instance of {@link Aggregator}
     * @param aggValueSerde aggregate value serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param storeSupplier the state store supplier {@link StateStoreSupplier}
     * @param name          the name of the resulted {@link KTable}
     * @param <T>           the value type of the resulted {@link KTable}
     * @return a {@link KTable} that represents the latest (rolling) aggregate for each key
     */
    <T> KTable<K, T> aggregate(final Initializer<T> initializer,
                               final Aggregator<K, V, T> aggregator,
                               final Serde<T> aggValueSerde,
                               final StateStoreSupplier storeSupplier,
                               final String name);
    /**
     * Aggregate values of this stream by key on a window basis into a new instance of windowed {@link KTable}.
     *
     * @param initializer   the instance of {@link Initializer}
     * @param aggregator    the instance of {@link Aggregator}
     * @param windows       the specification of the aggregation {@link Windows}
     * @param aggValueSerde aggregate value serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param <T>           the value type of the resulted {@link KTable}
     *
     * @return a windowed {@link KTable} which can be treated as a list of {@code KTable}s
     *         where each table contains records with unmodified keys and values with type {@code T}
     *         that represent the latest (rolling) aggregate for each key within that window
     */
    <W extends Window, T> KTable<Windowed<K>, T> aggregate(Initializer<T> initializer,
                                                           Aggregator<K, V, T> aggregator,
                                                           Windows<W> windows,
                                                           Serde<T> aggValueSerde);
    /**
     *
     * @param initializer   the instance of {@link Initializer}
     * @param aggregator    the instance of {@link Aggregator}
     * @param windows       the specification of the aggregation {@link Windows}
     * @param aggValueSerde aggregate value serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param storeSupplier the state store supplier {@link StateStoreSupplier}
     * @param <T>           the value type of the resulted {@link KTable}
     * @return a windowed {@link KTable} which can be treated as a list of {@code KTable}s
     *         where each table contains records with unmodified keys and values with type {@code T}
     *         that represent the latest (rolling) aggregate for each key within that window
     */
    <W extends Window, T> KTable<Windowed<K>, T> aggregate(Initializer<T> initializer,
                                                           Aggregator<K, V, T> aggregator,
                                                           Windows<W> windows,
                                                           Serde<T> aggValueSerde,
                                                           StateStoreSupplier storeSupplier);
    /**
     * Count number of records of this stream by key into a new instance of a {@link KTable}
     *
     * @param name  the name of the resulted {@link KTable}
     *
     * @return a {@link KTable} that contains records with unmodified keys and values that represent the latest (rolling) count (i.e., number of records) for each key
     */
    KTable<K, Long> count(String name);


    /**
     * Count number of records of this stream by key on a window basis into a new instance of windowed {@link KTable}.
     *
     * @param windows   the specification of the aggregation {@link Windows}
     *
     * @return a windowed {@link KTable} which can be treated as a list of {@code KTable}s
     *         where each table contains records with unmodified keys and values
     *         that represent the latest (rolling) count (i.e., number of records) for each key within that window
     */
    <W extends Window> KTable<Windowed<K>, Long> count(Windows<W> windows);

}
