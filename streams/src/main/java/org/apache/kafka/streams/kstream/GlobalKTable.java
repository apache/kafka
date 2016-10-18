/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * {@link GlobalKTable} is an abstraction of a <i>changelog stream</i> from a primary-keyed table.
 * Each record in this stream is an update on the primary-keyed table with the record key as the primary key.
 * <p>
 * A {@link GlobalKTable} is fully replicated per KafkaStreams instance. Every partition of the underlying topic
 * is consumed by each {@link GlobalKTable}, such that the full set of data is available in every KafkaStreams instance.
 * This provides the ability to perform joins with {@link KStream}, {@link KTable}, and other {@link GlobalKTable}s
 * without having to repartition the input streams. All joins with the {@link GlobalKTable} require that a {@link KeyValueMapper}
 * is provided that can map from the (key, value) of the left hand side to the key of the right hand side {@link GlobalKTable}
 * <p>
 * A {@link GlobalKTable} is created via a {@link KStreamBuilder}. For example:
 * <pre>
 *     builder.globalTable("topic-name", "queryable-store-name");
 * </pre>
 * all {@link GlobalKTable}s are backed by a {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore}
 * and are therefore queryable via the interactive queries API.
 * For example:
 * <pre>
 *     final GlobalKTable globalOne = builder.globalTable("g1", "g1-store");
 *     final GlobalKTable globalTwo = builder.globalTable("g2", "g2-store");
 *     final KeyValueMapper keyMapper = ...;
 *     final ValueJoiner joiner = ...;
 *     globalTwo.join(globalOne, keyMapper, joiner, "view-g1-joined-g2");
 *     ...
 *     final KafkaStreams streams = ...;
 *     streams.start()
 *     ...
 *     ReadOnlyKeyValueStore view = streams.store("view-g1-joined-g2", QueryableStoreTypes.keyValueStore());
 *     view.get(key);
 *</pre>
 *
 *
 * @param <K> Type of primary keys
 * @param <V> Type of value changes
 *
 * @see KStream
 */
@InterfaceStability.Unstable
public interface GlobalKTable<K, V> {

    /**
     * Join elements of this {@link GlobalKTable} with another {@link GlobalKTable} using inner join.
     * <p>
     * The {@link KeyValueMapper} is used to map the (key,value) from this {@link GlobalKTable} to the
     * key of the other {@link GlobalKTable}.
     *
     * The {@link GlobalKTable}s produced as the result of a join are just a view on top of the joined tables, with
     * the join being resolved on demand.
     *
     * @param otherTable            instance of {@link GlobalKTable} to join with
     * @param keyMapper             instance of {@link KeyValueMapper} used to map from the (key, value) of this
     *                              {@link GlobalKTable} to the key of the other {@link GlobalKTable}
     * @param joiner                the instance of {@link ValueJoiner}
     * @param queryableViewName     the name of the {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore}
     *                              that can be used to query the values of the resulting {@link GlobalKTable}
     * @param <K1>                  key type of the other {@link GlobalKTable}
     * @param <V1>                  value type of the other {@link GlobalKTable}
     * @param <R>                   value type of the resulting {@link GlobalKTable}
     * @return a {@link GlobalKTable} that contains join-records for each key and values computed by the given {@link ValueJoiner},
     *         one for each matched record-pair
     */
    <K1, V1, R> GlobalKTable<K, R> join(final GlobalKTable<K1, V1> otherTable,
                                        final KeyValueMapper<K, V, K1> keyMapper,
                                        final ValueJoiner<V, V1, R> joiner,
                                        final String queryableViewName);

    /**
     * Join elements of this {@link GlobalKTable} with another {@link GlobalKTable} using left join.
     * <p>
     * The {@link KeyValueMapper} is used to map the (key,value) from this {@link GlobalKTable} to the
     * key of the other {@link GlobalKTable}.
     *
     * The {@link GlobalKTable}s produced as the result of a leftJoin are just a view on top of the joined tables, with
     * the join being resolved on demand. 
     *
     * @param otherTable            instance of {@link GlobalKTable} to join with
     * @param keyMapper             instance of {@link KeyValueMapper} used to map from the (key, value) of this
     *                              {@link GlobalKTable} to the key of the other {@link GlobalKTable}
     * @param joiner                the instance of {@link ValueJoiner}
     * @param queryableViewName     the name of the {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore}
     *                              that can be used to query the values of the resulting {@link GlobalKTable}
     * @param <K1>                  key type of the other {@link GlobalKTable}
     * @param <V1>                  value type of the other {@link GlobalKTable}
     * @param <R>                   value type of the resulting {@link GlobalKTable}
     * @return a {@link GlobalKTable} that contains join-records for each key and values computed by the given {@link ValueJoiner},
     *         one for each matched record-pair
     */
    <K1, V1, R> GlobalKTable<K, R> leftJoin(final GlobalKTable<K1, V1> otherTable,
                                            final KeyValueMapper<K, V, K1> keyMapper,
                                            final ValueJoiner<V, V1, R> joiner,
                                            final String queryableViewName);

}
