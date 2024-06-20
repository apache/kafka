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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/**
 * {@code GlobalKTable} is an abstraction of a <i>changelog stream</i> from a primary-keyed table.
 * Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.
 * <p>
 * {@code GlobalKTable} can only be used as right-hand side input for {@link KStream stream}-table joins.
 * <p>
 * In contrast to a {@link KTable} that is partitioned over all {@link KafkaStreams} instances, a {@code GlobalKTable}
 * is fully replicated per {@link KafkaStreams} instance.
 * Every partition of the underlying topic is consumed by each {@code GlobalKTable}, such that the full set of data is
 * available in every {@link KafkaStreams} instance.
 * This provides the ability to perform joins with {@link KStream} without having to repartition the input stream.
 * All joins with the {@code GlobalKTable} require that a {@link KeyValueMapper} is provided that can map from the
 * {@link KeyValue} of the left hand side {@link KStream} to the key of the right hand side {@code GlobalKTable}.
 * <p>
 * A {@code GlobalKTable} is created via a {@link StreamsBuilder}. For example:
 * <pre>{@code
 * builder.globalTable("topic-name", "queryable-store-name");
 * }</pre>
 * all {@code GlobalKTable}s are backed by a {@link ReadOnlyKeyValueStore} and are therefore queryable via the
 * interactive queries API.
 * For example:
 * <pre>{@code
 * final GlobalKTable globalOne = builder.globalTable("g1", "g1-store");
 * final GlobalKTable globalTwo = builder.globalTable("g2", "g2-store");
 * ...
 * final KafkaStreams streams = ...;
 * streams.start()
 * ...
 * ReadOnlyKeyValueStore view = streams.store("g1-store", QueryableStoreTypes.timestampedKeyValueStore());
 * view.get(key); // can be done on any key, as all keys are present
 *}</pre>
 * Note that in contrast to {@link KTable} a {@code GlobalKTable}'s state holds a full copy of the underlying topic,
 * thus all keys can be queried locally.
 * <p>
 * Records from the source topic that have null keys are dropped.
 *
 * @param <K> Type of primary keys
 * @param <V> Type of value changes
 *
 * @see KTable
 * @see StreamsBuilder#globalTable(String)
 * @see KStream#join(GlobalKTable, KeyValueMapper, ValueJoiner)
 * @see KStream#leftJoin(GlobalKTable, KeyValueMapper, ValueJoiner)
 */
public interface GlobalKTable<K, V> {
    /**
     * Get the name of the local state store that can be used to query this {@code GlobalKTable}.
     *
     * @return the underlying state store name, or {@code null} if this {@code GlobalKTable} cannot be queried.
     */
    String queryableStoreName();
}
