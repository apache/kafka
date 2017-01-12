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
import org.apache.kafka.streams.KafkaStreams;

/**
 * {@link GlobalKTable} is an abstraction of a <i>changelog stream</i> from a primary-keyed table.
 * Each record in this stream is an update on the primary-keyed table with the record key as the primary key.
 * <p>
 * A {@link GlobalKTable} is fully replicated per {@link KafkaStreams} instance. Every partition of the underlying topic
 * is consumed by each {@link GlobalKTable}, such that the full set of data is available in every {@link KafkaStreams} instance.
 * This provides the ability to perform joins with {@link KStream}, and {@link KTable},
 * without having to repartition the input streams. All joins with the {@link GlobalKTable} require that a {@link KeyValueMapper}
 * is provided that can map from the (key, value) of the left hand side to the key of the right hand side {@link GlobalKTable}
 * <p>
 * A {@link GlobalKTable} is created via a {@link KStreamBuilder}. For example:
 * <pre>
 *     builder.globalTable("topic-name", "queryable-store-name");
 * </pre>
 * all {@link GlobalKTable}s are backed by a {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore ReadOnlyKeyValueStore}
 * and are therefore queryable via the interactive queries API.
 * For example:
 * <pre>{@code
 *     final GlobalKTable globalOne = builder.globalTable("g1", "g1-store");
 *     final GlobalKTable globalTwo = builder.globalTable("g2", "g2-store");
 *     ...
 *     final KafkaStreams streams = ...;
 *     streams.start()
 *     ...
 *     ReadOnlyKeyValueStore view = streams.store("g1-store", QueryableStoreTypes.keyValueStore());
 *     view.get(key);
 *}</pre>
 *
 *
 * @param <K> Type of primary keys
 * @param <V> Type of value changes
 *
 * @see KTable
 */
@InterfaceStability.Unstable
public interface GlobalKTable<K, V> {
}
