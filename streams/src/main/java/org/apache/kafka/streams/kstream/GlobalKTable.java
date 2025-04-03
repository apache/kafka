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
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * {@code GlobalKTable} is an abstraction of a <em>changelog stream</em> from a primary-keyed table.
 * Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.
 * Primary-keys in a table cannot be {@code null}, and thus, {@code null}-key {@link Record key-value} pairs are not
 * supported, and corresponding records will be dropped.
 * {@code KTables} follow Kafka "tombstone" semantics, and {@code null}-value {@link Record key-value} pairs are
 * interpreted and processed as deletes for the corresponding key.
 *
 * <p>A {@code GlobalKTable} is {@link StreamsBuilder#globalTable(String) defined from a single Kafka topic} that is
 * consumed message by message.
 *
 * <p>A {@code GlobalKTable} can only be used as right-hand side input for a
 * {@link KStream#join(GlobalKTable, KeyValueMapper, ValueJoiner) stream-globalTable join}.
 *
 * <p>In contrast to a {@link KTable} that is partitioned over all {@link KafkaStreams} instances, a {@code GlobalKTable}
 * is fully replicated per {@link KafkaStreams} instance.
 * Every partition of the underlying topic is consumed by each {@code GlobalKTable}, such that the full set of data is
 * available in every {@link KafkaStreams} instance.
 * This provides the ability to perform joins with {@link KStream} without having to repartition the input stream.
 * Furthermore, {@link GlobalKTable} are "bootstrapped" on startup, and are maintained by a separate thread.
 * Thus, updates to a {@link GlobalKTable} are not "stream-time synchronized" what may lead to non-deterministic results.
 *
 * <p>Furthermore, all {@link GlobalKTable} have an internal {@link StateStore state store} which can be accessed from
 * "outside" using the Interactive Queries (IQ) API (see {@link KafkaStreams#store(StoreQueryParameters) KafkaStreams#store(...)}
 * and {@link KafkaStreams#query(StateQueryRequest) KafkaStreams#query(...) [new API; evolving]} for details).
 * For example:
 * <pre>{@code
 * builder.globalTable("topic-name", "queryable-store-name");
 * ...
 * KafkaStreams streams = ...;
 * streams.start()
 * ...
 * StoreQueryParameters<ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>> storeQueryParams =
 *   StoreQueryParameters.fromNameAndType("queryable-store-name", QueryableStoreTypes.timestampedKeyValueStore());
 * ReadOnlyKeyValueStore view = streams.store(storeQueryParams);
 *
 * // query the value for a key
 * ValueAndTimestamp value = view.get(key);
 *}</pre>
 *
 * Note that in contrast to {@link KTable} a {@code GlobalKTable}'s state holds a full copy of the underlying topic,
 * thus all keys can be queried locally.
 *
 * @param <K> the key type of this table
 * @param <V> the value type of this table
 *
 * @see StreamsBuilder#addGlobalStore(StoreBuilder, String, Consumed, ProcessorSupplier)
 */
public interface GlobalKTable<K, V> {
    /**
     * Get the name of the local state store that can be used to query this {@code GlobalKTable}.
     *
     * @return the underlying state store name, or {@code null} if this {@code GlobalKTable} cannot be queried.
     */
    String queryableStoreName();
}
