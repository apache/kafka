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

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/**
 * {@code KTable} is an abstraction of a <i>changelog stream</i> from a primary-keyed table.
 * Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.
 * <p>
 * A {@code KTable} is either {@link StreamsBuilder#table(String) defined from a single Kafka topic} that is
 * consumed message by message or the result of a {@code KTable} transformation.
 * An aggregation of a {@link KStream} also yields a {@code KTable}.
 * <p>
 * A {@code KTable} can be transformed record by record, joined with another {@code KTable} or {@link KStream}, or
 * can be re-partitioned and aggregated into a new {@code KTable}.
 * <p>
 * Some {@code KTable}s have an internal state (a {@link ReadOnlyKeyValueStore}) and are therefore queryable via the
 * interactive queries API.
 * For example:
 * <pre>{@code
 *     final KTable table = ...
 *     ...
 *     final KafkaStreams streams = ...;
 *     streams.start()
 *     ...
 *     final String queryableStoreName = table.queryableStoreName(); // returns null if KTable is not queryable
 *     ReadOnlyKeyValueStore view = streams.store(queryableStoreName, QueryableStoreTypes.keyValueStore());
 *     view.get(key);
 *}</pre>
 *<p>
 * Records from the source topic that have null keys are dropped.
 *
 * @param <K> Type of primary keys
 * @param <V> Type of value changes
 * @see KStream
 * @see KGroupedTable
 * @see GlobalKTable
 * @see StreamsBuilder#table(String)
 */
@InterfaceStability.Evolving
public interface KTable<K, V> extends AbstractTable<K, V, KeyValueStore<Bytes, byte[]>> {

}
