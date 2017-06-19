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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

/**
 * {@code CogroupedKStream} is an abstraction of multiple <i>grouped</i> record streams of {@link KeyValue} pairs.
 * It is an intermediate representation of one or more {@link KStream}s in order to apply one or more aggregation
 * operations on the original {@link KStream} records.
 * <p>
 * It is an intermediate representation after a grouping of {@link KStream}s, before the aggregations are applied to
 * the new partitions resulting in a {@link KTable}.
 * <p>
 * A {@code CogroupedKStream} must be obtained from a {@link KGroupedStream} via 
 * {@link KGroupedStream#cogroup(Initializer, Aggregator, org.apache.kafka.common.serialization.Serde, String) cogroup(...)}.
 *
 * @param <K> Type of keys
 * @param <RK> Type of key in table, either K or Windowed&ltK&gt
 * @param <V> Type of aggregate values
 * @see KGroupedStream
 */
@InterfaceStability.Unstable
public interface CogroupedKStream<K, V> {

    <T> CogroupedKStream<K, V> cogroup(final KGroupedStream<K, T> groupedStream,
                                       final Aggregator<? super K, ? super T, V> aggregator);

    KTable<K, V> aggregate(final Initializer<V> initializer,
                           final Serde<V> valueSerde,
                           final String storeName);
    
    KTable<K, V> aggregate(final Initializer<V> initializer,
                           final StateStoreSupplier<KeyValueStore> storeSupplier);

    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                     final Merger<? super K, V> sessionMerger,
                                     final SessionWindows sessionWindows,
                                     final Serde<V> valueSerde,
                                     final String storeName);

    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                     final Merger<? super K, V> sessionMerger,
                                     final SessionWindows sessionWindows,
                                     final StateStoreSupplier<SessionStore> storeSupplier);

    <W extends Window> KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                                        final Windows<W> windows,
                                                        final Serde<V> valueSerde,
                                                        final String storeName);

    <W extends Window> KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                                        final Windows<W> windows,
                                                        final StateStoreSupplier<WindowStore> storeSupplier);
}
