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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

/**
 * {@code CogroupedKStream} is an abstraction of multiple <i>grouped</i> record streams of {@link
 * KeyValue} pairs. It is an intermediate representation of one or more {@link KStream}s in order to
 * apply one or more aggregation operations on the original {@link KStream} records.
 * <p>
 * It is an intermediate representation after a grouping of {@link KStream}s, before the
 * aggregations are applied to the new partitions resulting in a {@link KTable}.
 * <p>
 * A {@code CogroupedKStream} must be obtained from a {@link KGroupedStream} via {@link
 * KGroupedStream#cogroup(Aggregator, Materialized) cogroup(...)}.
 *
 * @param <K> Type of keys
 * @param <V> Type of aggregate values
 */
public interface KCogroupedStream<K, V, T> {

    KCogroupedStream<K, V, T> cogroup(final KGroupedStream<K, V> groupedStream,
        final Aggregator<? super K, ? super V, T> aggregator);

    KTable<K, T> aggregate(final Initializer<T> initializer,
        final Materialized<K, T, KeyValueStore<Bytes, byte[]>> materialized);

    KTable<K, T> aggregate(final Initializer<T> initializer,
        final StoreSupplier<KeyValueStore> storeSupplier);

    KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
        final Merger<? super K, V> sessionMerger,
        final SessionWindows sessionWindows,
        final Materialized<K, T, KeyValueStore<Bytes, byte[]>> materialized);

    KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
        final Merger<? super K, V> sessionMerger,
        final SessionWindows sessionWindows,
        final StoreSupplier<SessionStore> storeSupplier);

    <W extends Window> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
        final Windows<W> windows,
        final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    <W extends Window> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
        final Windows<W> windows,
        final StoreSupplier<WindowStore> storeSupplier);
}

