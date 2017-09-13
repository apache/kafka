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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedKStream;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.streams.kstream.internals.KGroupedStreamImpl.AGGREGATE_NAME;
import static org.apache.kafka.streams.kstream.internals.KGroupedStreamImpl.REDUCE_NAME;

public class WindowedKStreamImpl<K, V, W extends Window> extends AbstractStream<K> implements WindowedKStream<K, V> {

    private final Windows<W> windows;
    private final Serde<K> keySerde;
    private final Serde<V> valSerde;
    private final GroupedStreamAggregateBuilder<K, V> aggregateBuilder;

    WindowedKStreamImpl(final Windows<W> windows,
                        final InternalStreamsBuilder builder,
                        final Set<String> sourceNodes,
                        final String name,
                        final Serde<K> keySerde,
                        final Serde<V> valSerde,
                        final boolean repartitionRequired) {
        super(builder, name, sourceNodes);
        Objects.requireNonNull(windows, "windows can't be null");
        this.valSerde = valSerde;
        this.keySerde = keySerde;
        this.windows = windows;
        this.aggregateBuilder = new GroupedStreamAggregateBuilder<>(builder, keySerde, valSerde, repartitionRequired, sourceNodes, name);
    }

    @Override
    public KTable<Windowed<K>, Long> count() {
        return aggregate(
                new Initializer<Long>() {
                    @Override
                    public Long apply() {
                        return 0L;
                    }
                }, new Aggregator<K, V, Long>() {
                    @Override
                    public Long apply(K aggKey, V value, Long aggregate) {
                        return aggregate + 1;
                    }
                },
                Serdes.Long());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                                  final Aggregator<? super K, ? super V, VR> aggregator,
                                                  final Serde<VR> aggValueSerde) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        final String storeName = builder.newStoreName(AGGREGATE_NAME);
        return (KTable<Windowed<K>, VR>) aggregateBuilder.build(new KStreamWindowAggregate<>(windows, storeName, initializer, aggregator),
                                                                AGGREGATE_NAME,
                                                                windowStoreBuilder(storeName, aggValueSerde),
                                                                false);
    }

    @SuppressWarnings("unchecked")
    @Override
    public KTable<Windowed<K>, V> reduce(final Reducer<V> reducer) {
        Objects.requireNonNull(reducer, "reducer can't be null");
        final String storeName = builder.newStoreName(REDUCE_NAME);
        return (KTable<Windowed<K>, V>) aggregateBuilder.build(new KStreamWindowReduce<K, V, W>(windows, storeName, reducer),
                                                               REDUCE_NAME,
                                                               windowStoreBuilder(storeName, valSerde),
                                                               false);

    }

    private <VR> StoreBuilder<WindowStore<K, VR>> windowStoreBuilder(final String storeName, final Serde<VR> aggValueSerde) {
        return Stores.windowStoreBuilder(
                Stores.persistentWindowStore(
                        storeName,
                        windows.maintainMs(),
                        windows.segments,
                        windows.size(),
                        false),
                keySerde,
                aggValueSerde).withCachingEnabled();
    }

}
