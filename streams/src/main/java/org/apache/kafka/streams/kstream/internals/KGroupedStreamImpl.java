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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Set;

public class KGroupedStreamImpl<K, V> extends AbstractStream<K> implements KGroupedStream<K, V> {

    private static final String REDUCE_NAME = "KSTREAM-REDUCE-";
    private static final String AGGREGATE_NAME = "KSTREAM-AGGREGATE-";

    private final Serde<K> keySerde;
    private final Serde<V> valSerde;
    private final boolean repartitionRequired;

    public KGroupedStreamImpl(final KStreamBuilder topology,
                              final String name,
                              final Set<String> sourceNodes,
                              final Serde<K> keySerde,
                              final Serde<V> valSerde,
                              final boolean repartitionRequired) {
        super(topology, name, sourceNodes);
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        this.repartitionRequired = repartitionRequired;
    }

    @Override
    public KTable<K, V> reduce(final Reducer<V> reducer,
                               final String name) {
        return doAggregate(
            new KStreamReduce<K, V>(name, reducer),
            REDUCE_NAME,
            keyValueStore(valSerde, name));
    }


    @SuppressWarnings("unchecked")
    @Override
    public <W extends Window> KTable<Windowed<K>, V> reduce(Reducer<V> reducer,
                                                            Windows<W> windows) {
        return (KTable<Windowed<K>, V>) doAggregate(
            new KStreamWindowReduce<K, V, W>(windows, windows.name(), reducer),
            REDUCE_NAME,
            windowedStore(valSerde, windows)
        );
    }

    @Override
    public <T> KTable<K, T> aggregate(final Initializer<T> initializer,
                                      final Aggregator<K, V, T> aggregator,
                                      final Serde<T> aggValueSerde,
                                      final String name) {
        return doAggregate(
            new KStreamAggregate<>(name, initializer, aggregator),
            AGGREGATE_NAME,
            keyValueStore(aggValueSerde, name));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <W extends Window, T> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                                                  final Aggregator<K, V, T> aggregator,
                                                                  final Windows<W> windows,
                                                                  final Serde<T> aggValueSerde) {
        return (KTable<Windowed<K>, T>) doAggregate(
            new KStreamWindowAggregate<>(windows, windows.name(), initializer, aggregator),
            AGGREGATE_NAME,
            windowedStore(aggValueSerde, windows)
        );
    }

    @Override
    public KTable<K, Long> count(final String name) {
        return aggregate(new Initializer<Long>() {
            @Override
            public Long apply() {
                return 0L;
            }
        }, new Aggregator<K, V, Long>() {
            @Override
            public Long apply(K aggKey, V value, Long aggregate) {
                return aggregate + 1;
            }
        }, Serdes.Long(), name);
    }

    @Override
    public <W extends Window> KTable<Windowed<K>, Long> count(final Windows<W> windows) {
        return aggregate(new Initializer<Long>() {
            @Override
            public Long apply() {
                return 0L;
            }
        }, new Aggregator<K, V, Long>() {
            @Override
            public Long apply(K aggKey, V value, Long aggregate) {
                return aggregate + 1;
            }
        }, windows, Serdes.Long());
    }

    private <T> StateStoreSupplier keyValueStore(final Serde<T> aggValueSerde, final String name) {
        return storeFactory(aggValueSerde, name).build();
    }


    private <W extends Window, T> StateStoreSupplier windowedStore(final Serde<T> aggValSerde,
                                                                   final Windows<W> windows) {
        return storeFactory(aggValSerde, windows.name())
            .windowed(windows.maintainMs(), windows.segments, false)
            .build();

    }

    private <T> Stores.PersistentKeyValueFactory<K, T> storeFactory(final Serde<T> aggValueSerde,
                                                                    final String name) {
        return Stores.create(name)
            .withKeys(keySerde)
            .withValues(aggValueSerde)
            .persistent();

    }

    private <T> KTable<K, T> doAggregate(
        final KStreamAggProcessorSupplier<K, ?, V, T> aggregateSupplier,
        final String functionName,
        final StateStoreSupplier storeSupplier) {

        final String aggFunctionName = topology.newName(functionName);

        final String sourceName = repartitionIfRequired();

        topology.addProcessor(aggFunctionName, aggregateSupplier, sourceName);
        topology.addStateStore(storeSupplier, aggFunctionName);

        return new KTableImpl<>(topology,
                                aggFunctionName,
                                aggregateSupplier,
                                sourceName.equals(this.name) ? sourceNodes
                                                             : Collections.singleton(sourceName));
    }

    /**
     * @return the new sourceName if repartitioned. Otherwise the name of this stream
     */
    private String repartitionIfRequired() {
        if (!repartitionRequired) {
            return this.name;
        }
        return KStreamImpl.createReparitionedSource(this, keySerde, valSerde);
    }
}
