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
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.SessionStore;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

class KGroupedStreamImpl<K, V> extends AbstractStream<K> implements KGroupedStream<K, V> {

    private static final String REDUCE_NAME = "KSTREAM-REDUCE-";
    private static final String AGGREGATE_NAME = "KSTREAM-AGGREGATE-";

    private final Serde<K> keySerde;
    private final Serde<V> valSerde;
    private final boolean repartitionRequired;
    private boolean isQueryable = true;

    KGroupedStreamImpl(final KStreamBuilder topology,
                       final String name,
                       final Set<String> sourceNodes,
                       final Serde<K> keySerde,
                       final Serde<V> valSerde,
                       final boolean repartitionRequired) {
        super(topology, name, sourceNodes);
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        this.repartitionRequired = repartitionRequired;
        this.isQueryable = true;
    }

    private void determineIsQueryable(final String queryableStoreName) {
        if (queryableStoreName == null) {
            isQueryable = false;
        } // no need for else {} since isQueryable is true by default
    }

    @Override
    public KTable<K, V> reduce(final Reducer<V> reducer,
                               final String queryableStoreName) {
        determineIsQueryable(queryableStoreName);
        return reduce(reducer, keyValueStore(keySerde, valSerde, getOrCreateName(queryableStoreName, REDUCE_NAME)));
    }

    @Override
    public KTable<K, V> reduce(final Reducer<V> reducer) {
        determineIsQueryable(null);
        return reduce(reducer, (String) null);
    }

    @Override
    public KTable<K, V> reduce(final Reducer<V> reducer,
                               final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(reducer, "reducer can't be null");
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doAggregate(
                new KStreamReduce<K, V>(storeSupplier.name(), reducer),
                REDUCE_NAME,
                storeSupplier);
    }


    @SuppressWarnings("unchecked")
    @Override
    public <W extends Window> KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                                            final Windows<W> windows,
                                                            final String queryableStoreName) {
        determineIsQueryable(queryableStoreName);
        return reduce(reducer, windows, windowedStore(keySerde, valSerde, windows, getOrCreateName(queryableStoreName, REDUCE_NAME)));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <W extends Window> KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                                            final Windows<W> windows) {
        return reduce(reducer, windows, (String) null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <W extends Window> KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                                            final Windows<W> windows,
                                                            final StateStoreSupplier<WindowStore> storeSupplier) {
        Objects.requireNonNull(reducer, "reducer can't be null");
        Objects.requireNonNull(windows, "windows can't be null");
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return (KTable<Windowed<K>, V>) doAggregate(
                new KStreamWindowReduce<K, V, W>(windows, storeSupplier.name(), reducer),
                REDUCE_NAME,
                storeSupplier
        );
    }

    @Override
    public <T> KTable<K, T> aggregate(final Initializer<T> initializer,
                                      final Aggregator<? super K, ? super V, T> aggregator,
                                      final Serde<T> aggValueSerde,
                                      final String queryableStoreName) {
        determineIsQueryable(queryableStoreName);
        return aggregate(initializer, aggregator, keyValueStore(keySerde, aggValueSerde, getOrCreateName(queryableStoreName, AGGREGATE_NAME)));
    }

    @Override
    public <T> KTable<K, T> aggregate(final Initializer<T> initializer,
                                      final Aggregator<? super K, ? super V, T> aggregator,
                                      final Serde<T> aggValueSerde) {
        return aggregate(initializer, aggregator, aggValueSerde, null);
    }

    @Override
    public <T> KTable<K, T> aggregate(final Initializer<T> initializer,
                                      final Aggregator<? super K, ? super V, T> aggregator,
                                      final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doAggregate(
                new KStreamAggregate<>(storeSupplier.name(), initializer, aggregator),
                AGGREGATE_NAME,
                storeSupplier);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <W extends Window, T> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                                                  final Aggregator<? super K, ? super V, T> aggregator,
                                                                  final Windows<W> windows,
                                                                  final Serde<T> aggValueSerde,
                                                                  final String queryableStoreName) {
        determineIsQueryable(queryableStoreName);
        return aggregate(initializer, aggregator, windows, windowedStore(keySerde, aggValueSerde, windows, getOrCreateName(queryableStoreName, AGGREGATE_NAME)));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <W extends Window, T> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                                                  final Aggregator<? super K, ? super V, T> aggregator,
                                                                  final Windows<W> windows,
                                                                  final Serde<T> aggValueSerde) {
        return aggregate(initializer, aggregator, windows, aggValueSerde, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <W extends Window, T> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                                                  final Aggregator<? super K, ? super V, T> aggregator,
                                                                  final Windows<W> windows,
                                                                  final StateStoreSupplier<WindowStore> storeSupplier) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        Objects.requireNonNull(windows, "windows can't be null");
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return (KTable<Windowed<K>, T>) doAggregate(
                new KStreamWindowAggregate<>(windows, storeSupplier.name(), initializer, aggregator),
                AGGREGATE_NAME,
                storeSupplier
        );
    }

    @Override
    public KTable<K, Long> count(final String queryableStoreName) {
        determineIsQueryable(queryableStoreName);
        return count(keyValueStore(keySerde, Serdes.Long(), getOrCreateName(queryableStoreName, AGGREGATE_NAME)));
    }

    @Override
    public KTable<K, Long> count() {
        return count((String) null);
    }

    @Override
    public KTable<K, Long> count(final StateStoreSupplier<KeyValueStore> storeSupplier) {
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
        }, storeSupplier);
    }

    @Override
    public <W extends Window> KTable<Windowed<K>, Long> count(final Windows<W> windows,
                                                              final String queryableStoreName) {
        determineIsQueryable(queryableStoreName);
        return count(windows, windowedStore(keySerde, Serdes.Long(), windows, getOrCreateName(queryableStoreName, AGGREGATE_NAME)));
    }

    @Override
    public <W extends Window> KTable<Windowed<K>, Long> count(final Windows<W> windows) {
        return count(windows, (String) null);
    }

    @Override
    public <W extends Window> KTable<Windowed<K>, Long> count(final Windows<W> windows,
                                                              final StateStoreSupplier<WindowStore> storeSupplier) {
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
                windows,
                storeSupplier);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                                final Aggregator<? super K, ? super V, T> aggregator,
                                                final Merger<? super K, T> sessionMerger,
                                                final SessionWindows sessionWindows,
                                                final Serde<T> aggValueSerde,
                                                final String queryableStoreName) {
        determineIsQueryable(queryableStoreName);
        return aggregate(initializer,
                         aggregator,
                         sessionMerger,
                         sessionWindows,
                         aggValueSerde,
                         storeFactory(keySerde, aggValueSerde, getOrCreateName(queryableStoreName, AGGREGATE_NAME))
                          .sessionWindowed(sessionWindows.maintainMs()).build());


    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                                final Aggregator<? super K, ? super V, T> aggregator,
                                                final Merger<? super K, T> sessionMerger,
                                                final SessionWindows sessionWindows,
                                                final Serde<T> aggValueSerde) {
        return aggregate(initializer, aggregator, sessionMerger, sessionWindows, aggValueSerde, (String) null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                                final Aggregator<? super K, ? super V, T> aggregator,
                                                final Merger<? super K, T> sessionMerger,
                                                final SessionWindows sessionWindows,
                                                final Serde<T> aggValueSerde,
                                                final StateStoreSupplier<SessionStore> storeSupplier) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        Objects.requireNonNull(sessionWindows, "sessionWindows can't be null");
        Objects.requireNonNull(sessionMerger, "sessionMerger can't be null");
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");

        return (KTable<Windowed<K>, T>) doAggregate(
                new KStreamSessionWindowAggregate<>(sessionWindows, storeSupplier.name(), initializer, aggregator, sessionMerger),
                AGGREGATE_NAME,
                storeSupplier);

    }

    @SuppressWarnings("unchecked")
    public KTable<Windowed<K>, Long> count(final SessionWindows sessionWindows, final String queryableStoreName) {
        determineIsQueryable(queryableStoreName);
        return count(sessionWindows,
                     storeFactory(keySerde, Serdes.Long(), getOrCreateName(queryableStoreName, AGGREGATE_NAME))
                             .sessionWindowed(sessionWindows.maintainMs()).build());
    }

    public KTable<Windowed<K>, Long> count(final SessionWindows sessionWindows) {
        return count(sessionWindows, (String) null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public KTable<Windowed<K>, Long> count(final SessionWindows sessionWindows,
                                           final StateStoreSupplier<SessionStore> storeSupplier) {
        Objects.requireNonNull(sessionWindows, "sessionWindows can't be null");
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        final Initializer<Long> initializer = new Initializer<Long>() {
            @Override
            public Long apply() {
                return 0L;
            }
        };
        final Aggregator<K, V, Long> aggregator = new Aggregator<K, V, Long>() {
            @Override
            public Long apply(final K aggKey, final V value, final Long aggregate) {
                return aggregate + 1;
            }
        };
        final Merger<K, Long> sessionMerger = new Merger<K, Long>() {
            @Override
            public Long apply(final K aggKey, final Long aggOne, final Long aggTwo) {
                return aggOne + aggTwo;
            }
        };

        return aggregate(initializer, aggregator, sessionMerger, sessionWindows, Serdes.Long(), storeSupplier);
    }


    @SuppressWarnings("unchecked")
    @Override
    public KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                         final SessionWindows sessionWindows,
                                         final String queryableStoreName) {
        determineIsQueryable(queryableStoreName);

        return reduce(reducer, sessionWindows,
                      storeFactory(keySerde, valSerde, getOrCreateName(queryableStoreName, AGGREGATE_NAME))
                              .sessionWindowed(sessionWindows.maintainMs()).build());
    }

    @SuppressWarnings("unchecked")
    @Override
    public KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                         final SessionWindows sessionWindows) {

        return reduce(reducer, sessionWindows, (String) null);
    }

    @Override
    public KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                         final SessionWindows sessionWindows,
                                         final StateStoreSupplier<SessionStore> storeSupplier) {
        Objects.requireNonNull(reducer, "reducer can't be null");
        Objects.requireNonNull(sessionWindows, "sessionWindows can't be null");
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");

        final Initializer<V> initializer = new Initializer<V>() {
            @Override
            public V apply() {
                return null;
            }
        };

        final Aggregator<K, V, V> aggregator = new Aggregator<K, V, V>() {
            @Override
            public V apply(final K aggKey, final V value, final V aggregate) {
                if (aggregate == null) {
                    return value;
                }
                return reducer.apply(aggregate, value);
            }
        };

        final Merger<K, V> sessionMerger = new Merger<K, V>() {
            @Override
            public V apply(final K aggKey, final V aggOne, final V aggTwo) {
                return aggregator.apply(aggKey, aggTwo, aggOne);
            }
        };

        return aggregate(initializer, aggregator, sessionMerger, sessionWindows, valSerde, storeSupplier);
    }

    private <T> KTable<K, T> doAggregate(
            final KStreamAggProcessorSupplier<K, ?, V, T> aggregateSupplier,
            final String functionName,
            final StateStoreSupplier storeSupplier) {

        final String aggFunctionName = topology.newName(functionName);

        final String sourceName = repartitionIfRequired(storeSupplier.name());

        topology.addProcessor(aggFunctionName, aggregateSupplier, sourceName);
        topology.addStateStore(storeSupplier, aggFunctionName);

        return new KTableImpl<>(topology,
                aggFunctionName,
                aggregateSupplier,
                sourceName.equals(this.name) ? sourceNodes
                        : Collections.singleton(sourceName),
                storeSupplier.name(),
                isQueryable);
    }

    /**
     * @return the new sourceName if repartitioned. Otherwise the name of this stream
     */
    private String repartitionIfRequired(final String queryableStoreName) {
        if (!repartitionRequired) {
            return this.name;
        }
        return KStreamImpl.createReparitionedSource(this, keySerde, valSerde, queryableStoreName);
    }
}
