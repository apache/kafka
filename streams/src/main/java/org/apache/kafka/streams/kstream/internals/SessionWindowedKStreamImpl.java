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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.streams.kstream.internals.KGroupedStreamImpl.AGGREGATE_NAME;

public class SessionWindowedKStreamImpl<K, V> extends AbstractStream<K> implements SessionWindowedKStream<K, V> {
    private final SessionWindows windows;
    private final Serde<K> keySerde;
    private final Serde<V> valSerde;
    private final GroupedStreamAggregateBuilder<K, V> aggregateBuilder;
    private final Merger<K, Long> countMerger = new Merger<K, Long>() {
        @Override
        public Long apply(final K aggKey, final Long aggOne, final Long aggTwo) {
            return aggOne + aggTwo;
        }
    };
    private final Initializer<V> reduceInitializer = new Initializer<V>() {
        @Override
        public V apply() {
            return null;
        }
    };


    SessionWindowedKStreamImpl(final SessionWindows windows,
                               final InternalStreamsBuilder builder,
                               final Set<String> sourceNodes,
                               final String name,
                               final Serde<K> keySerde,
                               final Serde<V> valSerde,
                               final GroupedStreamAggregateBuilder<K, V> aggregateBuilder) {
        super(builder, name, sourceNodes);
        this.windows = windows;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        this.aggregateBuilder = aggregateBuilder;
    }

    @Override
    public KTable<Windowed<K>, Long> count() {
        return doAggregate(aggregateBuilder.countInitializer,
                           aggregateBuilder.countAggregator,
                           countMerger,
                           Serdes.Long());
    }

    @Override
    public KTable<Windowed<K>, Long> count(final Materialized<K, Long, SessionStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, Long, SessionStore<Bytes, byte[]>> materializedInternal
                = new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);
        if (materializedInternal.valueSerde() == null) {
            materialized.withValueSerde(Serdes.Long());
        }
        return aggregate(aggregateBuilder.countInitializer,
                         aggregateBuilder.countAggregator,
                         countMerger,
                         materialized);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                                final Aggregator<? super K, ? super V, T> aggregator,
                                                final Merger<? super K, T> sessionMerger) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        Objects.requireNonNull(sessionMerger, "sessionMerger can't be null");
        return doAggregate(initializer, aggregator, sessionMerger, (Serde<T>) valSerde);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                                  final Aggregator<? super K, ? super V, VR> aggregator,
                                                  final Merger<? super K, VR> sessionMerger,
                                                  final Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        Objects.requireNonNull(sessionMerger, "sessionMerger can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, SessionStore<Bytes, byte[]>> materializedInternal
                = new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);
        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }
        return (KTable<Windowed<K>, VR>) aggregateBuilder.build(
                new KStreamSessionWindowAggregate<>(windows, materializedInternal.storeName(), initializer, aggregator, sessionMerger),
                AGGREGATE_NAME,
                materialize(materializedInternal),
                true);
    }

    @Override
    public KTable<Windowed<K>, V> reduce(final Reducer<V> reducer) {
        Objects.requireNonNull(reducer, "reducer can't be null");
        return doAggregate(reduceInitializer, aggregatorForReducer(reducer), mergerForAggregator(aggregatorForReducer(reducer)), valSerde);
    }

    @Override
    public KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                         final Materialized<K, V, SessionStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(reducer, "reducer can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final Aggregator<K, V, V> reduceAggregator = aggregatorForReducer(reducer);
        return aggregate(reduceInitializer, reduceAggregator, mergerForAggregator(reduceAggregator), materialized);
    }


    private <VR> StoreBuilder<SessionStore<K, VR>> materialize(final MaterializedInternal<K, VR, SessionStore<Bytes, byte[]>> materialized) {
        SessionBytesStoreSupplier supplier = (SessionBytesStoreSupplier) materialized.storeSupplier();
        if (supplier == null) {
            supplier = Stores.persistentSessionStore(materialized.storeName(),
                                                     windows.maintainMs());
        }
        final StoreBuilder<SessionStore<K, VR>> builder = Stores.sessionStoreBuilder(supplier,
                                                                                     materialized.keySerde(),
                                                                                     materialized.valueSerde());

        if (materialized.loggingEnabled()) {
            builder.withLoggingEnabled(materialized.logConfig());
        } else {
            builder.withLoggingDisabled();
        }

        if (materialized.cachingEnabled()) {
            builder.withCachingEnabled();
        }
        return builder;
    }

    private Merger<K, V> mergerForAggregator(final Aggregator<K, V, V> aggregator) {
        return new Merger<K, V>() {
            @Override
            public V apply(final K aggKey, final V aggOne, final V aggTwo) {
                return aggregator.apply(aggKey, aggTwo, aggOne);
            }
        };
    }

    private Aggregator<K, V, V> aggregatorForReducer(final Reducer<V> reducer) {
        return new Aggregator<K, V, V>() {
            @Override
            public V apply(final K aggKey, final V value, final V aggregate) {
                if (aggregate == null) {
                    return value;
                }
                return reducer.apply(aggregate, value);
            }
        };
    }

    private <VR> StoreBuilder<SessionStore<K, VR>> storeBuilder(final String storeName, final Serde<VR> aggValueSerde) {
        return Stores.sessionStoreBuilder(
                Stores.persistentSessionStore(
                        storeName,
                        windows.maintainMs()),
                keySerde,
                aggValueSerde).withCachingEnabled();
    }


    @SuppressWarnings("unchecked")
    private <VR> KTable<Windowed<K>, VR> doAggregate(final Initializer<VR> initializer,
                                                     final Aggregator<? super K, ? super V, VR> aggregator,
                                                     final Merger<? super K, VR> merger,
                                                     final Serde<VR> serde) {
        final String storeName = builder.newStoreName(AGGREGATE_NAME);
        return (KTable<Windowed<K>, VR>) aggregateBuilder.build(new KStreamSessionWindowAggregate<>(windows, storeName, initializer, aggregator, merger),
                                                                AGGREGATE_NAME,
                                                                storeBuilder(storeName, serde),
                                                                false);
    }
}
