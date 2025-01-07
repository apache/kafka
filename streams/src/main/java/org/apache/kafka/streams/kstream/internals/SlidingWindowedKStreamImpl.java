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
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.streams.kstream.internals.KGroupedStreamImpl.AGGREGATE_NAME;
import static org.apache.kafka.streams.kstream.internals.KGroupedStreamImpl.REDUCE_NAME;

public class SlidingWindowedKStreamImpl<K, V> extends AbstractStream<K, V> implements TimeWindowedKStream<K, V> {
    private final SlidingWindows windows;
    private final GroupedStreamAggregateBuilder<K, V> aggregateBuilder;
    private EmitStrategy emitStrategy = EmitStrategy.onWindowUpdate();

    SlidingWindowedKStreamImpl(final SlidingWindows windows,
                               final InternalStreamsBuilder builder,
                               final Set<String> subTopologySourceNodes,
                               final String name,
                               final Serde<K> keySerde,
                               final Serde<V> valueSerde,
                               final GroupedStreamAggregateBuilder<K, V> aggregateBuilder,
                               final GraphNode graphNode) {
        super(name, keySerde, valueSerde, subTopologySourceNodes, graphNode, builder);
        this.windows = Objects.requireNonNull(windows, "windows can't be null");
        this.aggregateBuilder = aggregateBuilder;
    }

    @Override
    public KTable<Windowed<K>, Long> count() {
        return count(NamedInternal.empty());
    }

    @Override
    public KTable<Windowed<K>, Long> count(final Named named) {
        return doCount(named, Materialized.with(keySerde, Serdes.Long()));
    }

    @Override
    public KTable<Windowed<K>, Long> count(final Materialized<K, Long, WindowStore<Bytes, byte[]>> materialized) {
        return count(NamedInternal.empty(), materialized);
    }

    @Override
    public KTable<Windowed<K>, Long> count(final Named named, final Materialized<K, Long, WindowStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        return doCount(named, materialized);
    }

    private KTable<Windowed<K>, Long> doCount(final Named named,
                                              final Materialized<K, Long, WindowStore<Bytes, byte[]>> materialized) {
        final MaterializedInternal<K, Long, WindowStore<Bytes, byte[]>> materializedInternal =
                new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null) {
            materializedInternal.withValueSerde(Serdes.Long());
        }

        final String aggregateName = new NamedInternal(named).orElseGenerateWithPrefix(builder, AGGREGATE_NAME);
        final StoreFactory storeFactory = new SlidingWindowStoreMaterializer<>(materializedInternal, windows, emitStrategy);

        return aggregateBuilder.build(
                new NamedInternal(aggregateName),
                storeFactory,
                new KStreamSlidingWindowAggregate<>(windows, storeFactory, emitStrategy, aggregateBuilder.countInitializer, aggregateBuilder.countAggregator),
                materializedInternal.queryableStoreName(),
                materializedInternal.keySerde() != null ? new FullTimeWindowedSerde<>(materializedInternal.keySerde(), windows.timeDifferenceMs()) : null,
                materializedInternal.valueSerde(),
                false);
    }

    @Override
    public <VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                                  final Aggregator<? super K, ? super V, VR> aggregator) {
        return aggregate(initializer, aggregator, Materialized.with(keySerde, null));
    }

    @Override
    public <VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                                  final Aggregator<? super K, ? super V, VR> aggregator,
                                                  final Named named) {
        return aggregate(initializer, aggregator, named, Materialized.with(keySerde, null));
    }

    @Override
    public <VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                                  final Aggregator<? super K, ? super V, VR> aggregator,
                                                  final Materialized<K, VR, WindowStore<Bytes, byte[]>> materialized) {
        return aggregate(initializer, aggregator, NamedInternal.empty(), materialized);
    }

    @Override
    public <VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                                  final Aggregator<? super K, ? super V, VR> aggregator,
                                                  final Named named,
                                                  final Materialized<K, VR, WindowStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, WindowStore<Bytes, byte[]>> materializedInternal =
                new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);
        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }
        final String aggregateName = new NamedInternal(named).orElseGenerateWithPrefix(builder, AGGREGATE_NAME);
        final StoreFactory storeFactory = new SlidingWindowStoreMaterializer<>(materializedInternal, windows, emitStrategy);

        return aggregateBuilder.build(
                new NamedInternal(aggregateName),
                storeFactory,
                new KStreamSlidingWindowAggregate<>(windows, storeFactory, emitStrategy, initializer, aggregator),
                materializedInternal.queryableStoreName(),
                materializedInternal.keySerde() != null ? new FullTimeWindowedSerde<>(materializedInternal.keySerde(), windows.timeDifferenceMs()) : null,
                materializedInternal.valueSerde(),
                false);
    }

    @Override
    public KTable<Windowed<K>, V> reduce(final Reducer<V> reducer) {
        return reduce(reducer, NamedInternal.empty());
    }

    @Override
    public KTable<Windowed<K>, V> reduce(final Reducer<V> reducer, final Named named) {
        return reduce(reducer, named, Materialized.with(keySerde, valueSerde));
    }

    @Override
    public KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                         final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized) {
        return reduce(reducer, NamedInternal.empty(), materialized);
    }

    @Override
    public KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                         final Named named,
                                         final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(reducer, "reducer can't be null");
        Objects.requireNonNull(named, "named can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, V, WindowStore<Bytes, byte[]>> materializedInternal =
                new MaterializedInternal<>(materialized, builder, REDUCE_NAME);

        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null) {
            materializedInternal.withValueSerde(valueSerde);
        }

        final String reduceName = new NamedInternal(named).orElseGenerateWithPrefix(builder, REDUCE_NAME);
        final StoreFactory storeFactory = new SlidingWindowStoreMaterializer<>(materializedInternal, windows, emitStrategy);

        return aggregateBuilder.build(
                new NamedInternal(reduceName),
                storeFactory,
                new KStreamSlidingWindowAggregate<>(windows, storeFactory, emitStrategy, aggregateBuilder.reduceInitializer, aggregatorForReducer(reducer)),
                materializedInternal.queryableStoreName(),
                materializedInternal.keySerde() != null ? new FullTimeWindowedSerde<>(materializedInternal.keySerde(), windows.timeDifferenceMs()) : null,
                materializedInternal.valueSerde(),
                false);
    }

    @Override
    public TimeWindowedKStream<K, V> emitStrategy(final EmitStrategy emitStrategy) {
        this.emitStrategy = emitStrategy;
        return this;
    }

    private Aggregator<K, V, V> aggregatorForReducer(final Reducer<V> reducer) {
        return (aggKey, value, aggregate) -> aggregate == null ? value : reducer.apply(aggregate, value);
    }
}
