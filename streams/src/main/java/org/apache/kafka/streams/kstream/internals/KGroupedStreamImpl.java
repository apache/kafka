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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;
import java.util.Set;

class KGroupedStreamImpl<K, V> extends AbstractStream<K, V> implements KGroupedStream<K, V> {

    static final String REDUCE_NAME = "KSTREAM-REDUCE-";
    static final String AGGREGATE_NAME = "KSTREAM-AGGREGATE-";

    private final GroupedStreamAggregateBuilder<K, V> aggregateBuilder;

    KGroupedStreamImpl(final String name,
                       final Set<String> sourceNodes,
                       final GroupedInternal<K, V> groupedInternal,
                       final boolean repartitionRequired,
                       final StreamsGraphNode streamsGraphNode,
                       final InternalStreamsBuilder builder) {
        super(name, groupedInternal.keySerde(), groupedInternal.valueSerde(), sourceNodes, streamsGraphNode, builder);
        this.aggregateBuilder = new GroupedStreamAggregateBuilder<>(
            builder,
            groupedInternal,
            repartitionRequired,
            sourceNodes,
            name,
            streamsGraphNode
        );
    }

    @Override
    public KTable<K, V> reduce(final Reducer<V> reducer) {
        return reduce(reducer, Materialized.with(keySerde, valSerde));
    }

    @Override
    public KTable<K, V> reduce(final Reducer<V> reducer,
                               final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(reducer, "reducer can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, REDUCE_NAME);

        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null) {
            materializedInternal.withValueSerde(valSerde);
        }

        return doAggregate(
            new KStreamReduce<>(materializedInternal.storeName(), reducer),
            REDUCE_NAME,
            materializedInternal
        );
    }

    @Override
    public <VR> KTable<K, VR> aggregate(final Initializer<VR> initializer,
                                        final Aggregator<? super K, ? super V, VR> aggregator,
                                        final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }

        return doAggregate(
            new KStreamAggregate<>(materializedInternal.storeName(), initializer, aggregator),
            AGGREGATE_NAME,
            materializedInternal
        );
    }

    @Override
    public <VR> KTable<K, VR> aggregate(final Initializer<VR> initializer,
                                        final Aggregator<? super K, ? super V, VR> aggregator) {
        return aggregate(initializer, aggregator, Materialized.with(keySerde, null));
    }

    @Override
    public KTable<K, Long> count() {
        return doCount(Materialized.with(keySerde, Serdes.Long()));
    }

    @Override
    public KTable<K, Long> count(final Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(materialized, "materialized can't be null");

        // TODO: remove this when we do a topology-incompatible release
        // we used to burn a topology name here, so we have to keep doing it for compatibility
        if (new MaterializedInternal<>(materialized).storeName() == null) {
            builder.newStoreName(AGGREGATE_NAME);
        }

        return doCount(materialized);
    }

    private KTable<K, Long> doCount(final Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized) {
        final MaterializedInternal<K, Long, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null) {
            materializedInternal.withValueSerde(Serdes.Long());
        }

        return doAggregate(
            new KStreamAggregate<>(materializedInternal.storeName(), aggregateBuilder.countInitializer, aggregateBuilder.countAggregator),
            AGGREGATE_NAME,
            materializedInternal);
    }

    @Override
    public <W extends Window> TimeWindowedKStream<K, V> windowedBy(final Windows<W> windows) {

        return new TimeWindowedKStreamImpl<>(
            windows,
            builder,
            sourceNodes,
            name,
            keySerde,
            valSerde,
            aggregateBuilder,
            streamsGraphNode
        );
    }

    @Override
    public SessionWindowedKStream<K, V> windowedBy(final SessionWindows windows) {

        return new SessionWindowedKStreamImpl<>(
            windows,
            builder,
            sourceNodes,
            name,
            keySerde,
            valSerde,
            aggregateBuilder,
            streamsGraphNode
        );
    }

    private <T> KTable<K, T> doAggregate(final KStreamAggProcessorSupplier<K, K, V, T> aggregateSupplier,
                                         final String functionName,
                                         final MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materializedInternal) {
        return aggregateBuilder.build(
            functionName,
            new KeyValueStoreMaterializer<>(materializedInternal).materialize(),
            aggregateSupplier,
            materializedInternal.queryableStoreName(),
            materializedInternal.keySerde(),
            materializedInternal.valueSerde());
    }
}
