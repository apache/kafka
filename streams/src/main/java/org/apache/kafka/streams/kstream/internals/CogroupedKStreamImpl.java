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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;

public class CogroupedKStreamImpl<K, VOut> extends AbstractStream<K, VOut> implements CogroupedKStream<K, VOut> {

    static final String AGGREGATE_NAME = "COGROUPKSTREAM-AGGREGATE-";
    static final String MERGE_NAME = "COGROUPKSTREAM-MERGE-";

    final private Map<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, VOut>> groupPatterns;
    final private CogroupedStreamAggregateBuilder<K, VOut> aggregateBuilder;

    CogroupedKStreamImpl(final String name,
                         final Set<String> subTopologySourceNodes,
                         final GraphNode graphNode,
                         final InternalStreamsBuilder builder) {
        super(name, null, null, subTopologySourceNodes, graphNode, builder);
        groupPatterns = new LinkedHashMap<>();
        aggregateBuilder = new CogroupedStreamAggregateBuilder<>(builder);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <VIn> CogroupedKStream<K, VOut> cogroup(final KGroupedStream<K, VIn> groupedStream,
                                                   final Aggregator<? super K, ? super VIn, VOut> aggregator) {
        Objects.requireNonNull(groupedStream, "groupedStream can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        groupPatterns.put((KGroupedStreamImpl<K, ?>) groupedStream,
                          (Aggregator<? super K, ? super Object, VOut>) aggregator);
        return this;
    }

    @Override
    public KTable<K, VOut> aggregate(final Initializer<VOut> initializer,
                                     final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        return aggregate(initializer, NamedInternal.empty(), materialized);
    }

    @Override
    public KTable<K, VOut> aggregate(final Initializer<VOut> initializer, final Named named) {
        return aggregate(initializer, named, Materialized.with(keySerde, null));
    }

    @Override
    public KTable<K, VOut> aggregate(final Initializer<VOut> initializer,
                                     final Named named,
                                     final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(named, "named can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        return doAggregate(
            initializer,
            new NamedInternal(named),
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME));
    }

    @Override
    public KTable<K, VOut> aggregate(final Initializer<VOut> initializer) {
        return aggregate(initializer, Materialized.with(keySerde, null));
    }

    @Override
    public <W extends Window> TimeWindowedCogroupedKStream<K, VOut> windowedBy(final Windows<W> windows) {
        Objects.requireNonNull(windows, "windows can't be null");
        return new TimeWindowedCogroupedKStreamImpl<>(
            windows,
            builder,
            subTopologySourceNodes,
            name,
            aggregateBuilder,
            graphNode,
            groupPatterns);
    }

    @Override
    public TimeWindowedCogroupedKStream<K, VOut> windowedBy(final SlidingWindows slidingWindows) {
        Objects.requireNonNull(slidingWindows, "slidingWindows can't be null");
        return new SlidingWindowedCogroupedKStreamImpl<>(
            slidingWindows,
            builder,
            subTopologySourceNodes,
            name,
            aggregateBuilder,
            graphNode,
            groupPatterns);
    }

    @Override
    public SessionWindowedCogroupedKStream<K, VOut> windowedBy(final SessionWindows sessionWindows) {
        Objects.requireNonNull(sessionWindows, "sessionWindows can't be null");
        return new SessionWindowedCogroupedKStreamImpl<>(sessionWindows,
            builder,
            subTopologySourceNodes,
            name,
            aggregateBuilder,
            graphNode,
            groupPatterns);
    }

    private KTable<K, VOut> doAggregate(final Initializer<VOut> initializer,
                                        final NamedInternal named,
                                        final MaterializedInternal<K, VOut, KeyValueStore<Bytes, byte[]>> materializedInternal) {
        return aggregateBuilder.build(
            groupPatterns,
            initializer,
            named,
            new KeyValueStoreMaterializer<>(materializedInternal),
            materializedInternal.keySerde(),
            materializedInternal.valueSerde(),
            materializedInternal.queryableStoreName(),
            materializedInternal.storeSupplier() instanceof VersionedBytesStoreSupplier);
    }
}