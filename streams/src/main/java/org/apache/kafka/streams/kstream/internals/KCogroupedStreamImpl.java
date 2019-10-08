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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.KCogroupedStream;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindowedKCogroupedStream;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreSupplier;

public class KCogroupedStreamImpl<K, T, V> extends AbstractStream<K, V> implements
    KCogroupedStream<K, T, V> {

    static final String AGGREGATE_NAME = "KCOGROUPSTREAM-AGGREGATE-";

    final private Map<KGroupedStreamImpl<K, T>, Aggregator<? super K, ? super T, V>> groupPatterns;
    final private CogroupedStreamAggregateBuilder<K, V> aggregateBuilder;

    KCogroupedStreamImpl(final String name,
                         final Serde<K> keySerde,
                         final Serde<V> valueSerde,
                         final Set<String> sourceNodes,
                         final StreamsGraphNode streamsGraphNode,
                         final InternalStreamsBuilder builder) {
        super(name, keySerde, valueSerde, sourceNodes, streamsGraphNode, builder);
        this.groupPatterns = new HashMap<>();
        this.aggregateBuilder = new CogroupedStreamAggregateBuilder<>(builder);
    }

    @Override
    public KCogroupedStream<K, T, V> cogroup(final KGroupedStream<K, T> groupedStream,
                                             final Aggregator<? super K, ? super T, V> aggregator) {
        Objects.requireNonNull(groupedStream, "groupedStream can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        groupPatterns.put((KGroupedStreamImpl<K, T>) groupedStream, aggregator);
        return this;
    }

    @Override
    public KTable<K, V> aggregate(final Initializer<V> initializer,
                                  final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final NamedInternal named = NamedInternal.empty();
        return doAggregate(initializer, named,
                           new MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>>(
                               materialized, builder,
                               AGGREGATE_NAME));
    }

    @Override
    public KTable<K, V> aggregate(final Initializer<V> initializer,
                                  final StoreSupplier<KeyValueStore> storeSupplier) {
        return aggregate(initializer, Materialized.as(storeSupplier.get().name()));
    }

    @Override
    public <W extends Window> TimeWindowedKCogroupedStream<K, V> windowedBy(
        final Windows<W> windows) {
        Objects.requireNonNull(windows, "windows can't be null");
        return new TimeWindowedKCogroupedStreamImpl<K, T, V, W>(windows,
                                                                builder,
                                                                sourceNodes,
                                                                name,
                                                                keySerde,
                                                                valSerde,
                                                                aggregateBuilder,
                                                                streamsGraphNode,
                                                                groupPatterns);
    }


    private KTable<K, V> doAggregate(final Initializer<V> initializer,
                                     final NamedInternal named,
                                     final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal) {
        return this.aggregateBuilder.build(groupPatterns,
                                           initializer,
                                           named,
                                           new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize(),
                                           materializedInternal.keySerde(),
                                           materializedInternal.valueSerde(),
                                           null);
    }
}
