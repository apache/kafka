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
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SlidingWindowedCogroupedKStreamImpl<K, V> extends AbstractStream<K, V> implements TimeWindowedCogroupedKStream<K, V> {
    private final SlidingWindows windows;
    private final CogroupedStreamAggregateBuilder<K, V> aggregateBuilder;
    private final Map<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, V>> groupPatterns;

    SlidingWindowedCogroupedKStreamImpl(final SlidingWindows windows,
                                        final InternalStreamsBuilder builder,
                                        final Set<String> subTopologySourceNodes,
                                        final String name,
                                        final CogroupedStreamAggregateBuilder<K, V> aggregateBuilder,
                                        final GraphNode graphNode,
                                        final Map<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, V>> groupPatterns) {
        super(name, null, null, subTopologySourceNodes, graphNode, builder);
        //keySerde and valueSerde are null because there are many different groupStreams that they could be from
        this.windows = windows;
        this.aggregateBuilder = aggregateBuilder;
        this.groupPatterns = groupPatterns;
    }

    @Override
    public KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer) {
        return aggregate(initializer, Materialized.with(null, null));
    }

    @Override
    public KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                            final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized) {
        return aggregate(initializer, NamedInternal.empty(), materialized);
    }

    @Override
    public KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                            final Named named) {
        return aggregate(initializer, named, Materialized.with(null, null));
    }

    @Override
    public KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                            final Named named,
                                            final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(named, "named can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, V, WindowStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(
            materialized,
            builder,
            CogroupedKStreamImpl.AGGREGATE_NAME);
        return aggregateBuilder.build(
            groupPatterns,
            initializer,
            new NamedInternal(named),
            new SlidingWindowStoreMaterializer<>(materializedInternal, windows, EmitStrategy.onWindowUpdate()),
            materializedInternal.keySerde() != null ?
                new FullTimeWindowedSerde<>(materializedInternal.keySerde(), windows.timeDifferenceMs())
                : null,
            materializedInternal.valueSerde(),
            materializedInternal.queryableStoreName(),
            windows);
    }

}
