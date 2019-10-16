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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.state.KeyValueStore;

public class CogroupedKStreamImpl<K, Vout> extends AbstractStream<K, Vout> implements CogroupedKStream<K, Vout> {

    private static final String AGGREGATE_NAME = "COGROUPKSTREAM-AGGREGATE-";

    final private Map<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, Vout>> groupPatterns;
    final private CogroupedStreamAggregateBuilder<K, Vout> aggregateBuilder;

    CogroupedKStreamImpl(final String name,
                         final Set<String> sourceNodes,
                         final StreamsGraphNode streamsGraphNode,
                         final InternalStreamsBuilder builder) {
        super(name, null, null, sourceNodes, streamsGraphNode, builder);
        this.groupPatterns = new HashMap<>();
        this.aggregateBuilder = new CogroupedStreamAggregateBuilder<>(builder);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Vin> CogroupedKStream<K, Vout> cogroup(final KGroupedStream<K, Vin> groupedStream,
                                                   final Aggregator<? super K, ? super Vin, Vout> aggregator) {
        Objects.requireNonNull(groupedStream, "groupedStream can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        groupPatterns.put((KGroupedStreamImpl<K, ?>) groupedStream, (Aggregator<? super K, ? super Object, Vout>) aggregator);
        return this;
    }

    @Override
    public KTable<K, Vout> aggregate(final Initializer<Vout> initializer,
                                     final Materialized<K, Vout, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final NamedInternal named = NamedInternal.empty();
        return doAggregate(initializer, named,
                new MaterializedInternal<>(
                        materialized, builder,
                        AGGREGATE_NAME));
    }

    @Override
    public KTable<K, Vout> aggregate(final Initializer<Vout> initializer) {
        return aggregate(initializer, Materialized.with(keySerde, null));
    }


    private KTable<K, Vout> doAggregate(final Initializer<Vout> initializer,
                                        final NamedInternal named,
                                        final MaterializedInternal<K, Vout, KeyValueStore<Bytes, byte[]>> materializedInternal) {
        return this.aggregateBuilder.build(groupPatterns,
                initializer,
                named,
                new TimestampedKeyValueStoreMaterializer<>(
                        materializedInternal).materialize(),
                materializedInternal.keySerde(),
                materializedInternal.valueSerde(),
                null,
                null,
                null);
    }
}