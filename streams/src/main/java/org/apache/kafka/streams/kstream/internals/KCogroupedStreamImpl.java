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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreSupplier;

public class KCogroupedStreamImpl<K, V, T> extends AbstractStream<K, V> implements
    KCogroupedStream<K, V, T> {

    static final String AGGREGATE_NAME = "KCOGROUPSTREAM-AGGREGATE-";

    final private Map<KGroupedStreamImpl<K, V>, Aggregator<? super K, ? super V, T>> groupPatterns;

    KCogroupedStreamImpl(final String name,
                         final Serde<K> keySerde,
                         final Serde<V> valueSerde,
                         final Set<String> sourceNodes,
                         final StreamsGraphNode streamsGraphNode,
                         final InternalStreamsBuilder builder) {
        super(name, keySerde, valueSerde, sourceNodes, streamsGraphNode, builder);
        this.groupPatterns = new HashMap<>();
    }

    @Override
    public KCogroupedStream<K, V, T> cogroup(final KGroupedStream<K, V> groupedStream,
                                             final Aggregator<? super K, ? super V, T> aggregator) {
        Objects.requireNonNull(groupedStream, "groupedStream can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        groupPatterns.put((KGroupedStreamImpl<K, V>) groupedStream, aggregator);
        return this;
    }

    @Override
    public KTable<K, T> aggregate(final Initializer<T> initializer,
                                  final Materialized<K, T, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final NamedInternal named = NamedInternal.empty();
        return doAggregate(initializer, named,
                           new MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>>(
                               materialized, builder,
                               AGGREGATE_NAME));
    }

    @Override
    public KTable<K, T> aggregate(final Initializer<T> initializer,
                                  final StoreSupplier<KeyValueStore> storeSupplier) {
        return aggregate(initializer, Materialized.as(storeSupplier.get().name()));
    }

    //TODO: implement windowed stores
    @Override
    public KTable<Windowed<K>, T> aggregate(final Initializer initializer,
                                            final Merger sessionMerger,
                                            final SessionWindows sessionWindows,
                                            final Materialized materialized) {
        return null;
    }

    //TODO: implement windowed stores
    @Override
    public KTable<Windowed<K>, T> aggregate(final Initializer initializer,
                                            final Merger sessionMerger,
                                            final SessionWindows sessionWindows,
                                            final StoreSupplier storeSupplier) {
        return null;
    }

    //TODO: implement windowed stores
    @Override
    public KTable<Windowed<K>, T> aggregate(final Initializer initializer, final Windows windows,
                                            final Materialized materialized) {
        return null;
    }

    //TODO: implement windowed stores
    @Override
    public KTable<Windowed<K>, T> aggregate(final Initializer initializer, final Windows windows,
                                            final StoreSupplier storeSupplier) {
        return null;
    }

    private KTable<K, T> doAggregate(final Initializer<T> initializer,
                                     final NamedInternal named,
                                     final MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materializedInternal) {
        final Collection<StreamsGraphNode> processors = new ArrayList<>();
        boolean stateCreated = false;
        for (final Entry<KGroupedStreamImpl<K, V>, Aggregator<? super K, ? super V, T>> kGroupedStream : groupPatterns
            .entrySet()) {

            final String functionName = new NamedInternal(named)
                .orElseGenerateWithPrefix(builder, AGGREGATE_NAME);
            final Aggregator<? super K, ? super V, T> aggregator = kGroupedStream.getValue();
            final KStreamAggregate<K, V, T> kStreamAggregate = new KStreamAggregate<K, V, T>(
                materializedInternal.storeName(), initializer, aggregator);
            final StatefulProcessorNode<K, V> statefulProcessorNode;
            //TODO: improve
            if (!stateCreated) {
                statefulProcessorNode =
                    new StatefulProcessorNode<>(
                        functionName,
                        new ProcessorParameters<>(kStreamAggregate, functionName),
                        new TimestampedKeyValueStoreMaterializer<>(materializedInternal)
                            .materialize()
                    );
                stateCreated = true;
            } else {
                statefulProcessorNode =
                    new StatefulProcessorNode<>(
                        functionName,
                        new ProcessorParameters<>(kStreamAggregate, functionName),
                        new String[]{materializedInternal.storeName()}
                    );
            }
            processors.add(statefulProcessorNode);
            builder.addGraphNode(kGroupedStream.getKey().streamsGraphNode, statefulProcessorNode);
        }
        final String functionName = new NamedInternal(named)
            .orElseGenerateWithPrefix(builder, AGGREGATE_NAME);
        final KTableSource<K, V> tableSource = new KTableSource<>(
            materializedInternal.storeName(),
            materializedInternal
                .queryableStoreName());
        final StatefulProcessorNode<K, V> tableSourceNode =
            new StatefulProcessorNode<>(
                functionName,
                new ProcessorParameters<>(tableSource, functionName),
                new String[]{materializedInternal.storeName()}
            );

        builder.addGraphNode(processors, tableSourceNode);

        return new KTableImpl<K, V, T>(
            functionName,
            materializedInternal.keySerde(),
            materializedInternal.valueSerde(),
            Collections.singleton(tableSourceNode.nodeName()),
            materializedInternal.queryableStoreName(),
            tableSource,
            tableSourceNode,
            builder);
    }
}
