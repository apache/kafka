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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

class CogroupedKStreamImpl<K, V> implements CogroupedKStream<K, V> {

    private static final String COGROUP_AGGREGATE_NAME = "KSTREAM-COGROUP-AGGREGATE-";
    private static final String COGROUP_NAME = "KSTREAM-COGROUP-";
    private static enum AggregateType {
        AGGREGATE,
        SESSION_WINDOW_AGGREGATE,
        WINDOW_AGGREGATE
    }

    private final KStreamBuilder topology;
    private final Serde<K> keySerde;
    private final Map<KGroupedStream, Aggregator> pairs = new HashMap<>();
    private final Map<KGroupedStreamImpl, String> repartitionNames = new HashMap<>();

    <T> CogroupedKStreamImpl(final KStreamBuilder topology,
                         final KGroupedStream<K, T> groupedStream,
                         final Serde<K> keySerde,
                         final Aggregator<? super K, ? super T, V> aggregator) {
        this.topology = topology;
        this.keySerde = keySerde;
        cogroup(groupedStream, aggregator);
    }

    @Override
    public <T> CogroupedKStream<K, V> cogroup(final KGroupedStream<K, T> groupedStream,
                                              final Aggregator<? super K, ? super T, V> aggregator) {
        Objects.requireNonNull(groupedStream, "groupedStream can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        pairs.put(groupedStream, aggregator);
        return this;
    }

    @Override
    public KTable<K, V> aggregate(final Initializer<V> initializer,
                                  final Serde<V> valueSerde,
                                  final String storeName) {
        return aggregate(initializer, AbstractStream.keyValueStore(keySerde, valueSerde, storeName));
    }

    @Override
    public KTable<K, V> aggregate(final Initializer<V> initializer,
                                  final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doAggregate(AggregateType.AGGREGATE, initializer, storeSupplier, null, null, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                            final Merger<? super K, V> sessionMerger,
                                            final SessionWindows sessionWindows,
                                            final Serde<V> valueSerde,
                                            final String storeName) {
        Objects.requireNonNull(storeName, "storeName can't be null");
        Topic.validate(storeName);
        return aggregate(initializer, sessionMerger, sessionWindows, AbstractStream.storeFactory(keySerde, valueSerde, storeName).sessionWindowed(sessionWindows.maintainMs()).build());
    }

    @SuppressWarnings("unchecked")
    @Override
    public KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                            final Merger<? super K, V> sessionMerger,
                                            final SessionWindows sessionWindows,
                                            final StateStoreSupplier<SessionStore> storeSupplier) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(sessionMerger, "sessionMerger can't be null");
        Objects.requireNonNull(sessionWindows, "sessionWindows can't be null");
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return (KTable<Windowed<K>, V>) doAggregate(AggregateType.SESSION_WINDOW_AGGREGATE, initializer, storeSupplier, sessionMerger, sessionWindows, null);
    }

    @Override
    public <W extends Window> KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                                               final Windows<W> windows,
                                                               final Serde<V> valueSerde,
                                                               final String storeName) {
        return aggregate(initializer, windows, AbstractStream.windowedStore(keySerde, valueSerde, windows, storeName));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <W extends Window> KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                                               final Windows<W> windows,
                                                               final StateStoreSupplier<WindowStore> storeSupplier) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(windows, "windows can't be null");
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return (KTable<Windowed<K>, V>) doAggregate(AggregateType.WINDOW_AGGREGATE, initializer, storeSupplier, null, null, windows);
    }

    @SuppressWarnings("unchecked")
    private <W extends Window> KTable<K, V> doAggregate(final AggregateType aggregateType,
                                                        final Initializer<V> initializer,
                                                        final StateStoreSupplier storeSupplier,
                                                        final Merger<? super K, V> sessionMerger,
                                                        final SessionWindows sessionWindows,
                                                        final Windows<W> windows) {
        final Set<String> sourceNodes = new HashSet<>();
        final Collection<KStreamAggProcessorSupplier> processors = new ArrayList<>();
        final List<String> processorNames = new ArrayList<>();
        System.out.println("Starting adding.");
        for (final Map.Entry<KGroupedStream, Aggregator> pair : pairs.entrySet()) {
            final KGroupedStreamImpl groupedStream = (KGroupedStreamImpl) pair.getKey();
            final String sourceName = repartitionIfRequired(groupedStream);
            if (sourceName.equals(groupedStream.name)) {
                sourceNodes.addAll(groupedStream.sourceNodes);
            } else {
                sourceNodes.add(sourceName);
            }

            final KStreamAggProcessorSupplier processor;
            switch (aggregateType) {
                case AGGREGATE:
                    processor = new KStreamAggregate(storeSupplier.name(), initializer, pair.getValue());
                    break;
                case SESSION_WINDOW_AGGREGATE:
                    processor = new KStreamSessionWindowAggregate(sessionWindows, storeSupplier.name(), initializer, pair.getValue(), sessionMerger);
                    break;
                case WINDOW_AGGREGATE:
                    processor = new KStreamWindowAggregate(windows, storeSupplier.name(), initializer, pair.getValue());
                    break;
                default:
                    throw new IllegalStateException("Unrecognized AggregateType.");
            }
            processors.add(processor);
            
            final String processorName = topology.newName(COGROUP_AGGREGATE_NAME);
            processorNames.add(processorName);
            topology.addSource(sourceName, groupedStream.builder.internalTopologyBuilder.sourceTopicPattern());
            System.out.println("Corresponding sourceTopicPattern: " + groupedStream.builder.internalTopologyBuilder.sourceTopicPattern().toString());
            topology.addProcessor(processorName, processor, sourceName);
        }
        final String name = topology.newName(COGROUP_NAME);
        System.out.println("Finishing Name: " + name);
        final KStreamCogroup cogroup = new KStreamCogroup(processors);
        final String[] processorNamesArray = processorNames.toArray(new String[processorNames.size()]);
        topology.addProcessor(name, cogroup, processorNamesArray);
        topology.addStateStore(storeSupplier, processorNamesArray);
        topology.copartitionSources(sourceNodes);
        return new KTableImpl<K, String, V>(topology.internalStreamsBuilder(), name, cogroup, sourceNodes, storeSupplier.name(), true);
    }

    @SuppressWarnings("rawtypes")
    private String repartitionIfRequired(KGroupedStreamImpl groupedStream) {
        if (repartitionNames.containsKey(groupedStream)) {
            return repartitionNames.get(groupedStream);
        }
        final String sourceName = groupedStream.repartitionIfRequired(null);
        repartitionNames.put(groupedStream, sourceName);
        return sourceName;
    }
}
