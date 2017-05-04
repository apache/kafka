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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KCogroupedStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

class KCogroupedStreamImpl<K, RK, V> implements KCogroupedStream<K, RK, V> {

    private static final String COGROUP_AGGREGATE_NAME = "KSTREAM-COGROUP-AGGREGATE-";
    private static final String COGROUP_NAME = "KSTREAM-COGROUP-";
    private static enum AggregationType {
        AGGREGATE,
        SESSION_WINDOW_AGGREGATE,
        WINDOW_AGGREGATE
    }

    private final KStreamBuilder topology;
    private final Initializer<V> initializer;
    private final Merger<? super K, V> sessionMerger;
    private final SessionWindows sessionWindows;
    private final Windows<?> windows;
    private final StateStoreSupplier<?> storeSupplier;
    private final AggregationType aggregationType;
    private final Map<KGroupedStream<K, ?>, KStreamAggProcessorSupplier> cogroups = new HashMap<>();
    private boolean aggregated = false;

    <T> KCogroupedStreamImpl(final KStreamBuilder topology,
                             final KGroupedStream<K, T> groupedStream,
                             final Initializer<V> initializer,
                             final Aggregator<? super K, ? super T, V> aggregator,
                             final StateStoreSupplier<KeyValueStore> storeSupplier) {
        this.topology = topology;
        this.initializer = initializer;
        this.sessionMerger = null;
        this.sessionWindows = null;
        this.windows = null;
        this.storeSupplier = storeSupplier;
        this.aggregationType = AggregationType.AGGREGATE;
        cogroup(groupedStream, aggregator);
    }

    <T> KCogroupedStreamImpl(final KStreamBuilder topology,
                             final KGroupedStream<K, T> groupedStream,
                             final Initializer<V> initializer,
                             final Aggregator<? super K, ? super T, V> aggregator,
                             final Merger<? super K, V> sessionMerger,
                             final SessionWindows sessionWindows,
                             final StateStoreSupplier<SessionStore> storeSupplier) {
        this.topology = topology;
        this.initializer = initializer;
        this.sessionMerger = sessionMerger;
        this.sessionWindows = sessionWindows;
        this.windows = null;
        this.storeSupplier = storeSupplier;
        this.aggregationType = AggregationType.SESSION_WINDOW_AGGREGATE;
        cogroup(groupedStream, aggregator);
    }

    <W extends Window, T> KCogroupedStreamImpl(final KStreamBuilder topology,
                                               final KGroupedStream<K, T> groupedStream,
                                               final Initializer<V> initializer,
                                               final Aggregator<? super K, ? super T, V> aggregator,
                                               final Windows<W> windows,
                                               final StateStoreSupplier<WindowStore> storeSupplier) {
        this.topology = topology;
        this.initializer = initializer;
        this.sessionMerger = null;
        this.sessionWindows = null;
        this.windows = windows;
        this.storeSupplier = storeSupplier;
        this.aggregationType = AggregationType.WINDOW_AGGREGATE;
        cogroup(groupedStream, aggregator);
    }

    @Override
    public <T> KCogroupedStream<K, RK, V> cogroup(final KGroupedStream<K, T> groupedStream,
                                              final Aggregator<? super K, ? super T, V> aggregator) {
        Objects.requireNonNull(groupedStream, "groupedStream can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        if (aggregated) {
            throw new IllegalStateException("can't add additional streams after aggregate has been called");
        }
        switch (aggregationType) {
            case AGGREGATE:
                cogroups.put(groupedStream, new KStreamAggregate<>(storeSupplier.name(), initializer, aggregator));
                break;
            case SESSION_WINDOW_AGGREGATE:
                cogroups.put(groupedStream, new KStreamSessionWindowAggregate<>(sessionWindows, storeSupplier.name(), initializer, aggregator, sessionMerger));
                break;
            case WINDOW_AGGREGATE:
                cogroups.put(groupedStream, new KStreamWindowAggregate<>(windows, storeSupplier.name(), initializer, aggregator));
                break;
            default:
                throw new IllegalStateException(String.format("Unrecognized AggregationType %s", aggregationType));
        }
        return this;
    }

    @Override
    public KTable<RK, V> aggregate() {
        if (aggregated) {
            throw new IllegalStateException("can't call aggregate more than once");
        }
        aggregated = true;

        final List<String> processorNames = new ArrayList<>();
        final Set<String> sourceNodes = new HashSet<>();
        for (final Map.Entry<KGroupedStream<K, ?>, KStreamAggProcessorSupplier> cogroup : cogroups.entrySet()) {
            final KGroupedStreamImpl<K, ?> groupedStream = (KGroupedStreamImpl<K, ?>) cogroup.getKey();
            final String sourceName = groupedStream.repartitionIfRequired(null);
            if (sourceName.equals(groupedStream.name)) {
                sourceNodes.addAll(groupedStream.sourceNodes);
            } else {
                sourceNodes.add(sourceName);
            }

            final String processorName = topology.newName(COGROUP_AGGREGATE_NAME);
            processorNames.add(processorName);
            topology.addProcessor(processorName, cogroup.getValue(), sourceName);
        }
        final String name = topology.newName(COGROUP_NAME);
        final KStreamCogroup<RK, V> cogroup = new KStreamCogroup<>(cogroups.values());
        final String[] processorNamesArray = processorNames.toArray(new String[processorNames.size()]);
        topology.addProcessor(name, cogroup, processorNamesArray);
        topology.addStateStore(storeSupplier, processorNamesArray);
        topology.copartitionSources(sourceNodes);
        return new KTableImpl<>(topology, name, cogroup, sourceNodes, storeSupplier.name());
    }
}
