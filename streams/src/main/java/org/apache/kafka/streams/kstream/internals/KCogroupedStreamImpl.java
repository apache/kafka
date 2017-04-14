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

import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KCogroupedStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.StateStoreSupplier;

public class KCogroupedStreamImpl<K, V> extends AbstractStream<K> implements KCogroupedStream<K, V> {

    private static final String COGROUP_AGGREGATE_NAME = "KSTREAM-COGROUP-AGGREGATE-";
    private static final String COGROUP_MERGER_NAME = "KSTREAM-COGROUP-MERGER-";

    private final StateStoreSupplier storeSupplier;
    private final Initializer<V> initializer;
    private Map<KGroupedStream<K, ?>, KStreamAggregate<K, ?, V>> cogroups;

    <T> KCogroupedStreamImpl(final KStreamBuilder topology,
                             final KGroupedStream<K, T> groupedStream,
                             final Initializer<V> initializer,
                             final Aggregator<K, T, V> aggregator,
                             final StateStoreSupplier storeSupplier) {
        super(topology, topology.newName(COGROUP_MERGER_NAME), new HashSet<String>());
        this.initializer = initializer;
        this.storeSupplier = storeSupplier;
        this.cogroups = new HashMap<>();
        cogroup(groupedStream, aggregator);
    }

    @Override
    public <T> KCogroupedStream<K, V> cogroup(final KGroupedStream<K, T> groupedStream,
                                              final Aggregator<? super K, ? super T, V> aggregator) {
        cogroups.put(groupedStream, new KStreamAggregate<>(storeSupplier.name(), initializer, aggregator));
        return this;
    }

    @Override
    public KTable<K, V> aggregate() {
        final List<String> processorNames = new ArrayList<>();
        for (final Map.Entry<KGroupedStream<K, ?>, KStreamAggregate<K, ?, V>> cogroup : cogroups.entrySet()) {
            final KGroupedStreamImpl<K, ?> groupedStream = (KGroupedStreamImpl<K, ?>) cogroup.getKey();
            final String processorName = topology.newName(COGROUP_AGGREGATE_NAME);
            processorNames.add(processorName);

            final String sourceName = groupedStream.repartitionIfRequired(storeSupplier.name());
            if (sourceName.equals(groupedStream.name)) {
                sourceNodes.addAll(groupedStream.sourceNodes);
            } else {
                sourceNodes.add(sourceName);
            }

            topology.addProcessor(processorName, cogroup.getValue(), sourceName);
        }
        final KStreamCogroup<K, V> cogroup = new KStreamCogroup<>(storeSupplier.name(), cogroups.values().toArray(new KStreamAggregate[0]));
        topology.addProcessor(name, cogroup, processorNames.toArray(new String[0]));

        processorNames.add(name);
        topology.addStateStore(storeSupplier, processorNames.toArray(new String[0]));
        return new KTableImpl<>(topology, name, cogroup, sourceNodes, storeSupplier.name());
    }
}
