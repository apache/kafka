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
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import java.util.Collections;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;

class CogroupedStreamAggregateBuilder<K, V> {

    static final String AGGREGATE_NAME = "KCOGROUPSTREAM-AGGREGATE-";

    private final InternalStreamsBuilder builder;

    CogroupedStreamAggregateBuilder(final InternalStreamsBuilder builder) {
        this.builder = builder;
    }

    <KR, T, W extends Window> KTable<KR, V> build(
        final Map<KGroupedStreamImpl<K, T>, Aggregator<? super K, ? super T, V>> groupPatterns,
        final Initializer<V> initializer,
        final NamedInternal named,
        final StoreBuilder<? extends StateStore> storeBuilder,
        final Serde<KR> keySerde,
        final Serde<V> valSerde,
        final Windows<W> windows) {

        final Collection<StreamsGraphNode> processors = new ArrayList<>();
        boolean stateCreated = false;
        for (final Entry<KGroupedStreamImpl<K, T>, Aggregator<? super K, ? super T, V>> kGroupedStream : groupPatterns
            .entrySet()) {
            final StatefulProcessorNode statefulProcessorNode = getStatefulProcessorNode(
                kGroupedStream.getValue(), initializer, named, stateCreated, storeBuilder, windows);
            stateCreated = true;
            processors.add(statefulProcessorNode);
            builder.addGraphNode(kGroupedStream.getKey().streamsGraphNode, statefulProcessorNode);
        }
        final String functionName = named.orElseGenerateWithPrefix(builder, AGGREGATE_NAME);
        final ProcessorSupplier<K, V> tableSource = windows == null ? new KTableSource<>(
            storeBuilder.name(),
            storeBuilder.name()) :
            new KStreamWindowTableSource<>();
        final StatefulProcessorNode<K, V> tableSourceNode =
            new StatefulProcessorNode<>(
                functionName,
                new ProcessorParameters<>(tableSource, functionName),
                new String[]{storeBuilder.name()}
            );

        builder.addGraphNode(processors, tableSourceNode);

        return new KTableImpl<KR, T, V>(
            functionName,
            keySerde,
            valSerde,
            Collections.singleton(tableSourceNode.nodeName()),
            storeBuilder.name(),
            tableSource,
            tableSourceNode,
            builder);

    }

    private <T, W extends Window> StatefulProcessorNode getStatefulProcessorNode(
        final Aggregator<? super K, ? super T, V> aggregator,
        final Initializer<V> initializer, final NamedInternal named, final boolean stateCreated,
        final StoreBuilder<? extends StateStore> storeBuilder, final Windows<W> windows) {

        final String functionName = named.orElseGenerateWithPrefix(builder, AGGREGATE_NAME);

        final ProcessorSupplier<K, T> kStreamAggregate = windows == null ?  new KStreamAggregate<K, T, V>(storeBuilder.name(), initializer, aggregator) :
                new KStreamWindowAggregate<K, T, V, W>(windows, storeBuilder.name(), initializer, aggregator);

        final StatefulProcessorNode statefulProcessorNode;
        if (!stateCreated) {
            statefulProcessorNode =
                new StatefulProcessorNode<>(
                    functionName,
                    new ProcessorParameters<>(kStreamAggregate, functionName),
                    storeBuilder
                );
        } else {
            statefulProcessorNode =
                new StatefulProcessorNode<>(
                    functionName,
                    new ProcessorParameters<>(kStreamAggregate, functionName),
                    new String[]{storeBuilder.name()}
                );
        }
        return statefulProcessorNode;
    }


}
