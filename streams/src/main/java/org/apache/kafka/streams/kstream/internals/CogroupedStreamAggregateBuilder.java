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
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import java.util.Collections;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;

class CogroupedStreamAggregateBuilder<K, VOut> {

    private static final String AGGREGATE_NAME = "COGROUPKSTREAM-AGGREGATE-";

    private final InternalStreamsBuilder builder;

    CogroupedStreamAggregateBuilder(final InternalStreamsBuilder builder) {
        this.builder = builder;
    }

    <KR, VIn, W extends Window> KTable<KR, VOut> build(
            final Map<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, VOut>> groupPatterns,
            final Initializer<VOut> initializer,
            final NamedInternal named,
            final StoreBuilder<? extends StateStore> storeBuilder,
            final Serde<KR> keySerde,
            final Serde<VOut> valSerde,
            final Windows<W> windows,
            final SessionWindows sessionWindows,
            final Merger<? super K, VOut> sessionMerger) {

        final Collection<? extends AbstractStream<K, ?>> groupedStreams = new ArrayList<>(groupPatterns.keySet());
        final AbstractStream<K, ?> kGrouped = groupedStreams.iterator().next();
        groupedStreams.remove(kGrouped);
        kGrouped.ensureCopartitionWith(groupedStreams);


        final Collection<StreamsGraphNode> processors = new ArrayList<>();
        boolean stateCreated = false;
        for (final Entry<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, VOut>> kGroupedStream : groupPatterns
                .entrySet()) {
            final StatefulProcessorNode statefulProcessorNode = getStatefulProcessorNode(
                    kGroupedStream.getValue(),
                    initializer,
                    named.withName(AGGREGATE_NAME + kGroupedStream.getKey().name + named.name()),
                    stateCreated,
                    storeBuilder,
                    windows,
                    sessionWindows,
                    sessionMerger);
            stateCreated = true;
            processors.add(statefulProcessorNode);
            builder.addGraphNode(kGroupedStream.getKey().streamsGraphNode, statefulProcessorNode);
        }
        final String functionName = named.orElseGenerateWithPrefix(builder, AGGREGATE_NAME);
        final ProcessorSupplier<K, VOut> tableSource = new KTableSource<>(
                        storeBuilder.name(),
                        storeBuilder.name());
        final StatefulProcessorNode<K, VOut> tableSourceNode =
                new StatefulProcessorNode<>(
                        functionName,
                        new ProcessorParameters<>(tableSource, functionName),
                        new String[]{storeBuilder.name()}
                );

        builder.addGraphNode(processors, tableSourceNode);

        return new KTableImpl<KR, VIn, VOut>(
                functionName,
                keySerde,
                valSerde,
                Collections.singleton(tableSourceNode.nodeName()),
                storeBuilder.name(),
                tableSource,
                tableSourceNode,
                builder);

    }

    private <W extends Window> StatefulProcessorNode getStatefulProcessorNode(
            final Aggregator<? super K, ? super Object, VOut> aggregator,
            final Initializer<VOut> initializer,
            final NamedInternal named,
            final boolean stateCreated,
            final StoreBuilder<? extends StateStore> storeBuilder,
            final Windows<W> windows,
            final SessionWindows sessionWindows,
            final Merger<? super K, VOut> sessionMerger) {

        final String functionName = named.orElseGenerateWithPrefix(builder, AGGREGATE_NAME);

        final ProcessorSupplier<K, ?> kStreamAggregate;

        if (windows == null && sessionWindows == null) {
            kStreamAggregate = new KStreamAggregate<>(storeBuilder.name(), initializer, aggregator);
        } else if (windows != null && sessionWindows == null) {
            kStreamAggregate = new KStreamWindowAggregate<>(windows, storeBuilder.name(), initializer, aggregator);
        } else if (windows == null && sessionMerger != null) {
            kStreamAggregate = new KStreamSessionWindowAggregate<>(sessionWindows, storeBuilder.name(), initializer, aggregator, sessionMerger);
        } else {
            throw new IllegalArgumentException(
                    "must be a TimeWindowedStream or a SessionWindowedStream");
        }

        final StatefulProcessorNode<K, ?> statefulProcessorNode;
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