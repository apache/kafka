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
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import static org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder;
import static org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode.optimizableRepartitionNodeBuilder;


import java.util.Collections;
import java.util.Set;

class CogroupedStreamAggregateBuilder<K, V, T> {

    static final String AGGREGATE_NAME = "KCOGROUPSTREAM-AGGREGATE-";

    private final InternalStreamsBuilder builder;


    CogroupedStreamAggregateBuilder(final InternalStreamsBuilder builder) {
        this.builder = builder;
    }

    KTable<K, T> build(final Map<KGroupedStreamImpl<K, V>, Aggregator<? super K, ? super V, T>> groupPatterns,
                       final Initializer<T> initializer,
                       final NamedInternal named,
                       final MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materializedInternal) {

        final Collection<StreamsGraphNode> processors = new ArrayList<>();
        boolean stateCreated = false;
        for (final Entry<KGroupedStreamImpl<K, V>, Aggregator<? super K, ? super V, T>> kGroupedStream : groupPatterns.entrySet()) {
            final StatefulProcessorNode statefulProcessorNode = getStatefulProcessorNode(
                kGroupedStream, initializer, named, stateCreated, materializedInternal);
            stateCreated = true;
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

    private StatefulProcessorNode getStatefulProcessorNode(final Entry<KGroupedStreamImpl<K, V>, Aggregator<? super K, ? super V, T>> kGroupedStream,
                                                           final Initializer<T> initializer, final NamedInternal named, final boolean stateCreated,
                                                           final MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materializedInternal) {

        final String functionName = new NamedInternal(named).orElseGenerateWithPrefix(builder, AGGREGATE_NAME);
        final Aggregator<? super K, ? super V, T> aggregator = kGroupedStream.getValue();

        //TODO: switch on stream type
        final KStreamAggregate<K, V, T> kStreamAggregate = new KStreamAggregate<K, V, T>(
            materializedInternal.storeName(), initializer, aggregator);

        StatefulProcessorNode statefulProcessorNode;
        if (!stateCreated) {
            statefulProcessorNode =
                new StatefulProcessorNode<>(
                    functionName,
                    new ProcessorParameters<>(kStreamAggregate, functionName),
                    new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize()
                );
        } else {
            statefulProcessorNode =
                new StatefulProcessorNode<>(
                    functionName,
                    new ProcessorParameters<>(kStreamAggregate, functionName),
                    new String[]{materializedInternal.storeName()}
                );
        }
        return statefulProcessorNode;
    }


}
