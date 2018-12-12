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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Collections;
import java.util.Set;

class GroupedStreamAggregateBuilder<K, V> {

    private final InternalStreamsBuilder builder;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final boolean repartitionRequired;
    private final String userName;
    private final Set<String> sourceNodes;
    private final String name;
    private final StreamsGraphNode streamsGraphNode;

    final Initializer<Long> countInitializer = () -> 0L;

    final Aggregator<K, V, Long> countAggregator = (aggKey, value, aggregate) -> aggregate + 1;

    final Initializer<V> reduceInitializer = () -> null;

    GroupedStreamAggregateBuilder(final InternalStreamsBuilder builder,
                                  final GroupedInternal<K, V> groupedInternal,
                                  final boolean repartitionRequired,
                                  final Set<String> sourceNodes,
                                  final String name,
                                  final StreamsGraphNode streamsGraphNode) {

        this.builder = builder;
        this.keySerde = groupedInternal.keySerde();
        this.valueSerde = groupedInternal.valueSerde();
        this.repartitionRequired = repartitionRequired;
        this.sourceNodes = sourceNodes;
        this.name = name;
        this.streamsGraphNode = streamsGraphNode;
        this.userName = groupedInternal.name();
    }

    <KR, VR> KTable<KR, VR> build(final String functionName,
                                  final StoreBuilder<? extends StateStore> storeBuilder,
                                  final KStreamAggProcessorSupplier<K, KR, V, VR> aggregateSupplier,
                                  final String queryableStoreName,
                                  final Serde<KR> keySerde,
                                  final Serde<VR> valSerde) {
        assert queryableStoreName == null || queryableStoreName.equals(storeBuilder.name());

        final String aggFunctionName = builder.newProcessorName(functionName);

        final OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder<K, V> repartitionNodeBuilder = OptimizableRepartitionNode.optimizableRepartitionNodeBuilder();

        final String sourceName = repartitionIfRequired(userName != null ? userName : storeBuilder.name(), repartitionNodeBuilder);

        StreamsGraphNode parentNode = streamsGraphNode;

        if (!sourceName.equals(this.name)) {
            final StreamsGraphNode repartitionNode = repartitionNodeBuilder.build();
            builder.addGraphNode(parentNode, repartitionNode);
            parentNode = repartitionNode;
        }

        final StatefulProcessorNode<K, V> statefulProcessorNode =
            new StatefulProcessorNode<>(
                aggFunctionName,
                new ProcessorParameters<>(aggregateSupplier, aggFunctionName),
                storeBuilder,
                repartitionRequired
            );

        builder.addGraphNode(parentNode, statefulProcessorNode);

        return new KTableImpl<>(aggFunctionName,
                                keySerde,
                                valSerde,
                                sourceName.equals(this.name) ? sourceNodes : Collections.singleton(sourceName),
                                queryableStoreName,
                                aggregateSupplier,
                                statefulProcessorNode,
                                builder);

    }

    /**
     * @return the new sourceName if repartitioned. Otherwise the name of this stream
     */
    private String repartitionIfRequired(final String repartitionTopicNamePrefix,
                                         final OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder) {
        if (!repartitionRequired) {
            return this.name;
        }
        // if repartition required the operation
        // captured needs to be set in the graph
        return KStreamImpl.createRepartitionedSource(builder, keySerde, valueSerde, repartitionTopicNamePrefix, optimizableRepartitionNodeBuilder);

    }
}
