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
import org.apache.kafka.streams.kstream.internals.graph.GracePeriodGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;

import java.util.Collections;
import java.util.Set;

import static org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder;
import static org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode.optimizableRepartitionNodeBuilder;

class GroupedStreamAggregateBuilder<K, V> {

    private final InternalStreamsBuilder builder;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final boolean repartitionRequired;
    private final String userProvidedRepartitionTopicName;
    private final Set<String> subTopologySourceNodes;
    private final String name;
    private final GraphNode graphNode;
    private GraphNode repartitionNode;

    final Initializer<Long> countInitializer = () -> 0L;

    final Aggregator<K, V, Long> countAggregator = (aggKey, value, aggregate) -> aggregate + 1;

    final Initializer<V> reduceInitializer = () -> null;

    GroupedStreamAggregateBuilder(final InternalStreamsBuilder builder,
                                  final GroupedInternal<K, V> groupedInternal,
                                  final boolean repartitionRequired,
                                  final Set<String> subTopologySourceNodes,
                                  final String name,
                                  final GraphNode graphNode) {

        this.builder = builder;
        this.keySerde = groupedInternal.keySerde();
        this.valueSerde = groupedInternal.valueSerde();
        this.repartitionRequired = repartitionRequired;
        this.subTopologySourceNodes = subTopologySourceNodes;
        this.name = name;
        this.graphNode = graphNode;
        this.userProvidedRepartitionTopicName = groupedInternal.name();
    }

    <KR, VR> KTable<KR, VR> buildNonWindowed(final NamedInternal functionName,
                                             final String storeName,
                                             final KStreamAggProcessorSupplier<K, V, KR, VR> aggregateSupplier,
                                             final String queryableStoreName,
                                             final Serde<KR> keySerde,
                                             final Serde<VR> valueSerde,
                                             final boolean isOutputVersioned) {
        final String aggFunctionName = functionName.name();

        final ProcessorGraphNode<K, V> aggProcessorNode =
            new ProcessorGraphNode<>(
                aggFunctionName,
                new ProcessorParameters<>(aggregateSupplier, aggFunctionName)
            );

        aggProcessorNode.setOutputVersioned(isOutputVersioned);

        return build(aggFunctionName, storeName, aggregateSupplier, aggProcessorNode, queryableStoreName, keySerde, valueSerde);
    }

    <KR, VR> KTable<KR, VR> buildWindowed(final NamedInternal functionName,
                                          final String storeName,
                                          final long gracePeriod,
                                          final KStreamAggProcessorSupplier<K, V, KR, VR> aggregateSupplier,
                                          final String queryableStoreName,
                                          final Serde<KR> keySerde,
                                          final Serde<VR> valueSerde,
                                          final boolean isOutputVersioned) {
        final String aggFunctionName = functionName.name();

        final GracePeriodGraphNode<K, V> gracePeriodAggProcessorNode =
            new GracePeriodGraphNode<>(
                aggFunctionName,
                new ProcessorParameters<>(aggregateSupplier, aggFunctionName),
                gracePeriod
            );

        gracePeriodAggProcessorNode.setOutputVersioned(isOutputVersioned);

        return build(aggFunctionName, storeName, aggregateSupplier, gracePeriodAggProcessorNode, queryableStoreName, keySerde, valueSerde);
    }

    private <KR, VR> KTable<KR, VR> build(final String aggFunctionName,
                                          final String storeName,
                                          final KStreamAggProcessorSupplier<K, V, KR, VR> aggregateSupplier,
                                          final ProcessorGraphNode<K, V> aggProcessorNode,
                                          final String queryableStoreName,
                                          final Serde<KR> keySerde,
                                          final Serde<VR> valueSerde) {
        if (!(queryableStoreName == null || queryableStoreName.equals(storeName))) {
            throw new IllegalStateException(String.format("queryableStoreName should be null or equal to storeName"
                                                              + " but got storeName='%s' and queryableStoreName='%s'",
                                                          storeName, queryableStoreName));
        }

        String sourceName = this.name;
        GraphNode parentNode = graphNode;

        if (repartitionRequired) {
            final OptimizableRepartitionNodeBuilder<K, V> repartitionNodeBuilder = optimizableRepartitionNodeBuilder();

            final String repartitionTopicPrefix = userProvidedRepartitionTopicName != null ? userProvidedRepartitionTopicName : storeName;

            sourceName = createRepartitionSource(repartitionTopicPrefix, repartitionNodeBuilder, userProvidedRepartitionTopicName != null || queryableStoreName != null);

            // First time through we need to create a repartition node.
            // Any subsequent calls to GroupedStreamAggregateBuilder#build we check if
            // the user has provided a name for the repartition topic, is so we re-use
            // the existing repartition node, otherwise we create a new one.
            if (repartitionNode == null || userProvidedRepartitionTopicName == null) {
                repartitionNode = repartitionNodeBuilder.build();
            }

            builder.addGraphNode(parentNode, repartitionNode);
            parentNode = repartitionNode;
        }

        builder.addGraphNode(parentNode, aggProcessorNode);

        return new KTableImpl<>(aggFunctionName,
                                keySerde,
                                valueSerde,
                                sourceName.equals(this.name) ? subTopologySourceNodes : Collections.singleton(sourceName),
                                queryableStoreName,
                                aggregateSupplier,
                                aggProcessorNode,
                                builder);
    }

    /**
     * @return the new sourceName of the repartitioned source
     */
    private String createRepartitionSource(final String repartitionTopicNamePrefix,
                                           final OptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder,
                                           final boolean isRepartitionTopicNameProvidedByUser) {

        return KStreamImpl.createRepartitionedSource(builder,
                                                     keySerde,
                                                     valueSerde,
                                                     repartitionTopicNamePrefix,
                                                     null,
                                                     optimizableRepartitionNodeBuilder,
                                                     isRepartitionTopicNameProvidedByUser);

    }
}
