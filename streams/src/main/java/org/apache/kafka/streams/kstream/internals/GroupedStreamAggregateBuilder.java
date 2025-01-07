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
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.processor.internals.StoreFactory;

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

    <KR, VR> KTable<KR, VR> build(final NamedInternal functionName,
                                  final StoreFactory storeFactory,
                                  final KStreamAggProcessorSupplier<K, V, KR, VR> aggregateSupplier,
                                  final String queryableStoreName,
                                  final Serde<KR> keySerde,
                                  final Serde<VR> valueSerde,
                                  final boolean isOutputVersioned) {
        assert queryableStoreName == null || queryableStoreName.equals(storeFactory.storeName());

        final String aggFunctionName = functionName.name();

        String sourceName = this.name;
        GraphNode parentNode = graphNode;

        if (repartitionRequired) {
            final OptimizableRepartitionNodeBuilder<K, V> repartitionNodeBuilder = optimizableRepartitionNodeBuilder();
            final String repartitionTopicPrefix = userProvidedRepartitionTopicName != null ? userProvidedRepartitionTopicName : storeFactory.storeName();
            sourceName = createRepartitionSource(repartitionTopicPrefix, repartitionNodeBuilder);

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

        final StatefulProcessorNode<K, V> statefulProcessorNode =
            new StatefulProcessorNode<>(
                aggFunctionName,
                new ProcessorParameters<>(aggregateSupplier, aggFunctionName),
                new String[] {storeFactory.storeName()}
            );
        statefulProcessorNode.setOutputVersioned(isOutputVersioned);

        builder.addGraphNode(parentNode, statefulProcessorNode);

        return new KTableImpl<>(aggFunctionName,
                                keySerde,
                                valueSerde,
                                sourceName.equals(this.name) ? subTopologySourceNodes : Collections.singleton(sourceName),
                                queryableStoreName,
                                aggregateSupplier,
                                statefulProcessorNode,
                                builder);

    }

    /**
     * @return the new sourceName of the repartitioned source
     */
    private String createRepartitionSource(final String repartitionTopicNamePrefix,
                                           final OptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder) {

        return KStreamImpl.createRepartitionedSource(builder,
                                                     keySerde,
                                                     valueSerde,
                                                     repartitionTopicNamePrefix,
                                                     null,
                                                     optimizableRepartitionNodeBuilder);

    }
}
