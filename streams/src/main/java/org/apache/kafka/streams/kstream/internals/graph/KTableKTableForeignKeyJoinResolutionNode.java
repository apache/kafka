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

package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionResponseWrapper;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

/**
 * Too much specific information to generalize so the Foreign Key KTable-KTable join requires a specific node.
 */
public class KTableKTableForeignKeyJoinResolutionNode<K, V, KO, VO> extends StreamsGraphNode {
    private final ProcessorParameters<KO, SubscriptionWrapper<K>> joinOneToOneProcessorParameters;
    private final ProcessorParameters<KO, Change<VO>> joinByPrefixProcessorParameters;
    private final ProcessorParameters<K, SubscriptionResponseWrapper<VO>> resolverProcessorParameters;
    private final String finalRepartitionTopicName;
    private final String finalRepartitionSinkName;
    private final String finalRepartitionSourceName;
    private final Serde<K> keySerde;
    private final Serde<SubscriptionResponseWrapper<VO>> subResponseSerde;
    private final KTableValueGetterSupplier<K, V> originalValueGetter;

    public KTableKTableForeignKeyJoinResolutionNode(final String nodeName,
                                                    final ProcessorParameters<KO, SubscriptionWrapper<K>> joinOneToOneProcessorParameters,
                                                    final ProcessorParameters<KO, Change<VO>> joinByPrefixProcessorParameters,
                                                    final ProcessorParameters<K, SubscriptionResponseWrapper<VO>> resolverProcessorParameters,
                                                    final String finalRepartitionTopicName,
                                                    final String finalRepartitionSinkName,
                                                    final String finalRepartitionSourceName,
                                                    final Serde<K> keySerde,
                                                    final Serde<SubscriptionResponseWrapper<VO>> subResponseSerde,
                                                    final KTableValueGetterSupplier<K, V> originalValueGetter
    ) {
        super(nodeName);
        this.joinOneToOneProcessorParameters = joinOneToOneProcessorParameters;
        this.joinByPrefixProcessorParameters = joinByPrefixProcessorParameters;
        this.resolverProcessorParameters = resolverProcessorParameters;
        this.finalRepartitionTopicName = finalRepartitionTopicName;
        this.finalRepartitionSinkName = finalRepartitionSinkName;
        this.finalRepartitionSourceName = finalRepartitionSourceName;
        this.keySerde = keySerde;
        this.subResponseSerde = subResponseSerde;
        this.originalValueGetter = originalValueGetter;
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        topologyBuilder.addInternalTopic(finalRepartitionTopicName);
        //Repartition back to the original partitioning structure
        topologyBuilder.addSink(finalRepartitionSinkName, finalRepartitionTopicName,
                keySerde.serializer(), subResponseSerde.serializer(),
                null,
                joinByPrefixProcessorParameters.processorName(), joinOneToOneProcessorParameters.processorName());

        topologyBuilder.addSource(null, finalRepartitionSourceName, new FailOnInvalidTimestamp(),
                keySerde.deserializer(), subResponseSerde.deserializer(), finalRepartitionTopicName);

        //Connect highwaterProcessor to source, add the state store, and connect the statestore with the processor.
        topologyBuilder.addProcessor(resolverProcessorParameters.processorName(), resolverProcessorParameters.processorSupplier(), finalRepartitionSourceName);
        topologyBuilder.connectProcessorAndStateStores(resolverProcessorParameters.processorName(), originalValueGetter.storeNames());
    }
}
