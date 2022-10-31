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
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.InternalTopicProperties;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

/**
 * Repartition node that is not subject of optimization algorithm
 */
public class UnoptimizableRepartitionNode<K, V> extends BaseRepartitionNode<K, V> {

    private UnoptimizableRepartitionNode(final String nodeName,
                                         final String sourceName,
                                         final ProcessorParameters<K, V, ?, ?> processorParameters,
                                         final Serde<K> keySerde,
                                         final Serde<V> valueSerde,
                                         final String sinkName,
                                         final String repartitionTopic,
                                         final StreamPartitioner<K, V> partitioner,
                                         final InternalTopicProperties internalTopicProperties) {
        super(
            nodeName,
            sourceName,
            processorParameters,
            keySerde,
            valueSerde,
            sinkName,
            repartitionTopic,
            partitioner,
            internalTopicProperties
        );
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        topologyBuilder.addInternalTopic(repartitionTopic, internalTopicProperties);

        processorParameters.addProcessorTo(topologyBuilder, parentNodeNames());

        topologyBuilder.addSink(
            sinkName,
            repartitionTopic,
            keySerializer(),
            valueSerializer(),
            partitioner,
            processorParameters.processorName()
        );

        topologyBuilder.addSource(
            null,
            sourceName,
            new FailOnInvalidTimestamp(),
            keyDeserializer(),
            valueDeserializer(),
            repartitionTopic
        );
    }

    @Override
    public String toString() {
        return "UnoptimizableRepartitionNode{" + super.toString() + " }";
    }

    public static <K, V> UnoptimizableRepartitionNodeBuilder<K, V> unoptimizableRepartitionNodeBuilder() {
        return new UnoptimizableRepartitionNodeBuilder<>();
    }

    public static final class UnoptimizableRepartitionNodeBuilder<K, V> extends BaseRepartitionNodeBuilder<K, V, UnoptimizableRepartitionNode<K, V>> {

        @Override
        public UnoptimizableRepartitionNode<K, V> build() {
            return new UnoptimizableRepartitionNode<>(nodeName,
                                                      sourceName,
                                                      processorParameters,
                                                      keySerde,
                                                      valueSerde,
                                                      sinkName,
                                                      repartitionTopic,
                                                      partitioner,
                                                      internalTopicProperties);
        }
    }
}
