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

public class OptimizableRepartitionNode<K, V> extends BaseRepartitionNode<K, V> {

    private OptimizableRepartitionNode(final String nodeName,
                                       final String sourceName,
                                       final ProcessorParameters<K, V, ?, ?> processorParameters,
                                       final Serde<K> keySerde,
                                       final Serde<V> valueSerde,
                                       final String sinkName,
                                       final String repartitionTopic,
                                       final StreamPartitioner<K, V> partitioner) {
        super(
            nodeName,
            sourceName,
            processorParameters,
            keySerde,
            valueSerde,
            sinkName,
            repartitionTopic,
            partitioner,
            InternalTopicProperties.empty()
        );
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    public String repartitionTopic() {
        return repartitionTopic;
    }

    @Override
    public String toString() {
        return "OptimizableRepartitionNode{ " + super.toString() + " }";
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        topologyBuilder.addInternalTopic(repartitionTopic, internalTopicProperties);

        topologyBuilder.addProcessor(
            processorParameters.processorName(),
            processorParameters.processorSupplier(),
            parentNodeNames()
        );

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

    public static <K, V> OptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder() {
        return new OptimizableRepartitionNodeBuilder<>();
    }


    public static final class OptimizableRepartitionNodeBuilder<K, V> extends BaseRepartitionNodeBuilder<K, V, OptimizableRepartitionNode<K, V>> {

        @Override
        public OptimizableRepartitionNode<K, V> build() {

            return new OptimizableRepartitionNode<>(
                nodeName,
                sourceName,
                processorParameters,
                keySerde,
                valueSerde,
                sinkName,
                repartitionTopic,
                partitioner
            );

        }
    }
}
