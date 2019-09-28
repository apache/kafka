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
    private final StreamPartitioner<K, V> partitioner;
    private final InternalTopicProperties internalTopicProperties;

    private UnoptimizableRepartitionNode(final String nodeName,
                                         final String sourceName,
                                         final ProcessorParameters processorParameters,
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
            repartitionTopic
        );

        this.partitioner = partitioner;
        this.internalTopicProperties = internalTopicProperties;
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

    public static <K, V> UnoptimizableRepartitionNodeBuilder<K, V> repartitionNodeBuilder() {
        return new UnoptimizableRepartitionNodeBuilder<>();
    }

    public static final class UnoptimizableRepartitionNodeBuilder<K, V> {

        private String nodeName;
        private ProcessorParameters processorParameters;
        private Serde<K> keySerde;
        private Serde<V> valueSerde;
        private String sinkName;
        private String sourceName;
        private String repartitionTopic;
        private InternalTopicProperties internalTopicProperties;
        private StreamPartitioner<K, V> partitioner;

        public UnoptimizableRepartitionNodeBuilder<K, V> withProcessorParameters(final ProcessorParameters processorParameters) {
            this.processorParameters = processorParameters;
            return this;
        }

        public UnoptimizableRepartitionNodeBuilder<K, V> withKeySerde(final Serde<K> keySerde) {
            this.keySerde = keySerde;
            return this;
        }

        public UnoptimizableRepartitionNodeBuilder<K, V> withValueSerde(final Serde<V> valueSerde) {
            this.valueSerde = valueSerde;
            return this;
        }

        public UnoptimizableRepartitionNodeBuilder<K, V> withSinkName(final String sinkName) {
            this.sinkName = sinkName;
            return this;
        }

        public UnoptimizableRepartitionNodeBuilder<K, V> withSourceName(final String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public UnoptimizableRepartitionNodeBuilder<K, V> withRepartitionTopic(final String repartitionTopic) {
            this.repartitionTopic = repartitionTopic;
            return this;
        }

        public UnoptimizableRepartitionNodeBuilder<K, V> withStreamPartitioner(final StreamPartitioner<K, V> partitioner) {
            this.partitioner = partitioner;
            return this;
        }

        public UnoptimizableRepartitionNodeBuilder<K, V> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public UnoptimizableRepartitionNodeBuilder<K, V> withInternalTopicProperties(final InternalTopicProperties internalTopicProperties) {
            this.internalTopicProperties = internalTopicProperties;
            return this;
        }

        public UnoptimizableRepartitionNode<K, V> build() {

            return new UnoptimizableRepartitionNode<>(
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
    }
}
