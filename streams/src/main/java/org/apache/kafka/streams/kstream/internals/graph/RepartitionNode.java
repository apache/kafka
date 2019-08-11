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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.InternalTopicProperties;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

/**
 * Repartition node that is not subject of optimization algorithm
 */
public class RepartitionNode<K, V> extends BaseRepartitionNode<K, V> {
    private final StreamPartitioner<K, V> partitioner;
    private final InternalTopicProperties internalTopicProperties;

    private RepartitionNode(String nodeName,
                            String sourceName,
                            ProcessorParameters processorParameters,
                            Serde<K> keySerde,
                            Serde<V> valueSerde,
                            String sinkName,
                            String repartitionTopic,
                            StreamPartitioner<K, V> partitioner,
                            InternalTopicProperties internalTopicProperties) {
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
    Serializer<V> getValueSerializer() {
        return valueSerde != null ? valueSerde.serializer() : null;
    }

    @Override
    Deserializer<V> getValueDeserializer() {
        return valueSerde != null ? valueSerde.deserializer() : null;
    }

    @Override
    public void writeToTopology(InternalTopologyBuilder topologyBuilder) {
        final Serializer<K> keySerializer = getKeySerializer();
        final Deserializer<K> keyDeserializer = getKeyDeserializer();

        topologyBuilder.addInternalTopic(repartitionTopic, internalTopicProperties);

        topologyBuilder.addProcessor(
            processorParameters.processorName(),
            processorParameters.processorSupplier(),
            parentNodeNames()
        );

        topologyBuilder.addSink(
            sinkName,
            repartitionTopic,
            keySerializer,
            getValueSerializer(),
            partitioner,
            processorParameters.processorName()
        );

        topologyBuilder.addSource(
            null,
            sourceName,
            new FailOnInvalidTimestamp(),
            keyDeserializer,
            getValueDeserializer(),
            repartitionTopic
        );
    }

    private Serializer<K> getKeySerializer() {
        return keySerde != null ? keySerde.serializer() : null;
    }

    private Deserializer<K> getKeyDeserializer() {
        return keySerde != null ? keySerde.deserializer() : null;
    }

    public static <K, V> RepartitionNodeBuilder<K, V> repartitionNodeBuilder() {
        return new RepartitionNodeBuilder<>();
    }

    public static final class RepartitionNodeBuilder<K, V> {

        private String nodeName;
        private ProcessorParameters processorParameters;
        private Serde<K> keySerde;
        private Serde<V> valueSerde;
        private String sinkName;
        private String sourceName;
        private String repartitionTopic;
        private InternalTopicProperties internalTopicProperties;
        private StreamPartitioner<K, V> partitioner;

        public RepartitionNodeBuilder<K, V> withProcessorParameters(final ProcessorParameters processorParameters) {
            this.processorParameters = processorParameters;
            return this;
        }

        public RepartitionNodeBuilder<K, V> withKeySerde(final Serde<K> keySerde) {
            this.keySerde = keySerde;
            return this;
        }

        public RepartitionNodeBuilder<K, V> withValueSerde(final Serde<V> valueSerde) {
            this.valueSerde = valueSerde;
            return this;
        }

        public RepartitionNodeBuilder<K, V> withSinkName(final String sinkName) {
            this.sinkName = sinkName;
            return this;
        }

        public RepartitionNodeBuilder<K, V> withSourceName(final String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public RepartitionNodeBuilder<K, V> withRepartitionTopic(final String repartitionTopic) {
            this.repartitionTopic = repartitionTopic;
            return this;
        }

        public RepartitionNodeBuilder<K, V> withStreamPartitioner(final StreamPartitioner<K, V> partitioner) {
            this.partitioner = partitioner;
            return this;
        }

        public RepartitionNodeBuilder<K, V> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public RepartitionNodeBuilder<K, V> withInternalTopicProperties(final InternalTopicProperties internalTopicProperties) {
            this.internalTopicProperties = internalTopicProperties;
            return this;
        }

        public RepartitionNode<K, V> build() {

            return new RepartitionNode<>(
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
