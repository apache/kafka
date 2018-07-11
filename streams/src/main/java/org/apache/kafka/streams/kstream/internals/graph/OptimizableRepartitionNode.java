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
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

public class OptimizableRepartitionNode<K, V> extends BaseRepartitionNode {

    OptimizableRepartitionNode(final String nodeName,
                               final String sourceName,
                               final ProcessorParameters processorParameters,
                               final Serde<K> keySerde,
                               final Serde<V> valueSerde,
                               final String sinkName,
                               final String repartitionTopic) {

        super(nodeName,
              sourceName,
              processorParameters,
              keySerde,
              valueSerde,
              sinkName,
              repartitionTopic);

    }

    @Override
    Serializer<V> getValueSerializer() {
        return valueSerde != null ? valueSerde.serializer() : null;
    }

    @Override
    Deserializer<V> getValueDeserializer() {
        return  valueSerde != null ? valueSerde.deserializer() : null;
    }


    @Override
    public void writeToTopology(InternalTopologyBuilder topologyBuilder) {
        final Serializer<K> keySerializer = keySerde != null ? keySerde.serializer() : null;
        final Deserializer<K> keyDeserializer = keySerde != null ? keySerde.deserializer() : null;



        topologyBuilder.addInternalTopic(repartitionTopic);

        topologyBuilder.addProcessor(processorParameters.processorName(),
                                     processorParameters.processorSupplier(),
                                     parentNode().nodeName());

        topologyBuilder.addSink(sinkName,
                                repartitionTopic,
                                keySerializer,
                                getValueSerializer(),
                                null,
                                processorParameters.processorName());

        topologyBuilder.addSource(null,
                                  sourceName,
                                  new FailOnInvalidTimestamp(),
                                  keyDeserializer,
                                  getValueDeserializer(),
                                  repartitionTopic);

    }

    public static <K, V> OptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder() {
        return new OptimizableRepartitionNodeBuilder<>();
    }


    public static final class OptimizableRepartitionNodeBuilder<K, V> {

        private String nodeName;
        private ProcessorParameters processorParameters;
        private Serde<K> keySerde;
        private Serde<V> valueSerde;
        private String sinkName;
        private String sourceName;
        private String repartitionTopic;

        private OptimizableRepartitionNodeBuilder() {
        }

        public OptimizableRepartitionNodeBuilder<K, V> withProcessorParameters(final ProcessorParameters processorParameters) {
            this.processorParameters = processorParameters;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> withKeySerde(final Serde<K> keySerde) {
            this.keySerde = keySerde;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> withValueSerde(final Serde<V> valueSerde) {
            this.valueSerde = valueSerde;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> withSinkName(final String sinkName) {
            this.sinkName = sinkName;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> withSourceName(final String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> withRepartitionTopic(final String repartitionTopic) {
            this.repartitionTopic = repartitionTopic;
            return this;
        }


        public OptimizableRepartitionNodeBuilder<K, V> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public OptimizableRepartitionNode<K, V> build() {

            return new OptimizableRepartitionNode<>(nodeName,
                                                    sourceName,
                                                    processorParameters,
                                                    keySerde,
                                                    valueSerde,
                                                    sinkName,
                                                    repartitionTopic
            );

        }
    }
}
