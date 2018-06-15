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
import org.apache.kafka.streams.kstream.internals.ChangedDeserializer;
import org.apache.kafka.streams.kstream.internals.ChangedSerializer;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

public class GroupedTableOperationRepartitionNode<K, V> extends BaseRepartitionNode {


    GroupedTableOperationRepartitionNode(final String nodeName,
                                         final Serde<K> keySerde,
                                         final Serde<V> valueSerde,
                                         final String sinkName,
                                         final String sourceName,
                                         final String repartitionTopic,
                                         final ProcessorParameters processorParameters) {

        super(nodeName,
              sourceName,
              processorParameters,
              keySerde,
              valueSerde,
              sinkName,
              repartitionTopic
        );
    }

    @Override
    Serializer getValueSerializer() {
        final Serializer<? extends V> valueSerializer = valueSerde == null ? null : valueSerde.serializer();
        return new ChangedSerializer<>(valueSerializer);
    }

    @Override
    Deserializer getValueDeserializer() {
        final Deserializer<? extends V> valueDeserializer = valueSerde == null ? null : valueSerde.deserializer();
        return new ChangedDeserializer<>(valueDeserializer);
    }


    @Override
    public void writeToTopology(InternalTopologyBuilder topologyBuilder) {
        final Serializer<K> keySerializer = keySerde != null ? keySerde.serializer() : null;
        final Deserializer<K> keyDeserializer = keySerde != null ? keySerde.deserializer() : null;


        topologyBuilder.addInternalTopic(repartitionTopic);

        topologyBuilder.addSink(sinkName,
                                repartitionTopic,
                                keySerializer,
                                getValueSerializer(),
                                null,
                                parentNode().nodeName());

        topologyBuilder.addSource(null,
                                  sourceName,
                                  new FailOnInvalidTimestamp(),
                                  keyDeserializer,
                                  getValueDeserializer(),
                                  repartitionTopic);

    }

    public static GroupedTableOperationRepartitionNodeBuilder groupedTableOperationNodeBuilder() {
        return new GroupedTableOperationRepartitionNodeBuilder();
    }


    public static final class GroupedTableOperationRepartitionNodeBuilder<K, V> {

        private Serde<K> keySerde;
        private Serde<V> valueSerde;
        private String sinkName;
        private String nodeName;
        private String sourceName;
        private String repartitionTopic;
        private ProcessorParameters processorParameters;

        private GroupedTableOperationRepartitionNodeBuilder() {
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> withKeySerde(Serde<K> keySerde) {
            this.keySerde = keySerde;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> withValueSerde(Serde<V> valueSerde) {
            this.valueSerde = valueSerde;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> withSinkName(String sinkName) {
            this.sinkName = sinkName;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> withNodeName(String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> withSourceName(String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> withRepartitionTopic(String repartitionTopic) {
            this.repartitionTopic = repartitionTopic;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> withProcessorParameters(ProcessorParameters processorParameters) {
            this.processorParameters = processorParameters;
            return this;
        }

        public GroupedTableOperationRepartitionNode<K, V> build() {
            return new GroupedTableOperationRepartitionNode(nodeName,
                                                            keySerde,
                                                            valueSerde,
                                                            sinkName,
                                                            sourceName,
                                                            repartitionTopic,
                                                            processorParameters
            );
        }
    }
}
