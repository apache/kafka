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
import org.apache.kafka.streams.processor.internals.InternalTopicProperties;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

public class GroupedTableOperationRepartitionNode<K, V> extends BaseRepartitionNode<K, V> {


    private GroupedTableOperationRepartitionNode(final String nodeName,
                                                 final Serde<K> keySerde,
                                                 final Serde<V> valueSerde,
                                                 final String sinkName,
                                                 final String sourceName,
                                                 final String repartitionTopic,
                                                 final ProcessorParameters<K, V, ?, ?> processorParameters) {

        super(
            nodeName,
            sourceName,
            processorParameters,
            keySerde,
            valueSerde,
            sinkName,
            repartitionTopic,
            null,
            InternalTopicProperties.empty()
        );
    }

    @Override
    Serializer<V> valueSerializer() {
        final Serializer<V> valueSerializer = super.valueSerializer();
        return unsafeCastChangedToValueSerializer(valueSerializer);
    }

    @SuppressWarnings("unchecked")
    private Serializer<V> unsafeCastChangedToValueSerializer(final Serializer<V> valueSerializer) {
        return (Serializer<V>) new ChangedSerializer<>(valueSerializer);
    }

    @Override
    Deserializer<V> valueDeserializer() {
        final Deserializer<? extends V> valueDeserializer = super.valueDeserializer();
        return unsafeCastChangedToValueDeserializer(valueDeserializer);
    }

    @SuppressWarnings("unchecked")
    private Deserializer<V> unsafeCastChangedToValueDeserializer(final Deserializer<? extends V> valueDeserializer) {
        return (Deserializer<V>) new ChangedDeserializer<>(valueDeserializer);
    }

    @Override
    public String toString() {
        return "GroupedTableOperationRepartitionNode{} " + super.toString();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        topologyBuilder.addInternalTopic(repartitionTopic, internalTopicProperties);

        topologyBuilder.addSink(
            sinkName,
            repartitionTopic,
            keySerializer(),
            valueSerializer(),
            null,
            parentNodeNames()
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

    public static <K1, V1> GroupedTableOperationRepartitionNodeBuilder<K1, V1> groupedTableOperationNodeBuilder() {
        return new GroupedTableOperationRepartitionNodeBuilder<>();
    }

    public static final class GroupedTableOperationRepartitionNodeBuilder<K, V> extends BaseRepartitionNodeBuilder<K, V, GroupedTableOperationRepartitionNode<K, V>> {

        @Override
        public GroupedTableOperationRepartitionNode<K, V> build() {
            return new GroupedTableOperationRepartitionNode<>(
                nodeName,
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
