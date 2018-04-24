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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

class StatefulRepartitionNode<K, V, T> extends RepartitionNode<K, V> {

    private final ProcessorSupplier<K, Change<V>> statefulProcessorSupplier;
    private final MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materialized;

    StatefulRepartitionNode(final String predecessorNodeName,
                            final String nodeName,
                            final String sourceName,
                            final Serde<K> keySerde,
                            final Serde<V> valueSerde,
                            final String sinkName,
                            final String repartitionTopic,
                            final String processorName,
                            final ProcessorSupplier<K, Change<V>> statefulProcessorSupplier,
                            final MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materialized) {
        super(predecessorNodeName,
              nodeName,
              sourceName,
              null,
              keySerde,
              valueSerde,
              sinkName,
              repartitionTopic,
              processorName);

        this.statefulProcessorSupplier = statefulProcessorSupplier;
        this.materialized = materialized;
    }

    ProcessorSupplier<K, Change<V>> statefulProcessorSupplier() {
        return statefulProcessorSupplier;
    }

    MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materialized() {
        return materialized;
    }

    ChangedSerializer<? extends V> changedValueSerializer() {
        final Serializer<? extends V> valueSerializer = valueSerde() == null ? null : valueSerde().serializer();
        return new ChangedSerializer<>(valueSerializer);

    }

    ChangedDeserializer<? extends V> changedValueDeserializer() {
        final Deserializer<? extends V> valueDeserializer = valueSerde() == null ? null : valueSerde().deserializer();
        return new ChangedDeserializer<>(valueDeserializer);
    }

    static <K, V, T> StatefulRepartitionNodeBuilder<K, V, T> statefulRepartitionNodeBuilder() {
        return new StatefulRepartitionNodeBuilder<>();
    }


    static final class StatefulRepartitionNodeBuilder<K, V, T> {

        private String predecessorNodeName;
        private String nodeName;
        private Serde<K> keySerde;
        private Serde<V> valueSerde;
        private String sinkName;
        private String sourceName;
        private String repartitionTopic;
        private String processorName;
        private ProcessorSupplier<K, Change<V>> statefulProcessorSupplier;
        private MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materialized;

        private StatefulRepartitionNodeBuilder() {
        }

        StatefulRepartitionNodeBuilder withKeySerde(final Serde<K> keySerde) {
            this.keySerde = keySerde;
            return this;
        }


        StatefulRepartitionNodeBuilder withValueSerde(final Serde<V> valueSerde) {
            this.valueSerde = valueSerde;
            return this;
        }

        StatefulRepartitionNodeBuilder withPredecessorNodeName(final String predecessorNodeName) {
            this.predecessorNodeName = predecessorNodeName;
            return this;
        }

        StatefulRepartitionNodeBuilder withSinkName(final String sinkName) {
            this.sinkName = sinkName;
            return this;
        }

        StatefulRepartitionNodeBuilder withSourceName(final String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        StatefulRepartitionNodeBuilder withRepartitionTopic(final String repartitionTopic) {
            this.repartitionTopic = repartitionTopic;
            return this;
        }

        StatefulRepartitionNodeBuilder withProcessorName(final String processorName) {
            this.processorName = processorName;
            return this;
        }

        StatefulRepartitionNodeBuilder withStatefulProcessorSupplier(final ProcessorSupplier<K, Change<V>> statefulProcessorSupplier) {
            this.statefulProcessorSupplier = statefulProcessorSupplier;
            return this;
        }

        StatefulRepartitionNodeBuilder withMaterialized(final MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materialized) {
            this.materialized = materialized;
            return this;
        }

        StatefulRepartitionNodeBuilder withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public StatefulRepartitionNode build() {

            return new StatefulRepartitionNode<>(predecessorNodeName,
                                                 nodeName,
                                                 sourceName,
                                                 keySerde,
                                                 valueSerde,
                                                 sinkName,
                                                 repartitionTopic,
                                                 processorName,
                                                 statefulProcessorSupplier,
                                                 materialized);


        }
    }
}
