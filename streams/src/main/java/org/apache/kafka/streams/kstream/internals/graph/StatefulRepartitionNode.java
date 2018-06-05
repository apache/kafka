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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.ChangedDeserializer;
import org.apache.kafka.streams.kstream.internals.ChangedSerializer;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;

public class StatefulRepartitionNode<K, V, T> extends RepartitionNode<K, V> {

    private final MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materialized;

    public StatefulRepartitionNode(final String nodeName,
                            final String sourceName,
                            final Serde<K> keySerde,
                            final Serde<V> valueSerde,
                            final String sinkName,
                            final String repartitionTopic,
                            final ProcessorParameters processorParameters,
                            final MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materialized) {
        super(nodeName,
              sourceName,
              processorParameters,
              keySerde,
              valueSerde,
              sinkName,
              repartitionTopic
        );

        this.materialized = materialized;
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

    public static <K, V, T> StatefulRepartitionNodeBuilder<K, V, T> statefulRepartitionNodeBuilder() {
        return new StatefulRepartitionNodeBuilder<>();
    }

    @Override
   public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        //TODO will implement in follow-up pr
    }


   public static final class StatefulRepartitionNodeBuilder<K, V, T> {

        private String nodeName;
        private Serde<K> keySerde;
        private Serde<V> valueSerde;
        private String sinkName;
        private String sourceName;
        private String repartitionTopic;
        private ProcessorParameters processorParameters;
        private MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materialized;

        private StatefulRepartitionNodeBuilder() {
        }

       public StatefulRepartitionNodeBuilder<K, V, T> withKeySerde(final Serde<K> keySerde) {
            this.keySerde = keySerde;
            return this;
        }


        public StatefulRepartitionNodeBuilder<K, V, T> withValueSerde(final Serde<V> valueSerde) {
            this.valueSerde = valueSerde;
            return this;
        }

        public StatefulRepartitionNodeBuilder<K, V, T> withSinkName(final String sinkName) {
            this.sinkName = sinkName;
            return this;
        }

        public StatefulRepartitionNodeBuilder<K, V, T> withSourceName(final String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public StatefulRepartitionNodeBuilder<K, V, T> withRepartitionTopic(final String repartitionTopic) {
            this.repartitionTopic = repartitionTopic;
            return this;
        }

        public StatefulRepartitionNodeBuilder<K, V, T> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public StatefulRepartitionNodeBuilder<K, V, T> withProcessorParameters(final ProcessorParameters processorParameters) {
            this.processorParameters = processorParameters;
            return this;
        }

        public StatefulRepartitionNodeBuilder<K, V, T> withMaterialized(final MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materialized) {
            this.materialized = materialized;
            return this;
        }


        public StatefulRepartitionNode<K, V, T> build() {

            return new StatefulRepartitionNode<>(nodeName,
                                                 sourceName,
                                                 keySerde,
                                                 valueSerde,
                                                 sinkName,
                                                 repartitionTopic,
                                                 processorParameters,
                                                 materialized);


        }
    }
}
