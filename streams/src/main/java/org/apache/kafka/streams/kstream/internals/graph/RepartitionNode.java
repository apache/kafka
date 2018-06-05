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
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

public class RepartitionNode<K, V> extends StatelessProcessorNode<K, V> {

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final String sinkName;
    private final String sourceName;
    private final String repartitionTopic;


    RepartitionNode(final String nodeName,
                    final String sourceName,
                    final ProcessorParameters processorParameters,
                    final Serde<K> keySerde,
                    final Serde<V> valueSerde,
                    final String sinkName,
                    final String repartitionTopic) {

        super(nodeName,
              processorParameters,
              false);

        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.sinkName = sinkName;
        this.sourceName = sourceName;
        this.repartitionTopic = repartitionTopic;
    }

    Serde<K> keySerde() {
        return keySerde;
    }

    Serde<V> valueSerde() {
        return valueSerde;
    }

    String sinkName() {
        return sinkName;
    }

    String sourceName() {
        return sourceName;
    }

    String repartitionTopic() {
        return repartitionTopic;
    }


    @Override
    public void writeToTopology(InternalTopologyBuilder topologyBuilder) {
        //TODO will implement in follow-up pr
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

        private RepartitionNodeBuilder() {
        }

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


        public RepartitionNodeBuilder<K, V> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public RepartitionNode<K, V> build() {

            return new RepartitionNode<>(nodeName,
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
