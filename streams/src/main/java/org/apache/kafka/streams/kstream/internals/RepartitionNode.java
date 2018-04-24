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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.ProcessorSupplier;

class RepartitionNode<K, V> extends StatelessProcessorNode<K, V> {

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final String sinkName;
    private final String sourceName;
    private final String repartitionTopic;
    private final String processorName;


    RepartitionNode(final String predecessorNodeName,
                    final String nodeName,
                    final String sourceName,
                    final ProcessorSupplier<K, V> processorSupplier,
                    final Serde<K> keySerde,
                    final Serde<V> valueSerde,
                    final String sinkName,
                    final String repartitionTopic,
                    final String processorName) {

        super(predecessorNodeName,
              nodeName,
              processorSupplier,
              false);

        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.sinkName = sinkName;
        this.sourceName = sourceName;
        this.repartitionTopic = repartitionTopic;
        this.processorName = processorName;
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

    String processorName() {
        return processorName;
    }

    static <K, V> RepartitionNodeBuilder<K, V> repartitionNodeBuilder() {
        return new RepartitionNodeBuilder<>();
    }


    static final class RepartitionNodeBuilder<K, V> {

        private String nodeName;
        private ProcessorSupplier<K, V> processorSupplier;
        private Serde<K> keySerde;
        private Serde<V> valueSerde;
        private String sinkName;
        private String sourceName;
        private String repartitionTopic;
        private String processorName;
        private String predecessorName;

        private RepartitionNodeBuilder() {
        }

        RepartitionNodeBuilder withProcessorSupplier(final ProcessorSupplier<K, V> processorSupplier) {
            this.processorSupplier = processorSupplier;
            return this;
        }

        RepartitionNodeBuilder withKeySerde(final Serde<K> keySerde) {
            this.keySerde = keySerde;
            return this;
        }

        RepartitionNodeBuilder withValueSerde(final Serde<V> valueSerde) {
            this.valueSerde = valueSerde;
            return this;
        }

        RepartitionNodeBuilder withSinkName(final String sinkName) {
            this.sinkName = sinkName;
            return this;
        }

        RepartitionNodeBuilder withSourceName(final String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        RepartitionNodeBuilder withRepartitionTopic(final String repartitionTopic) {
            this.repartitionTopic = repartitionTopic;
            return this;
        }

        RepartitionNodeBuilder withProcessorName(final String processorName) {
            this.processorName = processorName;
            return this;
        }

        RepartitionNodeBuilder withPredecessorName(final String predecessorName) {
            this.predecessorName = predecessorName;
            return this;
        }

        RepartitionNodeBuilder withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        RepartitionNode build() {

            return new RepartitionNode<>(predecessorName,
                                         nodeName,
                                         sourceName,
                                         processorSupplier,
                                         keySerde,
                                         valueSerde,
                                         sinkName,
                                         repartitionTopic,
                                         processorName);

        }
    }
}
