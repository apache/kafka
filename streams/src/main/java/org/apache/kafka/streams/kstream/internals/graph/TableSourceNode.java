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

import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Collections;

/**
 * Used to represent either a KTable source or a GlobalKTable source. A boolean flag is used to indicate if this represents a GlobalKTable a {@link
 * org.apache.kafka.streams.kstream.GlobalKTable}
 */
public class TableSourceNode<K, V, S extends StateStore> extends StreamSourceNode<K, V> {

    private final StoreBuilder<S> storeBuilder;
    private final ProcessorParameters<K, V> processorParameters;
    private final String sourceName;
    private final boolean isGlobalKTable;

    TableSourceNode(final String nodeName,
                    final String sourceName,
                    final String topic,
                    final ConsumedInternal<K, V> consumedInternal,
                    final StoreBuilder<S> storeBuilder,
                    final ProcessorParameters<K, V> processorParameters,
                    final boolean isGlobalKTable) {

        super(nodeName,
              Collections.singletonList(topic),
              consumedInternal);

        this.processorParameters = processorParameters;
        this.sourceName = sourceName;
        this.isGlobalKTable = isGlobalKTable;
        this.storeBuilder = storeBuilder;
    }

    public boolean isGlobalKTable() {
        return isGlobalKTable;
    }

    @Override
    public String toString() {
        return "TableSourceNode{" +
               "storeBuilder=" + storeBuilder +
               ", processorParameters=" + processorParameters +
               ", sourceName='" + sourceName + '\'' +
               ", isGlobalKTable=" + isGlobalKTable +
               "} " + super.toString();
    }

    public static <K, V, S extends StateStore> TableSourceNodeBuilder<K, V, S> tableSourceNodeBuilder() {
        return new TableSourceNodeBuilder<>();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        final String topicName = getTopicNames().iterator().next();

        if (isGlobalKTable) {
            topologyBuilder.addGlobalStore((StoreBuilder<KeyValueStore>) storeBuilder,
                                           sourceName,
                                           consumedInternal().timestampExtractor(),
                                           consumedInternal().keyDeserializer(),
                                           consumedInternal().valueDeserializer(),
                                           topicName,
                                           processorParameters.processorName(),
                                           processorParameters.processorSupplier());
        } else {
            topologyBuilder.addSource(consumedInternal().offsetResetPolicy(),
                                      sourceName,
                                      consumedInternal().timestampExtractor(),
                                      consumedInternal().keyDeserializer(),
                                      consumedInternal().valueDeserializer(),
                                      topicName);

            topologyBuilder.addProcessor(processorParameters.processorName(), processorParameters.processorSupplier(), sourceName);

            topologyBuilder.addStateStore(storeBuilder, nodeName());
            topologyBuilder.markSourceStoreAndTopic(storeBuilder, topicName);
        }

    }

    public static final class TableSourceNodeBuilder<K, V, S extends StateStore> {

        private String nodeName;
        private String sourceName;
        private String topic;
        private ConsumedInternal<K, V> consumedInternal;
        private StoreBuilder<S> storeBuilder;
        private ProcessorParameters<K, V> processorParameters;
        private boolean isGlobalKTable = false;

        private TableSourceNodeBuilder() {
        }

        public TableSourceNodeBuilder<K, V, S> withSourceName(final String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public TableSourceNodeBuilder<K, V, S> withTopic(final String topic) {
            this.topic = topic;
            return this;
        }

        public TableSourceNodeBuilder<K, V, S> withStoreBuilder(final StoreBuilder<S> storeBuilder) {
            this.storeBuilder = storeBuilder;
            return this;
        }

        public TableSourceNodeBuilder<K, V, S> withConsumedInternal(final ConsumedInternal consumedInternal) {
            this.consumedInternal = consumedInternal;
            return this;
        }

        public TableSourceNodeBuilder<K, V, S> withProcessorParameters(final ProcessorParameters<K, V> processorParameters) {
            this.processorParameters = processorParameters;
            return this;
        }

        public TableSourceNodeBuilder<K, V, S> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public TableSourceNodeBuilder<K, V, S> isGlobalKTable(final boolean isGlobaKTable) {
            this.isGlobalKTable = isGlobaKTable;
            return this;
        }

        public TableSourceNode<K, V, S> build() {
            return new TableSourceNode<>(nodeName,
                                         sourceName,
                                         topic,
                                         consumedInternal,
                                         storeBuilder,
                                         processorParameters,
                                         isGlobalKTable);


        }
    }
}
