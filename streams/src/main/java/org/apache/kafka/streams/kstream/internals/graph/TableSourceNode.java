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
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Collections;

/**
 * Used to represent either a KTable source or a GlobalKTable source. A boolean flag is used to indicate if this represents a GlobalKTable a {@link
 * org.apache.kafka.streams.kstream.GlobalKTable}
 */
public class TableSourceNode<K, V> extends StreamSourceNode<K, V> {

    private StoreBuilder<KeyValueStore<K, V>> storeBuilder;
    private final ProcessorParameters<K, V> processorParameters;
    private final String sourceName;
    private final boolean isGlobalKTable;

    TableSourceNode(final String nodeName,
                    final String sourceName,
                    final String topic,
                    final ConsumedInternal<K, V> consumedInternal,
                    final StoreBuilder<KeyValueStore<K, V>> storeBuilder,
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

    StoreBuilder<KeyValueStore<K, V>> storeBuilder() {
        return storeBuilder;
    }

    ProcessorParameters<K, V> processorParameters() {
        return processorParameters;
    }

    String sourceName() {
        return sourceName;
    }

    boolean isGlobalKTable() {
        return isGlobalKTable;
    }

    public static <K, V> TableSourceNodeBuilder<K, V> tableSourceNodeBuilder() {
        return new TableSourceNodeBuilder<>();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        //TODO will implement in follow-up pr
    }

    public static final class TableSourceNodeBuilder<K, V> {

        private String nodeName;
        private String sourceName;
        private String topic;
        private ConsumedInternal<K, V> consumedInternal;
        private StoreBuilder<KeyValueStore<K, V>> storeBuilder;
        private ProcessorParameters<K, V> processorParameters;
        private boolean isGlobalKTable = false;

        private TableSourceNodeBuilder() {
        }

        public TableSourceNodeBuilder<K, V> withSourceName(final String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withTopic(final String topic) {
            this.topic = topic;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withStoreBuilder(final StoreBuilder<KeyValueStore<K, V>> storeBuilder) {
            this.storeBuilder = storeBuilder;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withConsumedInternal(final ConsumedInternal<K, V> consumedInternal) {
            this.consumedInternal = consumedInternal;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withProcessorParameters(final ProcessorParameters<K, V> processorParameters) {
            this.processorParameters = processorParameters;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public TableSourceNodeBuilder<K, V> isGlobalKTable(final boolean isGlobaKTable) {
            this.isGlobalKTable = isGlobaKTable;
            return this;
        }

        public TableSourceNode<K, V> build() {
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
