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

import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Collections;

/**
 * Used to represent either a KTable source or a GlobalKTable source.
 * The presence of a {@link KTableSource} indicates this source node supplies
 * a {@link org.apache.kafka.streams.kstream.GlobalKTable}
 */
class StatefulSourceNode<K, V> extends StreamSourceNode<K, V> {

    private org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier;
    private StoreBuilder<KeyValueStore<K, V>> storeBuilder;
    private final ProcessorSupplier<K, V> processorSupplier;
    private final String sourceName;
    private final String processorName;
    private final KTableSource<K, V> kTableSource;

    StatefulSourceNode(final String predecessorNodeName,
                       final String nodeName,
                       final String sourceName,
                       final String processorName,
                       final String topic,
                       final ConsumedInternal<K, V> consumedInternal,
                       final ProcessorSupplier<K, V> processorSupplier,
                       final KTableSource<K, V> kTableSource) {

        super(predecessorNodeName,
              nodeName,
              Collections.singletonList(topic),
              consumedInternal);

        this.processorSupplier = processorSupplier;
        this.sourceName = sourceName;
        this.processorName = processorName;
        this.kTableSource = kTableSource;
    }

    StateStoreSupplier<KeyValueStore> storeSupplier() {
        return storeSupplier;
    }

    void setStoreSupplier(StateStoreSupplier<KeyValueStore> storeSupplier) {
        this.storeSupplier = storeSupplier;
    }

    StoreBuilder<KeyValueStore<K, V>> storeBuilder() {
        return storeBuilder;
    }

    void setStoreBuilder(StoreBuilder<KeyValueStore<K, V>> storeBuilder) {
        this.storeBuilder = storeBuilder;
    }

    ProcessorSupplier<K, V> processorSupplier() {
        return processorSupplier;
    }

    String sourceName() {
        return sourceName;
    }

    KTableSource<K, V> kTableSource() {
        return kTableSource;
    }

    String processorName() {
        return processorName;
    }

    boolean isGlobalKTable() {
        return kTableSource != null;
    }

    static <K, V> StatefulSourceNodeBuilder<K, V> statefulSourceNodeBuilder() {
        return new StatefulSourceNodeBuilder<>();
    }

    @Override
    void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        //TODO will implement in follow-up pr
    }

    static final class StatefulSourceNodeBuilder<K, V> {

        private String predecessorNodeName;
        private String nodeName;
        private String sourceName;
        private String processorName;
        private String topic;
        private ConsumedInternal<K, V> consumedInternal;
        private StateStoreSupplier<KeyValueStore> storeSupplier;
        private StoreBuilder<KeyValueStore<K, V>> storeBuilder;
        private ProcessorSupplier<K, V> processorSupplier;
        private KTableSource<K, V> kTableSource;

        private StatefulSourceNodeBuilder() {
        }


        StatefulSourceNodeBuilder<K, V> withPredecessorNodeName(final String predecessorNodeName) {
            this.predecessorNodeName = predecessorNodeName;
            return this;
        }

        StatefulSourceNodeBuilder<K, V> withSourceName(final String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        StatefulSourceNodeBuilder<K, V> withProcessorName(final String processorName) {
            this.processorName = processorName;
            return this;
        }

        StatefulSourceNodeBuilder<K, V> withTopic(final String topic) {
            this.topic = topic;
            return this;
        }

        StatefulSourceNodeBuilder<K, V> withStoreSupplier(final StateStoreSupplier<KeyValueStore> storeSupplier) {
            this.storeSupplier = storeSupplier;
            return this;
        }


        StatefulSourceNodeBuilder<K, V> withStoreBuilder(final StoreBuilder<KeyValueStore<K, V>> storeBuilder) {
            this.storeBuilder = storeBuilder;
            return this;
        }

        StatefulSourceNodeBuilder<K, V> withConsumedInternal(final ConsumedInternal<K, V> consumedInternal) {
            this.consumedInternal = consumedInternal;
            return this;
        }

        StatefulSourceNodeBuilder<K, V> withProcessorSupplier(final ProcessorSupplier<K, V> processorSupplier) {
            this.processorSupplier = processorSupplier;
            return this;
        }

        StatefulSourceNodeBuilder<K, V> withKTableSource(final KTableSource<K, V> kTableSource) {
            this.kTableSource = kTableSource;
            return this;
        }

        StatefulSourceNodeBuilder<K, V> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        StatefulSourceNode<K, V> build() {
            StatefulSourceNode<K, V>
                statefulSourceNode =
                new StatefulSourceNode<>(predecessorNodeName,
                                         nodeName,
                                         sourceName,
                                         processorName,
                                         topic,
                                         consumedInternal,
                                         processorSupplier,
                                         kTableSource);

            statefulSourceNode.setRepartitionRequired(false);
            if (storeSupplier != null) {
                statefulSourceNode.setStoreSupplier(storeSupplier);
            } else if (storeBuilder != null) {
                statefulSourceNode.setStoreBuilder(storeBuilder);
            }

            return statefulSourceNode;
        }
    }
}
