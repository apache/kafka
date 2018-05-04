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

import java.util.Arrays;

class StatefulProcessorNode<K, V> extends StatelessProcessorNode<K, V> {

    private final String[] storeNames;
    private final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier;
    private final StoreBuilder<KeyValueStore<K, V>> storeBuilder;


    StatefulProcessorNode(final String parentNodeName,
                          final String processorNodeName,
                          final ProcessorSupplier<K, V> processorSupplier,
                          final String[] storeNames,
                          final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier,
                          final StoreBuilder<KeyValueStore<K, V>> storeBuilder,
                          final boolean repartitionRequired) {
        super(parentNodeName,
              processorNodeName,
              processorSupplier,
              repartitionRequired);

        this.storeNames = storeNames;
        this.storeSupplier = storeSupplier;
        this.storeBuilder = storeBuilder;
    }


    String[] storeNames() {
        return Arrays.copyOf(storeNames, storeNames.length);
    }

    org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier() {
        return storeSupplier;
    }

    StoreBuilder<KeyValueStore<K, V>> storeBuilder() {
        return storeBuilder;
    }

    @Override
    void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        //TODO will implement in follow-up pr
    }

    static <K, V> StatefulProcessorNodeBuilder<K, V> statefulProcessorNodeBuilder() {
        return new StatefulProcessorNodeBuilder<>();
    }

    static final class StatefulProcessorNodeBuilder<K, V> {

        private ProcessorSupplier processorSupplier;
        private String processorNodeName;
        private String parentProcessorNodeName;
        private boolean repartitionRequired;
        private String[] storeNames;
        private org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier;
        private StoreBuilder<KeyValueStore<K, V>> storeBuilder;

        private StatefulProcessorNodeBuilder() {
        }

        StatefulProcessorNodeBuilder<K, V> withProcessorSupplier(final ProcessorSupplier processorSupplier) {
            this.processorSupplier = processorSupplier;
            return this;
        }

        StatefulProcessorNodeBuilder<K, V> withProcessorNodeName(final String processorNodeName) {
            this.processorNodeName = processorNodeName;
            return this;
        }

        StatefulProcessorNodeBuilder<K, V> withParentProcessorNodeName(final String parentProcessorNodeName) {
            this.parentProcessorNodeName = parentProcessorNodeName;
            return this;
        }

        StatefulProcessorNodeBuilder<K, V> withStoreNames(final String[] storeNames) {
            this.storeNames = storeNames;
            return this;
        }

        StatefulProcessorNodeBuilder<K, V> withRepartitionRequired(final boolean repartitionRequired) {
            this.repartitionRequired = repartitionRequired;
            return this;
        }

        StatefulProcessorNodeBuilder<K, V> withStoreSupplier(final StateStoreSupplier<KeyValueStore> storeSupplier) {
            this.storeSupplier = storeSupplier;
            return this;
        }

        StatefulProcessorNodeBuilder<K, V> withStoreBuilder(final StoreBuilder<KeyValueStore<K, V>> storeBuilder) {
            this.storeBuilder = storeBuilder;
            return this;
        }

        StatefulProcessorNode<K, V> build() {
            return new StatefulProcessorNode<>(parentProcessorNodeName,
                                               processorNodeName,
                                               processorSupplier,
                                               storeNames,
                                               storeSupplier,
                                               storeBuilder,
                                               repartitionRequired);

        }
    }
}
