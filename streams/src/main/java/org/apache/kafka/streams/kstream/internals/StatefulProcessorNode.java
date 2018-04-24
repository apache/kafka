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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Arrays;

class StatefulProcessorNode<K, V> extends StatelessProcessorNode<K, V> {

    private final String[] storeNames;
    private final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier;
    private final StoreBuilder<KeyValueStore<K, V>> storeBuilder;


    StatefulProcessorNode(final String predecessorNodeName,
                          final String name,
                          final ProcessorSupplier<K, V> processorSupplier,
                          final String[] storeNames,
                          final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier,
                          final StoreBuilder<KeyValueStore<K, V>> storeBuilder,
                          final boolean repartitionRequired) {
        super(predecessorNodeName,
              name,
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

    static <K, V> StatefulProcessorNodeBuilder<K, V> statefulProcessorNodeBuilder() {
        return new StatefulProcessorNodeBuilder<>();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    static final class StatefulProcessorNodeBuilder<K, V> {

        private ProcessorSupplier<K, V> processorSupplier;
        private String name;
        private String predecessorNodeName;
        private boolean repartitionRequired;
        private String[] storeNames;
        private org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier;
        private StoreBuilder<KeyValueStore<K, V>> storeBuilder;

        private StatefulProcessorNodeBuilder() {
        }

        static StatefulProcessorNodeBuilder aStatefulProcessorNode() {
            return new StatefulProcessorNodeBuilder();
        }

        StatefulProcessorNodeBuilder withProcessorSupplier(ProcessorSupplier<K, V> processorSupplier) {
            this.processorSupplier = processorSupplier;
            return this;
        }

        StatefulProcessorNodeBuilder withName(String name) {
            this.name = name;
            return this;
        }

        StatefulProcessorNodeBuilder withPredecessorNodeName(String predecessorNodeName) {
            this.predecessorNodeName = predecessorNodeName;
            return this;
        }

        StatefulProcessorNodeBuilder withStoreNames(String[] storeNames) {
            this.storeNames = storeNames;
            return this;
        }

        StatefulProcessorNodeBuilder withRepartitionRequired(boolean repartitionRequired) {
            this.repartitionRequired = repartitionRequired;
            return this;
        }

        StatefulProcessorNodeBuilder withStoreSupplier(StateStoreSupplier<KeyValueStore> storeSupplier) {
            this.storeSupplier = storeSupplier;
            return this;
        }

        StatefulProcessorNodeBuilder withStoreBuilder(StoreBuilder<KeyValueStore<K, V>> storeBuilder) {
            this.storeBuilder = storeBuilder;
            return this;
        }

        StatefulProcessorNode build() {
            return new StatefulProcessorNode<>(predecessorNodeName,
                                               name,
                                               processorSupplier,
                                               storeNames,
                                               storeSupplier,
                                               storeBuilder,
                                               repartitionRequired);

        }
    }
}
