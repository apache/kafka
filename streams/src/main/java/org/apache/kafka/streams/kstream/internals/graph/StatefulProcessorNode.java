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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.StoreBuilder;

public class StatefulProcessorNode<K, V> extends ProcessorGraphNode<K, V> {

    private final String[] storeNames;
    private final StoreFactory storeFactory;

    /**
     * Create a node representing a stateful processor, where the named stores have already been registered.
     */
    public StatefulProcessorNode(final ProcessorParameters<K, V, ?, ?> processorParameters,
                                 final Set<StoreBuilder<?>> preRegisteredStores,
                                 final Set<KTableValueGetterSupplier<?, ?>> valueGetterSuppliers) {
        super(processorParameters.processorName(), processorParameters);
        final Stream<String> registeredStoreNames = preRegisteredStores.stream().map(StoreBuilder::name);
        final Stream<String> valueGetterStoreNames = valueGetterSuppliers.stream().flatMap(s -> Arrays.stream(s.storeNames()));
        storeNames = Stream.concat(registeredStoreNames, valueGetterStoreNames).toArray(String[]::new);
        storeFactory = null;
    }

    /**
     * Create a node representing a stateful processor, where the named stores have already been registered.
     */
    public StatefulProcessorNode(final String nodeName,
                                 final ProcessorParameters<K, V, ?, ?> processorParameters,
                                 final String[] storeNames) {
        super(nodeName, processorParameters);

        this.storeNames = storeNames;
        this.storeFactory = null;
    }


    /**
     * Create a node representing a stateful processor,
     * where the store needs to be built and registered as part of building this node.
     */
    public StatefulProcessorNode(final String nodeName,
                                 final ProcessorParameters<K, V, ?, ?> processorParameters,
                                 final StoreFactory materializedKTableStoreBuilder) {
        super(nodeName, processorParameters);

        this.storeNames = null;
        this.storeFactory = materializedKTableStoreBuilder;
    }

    @Override
    public String toString() {
        return "StatefulProcessorNode{" +
            "storeNames=" + Arrays.toString(storeNames) +
            ", storeBuilder=" + storeFactory +
            "} " + super.toString();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        processorParameters().addProcessorTo(topologyBuilder, parentNodeNames());

        if (storeNames != null && storeNames.length > 0) {
            topologyBuilder.connectProcessorAndStateStores(processorParameters().processorName(), storeNames);
        }

        if (storeFactory != null) {
            topologyBuilder.addStateStore(storeFactory, processorParameters().processorName());
        }
    }
}
